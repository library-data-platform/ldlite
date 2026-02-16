# pyright: reportArgumentType=false
from abc import abstractmethod
from collections import deque
from collections.abc import Callable, Sequence
from contextlib import closing
from datetime import timezone
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import psycopg
from psycopg import sql

from . import Database, LoadHistory
from ._expansion_node import ExpansionNode
from ._prefix import Prefix

if TYPE_CHECKING:
    import duckdb


DB = TypeVar("DB", bound="duckdb.DuckDBPyConnection | psycopg.Connection")


class TypedDatabase(Database, Generic[DB]):
    def __init__(self, conn_factory: Callable[[], DB]):
        self._conn_factory = conn_factory
        with closing(self._conn_factory()) as conn:
            with conn.cursor() as cur:
                cur.execute('CREATE SCHEMA IF NOT EXISTS "ldlite_system";')
                cur.execute("""
CREATE TABLE IF NOT EXISTS "ldlite_system"."load_history" (
    "table_name" TEXT UNIQUE
    ,"path" TEXT
    ,"query" TEXT
    ,"row_count" INTEGER
    ,"download_complete_utc" TIMESTAMP
    ,"start_utc" TIMESTAMP
    ,"download_time" INTERVAL
    ,"transform_time" INTERVAL
    ,"index_time" INTERVAL
);""")

            self._setup_jfuncs(conn)
            conn.commit()

    @staticmethod
    @abstractmethod
    def _setup_jfuncs(conn: DB) -> None: ...

    @property
    @abstractmethod
    def _default_schema(self) -> str: ...

    def drop_prefix(
        self,
        prefix: str,
    ) -> None:
        pfx = Prefix(prefix)
        with closing(self._conn_factory()) as conn:
            self._drop_extracted_tables(conn, pfx)
            self._drop_raw_table(conn, pfx)
            conn.execute(
                'DELETE FROM "ldlite_system"."load_history" WHERE "table_name" = $1',
                (pfx.load_history_key,),
            )
            conn.commit()

    def drop_raw_table(
        self,
        prefix: str,
    ) -> None:
        with closing(self._conn_factory()) as conn:
            self._drop_raw_table(conn, Prefix(prefix))
            conn.commit()

    def _drop_raw_table(
        self,
        conn: DB,
        prefix: Prefix,
    ) -> None:
        with closing(conn.cursor()) as cur:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {table};")
                .format(table=prefix.schemafy(prefix.raw_table))
                .as_string(),
            )

    def drop_extracted_tables(
        self,
        prefix: str,
    ) -> None:
        with closing(self._conn_factory()) as conn:
            self._drop_extracted_tables(conn, Prefix(prefix))
            conn.commit()

    def _drop_extracted_tables(
        self,
        conn: DB,
        prefix: Prefix,
    ) -> None:
        tables: list[Sequence[Sequence[Any]]] = []
        with closing(conn.cursor()) as cur:
            cur.execute(
                """
SELECT table_name FROM information_schema.tables
WHERE table_schema = $1 and table_name IN ($2, $3);""",
                (
                    prefix.schema or self._default_schema,
                    prefix.catalog_table,
                    prefix.legacy_jtable,
                ),
            )
            for (tname,) in cur.fetchall():
                if tname == prefix.catalog_table:
                    cur.execute(
                        sql.SQL("SELECT table_name FROM {catalog};")
                        .format(catalog=prefix.schemafy(prefix.catalog_table))
                        .as_string(),
                    )
                    tables.extend(cur.fetchall())

                if tname == prefix.legacy_jtable:
                    cur.execute(
                        sql.SQL("SELECT table_name FROM {catalog};")
                        .format(catalog=prefix.schemafy(prefix.legacy_jtable))
                        .as_string(),
                    )
                    tables.extend(cur.fetchall())

        with closing(conn.cursor()) as cur:
            for (et,) in tables:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {table};")
                    .format(table=sql.Identifier(cast("str", et)))
                    .as_string(),
                )
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {catalog};")
                .format(catalog=prefix.schemafy(prefix.catalog_table))
                .as_string(),
            )
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {catalog};")
                .format(catalog=prefix.schemafy(prefix.legacy_jtable))
                .as_string(),
            )

    @property
    @abstractmethod
    def _create_raw_table_sql(self) -> sql.SQL: ...
    def _prepare_raw_table(
        self,
        conn: DB,
        prefix: Prefix,
    ) -> None:
        with closing(conn.cursor()) as cur:
            if prefix.schema is not None:
                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema};")
                    .format(schema=sql.Identifier(prefix.schema))
                    .as_string(),
                )
        self._drop_raw_table(conn, prefix)
        with closing(conn.cursor()) as cur:
            cur.execute(
                self._create_raw_table_sql.format(
                    table=prefix.schemafy(prefix.raw_table),
                ).as_string(),
            )

    def expand_prefix(self, prefix: str, json_depth: int, keep_raw: bool) -> None:  # noqa: ARG002
        pfx = Prefix(prefix)
        with closing(self._conn_factory()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
CREATE TEMP TABLE {dest_table} ON COMMIT DROP AS
SELECT * from {raw_table};
""",
                    )
                    .format(
                        dest_table=pfx.transform_table(0),
                        raw_table=pfx.schemafy(pfx.raw_table),
                    )
                    .as_string(),
                )
                if not keep_raw:
                    # TODO: Refactor this to use DELETE RETURNING when DuckDb resolves
                    # https://github.com/duckdb/duckdb/issues/3417
                    cur.execute(
                        sql.SQL("DROP TABLE {raw_table}")
                        .format(
                            raw_table=pfx.schemafy(pfx.raw_table),
                        )
                        .as_string(),
                    )

            root = ExpansionNode("jsonb", None, None, values=["__id"])
            root.explode(
                conn,
                pfx.transform_table(0),
                pfx.transform_table(1),
            )

            count = 1
            expand_children_of = deque([root])
            while expand_children_of:
                n = expand_children_of.popleft()
                for c in n.children:
                    c.explode(
                        conn,
                        pfx.transform_table(count),
                        pfx.transform_table(count + 1),
                    )
                    expand_children_of.append(c)
                    count += 1

            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
CREATE TABLE {dest_table} AS
SELECT * FROM {transform_table}
""",
                    )
                    .format(
                        dest_table=pfx.schemafy(pfx.output_table),
                        transform_table=pfx.transform_table(count),
                    )
                    .as_string(),
                )

            conn.commit()

    def record_history(self, history: LoadHistory) -> None:
        with closing(self._conn_factory()) as conn, conn.cursor() as cur:
            cur.execute(
                """
INSERT INTO "ldlite_system"."load_history" VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT ("table_name") DO UPDATE SET
    "path" = EXCLUDED."path"
    ,"query" = EXCLUDED."query"
    ,"row_count" = EXCLUDED."row_count"
    ,"download_complete_utc" = EXCLUDED."download_complete_utc"
    ,"start_utc" = EXCLUDED."start_utc"
    ,"download_time" = EXCLUDED."download_time"
    ,"transform_time" = EXCLUDED."transform_time"
    ,"index_time" = EXCLUDED."index_time"
""",
                (
                    Prefix(history.table_name).load_history_key,
                    history.path,
                    history.query,
                    history.total,
                    history.download_time.astimezone(timezone.utc),
                    history.start_time.astimezone(timezone.utc),
                    history.download_interval,
                    history.transform_interval,
                    history.index_interval,
                ),
            )
            conn.commit()
