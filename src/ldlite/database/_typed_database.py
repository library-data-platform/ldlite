# pyright: reportArgumentType=false
from __future__ import annotations

from abc import abstractmethod
from contextlib import closing
from datetime import timezone
from typing import TYPE_CHECKING, Any, Generic, NoReturn, TypeVar, cast
from uuid import uuid4

import psycopg
from psycopg import sql

from . import Database, LoadHistory
from ._expansion import ExpandContext, expand_nonmarc
from ._prefix import Prefix

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from typing import NoReturn

    import duckdb
    from tqdm import tqdm


DB = TypeVar("DB", bound="duckdb.DuckDBPyConnection | psycopg.Connection")


class TypedDatabase(Database, Generic[DB]):
    def __init__(self, conn_factory: Callable[[], DB]):
        self._conn_factory = conn_factory
        with closing(self._conn_factory()) as conn:
            with conn.cursor() as cur:
                cur.execute('CREATE SCHEMA IF NOT EXISTS "ldlite_system";')
                cur.execute("""
CREATE TABLE IF NOT EXISTS "ldlite_system"."load_history_v1" (
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
                'DELETE FROM "ldlite_system"."load_history_v1" WHERE "table_name" = $1',
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
                .format(table=prefix.raw_table.id)
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
                    prefix.catalog_table.name,
                    prefix.legacy_jtable.name,
                ),
            )
            for (tname,) in cur.fetchall():
                if tname == prefix.catalog_table.name:
                    cur.execute(
                        sql.SQL("SELECT table_name FROM {catalog};")
                        .format(catalog=prefix.catalog_table.id)
                        .as_string(),
                    )
                    tables.extend(cur.fetchall())

                if tname == prefix.legacy_jtable.name:
                    cur.execute(
                        sql.SQL("SELECT table_name FROM {catalog};")
                        .format(catalog=prefix.legacy_jtable.id)
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
                .format(catalog=prefix.catalog_table.id)
                .as_string(),
            )
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {catalog};")
                .format(catalog=prefix.legacy_jtable.id)
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
                    table=prefix.raw_table.id,
                ).as_string(),
            )

    def preprocess_source_table(
        self,
        conn: DB,
        table_name: sql.Identifier,
        column_names: list[sql.Identifier],
    ) -> None: ...

    # TODO: Refactor this to use DELETE RETURNING when DuckDb resolves
    # https://github.com/duckdb/duckdb/issues/3417
    # Only postgres supports it which is why we have an abstraction here
    @abstractmethod
    def source_table_cte_stmt(self, keep_source: bool) -> str: ...

    def expand_prefix(
        self,
        prefix: str,
        json_depth: int,
        keep_raw: bool,
        progress: tqdm[NoReturn] | None = None,
    ) -> list[str]:
        pfx = Prefix(prefix)
        with closing(self._conn_factory()) as conn:
            self._drop_extracted_tables(conn, pfx)
            if json_depth < 1:
                conn.commit()
                return []

            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
CREATE TEMP TABLE {dest_table} ON COMMIT DROP AS
"""
                        + self.source_table_cte_stmt(keep_source=keep_raw)
                        + """
SELECT * from ld_source;
""",
                    )
                    .format(
                        dest_table=pfx.origin_table,
                        source_table=pfx.raw_table.id,
                    )
                    .as_string(),
                )

            if not keep_raw:
                self._drop_raw_table(conn, pfx)

            created_tables = expand_nonmarc(
                "jsonb",
                ["__id"],
                ExpandContext(
                    conn,
                    pfx.origin_table,
                    json_depth,
                    pfx.transform_table,
                    pfx.output_table,
                    self.preprocess_source_table,  # type: ignore [arg-type]
                    self.source_table_cte_stmt,
                    progress,
                ),
            )

            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
CREATE TABLE {catalog_table} (
    table_name text
)
""",
                    )
                    .format(catalog_table=pfx.catalog_table.id)
                    .as_string(),
                )
                cur.executemany(
                    sql.SQL("INSERT INTO {catalog_table} VALUES ($1)")
                    .format(
                        catalog_table=pfx.catalog_table.id,
                    )
                    .as_string(),
                    [(pfx.catalog_table_row(t),) for t in created_tables],
                )

            conn.commit()

        return created_tables

    def index_prefix(self, prefix: str, progress: tqdm[NoReturn] | None = None) -> None:
        pfx = Prefix(prefix)
        with closing(self._conn_factory()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(
                    """
SELECT table_name FROM information_schema.tables
WHERE table_schema = $1 and table_name = $2;""",
                    (
                        pfx.schema or self._default_schema,
                        pfx.catalog_table.name,
                    ),
                )
                if len(cur.fetchall()) < 1:
                    return

            with closing(conn.cursor()) as cur:
                cur.execute(
                    sql.SQL(
                        r"""
SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = $1 AND
    TABLE_NAME IN (SELECT TABLE_NAME FROM {catalog}) AND
    (
        DATA_TYPE IN ('UUID', 'uuid') OR
        COLUMN_NAME = 'id' OR
        (COLUMN_NAME LIKE '%\_id' AND COLUMN_NAME <> '__id')
    );
""",
                    )
                    .format(catalog=pfx.catalog_table.id)
                    .as_string(),
                    (pfx.schema or self._default_schema,),
                )
                indexes = cur.fetchall()

            if progress is not None:
                progress.total = len(indexes)
                progress.refresh()

            for index in indexes:
                with closing(conn.cursor()) as cur:
                    cur.execute(
                        sql.SQL("CREATE INDEX {name} ON {table} ({column});")
                        .format(
                            name=sql.Identifier(str(uuid4()).split("-")[0]),
                            table=sql.Identifier(*index[0].split(".")),
                            column=sql.Identifier(index[1]),
                        )
                        .as_string(),
                    )
                if progress is not None:
                    progress.update(1)

            conn.commit()

    def record_history(self, history: LoadHistory) -> None:
        with closing(self._conn_factory()) as conn, conn.cursor() as cur:
            cur.execute(
                """
INSERT INTO "ldlite_system"."load_history_v1" VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
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
