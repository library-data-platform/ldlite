# pyright: reportArgumentType=false
from abc import abstractmethod
from collections.abc import Callable, Sequence
from contextlib import closing
from datetime import timezone
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import psycopg
from psycopg import sql

from . import Database, LoadHistory, Prefix

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
        prefix: Prefix,
    ) -> None:
        with closing(self._conn_factory()) as conn:
            self._drop_extracted_tables(conn, prefix)
            self._drop_raw_table(conn, prefix)
            conn.execute(
                'DELETE FROM "ldlite_system"."load_history" WHERE "table_name" = $1',
                (prefix.load_history_key,),
            )
            conn.commit()

    def drop_raw_table(
        self,
        prefix: Prefix,
    ) -> None:
        with closing(self._conn_factory()) as conn:
            self._drop_raw_table(conn, prefix)
            conn.commit()

    def _drop_raw_table(
        self,
        conn: DB,
        prefix: Prefix,
    ) -> None:
        with closing(conn.cursor()) as cur:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {table};")
                .format(table=prefix.raw_table_identifier)
                .as_string(),
            )

    def drop_extracted_tables(
        self,
        prefix: Prefix,
    ) -> None:
        with closing(self._conn_factory()) as conn:
            self._drop_extracted_tables(conn, prefix)
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
                    prefix.catalog_table_name,
                    prefix.legacy_jtable_name,
                ),
            )
            for (tname,) in cur.fetchall():
                if tname == prefix.catalog_table_name:
                    cur.execute(
                        sql.SQL("SELECT table_name FROM {catalog};")
                        .format(catalog=prefix.catalog_table_identifier)
                        .as_string(),
                    )
                    tables.extend(cur.fetchall())

                if tname == prefix.legacy_jtable_name:
                    cur.execute(
                        sql.SQL("SELECT table_name FROM {catalog};")
                        .format(catalog=prefix.legacy_jtable_identifier)
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
                .format(catalog=prefix.catalog_table_identifier)
                .as_string(),
            )
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {catalog};")
                .format(catalog=prefix.legacy_jtable_identifier)
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
            if prefix.schema_identifier is not None:
                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema};")
                    .format(schema=prefix.schema_identifier)
                    .as_string(),
                )
        self._drop_raw_table(conn, prefix)
        with closing(conn.cursor()) as cur:
            cur.execute(
                self._create_raw_table_sql.format(
                    table=prefix.raw_table_identifier,
                ).as_string(),
            )

    def expand_prefix(self, prefix: Prefix, json_depth: int, keep_raw: bool) -> None:  # noqa: ARG002
        with closing(self._conn_factory()) as conn:
            with conn.cursor() as cur:
                # select column names and types
                # care about string, number, uuid, object, list

                cur.execute(
                    sql.SQL(
                        """
WITH
    one_object AS (SELECT {json_col} as json FROM {table} LIMIT 1),
    props AS (SELECT ldlite_system.jobject_keys(json) AS prop FROM one_object),
    prop_meta AS (
        SELECT
            prop
            ,ldlite_system.jtype_of(json->prop) AS json_type
        FROM one_object, props
    )
SELECT
    prop
    ,ANY_VALUE(json_type) as json_type
    ,bool_and({json_col}->>prop ~ '^[a-fA-F0-9]{{8}}-[a-fA-F0-9]{{4}}-[1-5][a-fA-F0-9]{{3}}-[89abAB][a-fA-F0-9]{{3}}-[a-fA-F0-9]{{12}}$') AS is_uuid
FROM {table}, prop_meta
GROUP BY prop
""",  # noqa: E501
                    )
                    .format(
                        table=prefix.raw_table_identifier,
                        json_col=sql.Identifier("jsonb"),
                    )
                    .as_string(),
                )

                create_columns = []
                for row in cur.fetchall():
                    if row[1] == "number":
                        stmt = sql.SQL("({json_col}->{prop})::numeric AS {prop_alias}")
                    elif row[1] == "string" and row[2]:
                        stmt = sql.SQL("({json_col}->>{prop})::uuid AS {prop_alias}")
                    elif row[1] == "object" or row[1] == "array":
                        stmt = sql.SQL("({json_col}->{prop}) AS {prop_alias}")
                    else:
                        stmt = sql.SQL("({json_col}->>{prop}) AS {prop_alias}")

                    create_columns.append(
                        stmt.format(
                            json_col=sql.Identifier("jsonb"),
                            prop=sql.Literal(row[0]),
                            prop_alias=sql.Identifier(row[0]),
                        ),
                    )

            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
CREATE TABLE {dest_table} AS SELECT
    {cols}
FROM {src_table};
""",
                    )
                    .format(
                        dest_table=prefix.expansion_table_identifier,
                        src_table=prefix.raw_table_identifier,
                        cols=sql.SQL("\n    ,").join(create_columns),
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
                    history.table_name.load_history_key,
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
