from __future__ import annotations

from abc import abstractmethod
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Generic, NoReturn, TypeVar, cast
from uuid import uuid4

import psycopg
from psycopg import sql
from tqdm import tqdm

from . import Database
from ._expansion.rewrite import non_srs_statements
from ._prefix import Prefix

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence
    from typing import NoReturn

    import duckdb


DB = TypeVar("DB", bound="duckdb.DuckDBPyConnection | psycopg.Connection")


class TypedDatabase(Database, Generic[DB]):
    def __init__(self, conn_factory: Callable[[bool], DB]):
        self._conn_factory = conn_factory
        with closing(self._conn_factory(True)) as conn:
            with conn.cursor() as cur:
                cur.execute('CREATE SCHEMA IF NOT EXISTS "ldlite_system";')
                cur.execute("""
CREATE TABLE IF NOT EXISTS "ldlite_system"."load_history_v1" (
    "table_prefix" TEXT UNIQUE
    ,"folio_path" TEXT -- 1
    ,"query_text" TEXT -- 2
    ,"load_start" TIMESTAMPTZ -- 3

    ,"rowcount" INTEGER -- 4
    ,"download_complete" TIMESTAMPTZ -- 5

    ,"final_rowcount" INTEGER -- 6
    ,"transform_complete" TIMESTAMPTZ -- 7
    ,"data_refresh_start" TIMESTAMPTZ -- 8
    ,"data_refresh_end" TIMESTAMPTZ -- 9

    ,"download_time" INTERVAL -- 10
    ,"transform_time" INTERVAL -- 11
    ,"index_time" INTERVAL -- 12
);""")

            conn.commit()

    @property
    @abstractmethod
    def _default_schema(self) -> str: ...

    @contextmanager
    def _begin(self, conn: DB) -> Iterator[None]:  # noqa: ARG002
        yield

    def drop_prefix(
        self,
        prefix: str,
    ) -> None:
        pfx = Prefix(prefix)
        with closing(self._conn_factory(True)) as conn:
            self._drop_extracted_tables(conn, pfx)
            self._drop_raw_table(conn, pfx)
            conn.execute(
                """
DELETE FROM "ldlite_system"."load_history_v1"
WHERE "table_prefix" = $1;
""",
                (pfx.load_history_key,),
            )
            conn.commit()

    def drop_raw_table(
        self,
        prefix: str,
    ) -> None:
        with closing(self._conn_factory(True)) as conn:
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
        with closing(self._conn_factory(True)) as conn:
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
                    .format(table=sql.Identifier(*cast("str", et).split(".")))
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
    def source_stmt(self, keep_source: bool) -> sql.SQL: ...

    def expand_prefix(
        self,
        prefix: str,
        json_depth: int,
        keep_raw: bool,
        scan_progress: tqdm[NoReturn] | None = None,
        transform_progress: tqdm[NoReturn] | None = None,
    ) -> list[str]:
        pfx = Prefix(prefix)
        transform_started = datetime.now(timezone.utc)
        if json_depth < 1:
            with closing(self._conn_factory(True)) as conn:
                self._drop_extracted_tables(conn, pfx)
                if not keep_raw:
                    self._drop_raw_table(conn, pfx)
                self._transform_complete(conn, pfx, 0, transform_started)

                conn.commit()
                return []

        transform_progress = (
            transform_progress
            if transform_progress is not None
            else tqdm(disable=True, total=0)
        )
        transform_progress.total = 1

        with closing(self._conn_factory(False)) as conn:
            tables_to_create = non_srs_statements(
                conn,
                pfx.raw_table[1],
                pfx.output_table,
                scan_progress
                if scan_progress is not None
                else tqdm(disable=True, total=0),
            )
            transform_progress.total += len(tables_to_create) + 1
            transform_progress.update(1)

            with self._begin(conn):
                self._drop_extracted_tables(conn, pfx)
                with conn.cursor() as cur:
                    for i, (_, table) in enumerate(tables_to_create):
                        create_table = table(
                            self.source_stmt(keep_source=(i == 0 and keep_raw)),
                        )
                        cur.execute(create_table.as_string())
                        transform_progress.update(1)

                # duckdb can't drop the raw table when creating the output table
                if not keep_raw:
                    self._drop_raw_table(conn, pfx)

                total = 0
                with conn.cursor() as cur:
                    create_catalog = sql.SQL(
                        """CREATE TABLE {catalog_table} (table_name text)""",
                    ).format(catalog_table=pfx.catalog_table.id)
                    cur.execute(create_catalog.as_string())
                    if len(tables_to_create) > 0:
                        insert_catalog = sql.SQL(
                            "INSERT INTO {catalog_table} VALUES ($1)",
                        ).format(catalog_table=pfx.catalog_table.id)
                        cur.executemany(
                            insert_catalog.as_string(),
                            [(pfx.catalog_table_row(t[0]),) for t in tables_to_create],
                        )

                        count = sql.SQL("SELECT COUNT(*) FROM {table}").format(
                            table=pfx.output_table("").id,
                        )
                        cur.execute(count.as_string())
                        total = cast("tuple[int]", cur.fetchone())[0]
                    transform_progress.update(1)

                self._transform_complete(conn, pfx, total, transform_started)

        return [pfx.catalog_table_row(t[0]) for t in tables_to_create]

    def index_prefix(self, prefix: str, progress: tqdm[NoReturn] | None = None) -> None:
        pfx = Prefix(prefix)
        index_started = datetime.now(timezone.utc)
        with closing(self._conn_factory(False)) as conn:
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
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = $1 AND
    TABLE_NAME IN (SELECT SPLIT_PART(TABLE_NAME, '.', -1) FROM {catalog}) AND
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
                            table=sql.Identifier(index[0], index[1]),
                            column=sql.Identifier(index[2]),
                        )
                        .as_string(),
                    )
                if progress is not None:
                    progress.update(1)

            self._index_complete(conn, pfx, index_started)
            conn.commit()

    def prepare_history(
        self,
        prefix: str,
        path: str,
        query: str | None,
    ) -> None:
        with closing(self._conn_factory(True)) as conn, closing(conn.cursor()) as cur:
            cur.execute(
                """
INSERT INTO "ldlite_system"."load_history_v1"
(
    "table_prefix"
    ,"folio_path"
    ,"query_text"
    ,"load_start"
)
VALUES($1,$2,$3,$4)
ON CONFLICT ("table_prefix") DO UPDATE SET
    "folio_path" = EXCLUDED."folio_path"
    ,"query_text" = EXCLUDED."query_text"
    ,"load_start" = EXCLUDED."load_start"
""",
                (
                    Prefix(prefix).load_history_key,
                    path,
                    query,
                    datetime.now(timezone.utc),
                ),
            )
            conn.commit()

    def _download_complete(
        self,
        conn: DB,
        pfx: Prefix,
        rowcount: int,
        download_start: datetime,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
UPDATE "ldlite_system"."load_history_v1" SET
    "rowcount" = $2
    ,"download_complete" = $3
    ,"download_time" = $4
WHERE "table_prefix" = $1;
""",
                (
                    pfx.load_history_key,
                    rowcount,
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc) - download_start,
                ),
            )

    def _transform_complete(
        self,
        conn: DB,
        pfx: Prefix,
        final_rowcount: int,
        transform_start: datetime,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
UPDATE "ldlite_system"."load_history_v1" SET
    "final_rowcount" = $2
    ,"transform_complete" = $3
    ,"transform_time" = $4
    ,"index_time" = NULL
    ,"data_refresh_start" = "load_start"
    ,"data_refresh_end" = "download_complete"
WHERE "table_prefix" = $1
""",
                (
                    pfx.load_history_key,
                    final_rowcount,
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc) - transform_start,
                ),
            )

    def _index_complete(
        self,
        conn: DB,
        pfx: Prefix,
        index_start: datetime,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
UPDATE "ldlite_system"."load_history_v1" SET
    "index_time" = $2
WHERE "table_prefix" = $1
""",
                (
                    pfx.load_history_key,
                    datetime.now(timezone.utc) - index_start,
                ),
            )
