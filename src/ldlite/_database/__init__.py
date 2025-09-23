import datetime
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Sequence
from contextlib import closing
from dataclasses import dataclass
from datetime import timezone
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from psycopg import sql

if TYPE_CHECKING:
    import duckdb
    import psycopg


class Prefix:
    def __init__(self, prefix: str):
        self.schema: str | None = None
        sandt = prefix.split(".")
        if len(sandt) > 2:
            msg = f"Expected one or two identifiers but got {prefix}"
            raise ValueError(msg)

        if len(sandt) == 1:
            (self._prefix,) = sandt
        else:
            (self.schema, self._prefix) = sandt

    def _identifier(self, table: str) -> sql.Identifier:
        if self.schema is None:
            return sql.Identifier(table)
        return sql.Identifier(self.schema, table)

    @property
    def schema_identifier(self) -> sql.Identifier | None:
        return None if self.schema is None else sql.Identifier(self.schema)

    @property
    def raw_table_identifier(self) -> sql.Identifier:
        return self._identifier(self._prefix)

    @property
    def catalog_table_name(self) -> str:
        return f"{self._prefix}__tcatalog"

    @property
    def catalog_table_identifier(self) -> sql.Identifier:
        return self._identifier(self.catalog_table_name)

    @property
    def legacy_jtable_name(self) -> str:
        return f"{self._prefix}_jtable"

    @property
    def legacy_jtable_identifier(self) -> sql.Identifier:
        return self._identifier(self.legacy_jtable_name)

    @property
    def load_history_key(self) -> str:
        if self.schema is None:
            return self._prefix

        return self.schema + "." + self._prefix


@dataclass(frozen=True)
class LoadHistory:
    table_name: Prefix
    query: str | None
    start: datetime.datetime
    download: datetime.datetime
    scan: datetime.datetime
    transform: datetime.datetime
    index: datetime.datetime
    total: int


class Database(ABC):
    @abstractmethod
    def drop_prefix(self, prefix: Prefix) -> None: ...

    @abstractmethod
    def drop_raw_table(self, prefix: Prefix) -> None: ...

    @abstractmethod
    def drop_extracted_tables(self, prefix: Prefix) -> None: ...

    @abstractmethod
    def ingest_records(self, prefix: Prefix, records: Iterator[bytes]) -> int: ...

    @abstractmethod
    def record_history(self, history: LoadHistory) -> None: ...


DB = TypeVar("DB", bound="duckdb.DuckDBPyConnection | psycopg.Connection")


class TypedDatabase(Database, Generic[DB]):
    def __init__(self, conn_factory: Callable[[], DB]):
        self._conn_factory = conn_factory
        with closing(self._conn_factory()) as conn, conn.cursor() as cur:
            cur.execute('CREATE SCHEMA IF NOT EXISTS "ldlite_system";')
            cur.execute("""
CREATE TABLE IF NOT EXISTS "ldlite_system"."load_history" (
    "table_name" TEXT UNIQUE
    ,"query" TEXT
    ,"start_utc" TIMESTAMP
    ,"download_complete_utc" TIMESTAMP
    ,"scan_complete_utc" TIMESTAMP
    ,"transformation_complete_utc" TIMESTAMP
    ,"index_complete_utc" TIMESTAMP
    ,"row_count" INTEGER
);""")
            conn.commit()

    @abstractmethod
    def _rollback(self, conn: DB) -> None: ...

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

    @property
    @abstractmethod
    def _missing_table_error(self) -> type[Exception]: ...
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

    def record_history(self, history: LoadHistory) -> None:
        with closing(self._conn_factory()) as conn, conn.cursor() as cur:
            cur.execute(
                """
INSERT INTO "ldlite_system"."load_history" VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT ("table_name") DO UPDATE SET
    "query" = EXCLUDED."query"
    ,"start_utc" = EXCLUDED."start_utc"
    ,"download_complete_utc" = EXCLUDED."download_complete_utc"
    ,"scan_complete_utc" = EXCLUDED."scan_complete_utc"
    ,"transformation_complete_utc" = EXCLUDED."transformation_complete_utc"
    ,"index_complete_utc" = EXCLUDED."index_complete_utc"
    ,"row_count" = EXCLUDED."row_count"
""",
                (
                    history.table_name.load_history_key,
                    history.query,
                    history.start.astimezone(timezone.utc),
                    history.download.astimezone(timezone.utc),
                    history.scan.astimezone(timezone.utc),
                    history.transform.astimezone(timezone.utc),
                    history.index.astimezone(timezone.utc),
                    history.total,
                ),
            )
            conn.commit()
