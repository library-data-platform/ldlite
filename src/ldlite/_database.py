from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import closing
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar, cast

from psycopg import sql

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from _typeshed import dbapi

DB = TypeVar("DB", bound="dbapi.DBAPIConnection")


class Prefix:
    def __init__(self, table: str):
        self._schema: str | None = None
        sandt = table.split(".")
        if len(sandt) == 1:
            (self._prefix,) = sandt
        else:
            (self._schema, self._prefix) = sandt

    @property
    def schema_name(self) -> sql.Identifier | None:
        return None if self._schema is None else sql.Identifier(self._schema)

    def identifier(self, table: str) -> sql.Identifier:
        if self._schema is None:
            return sql.Identifier(table)
        return sql.Identifier(self._schema, table)

    @property
    def raw_table_name(self) -> sql.Identifier:
        return self.identifier(self._prefix)

    @property
    def catalog_table_name(self) -> sql.Identifier:
        return self.identifier(f"{self._prefix}__tcatalog")

    @property
    def legacy_jtable(self) -> sql.Identifier:
        return self.identifier(f"{self._prefix}_jtable")


class Database(ABC, Generic[DB]):
    def __init__(self, conn_factory: Callable[[], DB]):
        self._conn_factory = conn_factory

    @abstractmethod
    def _rollback(self, conn: DB) -> None: ...

    def drop_prefix(
        self,
        prefix: Prefix,
    ) -> None:
        with closing(self._conn_factory()) as conn:
            self._drop_extracted_tables(conn, prefix)
            self._drop_raw_table(conn, prefix)
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
                .format(table=prefix.raw_table_name)
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
    def _missing_table_error(self) -> tuple[type[Exception], ...]: ...
    def _drop_extracted_tables(
        self,
        conn: DB,
        prefix: Prefix,
    ) -> None:
        tables: list[Sequence[Sequence[Any]]] = []
        with closing(conn.cursor()) as cur:
            try:
                cur.execute(
                    sql.SQL("SELECT table_name FROM {catalog};")
                    .format(catalog=prefix.catalog_table_name)
                    .as_string(),
                )
            except self._missing_table_error:
                self._rollback(conn)
            else:
                tables.extend(cur.fetchall())

        with closing(conn.cursor()) as cur:
            try:
                cur.execute(
                    sql.SQL("SELECT table_name FROM {catalog};")
                    .format(catalog=prefix.legacy_jtable)
                    .as_string(),
                )
            except self._missing_table_error:
                self._rollback(conn)
            else:
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
                .format(catalog=prefix.catalog_table_name)
                .as_string(),
            )
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {catalog};")
                .format(catalog=prefix.legacy_jtable)
                .as_string(),
            )

    @property
    @abstractmethod
    def _truncate_raw_table_sql(self) -> sql.SQL: ...
    @property
    @abstractmethod
    def _create_raw_table_sql(self) -> sql.SQL: ...
    def _prepare_raw_table(
        self,
        conn: DB,
        prefix: Prefix,
    ) -> None:
        with closing(conn.cursor()) as cur:
            if prefix.schema_name is not None:
                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema};")
                    .format(schema=prefix.schema_name)
                    .as_string(),
                )
        self._drop_raw_table(conn, prefix)
        with closing(conn.cursor()) as cur:
            cur.execute(
                self._create_raw_table_sql.format(
                    table=prefix.raw_table_name,
                ).as_string(),
            )

    @property
    @abstractmethod
    def _insert_record_sql(self) -> sql.SQL: ...
    def ingest_records(
        self,
        prefix: Prefix,
        on_processed: Callable[[], bool],
        records: Iterator[tuple[int, bytes]],
    ) -> None:
        with closing(self._conn_factory()) as conn:
            self._prepare_raw_table(conn, prefix)

            insert_sql = self._insert_record_sql.format(
                table=prefix.raw_table_name,
            ).as_string()
            with closing(conn.cursor()) as cur:
                for pkey, r in records:
                    cur.execute(insert_sql, (pkey, r.decode()))
                    if not on_processed():
                        break

            conn.commit()
