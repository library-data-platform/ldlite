from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import closing
from typing import TYPE_CHECKING, Callable, Generic, TypeVar

from psycopg import sql

if TYPE_CHECKING:
    from collections.abc import Iterator

    from _typeshed import dbapi
else:
    # I can't seem to figure out how to make this better
    from unittest.mock import MagicMock

    dbapi = MagicMock()

DB = TypeVar("DB", bound=dbapi.DBAPIConnection)
DBC = TypeVar("DBC", bound=dbapi.DBAPICursor)


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

    @property
    def raw_table_name(self) -> sql.Identifier:
        return (
            sql.Identifier(self._schema, self._prefix)
            if self._schema is not None
            else sql.Identifier(self._prefix)
        )


class Database(ABC, Generic[DB, DBC]):
    def __init__(self, conn_factory: Callable[[], DB]):
        self._conn_factory = conn_factory

    @property
    @abstractmethod
    def _truncate_raw_table_sql(self) -> sql.SQL: ...
    @property
    @abstractmethod
    def _create_raw_table_sql(self) -> sql.SQL: ...
    @property
    @abstractmethod
    def _insert_record_sql(self) -> sql.SQL: ...

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

            cur.execute(
                self._create_raw_table_sql.format(
                    table=prefix.raw_table_name,
                ).as_string(),
            )
            cur.execute(
                self._truncate_raw_table_sql.format(
                    table=prefix.raw_table_name,
                ).as_string(),
            )

    def ingest_records(
        self,
        conn: DB,
        prefix: Prefix,
        on_processed: Callable[[], bool],
        records: Iterator[tuple[int, str | bytes]],
    ) -> None:
        # the only implementation right now is a hack
        # the db connection is managed outside of the factory
        # for now it's taken as a parameter
        # with self._conn_factory() as conn:
        self._prepare_raw_table(conn, prefix)
        with closing(conn.cursor()) as cur:
            for pkey, d in records:
                cur.execute(
                    self._insert_record_sql.format(
                        table=prefix.raw_table_name,
                    ).as_string(),
                    [pkey, d if isinstance(d, str) else d.decode("utf-8")],
                )
                if not on_processed():
                    return
