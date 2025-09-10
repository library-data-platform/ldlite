from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import closing
from typing import TYPE_CHECKING, Callable, Generic, TypeVar

from psycopg import sql

from ._sqlx import DBType, server_cursor

if TYPE_CHECKING:
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

    @abstractmethod
    def _create_server_cursor(self, conn: DB) -> DBC: ...

    @property
    @abstractmethod
    def _truncate_raw_table_sql(self) -> sql.SQL: ...
    @property
    @abstractmethod
    def _create_raw_table_sql(self) -> sql.SQL: ...

    def prepare_raw_table(
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


class DBTypeDatabase(Database[dbapi.DBAPIConnection, dbapi.DBAPICursor]):
    def __init__(self, dbtype: DBType, db: dbapi.DBAPIConnection):
        self._dbtype = dbtype
        super().__init__(lambda: db)

    def _create_server_cursor(self, conn: dbapi.DBAPIConnection) -> dbapi.DBAPICursor:
        return server_cursor(conn, self._dbtype)

    @property
    def _create_raw_table_sql(self) -> sql.SQL:
        create_sql = "CREATE TABLE IF NOT EXISTS {table} (__id integer, jsonb text);"
        if self._dbtype == DBType.POSTGRES:
            create_sql = (
                "CREATE TABLE IF NOT EXISTS {table} (__id integer, jsonb jsonb);"
            )

        return sql.SQL(create_sql)

    @property
    def _truncate_raw_table_sql(self) -> sql.SQL:
        truncate_sql = "TRUNCATE TABLE {table}"
        if self._dbtype == DBType.SQLITE:
            truncate_sql = "DELETE FROM {table}"

        return sql.SQL(truncate_sql)
