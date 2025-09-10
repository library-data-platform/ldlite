from __future__ import annotations

import secrets
from enum import Enum
from typing import TYPE_CHECKING, cast

from psycopg import sql

from ._database import Database

if TYPE_CHECKING:
    import sqlite3

    import duckdb
    import psycopg
    from _typeshed import dbapi

    from ._jsonx import JsonValue
else:
    # I can't seem to figure out how to make this better
    from unittest.mock import MagicMock

    dbapi = MagicMock()


class DBType(Enum):
    UNDEFINED = 0
    DUCKDB = 1
    POSTGRES = 2
    SQLITE = 4


class DBTypeDatabase(Database[dbapi.DBAPIConnection, dbapi.DBAPICursor]):
    def __init__(self, dbtype: DBType, db: dbapi.DBAPIConnection):
        self._dbtype = dbtype
        super().__init__(lambda: db)

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


def as_duckdb(
    db: dbapi.DBAPIConnection,
    dbtype: DBType,
) -> duckdb.DuckDBPyConnection | None:
    if dbtype != DBType.DUCKDB:
        return None

    return cast("duckdb.DuckDBPyConnection", db)


def as_postgres(
    db: dbapi.DBAPIConnection,
    dbtype: DBType,
) -> psycopg.Connection | None:
    if dbtype != DBType.POSTGRES:
        return None

    return cast("psycopg.Connection", db)


def as_sqlite(
    db: dbapi.DBAPIConnection,
    dbtype: DBType,
) -> sqlite3.Connection | None:
    if dbtype != DBType.SQLITE:
        return None

    return cast("sqlite3.Connection", db)


def strip_schema(table: str) -> str:
    st = table.split(".")
    if len(st) == 1:
        return table
    if len(st) == 2:
        return st[1]
    raise ValueError("invalid table name: " + table)


def autocommit(db: dbapi.DBAPIConnection, dbtype: DBType, enable: bool) -> None:
    if (pgdb := as_postgres(db, dbtype)) is not None:
        pgdb.rollback()
        pgdb.set_autocommit(enable)

    if (sql3db := as_sqlite(db, dbtype)) is not None:
        sql3db.rollback()
        if enable:
            sql3db.isolation_level = None
        else:
            sql3db.isolation_level = "DEFERRED"


def server_cursor(db: dbapi.DBAPIConnection, dbtype: DBType) -> dbapi.DBAPICursor:
    if (pgdb := as_postgres(db, dbtype)) is not None:
        return cast(
            "dbapi.DBAPICursor",
            pgdb.cursor(name=("ldlite" + secrets.token_hex(4))),
        )
    return db.cursor()


def sqlid(ident: str) -> str:
    sp = ident.split(".")
    if len(sp) == 1:
        return '"' + ident + '"'
    return ".".join(['"' + s + '"' for s in sp])


def cast_to_varchar(ident: str, dbtype: DBType) -> str:
    if dbtype == DBType.SQLITE:
        return "CAST(" + ident + " as TEXT)"
    return ident + "::varchar"


def varchar_type(dbtype: DBType) -> str:
    if dbtype == DBType.POSTGRES or DBType.SQLITE:
        return "text"
    return "varchar"


def json_type(dbtype: DBType) -> str:
    if dbtype == DBType.POSTGRES:
        return "jsonb"
    if dbtype == DBType.SQLITE:
        return "text"
    return "varchar"


def encode_sql_str(dbtype: DBType, s: str | bytes) -> str:  # noqa: C901, PLR0912
    if isinstance(s, bytes):
        s = s.decode("utf-8")

    b = "E'" if dbtype == DBType.POSTGRES else "'"
    if dbtype in (DBType.SQLITE, DBType.DUCKDB):
        for c in s:
            if c == "'":
                b += "''"
            else:
                b += c
    if dbtype == DBType.POSTGRES:
        for c in s:
            if c == "'":
                b += "''"
            elif c == "\\":
                b += "\\\\"
            elif c == "\n":
                b += "\\n"
            elif c == "\r":
                b += "\\r"
            elif c == "\t":
                b += "\\t"
            elif c == "\f":
                b += "\\f"
            elif c == "\b":
                b += "\\b"
            else:
                b += c
    b += "'"
    return b


def encode_sql(dbtype: DBType, data: JsonValue) -> str:
    if data is None:
        return "NULL"
    if isinstance(data, str):
        return encode_sql_str(dbtype, data)
    if isinstance(data, int):
        return str(data)
    if isinstance(data, bool):
        return "TRUE" if data else "FALSE"
    return encode_sql_str(dbtype, str(data))
