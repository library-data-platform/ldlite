import secrets
from enum import Enum
from typing import TYPE_CHECKING, cast

import duckdb
import psycopg

if TYPE_CHECKING:
    from _typeshed import dbapi

    from ._jsonx import JsonValue


class DBType(Enum):
    UNDEFINED = 0
    DUCKDB = 1
    POSTGRES = 2


def as_duckdb(
    db: "dbapi.DBAPIConnection",
    dbtype: DBType,
) -> duckdb.DuckDBPyConnection | None:
    if dbtype != DBType.DUCKDB:
        return None

    return cast("duckdb.DuckDBPyConnection", db)


def as_postgres(
    db: "dbapi.DBAPIConnection",
    dbtype: DBType,
) -> psycopg.Connection | None:
    if dbtype != DBType.POSTGRES:
        return None

    return cast("psycopg.Connection", db)


def autocommit(db: "dbapi.DBAPIConnection", dbtype: DBType, enable: bool) -> None:
    if (pgdb := as_postgres(db, dbtype)) is not None:
        pgdb.rollback()
        pgdb.set_autocommit(enable)


def server_cursor(db: "dbapi.DBAPIConnection", dbtype: DBType) -> "dbapi.DBAPICursor":
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


def cast_to_varchar(ident: str) -> str:
    return ident + "::varchar"


def varchar_type(dbtype: DBType) -> str:
    if dbtype == DBType.POSTGRES:
        return "text"
    return "varchar"


def encode_sql_str(dbtype: DBType, s: str | bytes) -> str:  # noqa: C901, PLR0912
    if isinstance(s, bytes):
        s = s.decode("utf-8")

    b = "E'" if dbtype == DBType.POSTGRES else "'"
    if dbtype == DBType.DUCKDB:
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


def encode_sql(dbtype: DBType, data: "JsonValue") -> str:
    if data is None:
        return "NULL"
    if isinstance(data, str):
        return encode_sql_str(dbtype, data)
    if isinstance(data, int):
        return str(data)
    if isinstance(data, bool):
        return "TRUE" if data else "FALSE"
    return encode_sql_str(dbtype, str(data))
