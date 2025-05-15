from __future__ import annotations

import secrets
from enum import Enum
from typing import Any


class DBType(Enum):
    UNDEFINED = 0
    DUCKDB = 1
    POSTGRES = 2
    SQLITE = 4


def strip_schema(table: str) -> str:
    st = table.split(".")
    if len(st) == 1:
        return table
    if len(st) == 2:
        return st[1]
    raise ValueError("invalid table name: " + table)


def autocommit(db: Any, dbtype: DBType, enable: bool) -> None:
    if dbtype == DBType.POSTGRES:
        db.rollback()
        db.set_session(autocommit=enable)
    if dbtype == DBType.SQLITE:
        db.rollback()
        if enable:
            db.isolation_level = None
        else:
            db.isolation_level = "DEFERRED"


def server_cursor(db: Any, dbtype: DBType) -> Any:
    if dbtype == DBType.POSTGRES:
        return db.cursor(name=("ldlite" + secrets.token_hex(4)))
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


def encode_sql_str(dbtype: DBType, s: str) -> str:  # noqa: C901, PLR0912
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


def encode_sql(dbtype: DBType, data: None | str | int | bool | Any) -> str:
    if data is None:
        return "NULL"
    if isinstance(data, str):
        return encode_sql_str(dbtype, data)
    if isinstance(data, int):
        return str(data)
    if isinstance(data, bool):
        return "TRUE" if data else "FALSE"
    return encode_sql_str(dbtype, str(data))
