import secrets
from enum import Enum


class _DBType(Enum):
    UNDEFINED = 0
    DUCKDB = 1
    POSTGRES = 2
    SQLITE = 4


def _strip_schema(table):
    st = table.split(".")
    if len(st) == 1:
        return table
    if len(st) == 2:
        return st[1]
    raise ValueError("invalid table name: " + table)


def _autocommit(db, dbtype, enable):
    if dbtype == _DBType.POSTGRES:
        db.rollback()
        db.set_session(autocommit=enable)
    if dbtype == _DBType.SQLITE:
        db.rollback()
        if enable:
            db.isolation_level = None
        else:
            db.isolation_level = "DEFERRED"


def _server_cursor(db, dbtype):
    if dbtype == _DBType.POSTGRES:
        return db.cursor(name=("ldlite" + secrets.token_hex(4)))
    return db.cursor()


def _sqlid(ident):
    sp = ident.split(".")
    if len(sp) == 1:
        return '"' + ident + '"'
    return ".".join(['"' + s + '"' for s in sp])


def _cast_to_varchar(ident: str, dbtype: _DBType):
    if dbtype == _DBType.SQLITE:
        return "CAST(" + ident + " as TEXT)"
    return ident + "::varchar"


def _varchar_type(dbtype):
    if dbtype == _DBType.POSTGRES or _DBType.SQLITE:
        return "text"
    return "varchar"


def _json_type(dbtype):
    if dbtype == _DBType.POSTGRES:
        return "jsonb"
    if dbtype == _DBType.SQLITE:
        return "text"
    return "varchar"


def _encode_sql_str(dbtype, s):
    if dbtype == _DBType.POSTGRES:
        b = "E'"
    else:
        b = "'"
    if dbtype == _DBType.SQLITE or dbtype == _DBType.DUCKDB:
        for c in s:
            if c == "'":
                b += "''"
            else:
                b += c
    if dbtype == _DBType.POSTGRES:
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


def _encode_sql(dbtype, data):
    if data is None:
        return "NULL"
    if isinstance(data, str):
        return _encode_sql_str(dbtype, data)
    if isinstance(data, int):
        return str(data)
    if isinstance(data, bool):
        return "TRUE" if data else "FALSE"
    return _encode_sql_str(dbtype, str(data))
