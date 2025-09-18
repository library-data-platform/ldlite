from __future__ import annotations

import secrets
import sqlite3
from contextlib import closing
from enum import Enum
from typing import TYPE_CHECKING, Callable, cast

import duckdb
import psycopg
from psycopg import sql

from ._database import Database

if TYPE_CHECKING:
    from collections.abc import Iterator

    from _typeshed import dbapi

    from ._database import Prefix
    from ._jsonx import JsonValue


class DBType(Enum):
    UNDEFINED = 0
    DUCKDB = 1
    POSTGRES = 2
    SQLITE = 4


class DBTypeDatabase(Database["dbapi.DBAPIConnection"]):
    def __init__(self, dbtype: DBType, factory: Callable[[], dbapi.DBAPIConnection]):
        self._dbtype = dbtype
        super().__init__(factory)

    @property
    def _missing_table_error(self) -> tuple[type[Exception], ...]:
        return (
            psycopg.errors.UndefinedTable,
            sqlite3.OperationalError,
            duckdb.CatalogException,
        )

    def _rollback(self, conn: dbapi.DBAPIConnection) -> None:
        if sql3db := as_sqlite(conn, self._dbtype):
            sql3db.rollback()
        if pgdb := as_postgres(conn, self._dbtype):
            pgdb.rollback()

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
        truncate_sql = "TRUNCATE TABLE {table};"
        if self._dbtype == DBType.SQLITE:
            truncate_sql = "DELETE FROM {table};"

        return sql.SQL(truncate_sql)

    @property
    def _insert_record_sql(self) -> sql.SQL:
        insert_sql = "INSERT INTO {table} VALUES(?, ?);"
        if self._dbtype == DBType.POSTGRES:
            insert_sql = "INSERT INTO {table} VALUES(%s, %s);"

        return sql.SQL(insert_sql)

    def ingest_records(
        self,
        prefix: Prefix,
        on_processed: Callable[[], bool],
        records: Iterator[tuple[int, bytes]],
    ) -> None:
        if self._dbtype != DBType.POSTGRES:
            super().ingest_records(prefix, on_processed, records)
            return

        with closing(self._conn_factory()) as conn:
            self._prepare_raw_table(conn, prefix)

            if pgconn := as_postgres(conn, self._dbtype):
                with (
                    pgconn.cursor() as cur,
                    cur.copy(
                        sql.SQL(
                            "COPY {table} (__id, jsonb) FROM STDIN (FORMAT BINARY)",
                        ).format(table=prefix.raw_table_name),
                    ) as copy,
                ):
                    # postgres jsonb is always version 1
                    # and it always goes in front
                    jver = (1).to_bytes(1, "big")
                    for pkey, r in records:
                        rb = bytearray()
                        rb.extend(jver)
                        rb.extend(r)
                        copy.write_row((pkey.to_bytes(4, "big"), rb))
                        if not on_processed():
                            break

                pgconn.commit()


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
