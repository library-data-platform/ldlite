from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import duckdb
import psycopg
import pytest
from pytest_cases import parametrize, parametrize_with_cases

if TYPE_CHECKING:
    from _typeshed import dbapi


def _db() -> str:
    db = "db" + str(uuid4()).split("-")[0]
    print(db)  # noqa: T201
    return db


@dataclass
class JsonTC:
    query: str
    query_params: tuple[Any, ...]
    assertion: str
    assertion_params: tuple[Any, ...]


@parametrize(
    p=[
        ("str", '"str_val"'),
        ("num", "12"),
        ("obj", '{"k1":"v1","k2":"v2"}'),
        ("arr_str", '["s1","s2","s3"]'),
        ("arr_obj", '[{"k1":"v1"},{"k2":"v2"}]'),
    ],
)
def case_jextract(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """SELECT ldlite_system.jextract(jc, $1){assertion} FROM j;""",
        p[:1],
        """= $2::{jtype}""",
        p[1:],
    )


@parametrize(
    p=[
        ("str", "str_val"),
        ("num", "12"),
    ],
)
def case_jextract_string(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """SELECT ldlite_system.jextract_string(jc, $1){assertion} FROM j;""",
        p[:1],
        """ = $2""",
        p[1:],
    )


def _assert(conn: "dbapi.DBAPIConnection", jtype: str, tc: JsonTC) -> None:
    with closing(conn.cursor()) as cur:
        query = tc.query.format(assertion="", jtype=jtype)
        assertion = tc.query.format(
            assertion=tc.assertion.format(jtype=jtype),
            jtype=jtype,
        )

        cur.execute(assertion, (*tc.query_params, *tc.assertion_params))
        actual = cur.fetchone()
        assert actual is not None
        assert actual[0] is not None
        assert actual[0]
    if not actual[0]:
        cur.execute(query, tc.query_params)
        pytest.fail(str(cur.fetchone()))


@parametrize_with_cases("tc", cases=".")
def test_duckdb(tc: JsonTC) -> None:
    from ldlite import LDLite

    ld = LDLite()
    dsn = f":memory:{_db()}"
    ld.connect_db(dsn)

    with duckdb.connect(dsn) as conn:
        conn.execute("CREATE TABLE j (jc JSON)")
        conn.execute(
            "INSERT INTO j VALUES "
            "('{"
            """ "str": "str_val","""
            """ "num": 12,"""
            """ "obj": {"k1": "v1", "k2": "v2"},"""
            """ "arr_str": ["s1", "s2", "s3"],"""
            """ "arr_obj": [{"k1": "v1"}, {"k2": "v2"}]"""
            " }')",
        )

    with duckdb.connect(dsn) as conn, conn.begin() as tx:
        _assert(cast("dbapi.DBAPIConnection", tx), "JSON", tc)


@parametrize_with_cases("tc", cases=".")
def test_postgres(pg_dsn: None | Callable[[str], str], tc: JsonTC) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    from ldlite import LDLite

    ld = LDLite()
    dsn = pg_dsn(_db())
    ld.connect_db_postgresql(dsn)

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE TABLE j (jc JSONB)")
        cur.execute(
            "INSERT INTO j VALUES "
            "('{"
            """ "str": "str_val","""
            """ "num": 12,"""
            """ "obj": {"k1": "v1", "k2": "v2"},"""
            """ "arr_str": ["s1", "s2", "s3"],"""
            """ "arr_obj": [{"k1": "v1"}, {"k2": "v2"}]"""
            " }')",
        )

    with psycopg.connect(dsn, cursor_factory=psycopg.RawCursor) as conn:
        _assert(cast("dbapi.DBAPIConnection", conn), "JSONB", tc)
