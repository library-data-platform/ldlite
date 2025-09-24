from collections.abc import Sequence
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import duckdb
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
    assertion: str
    params: Sequence[Any]
    debug: str
    debug_params: Sequence[Any]


@parametrize(
    p=[
        ("str", '"str_val"'),
        ("num", 12),
        ("obj", '{"k1":"v1","k2":"v2"}'),
        ("arr_str", '["s1","s2","s3"]'),
        ("arr_obj", '[{"k1":"v1"},{"k2":"v2"}]'),
    ],
)
def case_jextract(p: Sequence[Any]) -> JsonTC:
    return JsonTC(
        """SELECT ldlite_system.jextract(jc, $1) == $2 FROM j;""",
        p,
        """SELECT ldlite_system.jextract(jc, $1) FROM j;""",
        p[:1],
    )


def _assert(conn: "dbapi.DBAPIConnection", tc: JsonTC) -> None:
    with closing(conn.cursor()) as cur:
        cur.execute(tc.assertion, tc.params)
        actual = cur.fetchone()
        assert actual is not None
        assert actual[0] is not None
    if not actual[0]:
        conn.rollback()  # type:ignore[attr-defined]
        with closing(conn.cursor()) as cur:
            cur.execute(tc.debug, tc.debug_params)
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
        _assert(cast("dbapi.DBAPIConnection", tx), tc)
