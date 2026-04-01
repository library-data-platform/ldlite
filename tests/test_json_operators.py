from collections.abc import Callable, Iterator
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import duckdb
import psycopg
import pytest
from pytest_cases import get_case_id, parametrize, parametrize_with_cases

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


def case_jsonb_each() -> JsonTC:
    return JsonTC(
        """
{assertion}
(SELECT e.jkey, a.jkey
FROM (SELECT 'k1' jkey UNION SELECT 'k2' jkey) as e
FULL OUTER JOIN (
    SELECT k.ld_key as jkey
    FROM j, jsonb_each(j.jc->'obj') k(ld_key)
) as a
    USING (jkey)
WHERE e.jkey IS NULL or a.jkey IS NULL) as q;""",
        (),
        "SELECT COUNT(1) = 0 FROM ",
        (),
    )


@parametrize(
    p=[
        ("str", "string"),
        ("num", "number"),
        ("float", "number"),
        ("bigfloat", "number"),
        ("bigint", "number"),
        ("bool", "boolean"),
        ("obj", "object"),
        ("arr_str", "array"),
        ("arr_obj", "array"),
        ("na", "null"),
    ],
)
def case_jsonb_typeof(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """
SELECT jsonb_typeof(jc->$1){assertion}
FROM j;""",
        p[:1],
        """ = $2""",
        p[1:],
    )


@parametrize(
    p=[
        ("1", False),
        ("1.2", True),
        ("0.03", True),
    ],
)
def case_scale(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """
SELECT (SCALE($1::numeric){assertion})
FROM j;""",
        p[:1],
        """ > 0) = $2 AND (TRUE""",
        p[1:],
    )


@parametrize(
    p=[
        ("10.0", True),
    ],
)
def case_whole_scale_postgres(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """
SELECT (SCALE($1::numeric){assertion})
FROM j;""",
        p[:1],
        """ > 0) = $2 AND (TRUE""",
        p[1:],
    )


# duckdb has no problems casting 10.0 to an integer
@parametrize(
    p=[
        ("10.0", False),
    ],
)
def case_whole_scale_duckdb(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """
SELECT (SCALE($1::numeric){assertion})
FROM j;""",
        p[:1],
        """ > 0) = $2 AND (TRUE""",
        p[1:],
    )


@parametrize(
    p=[
        ("arr_str", ['"s1"', '"s2"', '"s3"']),
        ("arr_obj", ['{"k1":"v1"}', '{"k2":"v2"}']),
        ("arr_num", [1, 2, 3]),
    ],
)
def case_jsonb_array_elements(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """
{assertion}
(
    SELECT a.ld_value FROM j, jsonb_array_elements(j.jc->$1) AS a(ld_value)
    EXCEPT SELECT value::{jtype} FROM unnest($2::text[]) AS expect(value)
    UNION ALL
    SELECT value::{jtype} FROM unnest($2::text[]) AS expect(value)
    EXCEPT SELECT a.ld_value FROM j, jsonb_array_elements(j.jc->$1) AS a(ld_value)
) act
""",
        p,
        """SELECT COUNT(1) = 0 FROM""",
        (),
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

        if actual[0] is None or not actual[0]:
            cur.execute(query, tc.query_params)
            diff = ""
            for r in cur.fetchall():
                diff += f"{r}\n"
            pytest.fail(diff)

        assert actual[0] is not None
        assert actual[0]


def _arrange(conn: "dbapi.DBAPIConnection") -> None:
    with closing(conn.cursor()) as cur:
        cur.execute(
            """
INSERT INTO j VALUES (
'{
    "str": "str_val",
    "num": 12,
    "float": 16.3,
    "evenfloat": 10.0,
    "bigint": 2147483648,
    "bigfloat": 2147483648.1,
    "bool": true,
    "obj": {"k1": "v1", "k2": "v2"},
    "arr_str": ["s1", "s2", "s3"],
    "arr_obj": [{"k1": "v1"}, {"k2": "v2"}],
    "arr_num": [1, 2, 3],
    "na": null
}')""",
        )


@pytest.fixture(scope="session")
def duckdb_jop_dsn() -> Iterator[str]:
    dsn = f":memory:{_db()}"

    with duckdb.connect(dsn) as conn:
        conn.execute("CREATE TABLE j (jc JSON)")
        _arrange(cast("dbapi.DBAPIConnection", conn))

        yield dsn


@parametrize_with_cases(
    "tc",
    cases=".",
    filter=lambda cf: "postgres" not in get_case_id(cf),
)
def test_duckdb(duckdb_jop_dsn: str, tc: JsonTC) -> None:
    from ldlite import LDLite

    ld = LDLite()
    ld.connect_db(duckdb_jop_dsn)

    with duckdb.connect(duckdb_jop_dsn) as conn:
        _assert(cast("dbapi.DBAPIConnection", conn), "JSON", tc)


@pytest.fixture(scope="session")
def pg_jop_dsn(pg_dsn: None | Callable[[str], str]) -> str:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    dsn = pg_dsn(_db())
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE TABLE j (jc JSONB)")
        _arrange(cast("dbapi.DBAPIConnection", conn))
    return dsn


@parametrize_with_cases(
    "tc",
    cases=".",
    filter=lambda cf: "duckdb" not in get_case_id(cf),
)
def test_postgres(pg_jop_dsn: str, tc: JsonTC) -> None:
    from ldlite import LDLite

    ld = LDLite()
    ld.connect_db_postgresql(pg_jop_dsn)

    with psycopg.connect(pg_jop_dsn, cursor_factory=psycopg.RawCursor) as conn:
        _assert(cast("dbapi.DBAPIConnection", conn), "JSONB", tc)
