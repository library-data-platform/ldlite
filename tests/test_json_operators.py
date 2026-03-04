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


@parametrize(
    p=[
        ("str", '"str_val"'),
        ("str_empty", "null"),
        ("num", "12"),
        ("float", "16.3"),
        ("bool", "true"),
        ("obj", '{"k1":"v1","k2":"v2"}'),
        ("obj_some", '{"k1":"v1","k2":null}'),
        ("obj_empty", "null"),
        ("arr_zero", "null"),
        ("na", "null"),
        ("na_str1", "null"),
        ("na_str2", "null"),
    ],
)
def case_jextract(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """SELECT ldlite_system.jextract(jc, $1){assertion} FROM j;""",
        p[:1],
        """= $2::{jtype}""",
        p[1:],
    )


# Duckdb through 1.3 and 1.4 have different json comparison behavior here
# Whitespace matters in 1.4 and not 1.3
# This makes the arrays text and compares the values as a workaround
@parametrize(
    p=[
        ("arr_str", '["s1", "s2", "s3"]'),
        ("arr_str_some", '["s1", "s2"]'),
        ("arr_obj_some", '[{"k1":"v1"}]'),
    ],
)
def case_jextract_duckdb(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """SELECT ldlite_system.jextract(jc, $1){assertion} FROM j;""",
        p[:1],
        """::text[] = $2::JSON::text[]""",
        p[1:],
    )


# The differences betweeen postgres/duckdb here only matters for tests
# This can all be rectified when duckdb 1.4 is the minimum version
@parametrize(
    p=[
        ("arr_str", '["s1", "s2", "s3"]'),
        ("arr_str_some", '["s1", "s2"]'),
        ("arr_obj_some", '[{"k1":"v1"}]'),
    ],
)
def case_jextract_postgres(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """SELECT ldlite_system.jextract(jc, $1){assertion} FROM j;""",
        p[:1],
        """ = $2::JSONB""",
        p[1:],
    )


@parametrize(
    p=[
        ("str", "str_val"),
        ("num", "12"),
        ("float", "16.3"),
        ("bool", "true"),
        ("na",),
        ("na_str1",),
        ("na_str2",),
    ],
)
def case_jextract_string(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """SELECT ldlite_system.jextract_string(jc, $1){assertion} FROM j;""",
        p[:1],
        """ = $2""" if len(p) == 2 else """ IS NULL""",
        p[1:],
    )


def case_jobject_keys() -> JsonTC:
    return JsonTC(
        """
{assertion}
(SELECT e.jkey, a.jkey
FROM (SELECT 'k1' jkey UNION SELECT 'k2' jkey) as e
FULL OUTER JOIN (SELECT ldlite_system.jobject_keys(jc->'obj') jkey FROM j) as a
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
        ("bool", "boolean"),
        ("obj", "object"),
        ("arr_str", "array"),
        ("arr_obj", "array"),
        ("na", "null"),
    ],
)
def case_jtype_of(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """
SELECT ldlite_system.jtype_of(jc->$1){assertion}
FROM j;""",
        p[:1],
        """ = $2""",
        p[1:],
    )


@parametrize(
    p=[
        ("arr_str", ['"s1"', '"s2"', '"s3"']),
        ("arr_obj", ['{"k1":"v1"}', '{"k2":"v2"}']),
        ("arr_num", [1, 2, 3]),
    ],
)
def case_jexplode(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """
{assertion}
(
    SELECT a.ld_value FROM j, ldlite_system.jexplode(j.jc->$1) a
    EXCEPT SELECT value::{jtype} FROM unnest($2::text[]) AS expect(value)
    UNION ALL
    SELECT value::{jtype} FROM unnest($2::text[]) AS expect(value)
    EXCEPT SELECT a.ld_value FROM j, ldlite_system.jexplode(j.jc->$1) a
)
""",
        p,
        """SELECT COUNT(1) = 0 FROM""",
        (),
    )


@parametrize(
    p=[
        ("str", False),
        ("str_empty", False),
        ("num", False),
        ("na", False),
        ("na_str1", False),
        ("na_str2", False),
        ("uuid_nof", False),
        ("uuid", True),
    ],
)
def case_jis_uuid(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """
SELECT {assertion}ldlite_system.jis_uuid(jc->$1)
FROM j;""",
        p[:1],
        "" if (p[1]) else """ NOT """,
        (),
    )


@parametrize(
    p=[
        ("na", True),
        ("obj_empty", False),
        ("arr_zero", False),
        ("na_str1", False),
        ("na_str2", False),
    ],
)
def case_jis_null(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """SELECT ldlite_system.jis_null(jc->$1){assertion} FROM j;""",
        p[:1],
        """= $2""",
        p[1:],
    )


@parametrize(
    p=[
        ("str", False),
        ("str_empty", False),
        ("num", False),
        ("na", False),
        ("na_str1", False),
        ("na_str2", False),
        ("uuid_nof", False),
        ("uuid", False),
        ("dt", True),
    ],
)
def case_jis_datetime(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """
SELECT {assertion}ldlite_system.jis_datetime(jc->$1)
FROM j;""",
        p[:1],
        "" if (p[1]) else """ NOT """,
        (),
    )


@parametrize(
    p=[
        ("str", False),
        ("str_empty", False),
        ("num", False),
        ("na", False),
        ("na_str1", False),
        ("na_str2", False),
        ("uuid_nof", False),
        ("uuid", False),
        ("dt", False),
        ("num", False),
        ("float", True),
    ],
)
def case_jis_float(p: tuple[Any, ...]) -> JsonTC:
    return JsonTC(
        """
SELECT {assertion}ldlite_system.jis_float(jc->$1)
FROM j;""",
        p[:1],
        "" if (p[1]) else """ NOT """,
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
    "str_empty": "",
    "num": 12,
    "float": 16.3,
    "bool": true,
    "uuid": "5b285d03-5490-1111-8888-52b2003b475c",
    "uuid_nof": "5b285d03-5490-FFFF-0000-52b2003b475c",
    "obj": {"k1": "v1", "k2": "v2"},
    "obj_some": {"k1": "v1", "k2": null},
    "obj_empty": {},
    "arr_zero": [],
    "arr_str": ["s1", "s2", "s3"],
    "arr_str_some": ["s1", "s2", null],
    "arr_obj": [{"k1": "v1"}, {"k2": "v2"}],
    "arr_obj_some": [{"k1": "v1"}, null],
    "arr_num": [1, 2, 3],
    "dt": "2022-04-21T18:47:33.581+00:00",
    "na": null,
    "na_str1": "null",
    "na_str2": "NULL"
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
