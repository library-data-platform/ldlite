from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import duckdb
import psycopg
import pytest
from pytest_cases import parametrize_with_cases

if TYPE_CHECKING:
    from _typeshed import dbapi


def _db() -> str:
    db = "db" + str(uuid4()).split("-")[0]
    print(db)  # noqa: T201
    return db


@dataclass
class Assertion:
    statement: str

    expect: int | str | None = None
    exp_pg: int | str | None = None
    exp_duck: int | str | None = None

    def expected(self, db: str) -> int | str | None:
        if db == "postgres":
            return self.exp_pg or self.expect
        if db == "duckdb":
            return self.exp_duck or self.expect
        return self.expect


@dataclass
class ExpansionTC:
    records: list[bytes]
    assertions: list[Assertion]
    json_depth: int = 999
    keep_raw: bool = True


def case_basic_object() -> ExpansionTC:
    return ExpansionTC(
        records=[
            b"""{"id": "id1", "value": "value1"}""",
            b"""{"id": "id2", "value": "value2"}""",
        ],
        assertions=[
            Assertion("SELECT COUNT(*) FROM tests.prefix__t;", 2),
            Assertion("SELECT id FROM tests.prefix__t WHERE __id = 1", "id1"),
            Assertion("SELECT value FROM tests.prefix__t WHERE __id = 1", "value1"),
            Assertion("SELECT id FROM tests.prefix__t WHERE __id = 2", "id2"),
            Assertion("SELECT value FROM tests.prefix__t WHERE __id = 2", "value2"),
        ],
    )


def case_typed_columns() -> ExpansionTC:
    return ExpansionTC(
        records=[
            b"""
{
    "id": "id1",
    "numeric": 1,
    "text": "value",
    "uuid": "88888888-8888-1888-8888-888888888888"
}
""",
            b"""
{
    "id": "id2",
    "numeric": 2,
    "text": "00000000-0000-1000-A000-000000000000",
    "uuid": "11111111-1111-1111-8111-111111111111"
}
""",
        ],
        assertions=[
            Assertion(
                f"""
SELECT DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'prefix__t' AND COLUMN_NAME = '{a[0]}'
""",
                exp_pg=a[0],
                exp_duck=a[1],
            )
            for a in [
                ("numeric", "DECIMAL(18,3)"),
                ("text", "VARCHAR"),
                ("uuid", "UUID"),
            ]
        ],
    )


# TODO: Remove this test after implementing array expansion
def case_arrays() -> ExpansionTC:
    return ExpansionTC(
        records=[
            b"""
{
    "id": "id1",
    "list": [{"id": "arr_id1"}]
}
""",
            b"""
{
    "id": "id2",
    "list": [{"id": "arr_id2"}]
}
""",
        ],
        assertions=[
            Assertion(
                """
SELECT DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'prefix__t' AND COLUMN_NAME = 'list'
""",
                exp_pg="jsonb",
                exp_duck="JSON",
            ),
        ],
    )


def case_nested_objects() -> ExpansionTC:
    return ExpansionTC(
        records=[
            b"""
{
    "id": "id1",
    "value": "value1",
    "sub": {
        "id": "sub_id1",
        "subsub": {"id": "subsub_id1"},
        "subsib": {"id": "subsib_id1"}
    },
    "sib": {
        "id": "sib_id1",
        "sibsub": {"id": "sibsub_id1"},
        "sibsib": {"id": "sibsib_id1"}
    }
}
""",
            b"""
{
    "id": "id2",
    "value": "value2",
    "sub": {
        "id": "sub_id2",
        "subsub": {"id": "subsub_id2"},
        "subsib": {"id": "subsib_id2"}
    },
    "sib": {
        "id": "sib_id2",
        "sibsub": {"id": "sibsub_id2"},
        "sibsib": {"id": "sibsib_id2"}
    }
}
""",
        ],
        assertions=[
            Assertion("SELECT COUNT(*) FROM tests.prefix__t;", 2),
            Assertion("SELECT id FROM tests.prefix__t WHERE __id = 1", "id1"),
            Assertion("SELECT sub__id FROM tests.prefix__t WHERE __id = 1", "sub_id1"),
            Assertion(
                "SELECT sub__subsub__id FROM tests.prefix__t WHERE __id = 1",
                "subsub_id1",
            ),
            Assertion(
                "SELECT sub__subsib__id FROM tests.prefix__t WHERE __id = 1",
                "subsib_id1",
            ),
            Assertion("SELECT id FROM tests.prefix__t WHERE __id = 2", "id2"),
            Assertion("SELECT sib__id FROM tests.prefix__t WHERE __id = 2", "sib_id2"),
            Assertion(
                "SELECT sib__sibsub__id FROM tests.prefix__t WHERE __id = 2",
                "sibsub_id2",
            ),
            Assertion(
                "SELECT sib__sibsib__id FROM tests.prefix__t WHERE __id = 2",
                "sibsib_id2",
            ),
        ],
    )


def case_keep_raw() -> ExpansionTC:
    return ExpansionTC(
        keep_raw=True,
        records=[
            b"""{ "id": "id1" }""",
            b"""{ "id": "id2" }""",
        ],
        assertions=[
            Assertion(
                """
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'prefix'
""",
                1,
            ),
        ],
    )


def case_dont_keep_raw() -> ExpansionTC:
    return ExpansionTC(
        keep_raw=False,
        records=[
            b"""{ "id": "id1" }""",
            b"""{ "id": "id2" }""",
        ],
        assertions=[
            Assertion(
                """
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'prefix'
""",
                0,
            ),
        ],
    )


def _assert(
    conn: "dbapi.DBAPIConnection",
    db: str,
    tc: ExpansionTC,
) -> None:
    with closing(conn.cursor()) as cur:
        for a in tc.assertions:
            cur.execute(a.statement.format())

            actual = cur.fetchone()
            assert actual is not None
            assert actual[0] == a.expected(db)


@parametrize_with_cases("tc", cases=".")
def test_duckdb(tc: ExpansionTC) -> None:
    from ldlite import LDLite

    dsn = f":memory:{_db()}"

    ld = LDLite()
    ld.connect_db(dsn)
    assert ld.database is not None

    ld.database.ingest_records("tests.prefix", iter(tc.records))
    ld.database.expand_prefix("tests.prefix", tc.json_depth, tc.keep_raw)

    with duckdb.connect(dsn) as conn:
        _assert(cast("dbapi.DBAPIConnection", conn), "duckdb", tc)


@parametrize_with_cases("tc", cases=".")
def test_postgres(pg_dsn: None | Callable[[str], str], tc: ExpansionTC) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    from ldlite import LDLite

    dsn = pg_dsn(_db())

    ld = LDLite()
    ld.connect_db_postgresql(dsn)
    assert ld.database is not None

    ld.database.ingest_records("tests.prefix", iter(tc.records))
    ld.database.expand_prefix("tests.prefix", tc.json_depth, tc.keep_raw)

    with psycopg.connect(dsn, cursor_factory=psycopg.RawCursor) as conn:
        _assert(cast("dbapi.DBAPIConnection", conn), "postgres", tc)
