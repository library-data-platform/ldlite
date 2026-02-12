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
    prefix: str
    records: list[bytes]
    assertions: list[Assertion]
    json_depth: int = 999
    keep_raw: bool = True


def case_basic_object() -> ExpansionTC:
    return ExpansionTC(
        prefix="basic",
        records=[
            b"""{"id": "id1", "value": "value1"}""",
            b"""{"id": "id2", "value": "value2"}""",
        ],
        assertions=[
            Assertion("SELECT COUNT(*) FROM basic__t;", 2),
            Assertion("SELECT value FROM basic__t WHERE id = 'id1'", "value1"),
            Assertion("SELECT value FROM basic__t WHERE id = 'id2'", "value2"),
        ],
    )


def case_typed_columns() -> ExpansionTC:
    return ExpansionTC(
        prefix="basic",
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
WHERE TABLE_NAME = 'basic__t' AND COLUMN_NAME = '{a[0]}'
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


def _assert(conn: "dbapi.DBAPIConnection", db: str, tc: ExpansionTC) -> None:
    with closing(conn.cursor()) as cur:
        for a in tc.assertions:
            cur.execute(a.statement)

            actual = cur.fetchone()
            assert actual is not None
            assert actual[0] == a.expected(db)


@parametrize_with_cases("tc", cases=".")
def test_duckdb(tc: ExpansionTC) -> None:
    from ldlite import LDLite
    from ldlite.database import Prefix

    dsn = f":memory:{_db()}"

    ld = LDLite()
    ld.connect_db(dsn)
    assert ld.database is not None

    prefix = Prefix(tc.prefix)
    ld.database.ingest_records(prefix, iter(tc.records))
    ld.database.expand_prefix(prefix, tc.json_depth, tc.keep_raw)

    with duckdb.connect(dsn) as conn:
        _assert(cast("dbapi.DBAPIConnection", conn), "duckdb", tc)


@parametrize_with_cases("tc", cases=".")
def test_postgres(pg_dsn: None | Callable[[str], str], tc: ExpansionTC) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    from ldlite import LDLite
    from ldlite.database import Prefix

    dsn = pg_dsn(_db())

    ld = LDLite()
    ld.connect_db_postgresql(dsn)
    assert ld.database is not None

    prefix = Prefix(tc.prefix)
    ld.database.ingest_records(prefix, iter(tc.records))
    ld.database.expand_prefix(prefix, tc.json_depth, tc.keep_raw)

    with psycopg.connect(dsn, cursor_factory=psycopg.RawCursor) as conn:
        _assert(cast("dbapi.DBAPIConnection", conn), "postgres", tc)
