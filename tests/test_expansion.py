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
class ExpansionTC:
    prefix: str
    records: list[bytes]
    assertions: list[tuple[str, int | str | bool]]
    catalog: list[str]
    json_depth: int = 999
    keep_raw: bool = True


def case_basic_object() -> ExpansionTC:
    return ExpansionTC(
        prefix="basic",
        records=[b"""{"id": "id1", "value": "value1"}"""],
        assertions=[
            ("SELECT COUNT(*) FROM basic__t;", 1),
            ("SELECT value FROM basic__t WHERE id = 'id1'", "value1"),
        ],
        catalog=["basic", "basic__t"],
    )


def _assert(conn: "dbapi.DBAPIConnection", tc: ExpansionTC) -> None:
    with closing(conn.cursor()) as cur:
        for assertion, expected in tc.assertions:
            cur.execute(assertion)

            actual = cur.fetchone()
            assert actual is not None
            assert actual[0] == expected


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
        _assert(cast("dbapi.DBAPIConnection", conn), tc)


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
        _assert(cast("dbapi.DBAPIConnection", conn), tc)
