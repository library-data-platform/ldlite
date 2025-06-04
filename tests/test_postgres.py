from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Callable
from unittest import mock
from unittest.mock import MagicMock

import psycopg2
import pytest
from pytest_cases import parametrize_with_cases

from .test_cases import drop_tables_cases as dtc

if TYPE_CHECKING:
    from unittest.mock import MagicMock


@pytest.fixture(scope="session")
def pg_dsn(pytestconfig: pytest.Config) -> None | Callable[[str], str]:
    host = pytestconfig.getoption("pg_host")
    if host is None:
        return None

    def setup(db: str) -> str:
        base_dsn = f"host={host} user=ldlite password=ldlite"
        with contextlib.closing(psycopg2.connect(base_dsn)) as base_conn:
            base_conn.autocommit = True
            with base_conn.cursor() as curr:
                curr.execute(f"CREATE DATABASE {db};")

        return base_dsn + f" dbname={db}"

    return setup


@mock.patch("ldlite.request_get")
@parametrize_with_cases("tc", cases=dtc.DropTablesCases)
def test_drop_tables(
    request_get_mock: MagicMock,
    pg_dsn: None | Callable[[str], str],
    tc: dtc.DropTablesCase,
) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    from ldlite import LDLite as uut

    ld = uut()
    tc.patch_request_get(ld, request_get_mock)
    dsn = pg_dsn(tc.db)
    ld.connect_db_postgresql(dsn)

    for prefix in tc.values:
        ld.query(table=prefix, path="/patched")
    ld.drop_tables(tc.drop)

    with psycopg2.connect(dsn) as conn, conn.cursor() as res:
        res.execute(
            """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public'
                """,
        )
        assert sorted([r[0] for r in res.fetchall()]) == sorted(tc.expected_values)
