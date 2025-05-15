from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Callable
from unittest import mock

import psycopg2
import pytest
from pytest_cases import parametrize_with_cases

from .expansion_cases import QueryCase, QueryTestCases

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


@mock.patch("ldlite._request_get")
@parametrize_with_cases("tc", cases=QueryTestCases)
def test_postgres(
    request_get_mock: MagicMock, pg_dsn: None | Callable[[str], str], tc: QueryCase
) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    from ldlite import LDLite as uut

    dsn = pg_dsn(tc.db)
    tc.patch__request_get(request_get_mock)

    ld = uut()

    # _check_okapi() hack
    ld.login_token = "token"
    ld.okapi_url = "url"
    # leave tqdm out of it
    ld.quiet(enable=True)

    prefix = "prefix"
    ld.connect_db_postgresql(dsn)
    # we're not testing the endpoint behavior so path doesn't matter
    ld.query(table=prefix, path="/pancakes", json_depth=tc.json_depth)

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as res:
            res.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public'
                """
            )
            assert sorted([r[0] for r in res.fetchall()]) == sorted(
                [prefix, *[f"{prefix}__{t}" for t in tc.expected_tables]]
            )

        for table, (cols, values) in tc.expected_values.items():
            with conn.cursor() as res:
                res.execute(f"SELECT {','.join(cols)} FROM {prefix}__{table};")
                for v in values:
                    assert res.fetchone() == v

                assert res.fetchone() is None
