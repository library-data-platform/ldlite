from __future__ import annotations

import contextlib
from difflib import unified_diff
from pathlib import Path
from typing import TYPE_CHECKING, Callable, cast
from unittest import mock
from unittest.mock import MagicMock

import psycopg2
import pytest
from pytest_cases import parametrize_with_cases

from tests.test_cases import drop_tables_cases as dtc
from tests.test_cases import query_cases as qc
from tests.test_cases import to_csv_cases as csvc

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


@mock.patch("httpx_folio.auth.httpx.post")
@mock.patch("httpx_folio.factories.httpx.Client.get")
@parametrize_with_cases("tc", cases=dtc.DropTablesCases)
def test_drop_tables(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    pg_dsn: None | Callable[[str], str],
    tc: dtc.DropTablesCase,
) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    from ldlite import LDLite as uut

    ld = uut()
    tc.patch_request_get(ld, httpx_post_mock, client_get_mock)
    dsn = pg_dsn(tc.db)
    ld.connect_folio("https://doesnt.matter", "", "", "")
    ld.connect_db_postgresql(dsn)

    for prefix in tc.values:
        ld.query(table=prefix, path="/patched", keep_raw=tc.keep_raw)
    ld.drop_tables(tc.drop)

    with psycopg2.connect(dsn) as conn, conn.cursor() as res:
        res.execute(
            """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public'
                """,
        )
        assert sorted([r[0] for r in res.fetchall()]) == sorted(tc.expected_tables)


@mock.patch("httpx_folio.auth.httpx.post")
@mock.patch("httpx_folio.factories.httpx.Client.get")
@parametrize_with_cases("tc", cases=qc.QueryTestCases)
def test_query(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    pg_dsn: None | Callable[[str], str],
    tc: qc.QueryCase,
) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    from ldlite import LDLite as uut

    ld = uut()
    tc.patch_request_get(ld, httpx_post_mock, client_get_mock)
    dsn = pg_dsn(tc.db)
    ld.connect_folio("https://doesnt.matter", "", "", "")
    ld.connect_db_postgresql(dsn)

    for prefix in tc.values:
        ld.query(
            table=prefix,
            path="/patched",
            json_depth=tc.json_depth,
            keep_raw=tc.keep_raw,
        )

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as res:
            res.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public'
                """,
            )
            assert sorted([r[0] for r in res.fetchall()]) == sorted(tc.expected_tables)

        for table, (cols, values) in tc.expected_values.items():
            with conn.cursor() as res:
                res.execute(f"SELECT {','.join(cols)} FROM {table};")
                for v in values:
                    assert res.fetchone() == v

                assert res.fetchone() is None

        if tc.expected_indexes is not None:
            with conn.cursor() as res:
                res.execute(
                    "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'public';",
                )
                assert cast("tuple[int]", res.fetchone())[0] == len(tc.expected_indexes)

                for t, c in tc.expected_indexes:
                    res.execute(f"""
SELECT COUNT(*) FROM pg_indexes
WHERE indexdef LIKE 'CREATE INDEX % ON public.{t} USING btree ({c})';
""")
                    assert cast("tuple[int]", res.fetchone())[0] == 1, f"{t}, {c}"


@mock.patch("httpx_folio.auth.httpx.post")
@mock.patch("httpx_folio.factories.httpx.Client.get")
@parametrize_with_cases("tc", cases=csvc.ToCsvCases)
def test_to_csv(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    pg_dsn: None | Callable[[str], str],
    tc: csvc.ToCsvCase,
    tmpdir: str,
) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    from ldlite import LDLite as uut

    ld = uut()
    tc.patch_request_get(ld, httpx_post_mock, client_get_mock)
    dsn = pg_dsn(tc.db)
    ld.connect_folio("https://doesnt.matter", "", "", "")
    ld.connect_db_postgresql(dsn)

    for prefix in tc.values:
        ld.query(table=prefix, path="/patched")

    for table, expected in tc.expected_csvs:
        actual = (Path(tmpdir) / table).with_suffix(".csv")

        ld.export_csv(str(actual), table)

        with expected.open("r") as f:
            expected_lines = f.readlines()
        with actual.open("r") as f:
            actual_lines = f.readlines()

        diff = list(unified_diff(expected_lines, actual_lines))
        if len(diff) > 0:
            pytest.fail("".join(diff))
