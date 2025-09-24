import contextlib
from collections.abc import Callable
from difflib import unified_diff
from pathlib import Path
from typing import Any, cast
from unittest import mock
from unittest.mock import MagicMock

import psycopg
import pytest
from psycopg import sql
from pytest_cases import parametrize_with_cases

from tests.test_cases import drop_tables_cases as dtc
from tests.test_cases import load_history_cases as lhc
from tests.test_cases import query_cases as qc
from tests.test_cases import to_csv_cases as csvc


@pytest.fixture(scope="session")
def pg_dsn(pytestconfig: pytest.Config) -> None | Callable[[str], str]:
    host = pytestconfig.getoption("pg_host")
    if host is None:
        return None

    def setup(db: str) -> str:
        base_dsn = f"host={host} user=ldlite password=ldlite"
        with contextlib.closing(psycopg.connect(base_dsn)) as base_conn:
            base_conn.autocommit = True
            with base_conn.cursor() as curr:
                curr.execute(
                    sql.SQL("CREATE DATABASE {db};").format(db=sql.Identifier(db)),
                )

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
    ld.drop_tables(tc.drop)

    for call in tc.calls_list:
        ld.query(table=call.prefix, path="/patched", keep_raw=call.keep_raw)
    ld.drop_tables(tc.drop)

    with psycopg.connect(dsn) as conn, conn.cursor() as res:
        res.execute(
            """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public'
                """,
        )
        assert sorted([r[0] for r in res.fetchall()]) == sorted(tc.expected_tables)

        res.execute('SELECT COUNT(*) FROM "ldlite_system"."load_history"')
        assert (ud := res.fetchone()) is not None
        assert ud[0] == len(tc.calls) - 1
        res.execute(
            'SELECT COUNT(*) FROM "ldlite_system"."load_history"'
            'WHERE "table_name" = %s',
            (tc.drop,),
        )
        assert (d := res.fetchone()) is not None
        assert d[0] == 0


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

    with psycopg.connect(dsn) as conn:
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
                res.execute(
                    sql.SQL("SELECT {cols}::text FROM {table};").format(
                        cols=sql.SQL("::text, ").join(
                            [sql.Identifier(c) for c in cols],
                        ),
                        table=sql.Identifier(table),
                    ),
                )
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
                    # this requires specific formatting to match the postgres strings
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


@mock.patch("httpx_folio.auth.httpx.post")
@mock.patch("httpx_folio.factories.httpx.Client.get")
@parametrize_with_cases("tc", cases=lhc.LoadHistoryTestCases)
def test_history(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    pg_dsn: None | Callable[[str], str],
    tc: lhc.LoadHistoryCase,
) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    from ldlite import LDLite as uut

    ld = uut()
    tc.patch_request_get(ld, httpx_post_mock, client_get_mock)
    dsn = pg_dsn(tc.db)
    ld.connect_folio("https://doesnt.matter", "", "", "")
    ld.connect_db_postgresql(dsn)

    for call in tc.calls_list:
        ld.query(table=call.prefix, path="/patched", query=call.query)

    with psycopg.connect(dsn) as conn, conn.cursor() as res:
        res.execute('SELECT COUNT(*) FROM "ldlite_system"."load_history"')
        assert (ud := res.fetchone()) is not None
        assert ud[0] == len(tc.expected_loads)

        for tn, (q, t) in tc.expected_loads.items():
            res.execute(
                'SELECT * FROM "ldlite_system"."load_history" WHERE "table_name" = %s',
                (tn,),
            )
            assert (d := res.fetchone()) is not None
            assert d[1] == q
            assert d[7] == t
            assert d[6] > d[5] > d[4] > d[3] > d[2]
