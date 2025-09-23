from difflib import unified_diff
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import duckdb
import pytest
from pytest_cases import parametrize_with_cases

from tests.test_cases import drop_tables_cases as dtc
from tests.test_cases import load_history_cases as lhc
from tests.test_cases import query_cases as qc
from tests.test_cases import to_csv_cases as csvc


@mock.patch("httpx_folio.auth.httpx.post")
@mock.patch("httpx_folio.factories.httpx.Client.get")
@parametrize_with_cases("tc", cases=dtc.DropTablesCases)
def test_drop_tables(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    tc: dtc.DropTablesCase,
) -> None:
    from ldlite import LDLite as uut

    ld = uut()
    tc.patch_request_get(ld, httpx_post_mock, client_get_mock)
    dsn = f":memory:{tc.db}"
    ld.connect_folio("https://doesnt.matter", "", "", "")
    ld.connect_db(dsn)

    for prefix in tc.values:
        ld.query(table=prefix, path="/patched", keep_raw=tc.keep_raw)
    ld.drop_tables(tc.drop)

    with duckdb.connect(dsn) as res:
        res.execute("SHOW TABLES;")
        assert sorted([r[0] for r in res.fetchall()]) == sorted(tc.expected_tables)

        res.execute('SELECT COUNT(*) FROM "ldlite_system"."load_history"')
        assert (ud := res.fetchone()) is not None
        assert ud[0] == len(tc.values) - 1
        res.execute(
            'SELECT COUNT(*) FROM "ldlite_system"."load_history" '
            'WHERE "table_name" = $1',
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
    tc: qc.QueryCase,
) -> None:
    from ldlite import LDLite as uut

    ld = uut()
    tc.patch_request_get(ld, httpx_post_mock, client_get_mock)
    dsn = f":memory:{tc.db}"
    ld.connect_folio("https://doesnt.matter", "", "", "")
    ld.connect_db(dsn)

    for prefix in tc.values:
        ld.query(
            table=prefix,
            path="/patched",
            json_depth=tc.json_depth,
            keep_raw=tc.keep_raw,
        )

    with duckdb.connect(dsn) as res:
        res.execute("SHOW TABLES;")
        assert sorted([r[0] for r in res.fetchall()]) == sorted(tc.expected_tables)

    for table, (cols, values) in tc.expected_values.items():
        with duckdb.connect(dsn) as res:
            res.execute(f"SELECT {'::text,'.join(cols)}::text FROM {table};")
            for v in values:
                assert res.fetchone() == v

            assert res.fetchone() is None


@mock.patch("httpx_folio.auth.httpx.post")
@mock.patch("httpx_folio.factories.httpx.Client.get")
@parametrize_with_cases("tc", cases=csvc.ToCsvCases)
def test_to_csv(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    tc: csvc.ToCsvCase,
    tmpdir: str,
) -> None:
    from ldlite import LDLite as uut

    ld = uut()
    tc.patch_request_get(ld, httpx_post_mock, client_get_mock)
    dsn = f":memory:{tc.db}"
    ld.connect_folio("https://doesnt.matter", "", "", "")
    ld.connect_db(dsn)

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
    tc: lhc.LoadHistoryCase,
) -> None:
    from ldlite import LDLite as uut

    ld = uut()
    tc.patch_request_get(ld, httpx_post_mock, client_get_mock)
    dsn = f":memory:{tc.db}"
    ld.connect_folio("https://doesnt.matter", "", "", "")
    ld.connect_db(dsn)

    for prefix in tc.values:
        ld.query(
            table=prefix,
            path="/patched",
            query=tc.queries[prefix],
        )

    with duckdb.connect(dsn) as res:
        res.execute('SELECT COUNT(*) FROM "ldlite_system"."load_history"')
        assert (ud := res.fetchone()) is not None
        assert ud[0] == len(tc.expected_loads)

        for tn, (q, t) in tc.expected_loads.items():
            res.execute(
                'SELECT * FROM "ldlite_system"."load_history" WHERE "table_name" = $1',
                (tn,),
            )
            assert (d := res.fetchone()) is not None
            assert d[1] == q
            assert d[7] == t
            assert d[6] > d[5] > d[4] > d[3] > d[2]
