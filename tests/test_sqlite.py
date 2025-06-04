import contextlib
import sqlite3
from unittest import mock
from unittest.mock import MagicMock

from pytest_cases import parametrize_with_cases

from .test_cases import drop_tables_cases as dtc
from .test_cases import query_cases as qc


@mock.patch("ldlite.request_get")
@parametrize_with_cases("tc", cases=dtc.DropTablesCases)
def test_drop_tables(request_get_mock: MagicMock, tc: dtc.DropTablesCase) -> None:
    from ldlite import LDLite as uut

    ld = uut()
    tc.patch_request_get(ld, request_get_mock)
    dsn = f"file:{tc.db}?mode=memory&cache=shared"
    ld.experimental_connect_db_sqlite(dsn)

    for prefix in tc.values:
        ld.query(table=prefix, path="/patched")
    ld.drop_tables(tc.drop)

    with sqlite3.connect(dsn) as conn, contextlib.closing(conn.cursor()) as res:
        res.execute("SELECT name FROM sqlite_master WHERE type='table';")
        assert sorted([r[0] for r in res.fetchall()]) == sorted(tc.expected_tables)


@mock.patch("ldlite.request_get")
@parametrize_with_cases("tc", cases=qc.QueryTestCases)
def test_query(request_get_mock: MagicMock, tc: qc.QueryCase) -> None:
    from ldlite import LDLite as uut

    ld = uut()
    tc.patch_request_get(ld, request_get_mock)
    dsn = f"file:{tc.db}?mode=memory&cache=shared"
    ld.experimental_connect_db_sqlite(dsn)

    for prefix in tc.values:
        ld.query(table=prefix, path="/patched", json_depth=tc.json_depth)

    catalog = f"{prefix}__tcatalog"
    fixed_cols = [prefix]
    # TODO: Clean this edge case up so tcatalog is always created
    if tc.json_depth > 0:
        fixed_cols.append(catalog)

    with sqlite3.connect(dsn) as conn:
        with contextlib.closing(conn.cursor()) as res:
            res.execute("SELECT name FROM sqlite_master WHERE type='table';")
            assert sorted([r[0] for r in res.fetchall()]) == sorted(
                [*fixed_cols, *[f"{prefix}__{t}" for t in tc.expected_tables]]
            )

        if tc.json_depth > 0:
            with contextlib.closing(conn.cursor()) as res:
                res.execute(f"SELECT table_name FROM {catalog};")
                assert sorted([r[0] for r in res.fetchall()]) == sorted(
                    [f"{prefix}__{t}" for t in tc.expected_tables]
                )

        for table, (cols, values) in tc.expected_values.items():
            with contextlib.closing(conn.cursor()) as res:
                res.execute(f"SELECT {','.join(cols)} FROM {prefix}__{table};")
                for v in values:
                    assert res.fetchone() == v

                assert res.fetchone() is None
