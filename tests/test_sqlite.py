import contextlib
import sqlite3
from unittest import mock
from unittest.mock import MagicMock

from pytest_cases import parametrize_with_cases

from .test_cases import drop_tables_cases as dtc


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
        assert sorted([r[0] for r in res.fetchall()]) == sorted(tc.expected_values)
