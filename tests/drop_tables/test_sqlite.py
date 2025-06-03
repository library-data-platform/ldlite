import contextlib
import sqlite3
from unittest import mock
from unittest.mock import MagicMock

from pytest_cases import parametrize_with_cases

from .drop_tables_cases import DropTablesCase, DropTablesCases


@mock.patch("ldlite.request_get")
@parametrize_with_cases("tc", cases=DropTablesCases)
def test_sqlite(request_get_mock: MagicMock, tc: DropTablesCase) -> None:
    from ldlite import LDLite as uut

    dsn = f"file:{tc.db}?mode=memory&cache=shared"
    tc.patch_request_get(request_get_mock)

    ld = uut()

    # _check_okapi() hack
    ld.login_token = "token"
    ld.okapi_url = "url"
    # leave tqdm out of it
    ld.quiet(enable=True)

    ld.experimental_connect_db_sqlite(dsn)
    # we're not testing the endpoint behavior so path doesn't matter
    for prefix in tc.values:
        ld.query(table=prefix, path="/pancakes")
    ld.drop_tables(tc.drop)

    with sqlite3.connect(dsn) as conn, contextlib.closing(conn.cursor()) as res:
        res.execute("SELECT name FROM sqlite_master WHERE type='table';")
        assert sorted([r[0] for r in res.fetchall()]) == sorted(tc.expected_tables)
