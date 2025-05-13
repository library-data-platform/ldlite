import sqlite3
from unittest import mock
from unittest.mock import MagicMock
from .cases import QueryTestCases, QueryCase

from pytest_cases import parametrize_with_cases

@mock.patch("ldlite._request_get")
@parametrize_with_cases("tc", cases=QueryTestCases)
def test_sqlite(_request_get_mock: MagicMock, tc: QueryCase) -> None:
    from ldlite import LDLite as uut

    dsn = f"file:{tc.db}?mode=memory&cache=shared"
    tc.patch__request_get(_request_get_mock)

    ld = uut()
    # _check_okapi() hack
    ld.login_token = "token"
    ld.okapi_url = "url"
    # leave tqdm out of it
    ld.quiet(enable=True)

    prefix = "prefix"
    ld.experimental_connect_db_sqlite(dsn)
    # we're not testing the endpoint behavior so path doesn't matter
    ld.query(table=prefix, path="/pancakes")

    with sqlite3.connect(dsn) as conn:
        res = conn.cursor()
        res.execute("SELECT name FROM sqlite_master WHERE type='table';")
        assert [r[0] for r in res.fetchall()] == [prefix, *[f"{prefix}__{t}" for t in tc.expected_tables], f"{prefix}__tcatalog"]

        for table, (cols, values) in tc.expected_values.items():
            res.execute(f"SELECT {','.join(cols)} FROM {prefix}__{table};")
            for v in values:
                assert res.fetchone() == v

            assert res.fetchone() is None
