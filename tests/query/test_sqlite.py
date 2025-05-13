import sqlite3
from unittest import mock
from unittest.mock import MagicMock
import contextlib

from pytest_cases import parametrize_with_cases

from .expansion_cases import QueryTestCases, QueryCase

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
    ld.query(table=prefix, path="/pancakes", json_depth=tc.json_depth)

    with sqlite3.connect(dsn) as conn:
        with contextlib.closing(conn.cursor()) as res:
            res.execute("SELECT name FROM sqlite_master WHERE type='table';")
            assert sorted([r[0] for r in res.fetchall()]) == sorted([prefix, *[f"{prefix}__{t}" for t in tc.expected_tables]])

        for table, (cols, values) in tc.expected_values.items():
            with contextlib.closing(conn.cursor()) as res:
                res.execute(f"SELECT {','.join(cols)} FROM {prefix}__{table};")
                for v in values:
                    assert res.fetchone() == v

                assert res.fetchone() is None
