from unittest import mock
from unittest.mock import MagicMock

import duckdb
from pytest_cases import parametrize_with_cases

from .drop_tables_cases import DropTablesCase, DropTablesCases


@mock.patch("ldlite.request_get")
@parametrize_with_cases("tc", cases=DropTablesCases)
def test_duckdb(request_get_mock: MagicMock, tc: DropTablesCase) -> None:
    from ldlite import LDLite as uut

    dsn = f":memory:{tc.db}"
    tc.patch_request_get(request_get_mock)

    ld = uut()

    # _check_okapi() hack
    ld.login_token = "token"
    ld.okapi_url = "url"
    # leave tqdm out of it
    ld.quiet(enable=True)

    ld.connect_db(dsn)
    # we're not testing the endpoint behavior so path doesn't matter
    for prefix in tc.values:
        ld.query(table=prefix, path="/pancakes")
    ld.drop_tables(tc.drop)

    with duckdb.connect(dsn) as res:
        res.execute("SHOW TABLES;")
        assert sorted([r[0] for r in res.fetchall()]) == sorted(tc.expected_tables)
