from uuid import uuid4
from unittest import mock
from unittest.mock import MagicMock
from typing import Union
import contextlib

import pytest

@pytest.fixture(scope="session")
def pg_dsn(pytestconfig) -> Union[None, str]:
    host =  pytestconfig.getoption("pg_host")
    if host is None:
        return None

    import psycopg2
    base_dsn = f"host={host} user=ldlite password=ldlite"
    db = "db_" + str(uuid4()).split("-")[0]
    print(db)
    with contextlib.closing(psycopg2.connect(base_dsn)) as base_conn:
        base_conn.autocommit = True
        with base_conn.cursor() as curr:
            curr.execute(f"CREATE DATABASE {db};")

    return base_dsn + f" dbname={db}"

@mock.patch("ldlite._request_get")
def test_one_table_postgres(_request_get_mock: MagicMock, pg_dsn: Union[None, str]) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    import psycopg2
    from ldlite import LDLite as uut

    ld = uut()
    ld.connect_db_postgresql(pg_dsn)

    # _check_okapi() hack
    ld.login_token = "token"
    ld.okapi_url = "url"
    # leave tqdm out of it
    ld.quiet(enable=True)

    total_mock = MagicMock()
    total_mock.status_code = 200
    # the total number of records is only used for TQDM
    total_mock.json.return_value = {}

    value_mock = MagicMock()
    value_mock.status_code = 200
    test_id = "b096504a-3d54-4664-9bf5-1b872466fd66"
    value_mock.json.return_value = {
        "purchaseOrders": [
            {
                "id": test_id,
                "maple": "syrup",
            }
        ]
    }

    end_mock = MagicMock()
    end_mock.status_code = 200
    end_mock.json.return_value = { "purchaseOrders": [] }

    _request_get_mock.side_effect = [total_mock, value_mock, end_mock]
    # we're not testing the endpoint behavior so path doesn't matter
    ld.query(table="banana", path="/pancakes")

    with psycopg2.connect(pg_dsn) as conn:
        with conn.cursor() as res:
            res.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            assert sorted([r[0] for r in res.fetchall()]) == ["banana", "banana__t", "banana__tcatalog"]

            res.execute("SELECT id, maple FROM banana__t;")
            assert res.fetchone() == (test_id, "syrup")
            assert res.fetchone() is None



