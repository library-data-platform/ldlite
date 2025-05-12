from uuid import uuid4
from unittest import mock
from unittest.mock import MagicMock
from typing import Union

import pytest

@pytest.fixture(scope="session")
def postgres(pytestconfig) -> Union[None, str]:
    host =  pytestconfig.getoption("pg_host")
    if host is None:
        return None

    import psycopg2
    base_dsn = f"host={host} user=ldlite password=ldlite"
    db = str(uuid4()).split("-")[0]
    print(db)
    with psycopg2.connect(base_dsn) as base_conn:
        base_conn.set_session(autocommit=True)
        curr = base_conn.cursor()
        curr.execute(f"CREATE DATABASE {db}")

    return base_dsn + f" dbname={db}"

@mock.patch("ldlite._request_get")
def test_one_table_sqlite(_request_get_mock: MagicMock) -> None:
    import sqlite3
    from ldlite import LDLite as uut

    ld = uut()
    ld.experimental_connect_db_sqlite("file:ldlite?mode=memory&cache=shared")

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

    res = sqlite3.connect("file:ldlite?mode=memory&cache=shared")
    cur = res.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    assert sorted([r[0] for r in cur.fetchall()]) == ["banana", "banana__t", "banana__tcatalog"]

    cur.execute("SELECT id, maple FROM banana__t;")
    assert cur.fetchone() == (test_id, "syrup")
    assert cur.fetchone() is None



@mock.patch("ldlite._request_get")
def test_one_table_duckdb(_request_get_mock: MagicMock) -> None:
    import duckdb
    from ldlite import LDLite as uut

    ld = uut()
    ld.connect_db(":memory:ldlite")

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

    res = duckdb.connect(":memory:ldlite")
    res.execute("SHOW TABLES;")
    assert sorted([r[0] for r in res.fetchall()]) == ["banana", "banana__t", "banana__tcatalog"]

    res.execute("SELECT id, maple FROM banana__t;")
    assert res.fetchone() == (test_id, "syrup")
    assert res.fetchone() is None


