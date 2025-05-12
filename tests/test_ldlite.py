import sqlite3
from unittest import mock
from unittest.mock import MagicMock

@mock.patch("ldlite._request_get")
def test_one_table_sqlite(_request_get_mock: MagicMock) -> None:
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


