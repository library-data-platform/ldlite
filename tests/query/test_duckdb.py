from unittest import mock
from unittest.mock import MagicMock

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

    with duckdb.connect(":memory:ldlite") as res:
        res.execute("SHOW TABLES;")
        assert sorted([r[0] for r in res.fetchall()]) == ["banana", "banana__t", "banana__tcatalog"]

        res.execute("SELECT id, maple FROM banana__t;")
        assert res.fetchone() == (test_id, "syrup")
        assert res.fetchone() is None
