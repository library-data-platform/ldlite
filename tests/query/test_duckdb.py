from unittest import mock
from unittest.mock import MagicMock

import duckdb
from pytest_cases import parametrize_with_cases

from .expansion_cases import QueryCase, QueryTestCases


@mock.patch("ldlite.request_get")
@parametrize_with_cases("tc", cases=QueryTestCases)
def test_duckdb(request_get_mock: MagicMock, tc: QueryCase) -> None:
    from ldlite import LDLite as uut

    dsn = f":memory:{tc.db}"
    tc.patch_request_get(request_get_mock)

    ld = uut()

    # _check_okapi() hack
    ld.login_token = "token"
    ld.okapi_url = "url"
    # leave tqdm out of it
    ld.quiet(enable=True)

    prefix = "prefix"
    ld.connect_db(dsn)
    # we're not testing the endpoint behavior so path doesn't matter
    ld.query(table=prefix, path="/pancakes", json_depth=tc.json_depth)

    catalog = f"{prefix}__tcatalog"
    fixed_cols = [prefix]
    # TODO: Clean this edge case up so tcatalog is always created
    if tc.json_depth > 0:
        fixed_cols.append(catalog)

    with duckdb.connect(dsn) as res:
        res.execute("SHOW TABLES;")
        assert sorted([r[0] for r in res.fetchall()]) == sorted(
            [*fixed_cols, *[f"{prefix}__{t}" for t in tc.expected_tables]]
        )

    if tc.json_depth > 0:
        with duckdb.connect(dsn) as res:
            res.execute(f"SELECT table_name FROM {catalog};")
            assert sorted([r[0] for r in res.fetchall()]) == sorted(
                [f"{prefix}__{t}" for t in tc.expected_tables]
            )

    for table, (cols, values) in tc.expected_values.items():
        with duckdb.connect(dsn) as res:
            res.execute(f"SELECT {','.join(cols)} FROM {prefix}__{table};")
            for v in values:
                assert res.fetchone() == v

            assert res.fetchone() is None
