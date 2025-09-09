from dataclasses import astuple
from typing import cast

import duckdb
import pytest
from httpx_folio.factories import FolioParams, default_client_factory
from httpx_folio.query import QueryParams, QueryType
from pytest_cases import parametrize


@parametrize(
    tc=[
        (True, "/finance/ledger-rollovers-logs", None),  # no id column
        (True, "/notes", "title=Key Permissions"),  # finicky about sorting
        (
            False,
            "/loan-policy-storage/loan-policies",
            "cql.allRecords=1 sortBy id/sort.descending",
        ),
        (False, "/groups", "cql.allRecords=1 sortBy group desc"),  # non id sort
    ],
)
def test_endtoend(
    folio_params: tuple[bool, FolioParams],
    tc: tuple[bool, str, QueryType],
) -> None:
    (non_snapshot_data, endpoint, query) = tc
    if non_snapshot_data and folio_params[0]:
        pytest.skip(
            "Specify an environment having data with --folio-base-url to run",
        )

    from ldlite import LDLite as uut

    ld = uut()
    ld.connect_db(":memory:shared")

    ld.page_size = 3
    ld.connect_folio(*astuple(folio_params[1]))
    ld.query(table="test", path=endpoint, query=query)  # type:ignore[arg-type]

    with default_client_factory(folio_params[1])() as client:
        res = client.get(
            endpoint,
            params=QueryParams(query).stats(),
        )
        res.raise_for_status()

        expected = res.json()["totalRecords"]
        assert expected > 3

    db = duckdb.connect(":memory:shared")
    db.execute("SELECT COUNT(DISTINCT COLUMNS(*)) FROM test__t;")
    actual = cast("tuple[int]", db.fetchone())[0]

    assert actual == expected
