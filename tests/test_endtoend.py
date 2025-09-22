from dataclasses import astuple
from typing import cast

import pytest
from httpx_folio.factories import FolioParams, default_client_factory
from httpx_folio.query import QueryParams, QueryType
from pytest_cases import parametrize


@parametrize(
    tc=[
        # no id column
        (True, "/finance/ledger-rollovers-logs", None),
        # finicky about sorting
        (True, "/notes", "title=Key Permissions"),
        # id descending
        (False, "/invoice/invoices", "vendorId==e0* sortBy id desc"),
        # non id sort
        (False, "/groups", "cql.allRecords=1 sortBy group desc"),
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
    db = ld.connect_db()

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

    db.execute("SELECT COUNT(DISTINCT COLUMNS(*)) FROM test__t;")
    actual = cast("tuple[int]", db.fetchone())[0]

    assert actual == expected


@parametrize(
    srs=[
        "/source-storage/records",
        "/source-storage/stream/records",
        "/source-storage/source-records",
        "/source-storage/stream/source-records",
    ],
)
def test_endtoend_srs(folio_params: tuple[bool, FolioParams], srs: str) -> None:
    from ldlite import LDLite as uut

    ld = uut()
    db = ld.connect_db()

    ld.connect_folio(*astuple(folio_params[1]))
    ld.query(table="test", path=srs, limit=10)

    db.execute("SELECT COUNT(DISTINCT COLUMNS(*)) FROM test__t;")
    actual = cast("tuple[int]", db.fetchone())[0]

    # snapshot a variable number of records
    assert actual >= 1
    if folio_params[0]:
        assert actual <= 10
    else:
        assert actual == 10
