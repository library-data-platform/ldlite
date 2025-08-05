import json

import pytest

from ldlite.folio import FolioParams


def test_ok(folio_params: tuple[bool, FolioParams]) -> None:
    from ldlite.folio import FolioClient as uut

    res = uut(folio_params[1]).iterate_records(
        "/groups",
        timeout=60.0,
        retries=0,
        page_size=10000,
    )

    (total, _) = next(res)
    assert total > 0

    read = 0
    prev_pkey = 0
    prev_id = "00000000-0000-0000-0000-000000000000"
    for pkey, j in res:
        assert pkey > prev_pkey

        this_id = json.loads(j)["id"]
        assert this_id > prev_id
        prev_id = this_id

        read += 1

    assert total == read


def test_multiple_pages(folio_params: tuple[bool, FolioParams]) -> None:
    from ldlite.folio import FolioClient as uut

    res = uut(folio_params[1]).iterate_records(
        "/groups",
        timeout=60.0,
        retries=0,
        page_size=1,
    )
    next(res)

    read = 0
    for _ in res:
        read += 1

    assert read > 0


def test_erm(folio_params: tuple[bool, FolioParams]) -> None:
    from ldlite.folio import FolioClient as uut

    res = uut(folio_params[1]).iterate_records(
        "/erm/org",
        timeout=60.0,
        retries=0,
        page_size=5,
    )

    (total, _) = next(res)
    assert total > 0

    read = 0
    prev_pkey = 0
    prev_id = "00000000-0000-0000-0000-000000000000"
    for pkey, j in res:
        assert pkey > prev_pkey

        this_id = json.loads(j)["id"]
        assert this_id > prev_id
        prev_id = this_id

        read += 1

    assert total == read


def test_srs(folio_params: tuple[bool, FolioParams]) -> None:
    if folio_params[0]:
        pytest.skip("Specify an environment with --folio-base-url to run")

    from ldlite.folio import FolioClient as uut

    res = uut(folio_params[1]).iterate_records(
        "/source-storage/source-records",
        timeout=60.0,
        retries=0,
        page_size=15,
    )
    (total, _) = next(res)
    assert total > 0

    prev = "0"
    read = 0
    for r in res:
        record_id = json.loads(r[1])["recordId"]
        assert record_id > prev
        prev = record_id

        read += 1
        if read >= 1000:
            break

    assert read > 0
