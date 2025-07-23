import json


def test_ok() -> None:
    from ldlite.folio import FolioClient as uut
    from ldlite.folio import FolioParams

    ld = uut(
        FolioParams(
            "https://folio-etesting-snapshot-kong.ci.folio.org",
            "diku",
            "diku_admin",
            "admin",
        ),
    )

    res = ld.iterate_records(
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


def test_multiple_pages() -> None:
    from ldlite.folio import FolioClient as uut
    from ldlite.folio import FolioParams

    ld = uut(
        FolioParams(
            "https://folio-etesting-snapshot-kong.ci.folio.org",
            "diku",
            "diku_admin",
            "admin",
        ),
    )

    res = ld.iterate_records(
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


def test_erm() -> None:
    from ldlite.folio import FolioClient as uut
    from ldlite.folio import FolioParams

    ld = uut(
        FolioParams(
            "https://folio-etesting-snapshot-kong.ci.folio.org",
            "diku",
            "diku_admin",
            "admin",
        ),
    )

    res = ld.iterate_records(
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
