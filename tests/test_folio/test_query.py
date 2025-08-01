from __future__ import annotations

import typing
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pytest_cases import parametrize_with_cases

from ldlite.folio import FolioParams
from tests.test_cases import folio_query_cases as fqc

if typing.TYPE_CHECKING:
    import httpx


@mock.patch("ldlite.folio.httpx.post")
@mock.patch("ldlite.folio.httpx.Client.get")
@parametrize_with_cases("tc", cases=fqc.NonSRSQueryTestCases)
def test_nonsrs(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    tc: fqc.NonSRSQueryTestCase,
) -> None:
    from ldlite.folio import FolioClient as uut

    httpx_post_mock.return_value.cookies.__getitem__.return_value = "token"

    total_mock = MagicMock()
    total_mock.text = '{"key": "", "totalRecords": 100000}'

    values_mock = MagicMock()
    values_mock.text = '{"key": [], "totalRecords": 100000}'

    calls = 0

    def check_calls(*_: str, **kwargs: dict[str, typing.Any]) -> MagicMock:
        assert "params" in kwargs
        q = typing.cast("httpx.QueryParams", kwargs["params"])
        nonlocal calls
        calls += 1
        if calls == 1:
            assert q["limit"] == "1"
            assert q["query"] == tc.expected_total_query
            # erm parameters
            assert q["perPage"] == "1"
            assert q["stats"]
            return total_mock

        if calls == 2:
            assert q["limit"] == str(tc.page_size)
            assert q["query"] == tc.expected_values_query + " sortBy id asc"
            # erm parameters
            assert q["perPage"] == str(tc.page_size)
            assert q["stats"]
            assert q["sort"] == "id;asc"
            assert q["filters"] == tc.expected_values_query
            return values_mock

        pytest.fail("Requested multiple pages of values")

    client_get_mock.side_effect = check_calls

    list(
        uut(FolioParams("", "", "", "")).iterate_records(
            "/literally/anything",
            timeout=60.0,
            retries=0,
            page_size=tc.page_size,
            query=tc.query,
        ),
    )

    assert calls == 2
