from __future__ import annotations

import typing
from unittest import mock
from unittest.mock import MagicMock

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

    mocks = []
    total_mock = MagicMock()
    total_mock.text = '{"key": [{"id": ""}], "totalRecords": 100000}'
    mocks.append(total_mock)

    values_mock = MagicMock()
    values_mock.text = '{"key": [], "totalRecords": 100000}'
    mocks.append(values_mock)

    calls: list[httpx.QueryParams] = []

    def check_calls(*_: str, **kwargs: dict[str, typing.Any]) -> MagicMock:
        assert "params" in kwargs
        calls.append(typing.cast("httpx.QueryParams", kwargs["params"]))
        return mocks.pop(0)

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

    assert len(calls) == 2

    assert calls[0]["limit"] == "1"
    assert calls[0]["query"] == tc.expected_total_query
    # erm parameters
    assert calls[0]["perPage"] == "1"
    assert calls[0]["stats"]
    for k, v in tc.expected_additional_params.items():
        assert k in calls[0]
        assert calls[0][k] == v

    assert calls[1]["limit"] == str(tc.page_size)
    assert calls[1]["query"] == tc.expected_values_query + " sortBy id"
    # erm parameters
    assert calls[1]["perPage"] == str(tc.page_size)
    assert calls[1]["stats"]
    assert calls[1]["sort"] == "id;asc"
    assert calls[1]["filters"] == tc.expected_filters_query
    for k, v in tc.expected_additional_params.items():
        assert k in calls[1]
        assert calls[1][k] == v


@mock.patch("ldlite.folio.httpx.post")
@mock.patch("ldlite.folio.httpx.Client.get")
@mock.patch("ldlite.folio.httpx.Client.stream")
def test_srs(
    client_stream_mock: MagicMock,
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
) -> None:
    from ldlite.folio import FolioClient as uut

    httpx_post_mock.return_value.cookies.__getitem__.return_value = "token"

    mocks = []
    total_mock = MagicMock()
    total_mock.text = '{"key": "", "totalRecords": 100000}'
    mocks.append(total_mock)

    values_mock = MagicMock()
    values_mock.iter_lines = ['{"key": [], "totalRecords": 100000}']
    mocks.append(values_mock)

    calls: list[httpx.QueryParams] = []

    def check_calls(*_: str, **kwargs: dict[str, typing.Any]) -> MagicMock:
        assert "params" in kwargs
        calls.append(typing.cast("httpx.QueryParams", kwargs["params"]))
        return mocks.pop(0)

    client_get_mock.side_effect = check_calls
    client_stream_mock.side_effect = check_calls

    list(
        uut(FolioParams("", "", "", "")).iterate_records(
            "/source-storage/then/literally/anything",
            timeout=60.0,
            retries=0,
            page_size=100,
            query={"state": "old"},
        ),
    )

    assert len(calls) == 2

    assert calls[0]["limit"] == "1"
    # erm parameters
    assert "state" in calls[0]
    assert calls[0]["state"] == "old"

    assert int(calls[1]["limit"]) > 10_000_000
    # erm parameters
    assert "state" in calls[1]
    assert calls[1]["state"] == "old"
