from collections.abc import Callable
from dataclasses import dataclass
from difflib import unified_diff
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pytest_cases import parametrize_with_cases

from .mock_response_case import Call, MockedResponseTestCase

if TYPE_CHECKING:
    import ldlite

_SAMPLE_PATH = Path() / "tests" / "export_csv_samples"


@dataclass(frozen=True)
class ExportCsvTC(MockedResponseTestCase):
    expected_csvs: list[tuple[str, Path]]


def case_basic() -> ExportCsvTC:
    return ExportCsvTC(
        Call("prefix", returns={"purchaseOrders": [{"id": "id", "val": "value"}]}),
        expected_csvs=[("prefix__t", _SAMPLE_PATH / "basic.csv")],
    )


def case_datatypes() -> ExportCsvTC:
    return ExportCsvTC(
        Call(
            "prefix",
            returns={
                "purchaseOrders": [
                    {
                        "id": "id",
                        "string": "string",
                        "integer": 1,
                        "numeric": 1.1,
                        "boolean": True,
                        "uuid": "6a31a12a-9570-405c-af20-6abf2992859c",
                    },
                ],
            },
        ),
        expected_csvs=[("prefix__t", _SAMPLE_PATH / "datatypes.csv")],
    )


def case_escaped_chars() -> ExportCsvTC:
    return ExportCsvTC(
        Call(
            "prefix",
            returns={
                "purchaseOrders": [
                    {
                        "id": "id",
                        "comma": "Double, double toil and trouble",
                        "doubleQuote": 'Cry "Havoc!" a horse',
                        "newLine": """To be
or not
to be""",
                        "singleQuote": "Cry 'Havoc!' a horse",
                    },
                    {
                        "id": "id",
                        "comma": "Z",
                        "doubleQuote": "Z",
                        "newLine": "Z",
                        "singleQuote": "Z",
                    },
                ],
            },
        ),
        expected_csvs=[("prefix__t", _SAMPLE_PATH / "escaped_chars.csv")],
    )


def case_sorting() -> ExportCsvTC:
    return ExportCsvTC(
        Call(
            "prefix",
            returns={
                "purchaseOrders": [
                    {"id": "id", "C": "YY", "B": "XX", "A": "ZZ"},
                    {"id": "id", "C": "Y", "B": "XX", "A": "ZZ"},
                    {"id": "id", "C": "Y", "B": "X", "A": "Z"},
                ],
            },
        ),
        expected_csvs=[("prefix__t", _SAMPLE_PATH / "sorting.csv")],
    )


def _arrange(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    tc: ExportCsvTC,
) -> "ldlite.LDLite":
    from ldlite import LDLite

    uut = LDLite()
    tc.patch_request_get(uut, httpx_post_mock, client_get_mock)
    uut.connect_folio("https://doesnt.matter", "", "", "")
    return uut


def _act(uut: "ldlite.LDLite", tc: ExportCsvTC) -> None:
    for call in tc.calls_list:
        uut.query(table=call.prefix, path="/patched")


def _assert(
    uut: "ldlite.LDLite",
    tc: ExportCsvTC,
    tmpdir: str,
) -> None:
    for table, expected in tc.expected_csvs:
        actual = (Path(tmpdir) / table).with_suffix(".csv")

        uut.export_csv(str(actual), table)

        with expected.open("r") as f:
            expected_lines = f.readlines()
        with actual.open("r") as f:
            actual_lines = f.readlines()

        diff = list(unified_diff(expected_lines, actual_lines))
        if len(diff) > 0:
            pytest.fail("".join(diff))


@mock.patch("httpx_folio.auth.httpx.post")
@mock.patch("httpx_folio.factories.httpx.Client.get")
@parametrize_with_cases("tc", cases=".")
def test_duckdb(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    tc: ExportCsvTC,
    tmpdir: str,
) -> None:
    uut = _arrange(client_get_mock, httpx_post_mock, tc)
    dsn = f":memory:{tc.db}"
    uut.connect_db(dsn)

    _act(uut, tc)
    _assert(uut, tc, tmpdir)


@mock.patch("httpx_folio.auth.httpx.post")
@mock.patch("httpx_folio.factories.httpx.Client.get")
@parametrize_with_cases("tc", cases=".")
def test_postgres(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    pg_dsn: None | Callable[[str], str],
    tc: ExportCsvTC,
    tmpdir: str,
) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    uut = _arrange(client_get_mock, httpx_post_mock, tc)
    dsn = pg_dsn(tc.db)
    uut.connect_db_postgresql(dsn)

    _act(uut, tc)
    _assert(uut, tc, tmpdir)
