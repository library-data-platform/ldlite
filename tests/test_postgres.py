from collections.abc import Callable
from difflib import unified_diff
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pytest_cases import parametrize_with_cases

from tests.test_cases import to_csv_cases as csvc


@mock.patch("httpx_folio.auth.httpx.post")
@mock.patch("httpx_folio.factories.httpx.Client.get")
@parametrize_with_cases("tc", cases=csvc.ToCsvCases)
def test_to_csv(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    pg_dsn: None | Callable[[str], str],
    tc: csvc.ToCsvCase,
    tmpdir: str,
) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    from ldlite import LDLite as uut

    ld = uut()
    tc.patch_request_get(ld, httpx_post_mock, client_get_mock)
    dsn = pg_dsn(tc.db)
    ld.connect_folio("https://doesnt.matter", "", "", "")
    ld.connect_db_postgresql(dsn)

    for call in tc.calls_list:
        ld.query(table=call.prefix, path="/patched")

    for table, expected in tc.expected_csvs:
        actual = (Path(tmpdir) / table).with_suffix(".csv")

        ld.export_csv(str(actual), table)

        with expected.open("r") as f:
            expected_lines = f.readlines()
        with actual.open("r") as f:
            actual_lines = f.readlines()

        diff = list(unified_diff(expected_lines, actual_lines))
        if len(diff) > 0:
            pytest.fail("".join(diff))
