from dataclasses import astuple, dataclass
from typing import cast

import httpx
import pytest
from httpx_folio.auth import FolioParams
from pytest_cases import parametrize_with_cases


def test_ok_legacy(folio_params: tuple[bool, FolioParams]) -> None:
    from ldlite import LDLite as uut

    ld = uut()
    ld.connect_folio(*astuple(folio_params[1]))
    ld.connect_db()
    ld.query(table="g", path="/groups", query="cql.allRecords=1 sortby id")
    ld.select(table="g__t")


def test_ok_limit(folio_params: tuple[bool, FolioParams]) -> None:
    from ldlite import LDLite as uut

    ld = uut()
    db = ld.connect_db()

    ld.connect_folio(*astuple(folio_params[1]))
    ld.page_size = 2
    ld.query(table="g", path="/groups", query="cql.allRecords=1 sortby id", limit=5)

    db.execute("SELECT COUNT(DISTINCT COLUMNS(*)) FROM g__t;")
    actual = cast("tuple[int]", db.fetchone())[0]
    assert actual == 5


def test_ok_trailing_slash(folio_params: tuple[bool, FolioParams]) -> None:
    if folio_params[0]:
        pytest.skip("Specify an okapi environment with --folio-base-url to run")

    from ldlite import LDLite as uut

    ld = uut()
    params = astuple(folio_params[1])
    ld.connect_folio(*[params[0] + "/", *params[1:]])
    ld.connect_db()
    ld.query(table="g", path="/groups")
    ld.select(table="g__t")


def test_ok(folio_params: tuple[bool, FolioParams]) -> None:
    from ldlite import LDLite as uut

    ld = uut()
    ld.connect_folio(*astuple(folio_params[1]))
    ld.connect_db()
    ld.query(table="g", path="/groups")
    ld.select(table="g__t")


def test_no_connect_folio() -> None:
    from ldlite import LDLite as uut

    ld = uut()
    ld.connect_db()
    with pytest.raises(RuntimeError):
        ld.query(table="g", path="/groups")


def test_no_connect_db() -> None:
    from ldlite import LDLite as uut

    ld = uut()
    ld.connect_folio(
        url="https://folio-etesting-snapshot-kong.ci.folio.org",
        tenant="diku",
        user="diku_admin",
        password="admin",
    )
    with pytest.raises(RuntimeError):
        ld.query(table="g", path="/groups")


@dataclass(frozen=True)
class FolioConnectionCase:
    expected: type[Exception]
    index: int
    value: str


class FolioConnectionCases:
    def case_url(self) -> FolioConnectionCase:
        return FolioConnectionCase(
            expected=httpx.ConnectError,
            index=0,
            value="https://not.folio.fivecolleges.edu",
        )

    def case_tenant(self) -> FolioConnectionCase:
        return FolioConnectionCase(
            expected=httpx.HTTPStatusError,
            index=1,
            value="not a tenant",
        )

    def case_user(self) -> FolioConnectionCase:
        return FolioConnectionCase(
            expected=httpx.HTTPStatusError,
            index=2,
            value="not a user",
        )

    def case_password(self) -> FolioConnectionCase:
        return FolioConnectionCase(
            expected=httpx.HTTPStatusError,
            index=3,
            value="not the password",
        )


@parametrize_with_cases("tc", cases=FolioConnectionCases)
def test_bad_folio_connection(
    folio_params: tuple[bool, FolioParams],
    tc: FolioConnectionCase,
) -> None:
    from ldlite import LDLite as uut

    ld = uut()
    params = astuple(folio_params[1])
    with pytest.raises(tc.expected):
        ld.connect_folio(*[*params[: tc.index], tc.value, *params[tc.index + 1 :]])
