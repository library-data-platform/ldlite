from dataclasses import astuple, dataclass

import pytest
from pytest_cases import parametrize_with_cases
from requests import exceptions


def test_ok() -> None:
    from ldlite import LDLite as uut

    ld = uut()
    ld.connect_folio(
        url="https://folio-etesting-snapshot-kong.ci.folio.org",
        tenant="diku",
        user="diku_admin",
        password="admin",
    )
    ld.connect_db()
    ld.query(table="g", path="/groups", query="cql.allRecords=1 sortby id")
    ld.select(table="g__t")


def test_no_connect_folio() -> None:
    from ldlite import LDLite as uut

    ld = uut()
    ld.connect_db()
    with pytest.raises(RuntimeError):
        ld.query(table="g", path="/groups", query="cql.allRecords=1 sortby id")


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
        ld.query(table="g", path="/groups", query="cql.allRecords=1 sortby id")


@dataclass(frozen=True)
class FolioConnectionCase:
    expected: type[Exception]
    url: str = "https://folio-etesting-snapshot-kong.ci.folio.org"
    tenant: str = "diku"
    user: str = "diku_admin"
    password: str = "admin"


class FolioConnectionCases:
    def case_url(self) -> FolioConnectionCase:
        return FolioConnectionCase(
            expected=exceptions.RequestException,
            url="https://not.folio.fivecolleges.edu",
        )

    def case_tenant(self) -> FolioConnectionCase:
        return FolioConnectionCase(
            expected=RuntimeError,
            tenant="not a tenant",
        )

    def case_user(self) -> FolioConnectionCase:
        return FolioConnectionCase(
            expected=RuntimeError,
            user="not a user",
        )

    def case_password(self) -> FolioConnectionCase:
        return FolioConnectionCase(
            expected=RuntimeError,
            password="",
        )


@parametrize_with_cases("tc", cases=FolioConnectionCases)
def test_bad_folio_connection(tc: FolioConnectionCase) -> None:
    from ldlite import LDLite as uut

    ld = uut()
    with pytest.raises(tc.expected):
        ld.connect_folio(*astuple(tc)[1:])
