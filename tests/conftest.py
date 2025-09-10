import pytest
from httpx_folio.auth import FolioParams


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--pg-host", action="store")
    parser.addoption("--folio-base-url", action="store")
    parser.addoption("--folio-tenant", action="store")
    parser.addoption("--folio-username", action="store")
    parser.addoption("--folio-password", action="store")


@pytest.fixture(scope="session")
def folio_params(pytestconfig: pytest.Config) -> tuple[bool, FolioParams]:
    base_url = pytestconfig.getoption("folio_base_url")
    default = base_url is None
    return (
        default,
        FolioParams(
            pytestconfig.getoption("folio_base_url")
            or "https://folio-etesting-snapshot-kong.ci.folio.org",
            pytestconfig.getoption("folio_tenant") or "diku",
            pytestconfig.getoption("folio_username") or "diku_admin",
            pytestconfig.getoption("folio_password") or "admin",
        ),
    )
