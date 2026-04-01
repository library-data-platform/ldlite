import contextlib
from collections.abc import Callable

import psycopg
import pytest
from httpx_folio.auth import FolioParams
from psycopg import sql


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


@pytest.fixture(scope="session")
def pg_dsn(pytestconfig: pytest.Config) -> None | Callable[[str], str]:
    host = pytestconfig.getoption("pg_host")
    if host is None:
        return None

    def setup(db: str) -> str:
        base_dsn = f"host={host} user=ldlite password=ldlite"
        with contextlib.closing(psycopg.connect(base_dsn)) as base_conn:
            base_conn.autocommit = True
            with base_conn.cursor() as curr:
                curr.execute(
                    sql.SQL("CREATE DATABASE {db};").format(db=sql.Identifier(db)),
                )

        return base_dsn + f" dbname={db}"

    return setup
