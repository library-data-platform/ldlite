import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--pg-host", action="store")
