def pytest_addoption(parser) -> None:
    parser.addoption("--pg-host", action="store")
