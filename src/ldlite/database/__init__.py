"""A module for implementing ldlite database targets."""

import datetime
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass

from psycopg import sql


class Prefix:
    """Represents the prefix containing the raw data, any expansion, and the catalog.

    Uses the sql module from psycopg for safe escaping of DDL and other
    non-parametrizable SQL. Should be compatible with a wide variety of databases.
    """

    def __init__(self, prefix: str):
        """Initializes a new prefix optionally with a schema."""
        self.schema: str | None = None
        sandt = prefix.split(".")
        if len(sandt) > 2:
            msg = f"Expected one or two identifiers but got {prefix}"
            raise ValueError(msg)

        if len(sandt) == 1:
            (self._prefix,) = sandt
        else:
            (self.schema, self._prefix) = sandt

    def _identifier(self, table: str) -> sql.Identifier:
        if self.schema is None:
            return sql.Identifier(table)
        return sql.Identifier(self.schema, table)

    @property
    def schema_identifier(self) -> sql.Identifier | None:
        """The sql.Identifier of the schema of this prefix if one is present."""
        return None if self.schema is None else sql.Identifier(self.schema)

    @property
    def raw_table_identifier(self) -> sql.Identifier:
        """The sql.Identifier of the raw table for this prefix (including schema)."""
        return self._identifier(self._prefix)

    @property
    def catalog_table_name(self) -> str:
        """The name of the catalog table for this prefix (without schema)."""
        return f"{self._prefix}__tcatalog"

    @property
    def catalog_table_identifier(self) -> sql.Identifier:
        """The sql.Identifier of the catalog for this prefix (including schema)."""
        return self._identifier(self.catalog_table_name)

    @property
    def legacy_jtable_name(self) -> str:
        """The name of the legacy catalog for this prefix (without schema).

        Maintained for backwards compatibility.
        """
        return f"{self._prefix}_jtable"

    @property
    def legacy_jtable_identifier(self) -> sql.Identifier:
        """The sql.Identifier of the legacy catalog for this prefix (including schema).

        Maintained for backwards compatibility.
        """
        return self._identifier(self.legacy_jtable_name)

    @property
    def load_history_key(self) -> str:
        """The unique key for this prefix to be used for the LoadHistory."""
        if self.schema is None:
            return self._prefix

        return self.schema + "." + self._prefix


@dataclass(frozen=True)
class LoadHistory:
    """Represents the statistics and history of a single ldlite operation."""

    table_name: Prefix
    path: str
    query: str | None
    total: int
    download_time: datetime.datetime
    start_time: datetime.datetime
    download_interval: datetime.timedelta
    transform_interval: datetime.timedelta
    index_interval: datetime.timedelta


class Database(ABC):
    """The required interface for LDLite to utilite a database."""

    @abstractmethod
    def drop_prefix(self, prefix: Prefix) -> None:
        """Drops all tables with the given prefix."""

    @abstractmethod
    def drop_raw_table(self, prefix: Prefix) -> None:
        """Drops the raw table for a given prefix.

        This is deprecated and will be removed before release of 4.0.0.
        """

    @abstractmethod
    def drop_extracted_tables(self, prefix: Prefix) -> None:
        """Drops any extracted tables for a given prefix.

        This is deprecated and will be removed before release of 4.0.0.
        """

    @abstractmethod
    def ingest_records(self, prefix: Prefix, records: Iterator[bytes]) -> int:
        """Ingests a stream of records dowloaded from FOLIO to the raw table."""

    @abstractmethod
    def record_history(self, history: LoadHistory) -> None:
        """Records the statistics and history of a single ldlite operation."""
