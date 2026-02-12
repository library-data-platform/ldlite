"""A module for implementing ldlite database targets."""

import datetime
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass


@dataclass(frozen=True)
class LoadHistory:
    """Represents the statistics and history of a single ldlite operation."""

    table_name: str
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
    def drop_prefix(self, prefix: str) -> None:
        """Drops all tables with the given prefix."""

    @abstractmethod
    def drop_raw_table(self, prefix: str) -> None:
        """Drops the raw table for a given prefix.

        This is deprecated and will be removed before release of 4.0.0.
        """

    @abstractmethod
    def drop_extracted_tables(self, prefix: str) -> None:
        """Drops any extracted tables for a given prefix.

        This is deprecated and will be removed before release of 4.0.0.
        """

    @abstractmethod
    def ingest_records(self, prefix: str, records: Iterator[bytes]) -> int:
        """Ingests a stream of records dowloaded from FOLIO to the raw table."""

    @abstractmethod
    def expand_prefix(self, prefix: str, json_depth: int, keep_raw: bool) -> None:
        """Unnests and explodes the raw data at the given prefix."""

    @abstractmethod
    def record_history(self, history: LoadHistory) -> None:
        """Records the statistics and history of a single ldlite operation."""
