"""A module for implementing ldlite database targets."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, NoReturn

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tqdm import tqdm


class Database(ABC):
    """The required interface for LDLite to utilite a database."""

    @abstractmethod
    def drop_prefix(self, prefix: str) -> None:
        """Drops all tables with the given prefix."""

    @abstractmethod
    def drop_raw_table(self, prefix: str) -> None:
        """Drops the raw table for a given prefix.

        This is deprecated and will be removed in a future release.
        """

    @abstractmethod
    def drop_extracted_tables(self, prefix: str) -> None:
        """Drops any extracted tables for a given prefix.

        This is deprecated and will be removed in a future release.
        """

    @abstractmethod
    def ingest_records(self, prefix: str, records: Iterator[bytes]) -> int:
        """Ingests a stream of records dowloaded from FOLIO to the raw table."""

    @abstractmethod
    def expand_prefix(
        self,
        prefix: str,
        json_depth: int,
        keep_raw: bool,
        scan_progress: tqdm[NoReturn] | None = None,
        transform_progress: tqdm[NoReturn] | None = None,
    ) -> list[str]:
        """Unnests and explodes the raw data at the given prefix."""

    @abstractmethod
    def index_prefix(self, prefix: str, progress: tqdm[NoReturn] | None = None) -> None:
        """Finds and indexes all tables at the given prefix."""

    @abstractmethod
    def prepare_history(self, prefix: str, path: str, query: str | None) -> None:
        """Creates an entry with the current parameters in the history table."""
