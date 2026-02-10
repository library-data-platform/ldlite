import datetime
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass

from psycopg import sql


class Prefix:
    def __init__(self, prefix: str):
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
        return None if self.schema is None else sql.Identifier(self.schema)

    @property
    def raw_table_identifier(self) -> sql.Identifier:
        return self._identifier(self._prefix)

    @property
    def catalog_table_name(self) -> str:
        return f"{self._prefix}__tcatalog"

    @property
    def catalog_table_identifier(self) -> sql.Identifier:
        return self._identifier(self.catalog_table_name)

    @property
    def legacy_jtable_name(self) -> str:
        return f"{self._prefix}_jtable"

    @property
    def legacy_jtable_identifier(self) -> sql.Identifier:
        return self._identifier(self.legacy_jtable_name)

    @property
    def load_history_key(self) -> str:
        if self.schema is None:
            return self._prefix

        return self.schema + "." + self._prefix


@dataclass(frozen=True)
class LoadHistory:
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
    @abstractmethod
    def drop_prefix(self, prefix: Prefix) -> None: ...

    @abstractmethod
    def drop_raw_table(self, prefix: Prefix) -> None: ...

    @abstractmethod
    def drop_extracted_tables(self, prefix: Prefix) -> None: ...

    @abstractmethod
    def ingest_records(self, prefix: Prefix, records: Iterator[bytes]) -> int: ...

    @abstractmethod
    def record_history(self, history: LoadHistory) -> None: ...
