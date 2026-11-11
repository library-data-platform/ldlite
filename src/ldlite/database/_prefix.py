from itertools import cycle
from typing import NamedTuple

from psycopg import sql


class PrefixedTable(NamedTuple):
    name: str
    id: sql.Identifier


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

    def _prefixed_table(self, name: str) -> PrefixedTable:
        if self.schema is None:
            return PrefixedTable(name, sql.Identifier(name))
        return PrefixedTable(name, sql.Identifier(self.schema, name))

    def __str__(self) -> str:
        return str((self.schema, self._prefix))

    @property
    def raw_table(self) -> PrefixedTable:
        return self._prefixed_table(self._prefix)

    @property
    def _output_table(self) -> str:
        return self._prefix + "__t"

    def _safe_output_table(self, path: str | None) -> tuple[str, str]:
        full_name = self._output_table + ("" if path is None else "__" + path)

        if len(base := self._output_table) > 63:
            msg = (
                f"Table name ({full_name}) is longer than 63 characters "
                "and cannot be shortened"
            )
            raise ValueError(msg)

        if path is None:
            return (full_name, full_name)

        parts = path.split("__")
        # The intermediate values can only be shortened to a single character
        # If the smallest possible size is still to big we have to bail
        minimum_possible = len(base + "__" + parts[-1]) + (len(parts[:-1]))
        if minimum_possible > 63:
            msg = (
                f"Table name ({full_name}) is longer than 63 characters "
                "and cannot be shortened"
            )
            raise ValueError(msg)

        shaveable_indices = cycle(range(len(parts) - 1))
        while len(safe_name := base + "__" + "__".join(parts)) > 63:
            s = next(shaveable_indices)
            if len(parts[s]) > 1:
                parts[s] = parts[s][:-1].strip("_")

        return (safe_name, full_name)

    def output_table(self, path: str | None) -> PrefixedTable:
        return self._prefixed_table(self._safe_output_table(path)[0])

    @property
    def catalog_table(self) -> PrefixedTable:
        return self._prefixed_table(self._prefix + "__tcatalog")

    def catalog_table_row(self, path: str | None) -> tuple[str, str]:
        schema = (self.schema + ".") if self.schema is not None else ""
        (actual_name, full_name) = self._safe_output_table(path)
        return (schema + actual_name, schema + full_name)

    @property
    def legacy_jtable(self) -> PrefixedTable:
        return self._prefixed_table(self._prefix + "_jtable")

    @property
    def load_history_key(self) -> str:
        if self.schema is None:
            return self._prefix

        return self.schema + "." + self._prefix

    @property
    def origin_table(self) -> sql.Identifier:
        return sql.Identifier(
            ("" if self.schema is None else self.schema + "_")
            + self._output_table
            + "_origin",
        )

    def transform_table(self, count: int) -> sql.Identifier:
        return sql.Identifier(
            ("" if self.schema is None else self.schema + "_")
            + self._output_table
            + f"_{count}",
        )
