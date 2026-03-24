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

    @property
    def raw_table(self) -> PrefixedTable:
        return self._prefixed_table(self._prefix)

    @property
    def _output_table(self) -> str:
        return self._prefix + "__t"

    def output_table(self, prefix: str | None) -> PrefixedTable:
        return self._prefixed_table(
            self._output_table + ("" if prefix is None else "__" + prefix),
        )

    @property
    def catalog_table(self) -> PrefixedTable:
        return self._prefixed_table(self._prefix + "__tcatalog")

    def catalog_table_row(self, created_table: str) -> str:
        return ((self.schema + ".") if self.schema is not None else "") + created_table

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
