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

    def schemafy(self, table: str) -> sql.Identifier:
        if self.schema is None:
            return sql.Identifier(table)
        return sql.Identifier(self.schema, table)

    @property
    def raw_table(self) -> str:
        return self._prefix

    @property
    def _output_table(self) -> str:
        return self._prefix + "__t"

    def output_table(self, prefix: str) -> sql.Identifier:
        if len(prefix) == 0:
            return self.schemafy(self._output_table)

        return self.schemafy(self._output_table + "__" + prefix)

    @property
    def catalog_table(self) -> str:
        return f"{self._prefix}__tcatalog"

    @property
    def legacy_jtable(self) -> str:
        return f"{self._prefix}_jtable"

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
