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
    def output_table(self) -> str:
        return self._prefix + "__t"

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

    def transform_table(self, depth: int, breadth: int) -> str:
        return (
            ("" if self.schema is None else self.schema + "_")
            + self.output_table
            + f"_d{depth}_b{breadth}"
        )
