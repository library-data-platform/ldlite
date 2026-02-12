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
    def raw_table_name(self) -> str:
        return self._prefix

    @property
    def raw_table_identifier(self) -> sql.Identifier:
        return self._identifier(self._prefix)

    @property
    def temp_expansion_table_name(self) -> str:
        return self._prefix + "__t_0"

    @property
    def expansion_table_identifier(self) -> sql.Identifier:
        return self._identifier(self._prefix + "__t")

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
