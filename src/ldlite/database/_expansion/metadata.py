from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from psycopg import sql


@dataclass
class Metadata:
    prop: str
    # array is technically a type
    # but the metadata query returns inner type of the array
    # the jtype_of function normalizes the names between pg and duckdb
    json_type: Literal["string", "number", "object", "boolean", "null"]
    is_array: bool
    is_uuid: bool
    is_datetime: bool
    is_float: bool

    @property
    def snake(self) -> str:
        return "".join("_" + c.lower() if c.isupper() else c for c in self.prop)

    def select_column(
        self,
        json_col: sql.Identifier,
        alias: str,
    ) -> sql.Composed:
        if self.is_array or self.json_type == "object":
            stmt = sql.SQL(
                "(ldlite_system.jextract({json_col}, {prop})) AS {alias}",
            )
        elif self.json_type == "number":
            stmt = sql.SQL(
                "(ldlite_system.jextract({json_col}, {prop}))::numeric AS {alias}",
            )
        elif self.json_type == "string" and self.is_uuid:
            stmt = sql.SQL(
                "(ldlite_system.jextract_string({json_col}, {prop}))::uuid AS {alias}",
            )
        else:
            stmt = sql.SQL(
                "(ldlite_system.jextract_string({json_col}, {prop})) AS {alias}",
            )

        return stmt.format(
            json_col=json_col,
            prop=self.prop,
            alias=sql.Identifier(alias),
        )
