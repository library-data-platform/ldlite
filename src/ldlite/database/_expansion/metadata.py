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
    json_type: Literal["string", "number", "object", "boolean"]
    is_array: bool
    is_uuid: bool
    is_datetime: bool
    is_float: bool

    def __post_init__(self) -> None:
        # Mixed json_type columns (which shouldn't really happen)
        # get STRING_AGG'ed with a pipe delimeter.
        # Fallback to the lowest common denominator in this case.
        if "|" in self.json_type:
            object.__setattr__(self, "json_type", "string")

    @property
    def is_object(self) -> bool:
        return self.json_type == "object"

    @property
    def snake(self) -> str:
        return "".join("_" + c.lower() if c.isupper() else c for c in self.prop).lstrip(
            "_",
        )

    def select_column(
        self,
        json_col: sql.Identifier,
        alias: str,
    ) -> sql.Composed:
        # '$' is a special character that means the root of the json
        # I couldn't figure out how to make the array expansion work
        # without it
        if self.is_array or self.is_object:
            stmt = sql.SQL(
                "{json_col}->{prop} AS {alias}"
                if self.prop != "$"
                else "{json_col} AS {alias}",
            )
        elif self.json_type == "number":
            stmt = sql.SQL(
                "({json_col}->>{prop})::numeric AS {alias}"
                if self.prop != "$"
                else "ldlite_system.jself_string({json_col})::numeric AS {alias}",
            )
        elif self.json_type == "boolean":
            stmt = sql.SQL(
                "NULLIF(NULLIF({json_col}->>{prop}, ''), 'null')::bool AS {alias}"
                if self.prop != "$"
                else "NULLIF(NULLIF("
                "ldlite_system.jself_string({json_col})"
                ", ''), 'null')::bool AS {alias}",
            )
        elif self.json_type == "string" and self.is_uuid:
            stmt = sql.SQL(
                "NULLIF(NULLIF({json_col}->>{prop}, ''), 'null')::uuid AS {alias}"
                if self.prop != "$"
                else "NULLIF(NULLIF("
                "ldlite_system.jself_string({json_col})"
                ", ''), 'null')::uuid AS {alias}",
            )
        else:
            stmt = sql.SQL(
                "NULLIF(NULLIF({json_col}->>{prop}, ''), 'null') AS {alias}"
                if self.prop != "$"
                else "NULLIF(NULLIF("
                "ldlite_system.jself_string({json_col})"
                ", ''), 'null') AS {alias}",
            )

        return stmt.format(
            json_col=json_col,
            prop=self.prop,
            alias=sql.Identifier(alias),
        )
