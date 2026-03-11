from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from psycopg import sql


@dataclass
class Metadata:
    prop: str | None
    # array is technically a type
    # but the metadata query returns inner type of the array
    # the jtype_of function normalizes the names between pg and duckdb
    json_type: Literal["string", "number", "object", "boolean"]
    is_array: bool
    is_uuid: bool
    is_datetime: bool
    is_float: bool
    is_bigint: bool

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
        if self.prop is None:
            # this doesn't really come up in practice
            return "$"

        snake = "".join("_" + c.lower() if c.isupper() else c for c in self.prop)

        # there's also sorts of weird edge cases here that don't come up in practice
        if (naked := self.prop.lstrip("_")) and len(naked) > 0 and naked[0].isupper():
            snake = snake.removeprefix("_")

        return snake

    def select_column(
        self,
        json_col: sql.Identifier,
        alias: str,
    ) -> sql.Composed:
        str_extract = (
            "{json_col}->>{prop}"
            if self.prop is not None
            else "ldlite_system.jself_string({json_col})"
        )
        nullable_str_extract = f"NULLIF(NULLIF({str_extract}, ''), 'null')"

        if self.is_array or self.is_object:
            stmt = "{json_col}" if self.prop is None else "{json_col}->{prop}"
        elif self.json_type == "number" and self.is_float:
            stmt = f"({str_extract})::numeric"
        elif self.json_type == "number" and self.is_bigint:
            stmt = f"({str_extract})::bigint"
        elif self.json_type == "number":
            stmt = f"({str_extract})::integer"
        elif self.json_type == "boolean":
            stmt = f"({nullable_str_extract})::bool"
        elif self.json_type == "string" and self.is_uuid:
            stmt = f"({nullable_str_extract})::uuid"
        elif self.json_type == "string" and self.is_datetime:
            stmt = f"({nullable_str_extract})::timestamptz"
        else:
            stmt = nullable_str_extract

        return sql.SQL(stmt + " AS {alias}").format(
            json_col=json_col,
            prop=self.prop,
            alias=sql.Identifier(alias),
        )
