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
            # this doesn't realy come up in practice
            return "$"

        return "".join("_" + c.lower() if c.isupper() else c for c in self.prop).lstrip(
            "_",
        )

    def select_column(
        self,
        json_col: sql.Identifier,
        alias: str,
    ) -> sql.Composed:
        str_extract = (
            "ldlite_system.jself_string({json_col})"
            if self.prop is None
            else "{json_col}->>{prop}"
        )

        if self.is_array or self.is_object:
            extract = "{json_col}" if self.prop is None else "{json_col}->{prop}"
            stmt = sql.SQL(extract + "AS {alias}")
        elif self.json_type == "number" and self.is_float:
            stmt = sql.SQL("(" + str_extract + ")::numeric AS {alias}")
        elif self.json_type == "number" and self.is_bigint:
            stmt = sql.SQL("(" + str_extract + ")::bigint AS {alias}")
        elif self.json_type == "number":
            stmt = sql.SQL("(" + str_extract + ")::integer AS {alias}")
        elif self.json_type == "boolean":
            stmt = sql.SQL(
                "NULLIF(NULLIF(" + str_extract + ", ''), 'null')::bool AS {alias}",
            )
        elif self.json_type == "string" and self.is_uuid:
            stmt = sql.SQL(
                "NULLIF(NULLIF(" + str_extract + ", ''), 'null')::uuid AS {alias}",
            )
        elif self.json_type == "string" and self.is_datetime:
            stmt = sql.SQL(
                "NULLIF(NULLIF("
                + str_extract
                + ", ''), 'null')::timestamptz AS {alias}",
            )
        else:
            stmt = sql.SQL(
                "NULLIF(NULLIF(" + str_extract + ", ''), 'null') AS {alias}",
            )

        return stmt.format(
            json_col=json_col,
            prop=self.prop,
            alias=sql.Identifier(alias),
        )
