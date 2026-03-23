from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Literal

from psycopg import sql


class Metadata(ABC):
    def __init__(self, prop: str | None):
        self.prop = prop

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

    @property
    @abstractmethod
    def select_stmt(self) -> str: ...

    def select_column(
        self,
        json_col: sql.Identifier,
        alias: str,
    ) -> sql.Composed:
        return sql.SQL(self.select_stmt + " AS {alias}").format(
            json_col=json_col,
            prop=self.prop,
            alias=sql.Identifier(alias),
        )


class ObjectMeta(Metadata):
    @property
    def select_stmt(self) -> str:
        return "{json_col}" if self.prop is None else "{json_col}->{prop}"


class ArrayMeta(Metadata):
    @property
    def select_stmt(self) -> str:
        return "{json_col}" if self.prop is None else "{json_col}->{prop}"

    @abstractmethod
    def unwrap(self) -> ObjectMeta | TypedMeta: ...


class TypedMeta(Metadata):
    def __init__(  # noqa: PLR0913
        self,
        prop: str | None,
        json_type: Literal["string", "number", "boolean"],
        other_json_type: Literal["string", "number", "boolean"],
        is_uuid: bool,
        is_datetime: bool,
        is_float: bool,
        is_bigint: bool,
    ):
        super().__init__(prop)

        mixed_type = json_type != other_json_type
        self.json_type: Literal["string", "number", "boolean"] = (
            json_type if not mixed_type else "string"
        )
        self.is_uuid = is_uuid and not mixed_type
        self.is_datetime = is_datetime and not mixed_type
        self.is_float = is_float and not mixed_type
        self.is_bigint = is_bigint and not mixed_type

    @property
    def select_stmt(self) -> str:  # noqa: PLR0911
        str_extract = (
            "{json_col}->>{prop}"
            if self.prop is not None
            else """TRIM(BOTH '"' FROM ({json_col})::text)"""
        )
        str_extract = f"NULLIF(NULLIF({str_extract}, ''), 'null')"

        if self.json_type == "number" and self.is_float:
            return f"{str_extract}::numeric"
        if self.json_type == "number" and self.is_bigint:
            return f"{str_extract}::bigint"
        if self.json_type == "number":
            return f"{str_extract}::integer"
        if self.json_type == "boolean":
            return f"{str_extract}::bool"
        if self.json_type == "string" and self.is_uuid:
            return f"{str_extract}::uuid"
        if self.json_type == "string" and self.is_datetime:
            return f"{str_extract}::timestamptz"

        return str_extract


class MixedMeta(TypedMeta):
    def __init__(
        self,
        prop: str | None,
    ):
        super().__init__(prop, "string", "string", False, False, False, False)


class ObjectArrayMeta(ObjectMeta, ArrayMeta):
    def unwrap(self) -> ObjectMeta:
        return ObjectMeta(None)


class MixedArrayMeta(MixedMeta, ArrayMeta):
    @property
    def select_stmt(self) -> str:
        return "{json_col}" if self.prop is None else "{json_col}->{prop}"

    def unwrap(self) -> MixedMeta:
        return MixedMeta(None)


class TypedArrayMeta(TypedMeta, ArrayMeta):
    @property
    def select_stmt(self) -> str:
        return "{json_col}" if self.prop is None else "{json_col}->{prop}"

    def unwrap(self) -> TypedMeta:
        return TypedMeta(
            None,
            self.json_type,
            self.json_type,
            self.is_uuid,
            self.is_datetime,
            self.is_float,
            self.is_bigint,
        )
