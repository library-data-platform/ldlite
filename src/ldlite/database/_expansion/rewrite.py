from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import Literal, TypeAlias

import duckdb
import psycopg
from psycopg import sql

Conn: TypeAlias = duckdb.DuckDBPyConnection | psycopg.Connection


@dataclass(frozen=True)
class NodeContext:
    source: sql.Identifier
    column: sql.Identifier
    prefixes: frozenset[str]
    prop: str | None


class Node:
    def __init__(self, ctx: NodeContext):
        self.ctx = ctx


class FixedValueNode(Node):
    @property
    @abstractmethod
    def alias(self) -> str: ...

    @property
    @abstractmethod
    def stmt(self) -> sql.Composed: ...


class TypedNode(FixedValueNode):
    def __init__(
        self,
        ctx: NodeContext,
        json_type: Literal["string", "number", "boolean"],
        other_json_type: Literal["string", "number", "boolean"],
    ):
        super().__init__(ctx)

        self.is_mixed = json_type = other_json_type
        self.json_type: Literal["string", "number", "boolean", "null"] = (
            "string" if self.is_mixed else json_type
        )
        self.is_uuid = False
        self.is_datetime = False
        self.is_float = False
        self.is_bigint = False

    @property
    def alias(self) -> str:
        return "__".join(self.ctx.prefixes) + (
            ("__" + self.ctx.prop) if self.ctx.prop is not None else ""
        )

    @property
    def str_extract(self) -> sql.Composed:
        path = sql.SQL("->").join([sql.Literal(p) for p in self.ctx.prefixes])
        if self.ctx.prop is None:
            str_extract = (
                sql.SQL("""TRIM(BOTH '"' FROM ({json_col}""").format(self.ctx.column)
                + path
                + sql.SQL(")::text)")
            )
        else:
            str_extract = path + sql.SQL("->>{prop}").format(self.ctx.prop)

        return sql.Composed(
            [
                sql.SQL("NULLIF(NULLIF("),
                str_extract,
                sql.SQL(", ''), 'null')"),
            ],
        )

    @property
    def stmt(self) -> sql.Composed:
        str_extract = self.str_extract

        if self.json_type == "number" and self.is_float:
            type_extract = str_extract + sql.SQL("::numeric")
        elif self.json_type == "number" and self.is_bigint:
            type_extract = str_extract + sql.SQL("::bigint")
        elif self.json_type == "number":
            type_extract = str_extract + sql.SQL("::integer")
        elif self.json_type == "boolean":
            type_extract = str_extract + sql.SQL("::bool")
        elif self.json_type == "string" and self.is_uuid:
            type_extract = str_extract + sql.SQL("::uuid")
        elif self.json_type == "string" and self.is_datetime:
            type_extract = str_extract + sql.SQL("::timestamptz")
        else:
            type_extract = str_extract

        return type_extract + sql.SQL(" AS {alias}").format(alias=self.alias)

    def specify_type(self, conn: Conn) -> None:
        if self.is_mixed or self.json_type == "boolean":
            return

        cte = (
            sql.SQL("""
SELECT string_values AS MATERIALIZED (
    SELECT """)
            + self.str_extract
            + sql.SQL(""" AS string_value
    FROM {source}
)""").format(source=self.ctx.source)
        )

        if self.json_type == "string":
            with conn.cursor() as cur:
                specify = cte + sql.SQL("""
SELECT
    NOT EXISTS(
        SELECT 1 FROM string_values
        WHERE
            string_value IS NOT NULL AND
            string_value NOT LIKE '________-____-____-____-____________'
    ) AS is_uuid
    ,NOT EXISTS(
        SELECT 1 FROM string_values
        WHERE
            string_value IS NOT NULL AND
            (
                string_value NOT LIKE '____-__-__T__:__:__.___' OR
                string_value NOT LIKE '____-__-__T__:__:__.___+__:__'
            )
    ) AS is_uuid;""")
                cur.execute(specify.as_string())
                if row := cur.fetchone():
                    (self.is_uuid, self.is_datetime) = row
            return

        with conn.cursor() as cur:
            specify = cte + sql.SQL("""
SELECT
    EXISTS(
        SELECT 1 FROM string_values
        WHERE
            string_value IS NOT NULL AND
            string_value::numeric % 1 <> 0
    ) AS is_float
    ,NOT EXISTS(
        SELECT 1 FROM string_values
        WHERE
            string_value IS NOT NULL AND
            string_value::numeric > 2147483647
    ) AS is_bigint;""")
            cur.execute(specify.as_string())
            if row := cur.fetchone():
                (self.is_float, self.is_bigint) = row
            else:
                self.json_type = "string"


class OrdinalNode(FixedValueNode):
    @property
    def alias(self) -> str:
        return "__".join(self.ctx.prefixes) + "__o"

    @property
    def stmt(self) -> sql.Composed:
        return sql.SQL('"ordinality" AS {alias}').format(alias=self.alias)


class ArrayIdentityNode(FixedValueNode):
    @property
    def alias(self) -> str:
        return "__id"

    @property
    def stmt(self) -> sql.Composed:
        return sql.SQL("ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS __id").format(
            alias=self.alias,
        )


class RecursiveNode(Node): ...


class ObjectNode(RecursiveNode): ...


class RootNode(ObjectNode): ...


class ArrayNode(RecursiveNode): ...
