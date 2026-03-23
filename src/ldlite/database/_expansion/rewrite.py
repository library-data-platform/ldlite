from __future__ import annotations

from abc import abstractmethod
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeAlias, TypeVar, cast

import duckdb
import psycopg
from psycopg import sql

if TYPE_CHECKING:
    from collections.abc import Iterator

Conn: TypeAlias = duckdb.DuckDBPyConnection | psycopg.Connection
JsonType: TypeAlias = Literal["array", "object", "string", "number", "boolean"]


@dataclass
class NodeContext:
    source: sql.Identifier
    column: sql.Identifier
    prefixes: list[str]
    prop: str | None

    def sub_prefix(self, prefix: str | None, prop: str | None) -> NodeContext:
        return NodeContext(
            self.source,
            self.column,
            [*self.prefixes, *([prefix] if prefix is not None else [])],
            prop,
        )


class Node:
    def __init__(self, ctx: NodeContext):
        self.ctx = ctx

    @property
    def path(self) -> sql.Composed:
        return sql.SQL("->").join([sql.Literal(p) for p in self.ctx.prefixes])


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
        json_type: JsonType,
        other_json_type: JsonType,
    ):
        super().__init__(ctx)

        self.is_mixed = json_type = other_json_type
        self.json_type: JsonType = "string" if self.is_mixed else json_type
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
        if self.ctx.prop is None:
            str_extract = (
                sql.SQL("""TRIM(BOTH '"' FROM ({json_col}""").format(self.ctx.column)
                + self.path
                + sql.SQL(")::text)")
            )
        else:
            str_extract = self.path + sql.SQL("->>{prop}").format(self.ctx.prop)

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
        return sql.SQL('"ordinality"::smallint AS {alias}').format(alias=self.alias)


TNode = TypeVar("TNode", bound="Node")
TRode = TypeVar("TRode", bound="RecursiveNode")


class RecursiveNode(Node):
    def __init__(self, ctx: NodeContext, parent: RecursiveNode | None):
        super().__init__(ctx)

        self.parent = parent
        self._children: list[Node] = []

    def _direct(self, cls: type[TNode]) -> Iterator[TNode]:
        yield from [n for n in self._children if isinstance(n, cls)]

    def direct(self, cls: type[TNode]) -> list[TNode]:
        return list(self._direct(cls))

    def _descendents(self, cls: type[TRode]) -> Iterator[TRode]:
        to_visit = deque([self])
        while to_visit:
            n = to_visit.pop()
            if isinstance(n, cls):
                yield n

            to_visit.extend(n.direct(RecursiveNode))

    def descendents(self, cls: type[TRode]) -> list[TRode]:
        return list(self._descendents(cls))


class ObjectNode(RecursiveNode):
    def load_columns(self, conn: Conn) -> None:
        with conn.cursor() as cur:
            key_discovery = (
                sql.SQL("""
SELECT
    j.json_key
    ,MIN(j.json_type) AS json_type
    ,MAX(j.json_type) AS other_json_type
FROM
(
    SELECT
      json_key
      ,jsonb_typeof(json_value) AS json_type
      ,ord
    FROM {table} t
    CROSS JOIN LATERAL jsonb_each(t.{column}""").format(
                    table=self.ctx.source,
                    json_column=self.ctx.column,
                )
                + self.path
                + (
                    sql.SQL("->{prop})").format(prop=self.ctx.prop)
                    if self.ctx.prop is not None
                    else sql.SQL(")")
                )
                + sql.Composed(""" WITH ORDINALITY k(json_key, json_value, ord)
) j
WHERE json_type <> 'null'
GROUP BY json_key
ORDER BY MAX(j.ord), COUNT(*)
                        """)
            )
            cur.execute(key_discovery.as_string())
            for row in cur.fetchall():
                (key, jt, ojt) = cast("tuple[str, JsonType, JsonType]", row)
                if jt == "array" and ojt == "array":
                    anode = ArrayNode(self.ctx.sub_prefix(self.ctx.prop, key), self)
                    self._children.append(anode)
                elif jt == "object" and ojt == "object":
                    onode = ObjectNode(self.ctx.sub_prefix(self.ctx.prop, key), self)
                    self._children.append(onode)
                else:
                    tnode = TypedNode(self.ctx.sub_prefix(self.ctx.prop, key), jt, ojt)
                    self._children.append(tnode)


class RootNode(ObjectNode): ...


class ArrayNode(RecursiveNode): ...
