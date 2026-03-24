from __future__ import annotations

from abc import abstractmethod
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeAlias, TypeVar, cast
from uuid import uuid4

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

    def array_prefix(
        self,
        source: sql.Identifier,
    ) -> NodeContext:
        return NodeContext(
            source,
            sql.Identifier("jsonb"),
            [],
            None,
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
            str_extract = self.path + sql.SQL("->>{prop}").format(
                sql.Literal(self.ctx.prop),
            )

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

        return type_extract + sql.SQL(" AS {alias}").format(
            alias=sql.Identifier(self.alias),
        )

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
        return sql.SQL("__o AS {alias}").format(
            alias=sql.Identifier(self.alias),
        )


TNode = TypeVar("TNode", bound="Node")
TRode = TypeVar("TRode", bound="RecursiveNode")


class RecursiveNode(Node):
    def __init__(self, ctx: NodeContext, parent: RecursiveNode | None):
        super().__init__(ctx)

        self.parent = parent
        self._children: list[Node] = []

    @property
    def source_cte(self) -> sql.Composed:
        return (
            sql.SQL("""
WITH source (
    SELECT
        t.{column}""").format(column=self.ctx.column)
            + self.path
            + (
                sql.SQL("->{prop}").format(prop=sql.Literal(self.ctx.prop))
                if self.ctx.prop is not None
                else sql.SQL("")
            )
            + sql.SQL(""" AS ld_value
    FROM {source} t
)""").format(source=self.ctx.source)
        )

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

    def _typed_columns(self) -> Iterator[TypedNode]:
        for n in self._descendents(RecursiveNode):
            yield from n.direct(TypedNode)

    def typed_nodes(self) -> list[TypedNode]:
        return list(self._typed_columns())

    def remove(self, node: RecursiveNode) -> None:
        self._children.remove(node)

    @property
    def create_statement(self) -> sql.Composed:
        return sql.Composed("")


class ObjectNode(RecursiveNode):
    def load_columns(self, conn: Conn) -> None:
        with conn.cursor() as cur:
            key_discovery = self.source_cte + sql.Composed("""
SELECT
    json_key
    ,MIN(json_type) AS json_type
    ,MAX(json_type) AS other_json_type
FROM (
    SELECT
        j."key" AS json_key
        ,jsonb_typeof(j."value") AS json_type
        ,j.ord
    FROM source t
    CROSS JOIN LATERAL jsonb_each(t.ld_value) WITH ORDINALITY j("key", "value", ord)
    WHERE jsonb_typeof(t.ld_value) = 'object'
) j
WHERE json_type <> 'null'
GROUP BY json_key
ORDER BY MAX(j.ord), COUNT(*);""")

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


class RootNode(ObjectNode):
    def __init__(self, source: sql.Identifier):
        super().__init__(
            NodeContext(
                source,
                sql.Identifier("jsonb"),
                [],
                None,
            ),
            None,
        )


class ArrayNode(RecursiveNode):
    def __init__(self, ctx: NodeContext, parent: RecursiveNode | None):
        super().__init__(ctx, parent)
        self.temp = sql.Identifier(str(uuid4()).split("-")[0])

    def make_temp(self, conn: Conn) -> Node | None:
        with conn.cursor() as cur:
            expansion = (
                sql.SQL("CREATE TEMPORARY TABLE {temp} AS").format(temp=self.temp)
                + self.source_cte
                + sql.Composed("""
SELECT
    __id AS parent__id
    ,(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))::integer AS __id
    ,ord::smallint AS __o
    ,jsonb
    ,json_type
FROM (
    SELECT
        t.__id
        ,a."value" AS jsonb
        ,jsonb_typeof(a."value") AS json_type
        ,a.ord
    FROM source t
    CROSS JOIN LATERAL jsonb_each(t.ld_value) WITH ORDINALITY a("value", ord)
    WHERE jsonb_typeof(t.ld_value) = 'array'
) a
WHERE json_type <> 'null'
""")
            )
            cur.execute(expansion.as_string())

            type_discovery = sql.SQL("""
SELECT
    MIN(json_type) AS json_type
    ,MAX(json_type) AS other_json_type
FROM {temp}""").format(temp=self.temp)

            cur.execute(type_discovery.as_string())
            self._children.append(OrdinalNode(self.ctx.array_prefix(self.temp)))
            if row := cur.fetchone():
                (jt, ojt) = cast("tuple[JsonType, JsonType]", row)
                node: Node
                if jt == "array" and ojt == "array":
                    node = ArrayNode(self.ctx.array_prefix(self.temp), self)
                    self._children.append(node)
                elif jt == "object" and ojt == "object":
                    node = ObjectNode(self.ctx.array_prefix(self.temp), self)
                    self._children.append(node)
                else:
                    node = TypedNode(
                        self.ctx.array_prefix(self.temp),
                        jt,
                        ojt,
                    )
                    self._children.append(node)

                return node

        return None


def _non_srs_statements(
    conn: Conn,
    source_table: sql.Identifier,
) -> Iterator[sql.Composed]:
    # Here be dragons! The nodes have inner state manipulations
    # that violate the space/time continuum:
    # * o.load_columns
    # * a.make_temp
    # * t.specify_type
    # These all are expected to be called before generating the sql
    # as they load/prepare database information.
    # Because building up to the transformation statements takes a long time
    # we're doing all that work up front to keep the time
    # that a transaction is opened to a minimum (which is a leaky abstraction).

    root = RootNode(source_table)
    onodes: deque[ObjectNode] = deque([root])
    while o := onodes.popleft():
        o.load_columns(conn)
        onodes.extend(o.direct(ObjectNode))
        anodes = deque(o.direct(ArrayNode))
        while a := anodes.popleft():
            if n := a.make_temp(conn):
                if isinstance(n, ObjectNode):
                    onodes.append(n)
                if isinstance(n, ArrayNode):
                    anodes.append(n)
            else:
                cast("RecursiveNode", a.parent).remove(a)

    for t in root.typed_nodes():
        t.specify_type(conn)

    yield root.create_statement
    for a in root.descendents(ArrayNode):
        yield a.create_statement


def non_srs_statements(
    conn: Conn,
    source_table: sql.Identifier,
) -> list[sql.Composed]:
    return list(_non_srs_statements(conn, source_table))
