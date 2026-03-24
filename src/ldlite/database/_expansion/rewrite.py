from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeVar, cast
from uuid import uuid4

import duckdb
import psycopg
from psycopg import sql

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from typing import NoReturn, TypeAlias

    from tqdm import tqdm

Conn: TypeAlias = duckdb.DuckDBPyConnection | psycopg.Connection
JsonType: TypeAlias = Literal["array", "object", "string", "number", "boolean"]


@dataclass
class NodeContext:
    source: sql.Identifier
    column: sql.Identifier
    prefixes: list[str]
    prop: str | None

    @property
    def snake(self) -> str | None:
        if self.prop is None:
            return None

        snake = "".join("_" + c.lower() if c.isupper() else c for c in self.prop)
        # there's also sorts of weird edge cases here that don't come up in practice
        if (naked := self.prop.lstrip("_")) and len(naked) > 0 and naked[0].isupper():
            snake = snake.removeprefix("_")

        return snake

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

    @property
    def _json_source(self) -> sql.Composed:
        return self.ctx.column + sql.SQL("->").join(
            [sql.Literal(p) for p in self.ctx.prefixes],
        )

    @property
    def json_value(self) -> sql.Composed:
        if self.ctx.prop is None:
            return self._json_source
        return self._json_source + sql.SQL("->") + sql.Literal(self.ctx.prop)

    @property
    def json_string(self) -> sql.Composed:
        if self.ctx.prop is None:
            str_extract = (
                sql.SQL("""TRIM(BOTH '"' FROM """)
                + self._json_source
                + sql.SQL(")::text)")
            )
        else:
            str_extract = (
                self._json_source + sql.SQL("->>") + sql.Literal(self.ctx.prop)
            )

        return sql.SQL("NULLIF(NULLIF(") + str_extract + sql.SQL(", ''), 'null')")


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

        self.is_mixed = json_type != other_json_type
        self.json_type: JsonType = "string" if self.is_mixed else json_type
        self.is_uuid = False
        self.is_datetime = False
        self.is_float = False
        self.is_bigint = False

    @property
    def alias(self) -> str:
        if len(self.ctx.prefixes) == 0:
            return self.ctx.snake if self.ctx.snake is not None else ""

        return "__".join(self.ctx.prefixes) + (
            ("_" + self.ctx.snake) if self.ctx.snake is not None else ""
        )

    @property
    def stmt(self) -> sql.Composed:
        if self.json_type == "number" and self.is_float:
            type_extract = self.json_string + sql.SQL("::numeric")
        elif self.json_type == "number" and self.is_bigint:
            type_extract = self.json_string + sql.SQL("::bigint")
        elif self.json_type == "number":
            type_extract = self.json_string + sql.SQL("::integer")
        elif self.json_type == "boolean":
            type_extract = self.json_string + sql.SQL("::bool")
        elif self.json_type == "string" and self.is_uuid:
            type_extract = self.json_string + sql.SQL("::uuid")
        elif self.json_type == "string" and self.is_datetime:
            type_extract = self.json_string + sql.SQL("::timestamptz")
        else:
            type_extract = self.json_string

        return type_extract + sql.SQL(" AS ") + sql.Identifier(self.alias)

    def specify_type(self, conn: Conn) -> None:
        if self.is_mixed or self.json_type == "boolean":
            return

        cte = (
            sql.SQL("""
WITH string_values AS MATERIALIZED (
    SELECT """)
            + self.json_string
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
                string_value NOT LIKE '____-__-__T__:__:__.___' AND
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
    ,EXISTS(
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

    def _typed_nodes(self) -> Iterator[TypedNode]:
        for n in self._descendents(RecursiveNode):
            yield from n.direct(TypedNode)

    def typed_nodes(self) -> list[TypedNode]:
        return list(self._typed_nodes())

    def remove(self, node: RecursiveNode) -> None:
        self._children.remove(node)


class ObjectNode(RecursiveNode):
    def load_columns(self, conn: Conn) -> None:
        with conn.cursor() as cur:
            key_discovery = (
                sql.SQL("""
SELECT
    json_key
    ,MIN(json_type) AS json_type
    ,MAX(json_type) AS other_json_type
FROM
(
    SELECT
        k."key" AS json_key
        ,jsonb_typeof(k."value") AS json_type
        ,k.ord
    FROM
    (
        SELECT """)
                + self.json_value
                + sql.SQL(""" AS ld_value FROM {source_table}
    ) j
    CROSS JOIN LATERAL jsonb_each(j.ld_value) WITH ORDINALITY k("key", "value", ord)
    WHERE jsonb_typeof(j.ld_value) = 'object'
) key_discovery
WHERE json_type <> 'null'
GROUP BY json_key
ORDER BY MAX(ord), COUNT(*);
""").format(source_table=self.ctx.source)
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


class StampableNode(ABC):
    @property
    @abstractmethod
    # The Callable construct is necessary until DuckDB implements CTAS RETURNING
    def create_statement(self) -> tuple[str, Callable[[sql.SQL], sql.Composed]]: ...


class RootNode(ObjectNode, StampableNode):
    def __init__(
        self,
        source: sql.Identifier,
        get_output_table: Callable[[str | None], tuple[str, sql.Identifier]],
    ):
        super().__init__(
            NodeContext(
                source,
                sql.Identifier("jsonb"),
                [],
                None,
            ),
            None,
        )
        self.get_output_table = get_output_table

    @property
    def create_statement(self) -> tuple[str, Callable[[sql.SQL], sql.Composed]]:
        (output_table_name, output_table) = self.get_output_table(None)

        def create_root_table(source_stmt: sql.SQL) -> sql.Composed:
            return (
                sql.SQL("""
CREATE TABLE {output_table} AS
WITH root_source AS (""").format(output_table=output_table)
                + source_stmt.format(source_table=self.ctx.source)
                + sql.SQL(
                    """)
SELECT
    """,
                )
                + sql.SQL("\n    ,").join(
                    [
                        t.stmt
                        for o in self.descendents(ObjectNode)
                        for t in o.direct(TypedNode)
                    ],
                )
                + sql.SQL("""
FROM root_source""")
            )

        return (output_table_name, create_root_table)


class ArrayNode(RecursiveNode, StampableNode):
    def __init__(self, ctx: NodeContext, parent: RecursiveNode | None):
        super().__init__(ctx, parent)
        self.temp = sql.Identifier(str(uuid4()).split("-")[0])

    def make_temp(self, conn: Conn) -> Node | None:
        with conn.cursor() as cur:
            expansion = (
                sql.SQL("CREATE TEMPORARY TABLE {temp} AS").format(temp=self.temp)
                + sql.SQL("""
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
    FROM
    (
        SELECT """)
                + self.json_value
                + sql.SQL(""" AS ld_value FROM {source}
    ) j
    CROSS JOIN LATERAL jsonb_array_elements(j.ld_value) WITH ORDINALITY a("value", ord)
    WHERE jsonb_typeof(j.ld_value) = 'array'
) expansion
WHERE json_type <> 'null'
""").format(source=self.ctx.source)
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

    @property
    def create_statement(self) -> tuple[str, Callable[[sql.SQL], sql.Composed]]:
        p: RecursiveNode | None = self
        parents: list[RecursiveNode] = []
        while p is not None and (p := p.parent):
            parents.append(p)
        root = cast("RootNode", parents[-1])
        (output_table_name, output_table) = root.get_output_table(
            "__" + "__".join(self.ctx.prefixes),
        )
        (_, parent_table) = root.get_output_table(
            "__" + "__".join(cast("Node", parents[0]).ctx.prefixes),
        )

        def create_array_table(source_stmt: sql.SQL) -> sql.Composed:
            return (
                sql.SQL("""
CREATE TABLE {output_table} AS
WITH array_source AS (""").format(output_table=output_table)
                + source_stmt.format(source_table=self.temp)
                + sql.SQL(
                    """)
SELECT
    """,
                )
                + sql.SQL("\n    ,").join(
                    [
                        sql.Identifier("a", "__id"),
                        *[
                            t.alias
                            for p in reversed(parents)
                            for t in p.direct(TypedNode)
                        ],
                        *[t.stmt for t in self.direct(TypedNode)],
                        *[
                            t.stmt
                            for o in self.descendents(ObjectNode)
                            for t in o.direct(TypedNode)
                        ],
                    ],
                )
                + sql.SQL("""
FROM array_source a
JOIN {parent_table} p ON
    a.p__id = p.__id;
""").format(parent_table=parent_table)
            )

        return (output_table_name, create_array_table)


def _non_srs_statements(
    conn: Conn,
    source_table: sql.Identifier,
    output_table: Callable[[str | None], tuple[str, sql.Identifier]],
    scan_progress: tqdm[NoReturn],
) -> Iterator[tuple[str, Callable[[sql.SQL], sql.Composed]]]:
    # Here be dragons! The nodes have inner state manipulations
    # that violate the space/time continuum:
    # * o.load_columns
    # * a.make_temp
    # * t.specify_type
    # These all are expected to be called before generating the sql
    # as they load/prepare database information.
    # Because building up to the transformation statements takes a long time
    # we're doing all that work up front to keep the time that
    # a transaction is opened to a minimum (which is a leaky abstraction).

    root = RootNode(source_table, output_table)
    onodes: deque[ObjectNode] = deque([root])
    while onodes:
        o = onodes.popleft()
        o.load_columns(conn)
        scan_progress.total += len(o.direct(Node))
        scan_progress.update(1)

        onodes.extend(o.direct(ObjectNode))
        anodes = deque(o.direct(ArrayNode))
        while anodes:
            a = anodes.popleft()
            if n := a.make_temp(conn):
                if isinstance(n, ObjectNode):
                    onodes.append(n)
                if isinstance(n, ArrayNode):
                    anodes.append(n)
                scan_progress.total += 1
            else:
                cast("RecursiveNode", a.parent).remove(a)

            scan_progress.update(1)

    for t in root.typed_nodes():
        t.specify_type(conn)
        scan_progress.update(1)

    yield root.create_statement
    for a in root.descendents(ArrayNode):
        yield a.create_statement


def non_srs_statements(
    conn: Conn,
    source_table: sql.Identifier,
    output_table: Callable[[str | None], tuple[str, sql.Identifier]],
    scan_progress: tqdm[NoReturn],
) -> list[tuple[str, Callable[[sql.SQL], sql.Composed]]]:
    return list(_non_srs_statements(conn, source_table, output_table, scan_progress))
