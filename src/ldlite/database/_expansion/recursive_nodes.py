from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from typing import TYPE_CHECKING, TypeVar, cast
from uuid import uuid4

from psycopg import sql

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


from .fixed_nodes import FixedValueNode, JsonbNode, JsonType, OrdinalNode, TypedNode
from .node import Conn, Node

TNode = TypeVar("TNode", bound="Node")
TRode = TypeVar("TRode", bound="RecursiveNode")


class RecursiveNode(Node):
    def __init__(
        self,
        source: sql.Identifier,
        prop: str | None,
        column: sql.Identifier,
        parent: RecursiveNode | None,
    ):
        super().__init__(source, prop)

        self.parent = parent
        self.column = column
        self._children: list[Node] = []

    def _parents(self) -> Iterator[RecursiveNode]:
        p = self.parent
        while p is not None:
            yield p
            p = p.parent

    @property
    def parents(self) -> list[RecursiveNode]:
        return list(self._parents())

    @property
    def table_parent(self) -> RecursiveNode:
        for p in self.parents:
            if isinstance(p, (ArrayNode, RootNode)):
                return p

        # There's "always" a root node
        return None  # type: ignore[return-value]

    @property
    def depth(self) -> int:
        depth = 0
        prev = None
        for p in self.parents:
            # arrays of objects only count for a single level of depth
            if not (isinstance(p, ObjectNode) and isinstance(prev, ArrayNode)):
                depth += 1
            prev = p

        return depth

    def replace(self, original: Node, replacement: Node | None) -> None:
        if replacement is None:
            self._children.remove(original)
            return

        self._children = [(replacement if n == original else n) for n in self._children]

    def unparent(self) -> None:
        cast("RecursiveNode", self.parent).replace(self, None)

    def make_jsonb(self) -> None:
        cast("RecursiveNode", self.parent).replace(
            self,
            JsonbNode(self.source, self.path, self.prefix),
        )

    @property
    def path(self) -> sql.Composable:
        prop_accessor: sql.Composable
        if self.prop is None:
            prop_accessor = sql.SQL("")
        else:
            prop_accessor = sql.SQL("->") + sql.Literal(self.prop)

        path: list[str] = []
        for p in self.parents:
            if isinstance(p, (ArrayNode, RootNode)):
                break
            if p.prop is not None:
                path.append(p.prop)

        if len(path) == 0:
            return self.column + prop_accessor

        return (
            self.column
            + sql.SQL("->")
            + sql.SQL("->").join([sql.Literal(p) for p in reversed(path)])
            + prop_accessor
        )

    @property
    def prefix(self) -> str:
        if len(self.parents) == 0 or isinstance(self.parents[0], RootNode):
            return self.snake_prop or ""

        return "__".join(
            [p.snake_prop for p in reversed(self.parents) if p.snake_prop is not None],
        ) + (("__" + self.snake_prop) if self.snake_prop is not None else "")

    def _direct(self, cls: type[TNode]) -> Iterator[TNode]:
        yield from [n for n in self._children if isinstance(n, cls)]

    def direct(self, cls: type[TNode]) -> list[TNode]:
        return list(self._direct(cls))

    def _descendents(
        self,
        cls: type[TRode],
        to_cls: type[TRode] | None = None,
    ) -> Iterator[TRode]:
        to_visit = deque(self.direct(RecursiveNode))
        while to_visit:
            n = to_visit.pop()
            if isinstance(n, cls):
                yield n

            if to_cls is not None and isinstance(n, to_cls):
                continue

            to_visit.extend(n.direct(RecursiveNode))

    def descendents(
        self,
        cls: type[TRode],
        to_cls: type[TRode] | None = None,
    ) -> list[TRode]:
        return list(self._descendents(cls, to_cls))

    def _typed_nodes(self) -> Iterator[TypedNode]:
        yield from self.direct(TypedNode)
        for n in self._descendents(RecursiveNode):
            yield from n.direct(TypedNode)

    def typed_nodes(self) -> list[TypedNode]:
        return list(self._typed_nodes())


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
                + self.path
                + sql.SQL(""" AS ld_value FROM {source_table}
    ) j
    CROSS JOIN LATERAL jsonb_each(j.ld_value) WITH ORDINALITY k("key", "value", ord)
    WHERE jsonb_typeof(j.ld_value) = 'object'
) key_discovery
WHERE json_type <> 'null'
GROUP BY json_key
ORDER BY MAX(ord), COUNT(*);
""").format(source_table=self.source)
            )

            cur.execute(key_discovery.as_string())
            for row in cur.fetchall():
                (key, jt, ojt) = cast("tuple[str, JsonType, JsonType]", row)
                if jt == "array" and ojt == "array":
                    anode = ArrayNode(self.source, key, self.column, self)
                    self._children.append(anode)
                elif jt == "object" and ojt == "object":
                    onode = ObjectNode(self.source, key, self.column, self)
                    self._children.append(onode)
                else:
                    tnode = TypedNode(
                        self.source,
                        key,
                        self.path,
                        self.prefix,
                        (jt, ojt),
                    )
                    self._children.append(tnode)


class StampableTable(ABC):
    @property
    @abstractmethod
    def create_statement(self) -> tuple[str, sql.Composed]: ...


class RootNode(ObjectNode, StampableTable):
    def __init__(
        self,
        source: sql.Identifier,
        get_output_table: Callable[[str | None], tuple[str, sql.Identifier]],
    ):
        super().__init__(
            source,
            None,
            sql.Identifier("jsonb"),
            None,
        )
        self.get_output_table = get_output_table

    @property
    def create_statement(self) -> tuple[str, sql.Composed]:
        (output_table_name, output_table) = self.get_output_table(None)

        return (
            output_table_name,
            sql.SQL("""
CREATE TABLE {output_table} AS
SELECT
    """).format(output_table=output_table)
            + sql.SQL("\n    ,").join(
                [
                    sql.Identifier("__id"),
                    *[t.stmt for t in self.direct(TypedNode)],
                    *[
                        t.stmt
                        for o in self.descendents(ObjectNode, ArrayNode)
                        for t in o.direct(TypedNode)
                    ],
                ],
            )
            + sql.SQL("""
FROM {source_table};
ANALYZE {output_table} (__id);""").format(
                source_table=self.source,
                output_table=output_table,
            ),
        )


class ArrayNode(RecursiveNode, StampableTable):
    def __init__(
        self,
        source: sql.Identifier,
        prop: str | None,
        column: sql.Identifier,
        parent: RecursiveNode | None,
    ):
        super().__init__(source, prop, column, parent)
        self.temp = sql.Identifier(str(uuid4()).split("-")[0])

    def make_temp(self, conn: Conn) -> Node | None:
        with conn.cursor() as cur:
            expansion = (
                sql.SQL("""
CREATE TEMPORARY TABLE {temp} AS
SELECT
    __id AS p__id
    ,(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))::integer AS __id
    ,ord::smallint AS __o
    ,array_jsonb
    ,json_type
FROM (
    SELECT
        j.__id
        ,a."value" AS array_jsonb
        ,jsonb_typeof(a."value") AS json_type
        ,a.ord
    FROM
    (
        SELECT """).format(temp=self.temp)
                + self.path
                + sql.SQL(""" AS ld_value, __id FROM {source}
    ) j
    CROSS JOIN LATERAL jsonb_array_elements(j.ld_value) WITH ORDINALITY a("value", ord)
    WHERE jsonb_typeof(j.ld_value) = 'array'
) expansion
WHERE json_type <> 'null';
ANALYZE {temp} (p__id, array_jsonb, json_type);
""").format(source=self.source, temp=self.temp)
            )
            cur.execute(expansion.as_string())

            type_discovery = sql.SQL("""
SELECT
    MIN(json_type) AS json_type
    ,MAX(json_type) AS other_json_type
FROM {temp}""").format(temp=self.temp)

            cur.execute(type_discovery.as_string())
            self._children.append(OrdinalNode(self.temp, self.path, self.prefix))
            if row := cur.fetchone():
                (jt, ojt) = cast("tuple[JsonType, JsonType]", row)
                node: Node
                if jt == "array" and ojt == "array":
                    node = ArrayNode(
                        self.temp,
                        None,
                        sql.Identifier("array_jsonb"),
                        self,
                    )
                elif jt == "object" and ojt == "object":
                    node = ObjectNode(
                        self.temp,
                        None,
                        sql.Identifier("array_jsonb"),
                        self,
                    )
                else:
                    node = TypedNode(
                        self.temp,
                        None,
                        sql.Identifier("array_jsonb"),
                        self.prefix,
                        (jt, ojt),
                    )
                self._children.append(node)
                return node

        return None

    @property
    def create_statement(self) -> tuple[str, sql.Composed]:
        table_parent: RecursiveNode = self
        parents: list[RecursiveNode] = []
        for p in self.parents:
            if table_parent == self and isinstance(p, (RootNode, ArrayNode)):
                table_parent = p
            parents.append(p)

        root = cast("RootNode", parents[-1])
        (output_table_name, output_table) = root.get_output_table(self.prefix)
        if not isinstance(table_parent, ArrayNode):
            (_, parent_table) = root.get_output_table(None)
        else:
            (_, parent_table) = root.get_output_table(table_parent.prefix)

        return (
            output_table_name,
            (
                sql.SQL(
                    """
CREATE TABLE {output_table} AS
SELECT
    """,
                ).format(output_table=output_table)
                + sql.SQL("\n    ,").join(
                    [
                        sql.Identifier("a", "__id"),
                        *[
                            sql.Identifier("p", t.alias)
                            for p in reversed(parents)
                            for t in p.direct(FixedValueNode)
                        ],
                        *[t.stmt for t in self.direct(FixedValueNode)],
                        *[
                            t.stmt
                            for o in self.descendents(ObjectNode, ArrayNode)
                            for t in o.direct(TypedNode)
                        ],
                    ],
                )
                + sql.SQL("""
FROM {source_table} a
JOIN {parent_table} p ON
    a.p__id = p.__id;
ANALYZE {output_table} (__id);
""").format(
                    source_table=self.temp,
                    parent_table=parent_table,
                    output_table=output_table,
                )
            ),
        )
