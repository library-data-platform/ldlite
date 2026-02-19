# pyright: reportArgumentType=false
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from psycopg import sql

if TYPE_CHECKING:
    from collections.abc import Iterator

    import duckdb
    import psycopg


@dataclass
class Metadata:
    prop: str
    json_type: Literal["string", "number", "object", "boolean", "null"]
    is_array: bool
    is_uuid: bool
    is_datetime: bool
    is_float: bool


class ExpansionNode:
    def __init__(
        self,
        name: str,
        path: str | None,
        parent: ExpansionNode | None,
        values: list[str] | None = None,
    ):
        self.name = name
        self.path = path
        self.identifier = sql.Identifier(name)
        self.parent = parent
        self.values: list[str] = values or []
        self.children: list[ExpansionNode] = []

    @staticmethod
    def _c_a_s_e(camel: str) -> str:
        return "".join("_" + c.lower() if c.isupper() else c for c in camel)

    def add(self, meta: Metadata) -> tuple[sql.Identifier, sql.SQL]:
        snake = self._c_a_s_e(meta.prop)
        prefixed_name = self.prefix + snake

        if meta.is_array:
            self.children.append(ArrayNode(prefixed_name, snake, self, meta))
        elif meta.json_type == "object":
            self.children.append(ObjectNode(prefixed_name, snake, self))
        else:
            prefixed_name = self.prefix + snake
            self.values.append(prefixed_name)

        return (sql.Identifier(prefixed_name), self.extract_typed(meta))

    def extract_typed(self, meta: Metadata) -> sql.SQL:
        if meta.is_array:
            return sql.SQL(
                "(ldlite_system.jextract({json_col}, {prop})) AS {alias}",
            )

        if meta.json_type == "object":
            return sql.SQL(
                "(ldlite_system.jextract({json_col}, {prop})) AS {alias}",
            )

        if meta.json_type == "number":
            return sql.SQL(
                "(ldlite_system.jextract({json_col}, {prop}))::numeric AS {alias}",
            )

        if meta.json_type == "string" and meta.is_uuid:
            return sql.SQL(
                "(ldlite_system.jextract_string({json_col}, {prop}))::uuid AS {alias}",
            )

        return sql.SQL(
            "(ldlite_system.jextract_string({json_col}, {prop})) AS {alias}",
        )

    def _parents(self) -> Iterator[ExpansionNode]:
        n = self
        while n.parent is not None:
            yield n.parent
            n = n.parent

    @property
    def parents(self) -> list[ExpansionNode]:
        return list(self._parents())

    @property
    def prefix(self) -> str:
        if len(self.parents) == 0 or self.path is None:
            return ""

        return (
            "__".join(
                [*[p.path for p in self.parents if p.path is not None], self.path],
            )
            + "__"
        )

    @property
    def root(self) -> ExpansionNode:
        if self.parent is None:
            return self

        root = [p for p in self.parents if p.parent is None]
        return root[0]

    def _object_children(self) -> Iterator[ObjectNode]:
        for c in self.children:
            if isinstance(c, ObjectNode):
                yield c

    @property
    def object_children(self) -> list[ObjectNode]:
        return list(self._object_children())

    def _descendents(self) -> Iterator[ExpansionNode]:
        to_visit = deque([self])
        while to_visit:
            n = to_visit.pop()
            yield n

            to_visit.extend(n.children)

    @property
    def descendents(self) -> list[ExpansionNode]:
        return list(self._descendents())

    def _object_descendents(self) -> Iterator[ObjectNode]:
        for n in self._descendents():
            if isinstance(n, ObjectNode):
                yield n

    @property
    def object_descendents(self) -> list[ObjectNode]:
        return list(self._object_descendents())

    def _array_descendents(self) -> Iterator[ArrayNode]:
        for n in self._descendents():
            if isinstance(n, ArrayNode):
                yield n

    @property
    def array_descendents(self) -> list[ArrayNode]:
        return list(self._array_descendents())

    def __str__(self) -> str:
        return "->".join([n.name for n in reversed([self, *self.parents])])


class ObjectNode(ExpansionNode):
    def __init__(
        self,
        name: str,
        path: str | None,
        parent: ExpansionNode | None,
        values: list[str] | None = None,
    ):
        super().__init__(name, path, parent, values)
        self.unnested = False

    def unnest(
        self,
        conn: duckdb.DuckDBPyConnection | psycopg.Connection,
        source_table: sql.Identifier,
        dest_table: sql.Identifier,
    ) -> None:
        self.unnested = True
        create_columns: list[sql.Composable] = [
            sql.Identifier(v) for v in self.carryover
        ]

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
WITH
    one_object AS (SELECT {json_col} as json FROM {table} LIMIT 1),
    props AS (SELECT ldlite_system.jobject_keys(json) AS prop FROM one_object),
    values AS (
        SELECT
            prop
            ,ldlite_system.jextract({json_col}, prop) as value
        FROM {table}, props
    ),
    value_and_types AS (
        SELECT
            prop
            ,ldlite_system.jtype_of(value) AS json_type
            ,value
        FROM values
    ),
    array_values AS (
        SELECT
            v.prop
            ,ldlite_system.jtype_of(a.value) AS json_type
            ,v.value
        FROM value_and_types v, ldlite_system.jexplode(v.value) a
        WHERE v.json_type = 'array'
    ),
    all_values AS (
        SELECT
            prop
            ,json_type
            ,value
            ,FALSE AS is_array
        FROM value_and_types
        WHERE json_type <> 'array'
        UNION
        SELECT
            prop
            ,json_type
            ,value
            ,TRUE AS is_array
        FROM array_values
    )
SELECT
    prop
    ,ANY_VALUE(json_type) AS json_type
    ,ANY_VALUE(is_array) AS is_array
    ,bool_and(ldlite_system.jis_uuid(value)) AS is_uuid
    ,bool_and(ldlite_system.jis_datetime(value)) AS is_datetime
    ,bool_and(ldlite_system.jis_float(value)) AS is_float
FROM all_values
GROUP BY prop
""",
                )
                .format(table=source_table, json_col=self.identifier)
                .as_string(),
            )

            for row in [Metadata(*r) for r in cur.fetchall()]:
                (alias, stmt) = self.add(row)
                create_columns.append(
                    stmt.format(
                        json_col=self.identifier,
                        prop=sql.Literal(row.prop),
                        alias=alias,
                    ),
                )

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
CREATE TEMP TABLE {dest_table} ON COMMIT DROP AS SELECT
    {cols}
FROM {source_table};
""",
                )
                .format(
                    source_table=source_table,
                    dest_table=dest_table,
                    cols=sql.SQL("\n    ,").join(create_columns),
                )
                .as_string(),
            )

            cur.execute(
                sql.SQL("DROP TABLE {source_table};")
                .format(
                    source_table=source_table,
                )
                .as_string(),
            )

    def _carryover(self) -> Iterator[str]:
        for n in self.root.object_descendents:
            if not n.unnested:
                yield n.name
            yield from n.values

    @property
    def carryover(self) -> list[str]:
        return list(self._carryover())


class ArrayNode(ExpansionNode):
    def __init__(
        self,
        name: str,
        path: str | None,
        parent: ExpansionNode | None,
        meta: Metadata,
        values: list[str] | None = None,
    ):
        super().__init__(name, path, parent, values)
        self.meta = Metadata(
            meta.prop,
            meta.json_type,
            False,
            meta.is_uuid,
            meta.is_datetime,
            meta.is_float,
        )

    def explode(
        self,
        conn: duckdb.DuckDBPyConnection | psycopg.Connection,
        source_table: sql.Identifier,
        dest_table: sql.Identifier,
    ) -> ExpansionNode:
        with conn.cursor() as cur:
            create_columns: list[sql.Composable] = [
                sql.SQL("ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS __id"),
                *[sql.Identifier(v) for v in self.carryover],
                sql.SQL("ROW_NUMBER() OVER (PARTITION BY s.__id) AS {id_alias}").format(
                    id_alias=sql.Identifier((self.path or "") + "_o"),
                ),
                self.extract_typed(self.meta).format(
                    json_col=sql.Identifier("a", "value"),
                    prop=sql.Literal("0"),
                    alias=sql.Identifier(self.path or ""),
                ),
            ]

            cur.execute(
                sql.SQL(
                    """
CREATE TEMP TABLE {dest_table} ON COMMIT DROP AS SELECT
    {cols}
FROM
    {source_table} s
    ,ldlite_system.jexplode({json_col}) a
""",
                )
                .format(
                    source_table=source_table,
                    dest_table=dest_table,
                    cols=sql.SQL("\n    ,").join(create_columns),
                    json_col=sql.Identifier("s", self.name),
                )
                .as_string(),
            )

        if self.meta.json_type == "object":
            return ObjectNode(
                self.path or "",
                self.path,
                None,
                [*self.carryover, (self.path or "") + "_o"],
            )

        return ExpansionNode(
            self.path or "",
            self.path,
            None,
            [*self.carryover, (self.path or "") + "_o"],
        )

    def _carryover(self) -> Iterator[str]:
        for n in reversed(self.parents):
            if isinstance(n, ObjectNode):
                yield from [v for v in n.values if v != "__id"]

    @property
    def carryover(self) -> list[str]:
        return list(self._carryover())
