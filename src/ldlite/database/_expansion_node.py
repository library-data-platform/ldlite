# pyright: reportArgumentType=false
from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING

from psycopg import sql

if TYPE_CHECKING:
    from collections.abc import Iterator

    import duckdb
    import psycopg


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

    def _add_child(self, name: str, ctor: type[ExpansionNode]) -> str:
        snake = self._c_a_s_e(name)
        prefixed_name = self.prefix + snake
        n = ctor(prefixed_name, snake, self)
        self.children.append(n)
        return prefixed_name

    def add_object(self, name: str) -> str:
        return self._add_child(name, ObjectNode)

    def add_array(self, name: str) -> str:
        return self._add_child(name, ArrayNode)

    def add_value(self, name: str) -> str:
        snake = self._c_a_s_e(name)
        prefixed_name = self.prefix + snake
        self.values.append(prefixed_name)
        return prefixed_name

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
    )
SELECT
    prop
    ,ANY_VALUE(ldlite_system.jtype_of(value)) as json_type
    ,bool_and(ldlite_system.jis_uuid(value)) as is_uuid
FROM values
GROUP BY prop
""",
                )
                .format(table=source_table, json_col=self.identifier)
                .as_string(),
            )

            for row in cur.fetchall():
                if row[1] == "number":
                    alias = self.add_value(row[0])
                    stmt = sql.SQL(
                        "(ldlite_system.jextract({json_col}, {prop}))::numeric "
                        "AS {prop_alias}",
                    )
                elif row[1] == "string" and row[2]:
                    alias = self.add_value(row[0])
                    stmt = sql.SQL(
                        "(ldlite_system.jextract_string({json_col}, {prop}))::uuid "
                        "AS {prop_alias}",
                    )
                elif row[1] == "object":
                    alias = self.add_object(row[0])
                    stmt = sql.SQL(
                        "(ldlite_system.jextract({json_col}, {prop})) AS {prop_alias}",
                    )
                elif row[1] == "array":
                    alias = self.add_array(row[0])
                    stmt = sql.SQL(
                        "(ldlite_system.jextract({json_col}, {prop})) AS {prop_alias}",
                    )
                else:
                    alias = self.add_value(row[0])
                    stmt = sql.SQL(
                        "(ldlite_system.jextract_string({json_col}, {prop})) "
                        "AS {prop_alias}",
                    )

                create_columns.append(
                    stmt.format(
                        json_col=self.identifier,
                        prop=sql.Literal(row[0]),
                        prop_alias=sql.Identifier(alias),
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
        values: list[str] | None = None,
    ):
        super().__init__(name, path, parent, values)

    def explode(
        self,
        conn: duckdb.DuckDBPyConnection | psycopg.Connection,
        source_table: sql.Identifier,
        dest_table: sql.Identifier,
    ) -> ExpansionNode:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
CREATE TEMP TABLE {dest_table} ON COMMIT DROP AS SELECT
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS __id
    ,{carryover}
    ,ROW_NUMBER() OVER (PARTITION BY s.__id) AS {id_alias}
    ,a.value AS {alias}
FROM
    {source_table} s
    ,ldlite_system.jexplode({json_col}) a
""",
                )
                .format(
                    dest_table=dest_table,
                    alias=sql.Identifier(self.path or ""),
                    id_alias=sql.Identifier((self.path or "") + "_o"),
                    carryover=sql.SQL("\n    ,").join(
                        [sql.Identifier("s", v) for v in self.carryover],
                    ),
                    source_table=source_table,
                    json_col=sql.Identifier("s", self.name),
                )
                .as_string(),
            )

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT ldlite_system.jtype_of({alias}) FROM {dest_table} LIMIT 1",
                )
                .format(
                    dest_table=dest_table,
                    alias=sql.Identifier(self.path or ""),
                )
                .as_string(),
            )

            if (r := cur.fetchone()) and r[0] == "object":
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
