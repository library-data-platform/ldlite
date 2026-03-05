from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, TypeVar

from psycopg import sql

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import NoReturn

    import duckdb
    import psycopg
    from tqdm import tqdm

from .metadata import Metadata

TNode = TypeVar("TNode", bound="ExpansionNode")


class ExpansionNode:
    def __init__(
        self,
        name: str,
        path: str,
        parent: ExpansionNode | None,
        values: list[str] | None = None,
    ):
        self.name = name
        self.path = path
        self.identifier = sql.Identifier(name)
        self.parent = parent
        self.values: list[str] = values or []
        self.children: list[ExpansionNode] = []

    def add(self, meta: Metadata) -> str:
        snake = meta.snake
        prefixed_name = self.prefix + snake

        if meta.is_array:
            self.children.append(ArrayNode(prefixed_name, snake, self, meta))
        elif meta.is_object:
            self.children.append(ObjectNode(prefixed_name, snake, self))
        else:
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
        if len(self.parents) == 0 and len(self.path) == 0:
            return ""

        return (
            "__".join(
                [*[p.path for p in self.parents if len(p.path) != 0], self.path],
            )
            + "__"
        )

    @property
    def root(self) -> ExpansionNode:
        if self.parent is None:
            return self

        root = [p for p in self.parents if p.parent is None]
        return root[0]

    def _descendents(self, cls: type[TNode]) -> Iterator[TNode]:
        to_visit = deque([self])
        while to_visit:
            n = to_visit.pop()
            if isinstance(n, cls):
                yield n

            to_visit.extend(n.children)

    @property
    def descendents(self) -> list[ExpansionNode]:
        return list(self._descendents(ExpansionNode))

    def descendents_oftype(self, cls: type[TNode]) -> list[TNode]:
        return list(self._descendents(cls))

    def __str__(self) -> str:
        return "->".join([n.name for n in reversed([self, *self.parents])])


class ObjectNode(ExpansionNode):
    def __init__(
        self,
        name: str,
        path: str,
        parent: ExpansionNode | None,
        values: list[str] | None = None,
    ):
        super().__init__(name, path, parent, values)
        self.unnested = False

    def _object_children(self) -> Iterator[ObjectNode]:
        for c in self.children:
            if isinstance(c, ObjectNode):
                yield c

    @property
    def object_children(self) -> list[ObjectNode]:
        return list(self._object_children())

    def unnest(
        self,
        conn: duckdb.DuckDBPyConnection | psycopg.Connection,
        source_table: sql.Identifier,
        dest_table: sql.Identifier,
        source_cte: str,
        progress: tqdm[NoReturn] | None,
    ) -> None:
        self.unnested = True
        create_columns: list[sql.Composable] = [
            sql.Identifier(v) for v in self.carryover
        ]

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
SELECT ldlite_system.jobject_keys(j) FROM (
    SELECT {json_col} AS j
    FROM {table}
    WHERE NOT ldlite_system.jis_null({json_col})
    LIMIT 1
)
""",
                )
                .format(table=source_table, json_col=self.identifier)
                .as_string(),
            )
            props = [prop[0] for prop in cur.fetchall()]

        prop_count = len(props)
        if progress is not None:
            progress.total += prop_count * 2

        for prop in props:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
WITH
    values AS (
        SELECT ldlite_system.jextract({json_col}, $1) as ld_value
        FROM {table}
    ),
    value_and_types AS (
        SELECT
            ldlite_system.jtype_of(ld_value) AS json_type
            ,ld_value
        FROM values
        WHERE NOT ldlite_system.jis_null(ld_value)
    ),
    array_values AS (
        SELECT
            ldlite_system.jtype_of(a.ld_value) AS json_type
            ,a.ld_value
        FROM value_and_types v
        CROSS JOIN LATERAL (
            SELECT s.ld_value
            FROM ldlite_system.jexplode(v.ld_value) AS s
            WHERE NOT ldlite_system.jis_null(s.ld_value)
            LIMIT 3
        ) a
        WHERE v.json_type = 'array'
    ),
    all_values AS (
        SELECT
            json_type
            ,ld_value
            ,FALSE AS is_array
        FROM value_and_types
        WHERE json_type <> 'array'
        UNION
        SELECT
            json_type
            ,ld_value
            ,TRUE AS is_array
        FROM array_values
        WHERE NOT ldlite_system.jis_null(ld_value)
    )
SELECT
    STRING_AGG(DISTINCT json_type, '|') AS json_type
    ,bool_and(is_array) AS is_array
    ,bool_and(ldlite_system.jis_uuid(ld_value)) AS is_uuid
    ,bool_and(ldlite_system.jis_datetime(ld_value)) AS is_datetime
    ,bool_and(ldlite_system.jis_float(ld_value)) AS is_float
FROM all_values
HAVING COUNT(*) > 0
""",
                    )
                    .format(table=source_table, json_col=self.identifier)
                    .as_string(),
                    (prop,),
                )
                if (row := cur.fetchone()) is not None:
                    meta = Metadata(prop, *row)
                    create_columns.append(
                        meta.select_column(self.identifier, self.add(meta)),
                    )
                    if progress is not None and meta.is_array:
                        progress.total += 1

                if progress is not None:
                    progress.update(1)

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
CREATE TEMP TABLE {dest_table} ON COMMIT DROP AS
"""
                    + source_cte
                    + """
SELECT
    {cols}
FROM ld_source;
""",
                )
                .format(
                    source_table=source_table,
                    dest_table=dest_table,
                    cols=sql.SQL("\n    ,").join(create_columns),
                )
                .as_string(),
            )
            if progress is not None:
                progress.update(prop_count)

    def _carryover(self) -> Iterator[str]:
        for n in self.root.descendents:
            if isinstance(n, ObjectNode) and not n.unnested:
                yield n.name
            else:
                yield n.name
            yield from n.values

    @property
    def carryover(self) -> list[str]:
        return list(self._carryover())


class ArrayNode(ExpansionNode):
    def __init__(
        self,
        name: str,
        path: str,
        parent: ExpansionNode | None,
        meta: Metadata,
        values: list[str] | None = None,
    ):
        super().__init__(name, path, parent, values)
        self.meta = Metadata(
            "$",
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
        source_cte: str,
        progress: tqdm[NoReturn] | None,
    ) -> list[str]:
        with conn.cursor() as cur:
            o_col = self.name + "__o"
            create_columns: list[sql.Composable] = [
                sql.SQL(
                    "(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))::integer AS __id",
                ),
                *[sql.Identifier(v) for v in self.carryover],
                sql.SQL(
                    "(ROW_NUMBER() OVER (PARTITION BY s.__id))::smallint AS {id_alias}",
                ).format(
                    id_alias=sql.Identifier(o_col),
                ),
                self.meta.select_column(
                    sql.Identifier("a", "ld_value"),
                    self.name,
                ),
            ]

            cur.execute(
                sql.SQL(
                    """
CREATE TEMP TABLE {dest_table} ON COMMIT DROP AS
"""
                    + source_cte
                    + """
SELECT
    {cols}
FROM
    ld_source s
    ,ldlite_system.jexplode({json_col}) a
WHERE NOT ldlite_system.jis_null({json_col})
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
            if progress is not None:
                progress.update(1)

        return ["__id", *self.carryover, o_col]

    def _carryover(self) -> Iterator[str]:
        for n in reversed(self.parents):
            yield from [v for v in n.values if v != "__id"]

    @property
    def carryover(self) -> list[str]:
        return list(self._carryover())
