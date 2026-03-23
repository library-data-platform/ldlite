from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, TypeVar, cast

from psycopg import sql

if TYPE_CHECKING:
    from collections.abc import Iterator

    import duckdb
    import psycopg

    from .context import ExpandContext

from .metadata import (
    ArrayMeta,
    Metadata,
    MixedArrayMeta,
    MixedMeta,
    ObjectArrayMeta,
    ObjectMeta,
    TypedArrayMeta,
    TypedMeta,
)

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

        if isinstance(meta, ArrayMeta):
            self.children.append(ArrayNode(prefixed_name, snake, self, meta))
        elif isinstance(meta, ObjectMeta):
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
        ctx: ExpandContext,
        source_table: sql.Identifier,
        dest_table: sql.Identifier,
        source_cte: str,
    ) -> bool:
        self.unnested = True
        create_columns: list[sql.Composable] = [
            sql.Identifier(v) for v in self.carryover
        ]

        with ctx.conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT 1 FROM {table} LIMIT 1;")
                .format(table=source_table)
                .as_string(),
            )
            if not cur.fetchone():
                return False

        with ctx.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
SELECT k.ld_key
FROM
    {source_table} t
    ,jsonb_object_keys(t.{json_col}) WITH ORDINALITY k(ld_key, "ordinality")
WHERE t.{json_col} IS NOT NULL AND jsonb_typeof(t.{json_col}) = 'object'
GROUP BY k.ld_key
ORDER BY MAX(k.ordinality), COUNT(k.ordinality)
""",
                )
                .format(source_table=source_table, json_col=self.identifier)
                .as_string(),
            )
            props = [prop[0] for prop in cur.fetchall()]

        ctx.scan_progress.total += len(props) * 3
        ctx.scan_progress.refresh()
        ctx.scan_progress.update(1)

        metadata: list[Metadata] = []
        for prop in props:
            with ctx.conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
SELECT
    BOOL_AND(json_type = 'array') AS only_array
    ,BOOL_OR(json_type = 'array') AS some_array
    ,BOOL_AND(json_type = 'object') AS only_object
    ,BOOL_OR(json_type = 'object') AS some_object
FROM
(
    SELECT jsonb_typeof(t.{json_col}->$1) AS json_type
    FROM {table} t
) j
WHERE json_type <> 'null'
""",
                    )
                    .format(
                        table=source_table,
                        json_col=self.identifier,
                    )
                    .as_string(),
                    (prop,),
                )
                (only_array, some_array, only_object, some_object) = cast(
                    "tuple[bool, bool, bool, bool]",
                    cur.fetchone(),
                )

                if (some_array and not only_array) or (some_object and not only_object):
                    metadata.append(MixedMeta(prop))
                    ctx.scan_progress.update(3)
                    continue

                if only_object:
                    metadata.append(ObjectMeta(prop))
                    ctx.scan_progress.total += 1
                    ctx.scan_progress.update(3)
                    continue

                if only_array:
                    ctx.scan_progress.update(1)
                    cur.execute(
                        sql.SQL(
                            """
SELECT
    -- Technically arrays could be nested but I haven't seen any
    BOOL_AND(json_type = 'object') AS only_object
    ,BOOL_OR(json_type = 'object') AS some_object
FROM
(
    SELECT a.json_type
    FROM {table} t
    CROSS JOIN LATERAL
    (
        SELECT jsonb_typeof(ld_value) AS json_type
        FROM jsonb_array_elements(t.{json_col}->$1) a(ld_value)
        WHERE jsonb_typeof(t.{json_col}->$1) = 'array'
    ) a
) j
WHERE json_type <> 'null'
""",
                        )
                        .format(
                            table=source_table,
                            json_col=self.identifier,
                        )
                        .as_string(),
                        (prop,),
                    )
                    (inner_only_object, inner_some_object) = cast(
                        "tuple[bool, bool]",
                        cur.fetchone(),
                    )

                    if inner_some_object and not inner_only_object:
                        metadata.append(MixedArrayMeta(prop))
                        ctx.scan_progress.update(2)
                        continue

                    if inner_only_object:
                        metadata.append(ObjectArrayMeta(prop))
                        ctx.scan_progress.total += 1
                        ctx.scan_progress.update(2)
                        continue

                    ctx.scan_progress.update(1)
                    typed_from_sql = """
FROM {table} t
CROSS JOIN LATERAL
(
    SELECT *
    FROM jsonb_array_elements(t.{json_col}->$1) a(ld_value)
    WHERE jsonb_typeof(t.{json_col}->$1) = 'array'
    LIMIT 3
) j"""
                else:
                    ctx.scan_progress.update(2)
                    typed_from_sql = """
FROM (SELECT t.{json_col}->$1 AS ld_value FROM {table} t) j
"""
            with ctx.conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
SELECT
    MIN(json_type) AS json_type
    ,MAX(json_type) AS other_json_type
    ,BOOL_AND(CASE WHEN json_type = 'string' THEN (ld_value)::text LIKE '"________-____-____-____-____________"' ELSE FALSE END) AS is_uuid
    ,BOOL_AND(CASE WHEN json_type = 'string' THEN (ld_value)::text LIKE '"____-__-__T__:__:__.___%"' ELSE FALSE END) AS is_datetime
    ,BOOL_OR(CASE WHEN json_type = 'number' THEN (ld_value)::numeric % 1 <> 0 ELSE FALSE END) AS is_float
    ,BOOL_OR(CASE WHEN json_type = 'number' THEN (ld_value)::numeric > 2147483647 ELSE FALSE END) AS is_bigint
FROM
(
    SELECT
        ld_value
        ,jsonb_typeof(ld_value) json_type """  # noqa: E501
                        + typed_from_sql
                        + """
        WHERE ld_value IS NOT NULL
) i
WHERE
    ld_value IS NOT NULL AND
    json_type <> 'null' AND
    (
        json_type <> 'string' OR
        (json_type = 'string' AND ld_value::text NOT IN ('"null"', '""'))
    )
""",
                    )
                    .format(
                        table=source_table,
                        json_col=self.identifier,
                    )
                    .as_string(),
                    (prop,),
                )
                if (row := cur.fetchone()) is not None and all(
                    c is not None for c in row
                ):
                    metadata.append(
                        TypedArrayMeta(prop, *row)
                        if only_array
                        else TypedMeta(prop, *row),
                    )
                ctx.scan_progress.update(1)

        create_columns.extend(
            [meta.select_column(self.identifier, self.add(meta)) for meta in metadata],
        )

        with ctx.conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
CREATE TEMP TABLE {dest_table} AS
"""
                    + source_cte
                    + """
SELECT
    {cols}
FROM ld_source
""",
                )
                .format(
                    source_table=source_table,
                    dest_table=dest_table,
                    json_col=self.identifier,
                    cols=sql.SQL("\n    ,").join(create_columns),
                )
                .as_string(),
            )

        return True

    def _carryover(self) -> Iterator[str]:
        for n in self.root.descendents:
            if isinstance(n, ObjectNode) and not n.unnested and n.name != "jsonb":
                yield n.name
            if isinstance(n, ArrayNode):
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
        meta: ArrayMeta,
        values: list[str] | None = None,
    ):
        super().__init__(name, path, parent, values)
        self.meta = meta.unwrap()

    @property
    def is_object(self) -> bool:
        return isinstance(self.meta, ObjectMeta)

    def explode(
        self,
        conn: duckdb.DuckDBPyConnection | psycopg.Connection,
        source_table: sql.Identifier,
        dest_table: sql.Identifier,
        source_cte: str,
    ) -> list[str]:
        with conn.cursor() as cur:
            o_col = self.name + "__o"
            create_columns: list[sql.Composable] = [
                sql.SQL(
                    "(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))::integer AS __id",
                ),
                *[sql.Identifier(v) for v in self.carryover],
                sql.SQL(
                    """a."ordinality"::smallint AS {id_alias}""",
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
CREATE TEMP TABLE {dest_table} AS
"""
                    + source_cte
                    + """
SELECT
    {cols}
FROM
    ld_source s
    ,jsonb_array_elements(s.{json_col}) WITH ORDINALITY a(ld_value, "ordinality")
WHERE jsonb_typeof(s.{json_col}) = 'array'
""",
                )
                .format(
                    source_table=source_table,
                    dest_table=dest_table,
                    cols=sql.SQL("\n    ,").join(create_columns),
                    json_col=sql.Identifier(self.name),
                )
                .as_string(),
            )

        return ["__id", *self.carryover, o_col]

    def _carryover(self) -> Iterator[str]:
        for n in reversed(self.parents):
            yield from [v for v in n.values if v not in ("__id", "jsonb")]

    @property
    def carryover(self) -> list[str]:
        return list(self._carryover())
