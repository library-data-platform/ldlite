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
        self.exploded = False
        self.values: list[str] = values or []
        self.children: list[ExpansionNode] = []

    @staticmethod
    def _c_a_s_e(camel: str) -> str:
        return "".join("_" + c.lower() if c.isupper() else c for c in camel)

    def _add_child(self, name: str) -> str:
        snake = self._c_a_s_e(name)
        prefixed_name = self.prefix + snake
        n = ExpansionNode(prefixed_name, snake, self)
        self.children.append(n)
        return prefixed_name

    def _add_value(self, name: str) -> str:
        snake = self._c_a_s_e(name)
        prefixed_name = self.prefix + snake
        self.values.append(prefixed_name)
        return prefixed_name

    def explode(
        self,
        conn: duckdb.DuckDBPyConnection | psycopg.Connection,
        source_table: sql.Identifier,
        dest_table: sql.Identifier,
    ) -> None:
        self.exploded = True
        create_columns: list[sql.Composable] = [
            sql.Identifier(v) for v in self.carryover
        ]

        # TODO: Run ANALYZE on the column for exploding it for postgres
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
WITH
    one_object AS (SELECT {json_col} as json FROM {table} LIMIT 1),
    props AS (SELECT ldlite_system.jobject_keys(json) AS prop FROM one_object),
    prop_meta AS (
        SELECT
            prop
            ,ldlite_system.jtype_of(json->prop) AS json_type
        FROM one_object, props
    )
SELECT
    prop
    ,ANY_VALUE(json_type) as json_type
    ,bool_and({json_col}->>prop ~ '^[a-fA-F0-9]{{8}}-[a-fA-F0-9]{{4}}-[1-5][a-fA-F0-9]{{3}}-[89abAB][a-fA-F0-9]{{3}}-[a-fA-F0-9]{{12}}$') AS is_uuid
FROM {table}, prop_meta
GROUP BY prop
""",  # noqa: E501
                )
                .format(table=source_table, json_col=self.identifier)
                .as_string(),
            )

            for row in cur.fetchall():
                if row[1] == "number":
                    alias = self._add_value(row[0])
                    stmt = sql.SQL("({json_col}->{prop})::numeric AS {prop_alias}")
                elif row[1] == "string" and row[2]:
                    alias = self._add_value(row[0])
                    stmt = sql.SQL("({json_col}->>{prop})::uuid AS {prop_alias}")
                elif row[1] == "object":
                    alias = self._add_child(row[0])
                    stmt = sql.SQL("({json_col}->{prop}) AS {prop_alias}")
                elif row[1] == "array":
                    alias = self._add_value(row[0])
                    stmt = sql.SQL("({json_col}->{prop}) AS {prop_alias}")
                else:
                    alias = self._add_value(row[0])
                    stmt = sql.SQL("({json_col}->>{prop}) AS {prop_alias}")

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

    def _carryover(self) -> Iterator[str]:
        yield from self.root.values

        to_visit = deque([self.root])
        while to_visit:
            n = to_visit.popleft()
            for c in n.children:
                if not c.exploded:
                    yield c.name

                yield from c.values
                to_visit.append(c)

    @property
    def carryover(self) -> list[str]:
        return list(self._carryover())

    def __str__(self) -> str:
        return "->".join([n.name for n in reversed([self, *self.parents])])
