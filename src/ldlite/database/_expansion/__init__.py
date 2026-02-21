from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING

from psycopg import sql

from .nodes import ArrayNode, ObjectNode

if TYPE_CHECKING:
    from collections.abc import Callable

    import duckdb
    import psycopg


@dataclass
class ExpandContext:
    conn: duckdb.DuckDBPyConnection | psycopg.Connection
    source_table: sql.Identifier
    json_depth: int
    get_transform_table: Callable[[int], sql.Identifier]
    get_output_table: Callable[[str], sql.Identifier]

    def array_context(
        self,
        new_source_table: sql.Identifier,
        new_json_depth: int,
    ) -> ExpandContext:
        return ExpandContext(
            self.conn,
            new_source_table,
            new_json_depth,
            self.get_transform_table,
            self.get_output_table,
        )


def expand_nonmarc(
    root_name: str,
    root_values: list[str],
    ctx: ExpandContext,
) -> None:
    _expand_nonmarc(
        ObjectNode(root_name, "", None, root_values),
        0,
        ctx,
    )


def _expand_nonmarc(
    root: ObjectNode,
    count: int,
    ctx: ExpandContext,
) -> int:
    initial_count = count
    root.unnest(ctx.conn, ctx.source_table, ctx.get_transform_table(count))

    expand_children_of = deque([root])
    while expand_children_of:
        on = expand_children_of.popleft()
        for c in on.object_children:
            if len(c.parents) >= ctx.json_depth:
                if c.parent is not None:
                    c.parent.values.append(c.name)
                continue
            c.unnest(
                ctx.conn,
                ctx.get_transform_table(count),
                ctx.get_transform_table(count + 1),
            )
            expand_children_of.append(c)
            count += 1

    new_source_table = ctx.get_transform_table(count)
    arrays = root.descendents_oftype(ArrayNode)
    for an in arrays:
        if len(an.parents) >= ctx.json_depth:
            continue
        values = an.explode(
            ctx.conn,
            new_source_table,
            ctx.get_transform_table(count + 1),
        )
        count += 1

        if an.meta.json_type == "object":
            count += _expand_nonmarc(
                ObjectNode(
                    an.name,
                    an.name,
                    None,
                    values,
                ),
                count + 1,
                ctx.array_context(
                    ctx.get_transform_table(count),
                    ctx.json_depth - len(an.parents),
                ),
            )
        else:
            with ctx.conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
CREATE TABLE {dest_table} AS
SELECT {cols} FROM {transform_table}
""",
                    )
                    .format(
                        dest_table=ctx.get_output_table(an.name),
                        transform_table=ctx.get_transform_table(count),
                        cols=sql.SQL("\n    ,").join(
                            [sql.Identifier(v) for v in [*values, an.name]],
                        ),
                    )
                    .as_string(),
                )

    stamped_values = [
        sql.Identifier(v)
        for n in set(root.descendents).difference(arrays)
        for v in n.values
    ]

    with ctx.conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
CREATE TABLE {dest_table} AS
SELECT {cols} FROM {transform_table}
""",
            )
            .format(
                dest_table=ctx.get_output_table(root.path),
                transform_table=new_source_table,
                cols=sql.SQL("\n    ,").join(stamped_values),
            )
            .as_string(),
        )

    return count + 1 - initial_count
