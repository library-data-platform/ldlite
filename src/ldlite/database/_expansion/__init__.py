from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING

from psycopg import sql

from .nodes import ArrayNode, ObjectNode

if TYPE_CHECKING:
    from .context import ExpandContext


def expand_nonmarc(
    root_name: str,
    root_values: list[str],
    ctx: ExpandContext,
) -> list[str]:
    (_, created_tables) = _expand_nonmarc(
        ObjectNode(root_name, "", None, root_values),
        0,
        ctx,
    )
    return created_tables


def _expand_nonmarc(
    root: ObjectNode,
    count: int,
    ctx: ExpandContext,
) -> tuple[int, list[str]]:
    ctx.scan_progress.total = (ctx.scan_progress.total or 0) + 1
    ctx.scan_progress.refresh()
    ctx.transform_progress.total = (ctx.transform_progress.total or 0) + 1
    ctx.transform_progress.refresh()
    initial_count = count
    ctx.preprocess(ctx.conn, ctx.source_table, [root.identifier])
    has_rows = root.unnest(
        ctx,
        ctx.source_table,
        ctx.get_transform_table(count),
        ctx.source_cte(False),
    )
    ctx.transform_progress.update(1)
    if not has_rows:
        return (0, [])

    expand_children_of = deque([root])
    while expand_children_of:
        on = expand_children_of.popleft()
        if ctx.transform_progress:
            ctx.transform_progress.total += len(on.object_children)
            ctx.transform_progress.refresh()
        for c in on.object_children:
            if len(c.parents) >= ctx.json_depth:
                if c.parent is not None:
                    c.parent.values.append(c.name)
                continue
            ctx.preprocess(ctx.conn, ctx.get_transform_table(count), [c.identifier])
            c.unnest(
                ctx,
                ctx.get_transform_table(count),
                ctx.get_transform_table(count + 1),
                ctx.source_cte(False),
            )
            expand_children_of.append(c)
            count += 1
            ctx.transform_progress.update(1)

    created_tables = []

    new_source_table = ctx.get_transform_table(count)
    arrays = root.descendents_oftype(ArrayNode)
    ctx.transform_progress.total += len(arrays)
    ctx.transform_progress.refresh()
    ctx.preprocess(ctx.conn, new_source_table, [a.identifier for a in arrays])
    for an in arrays:
        if len(an.parents) >= ctx.json_depth:
            continue
        values = an.explode(
            ctx.conn,
            new_source_table,
            ctx.get_transform_table(count + 1),
            ctx.source_cte(True),
        )
        count += 1
        ctx.transform_progress.update(1)

        if an.meta.is_object:
            (sub_index, array_tables) = _expand_nonmarc(
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
            count += sub_index
            created_tables.extend(array_tables)
        else:
            with ctx.conn.cursor() as cur:
                (tname, tid) = ctx.get_output_table(an.name)
                created_tables.append(tname)
                cur.execute(
                    sql.SQL(
                        """
CREATE TABLE {dest_table} AS
"""
                        + ctx.source_cte(False)
                        + """
SELECT {cols} FROM ld_source
""",
                    )
                    .format(
                        dest_table=tid,
                        source_table=ctx.get_transform_table(count),
                        cols=sql.SQL("\n    ,").join(
                            [sql.Identifier(v) for v in [*values, an.name]],
                        ),
                    )
                    .as_string(),
                )

    stamped_values = [
        sql.Identifier(v) for n in root.descendents if n not in arrays for v in n.values
    ]

    with ctx.conn.cursor() as cur:
        (tname, tid) = ctx.get_output_table(root.path)
        created_tables.append(tname)
        cur.execute(
            sql.SQL(
                """
CREATE TABLE {dest_table} AS
"""
                + ctx.source_cte(False)
                + """
SELECT {cols} FROM ld_source
""",
            )
            .format(
                dest_table=tid,
                source_table=new_source_table,
                cols=sql.SQL("\n    ,").join(stamped_values),
            )
            .as_string(),
        )

    return (count + 1 - initial_count, created_tables)
