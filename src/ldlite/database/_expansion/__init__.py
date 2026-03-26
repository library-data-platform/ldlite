from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from typing import NoReturn

    from psycopg import sql
    from tqdm import tqdm


from .node import Conn, Node
from .recursive_nodes import ArrayNode, ObjectNode, RecursiveNode, RootNode


def _non_srs_statements(
    conn: Conn,
    source_table: sql.Identifier,
    output_table: Callable[[str | None], tuple[str, sql.Identifier]],
    json_depth: int,
    scan_progress: tqdm[NoReturn],
) -> Iterator[tuple[str, sql.Composed]]:
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
    scan_progress.total = scan_progress.total if scan_progress.total is not None else 1

    root = RootNode(source_table, output_table)
    onodes: deque[ObjectNode] = deque([root])
    while onodes:
        o = onodes.popleft()
        if o.depth < json_depth:
            o.load_columns(conn)
            scan_progress.total += len(o.direct(Node))
        else:
            o.make_jsonb()
        scan_progress.update(1)

        onodes.extend(o.direct(ObjectNode))
        anodes = deque(o.direct(ArrayNode))
        while anodes:
            a = anodes.popleft()
            if a.depth < json_depth:
                if n := a.make_temp(conn):
                    if isinstance(n, ObjectNode):
                        onodes.append(n)
                    if isinstance(n, ArrayNode):
                        anodes.append(n)
                    scan_progress.total += 1
                else:
                    cast("RecursiveNode", a.parent).remove(a)
            else:
                a.make_jsonb()

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
    json_depth: int,
    scan_progress: tqdm[NoReturn],
) -> list[tuple[str, sql.Composed]]:
    return list(
        _non_srs_statements(
            conn,
            source_table,
            output_table,
            json_depth,
            scan_progress,
        ),
    )
