from __future__ import annotations

from typing import TYPE_CHECKING, LiteralString, cast

import duckdb
import psycopg

if TYPE_CHECKING:
    from typing import TypeAlias

    from psycopg import sql


Conn: TypeAlias = duckdb.DuckDBPyConnection | psycopg.Connection


def literalize(stmt: sql.Composed) -> LiteralString:
    # There isn't an overlap in types between duckdb/psycopg for execute
    # This is defined twice rather than importing from a parent package
    return cast("LiteralString", stmt.as_string())


class Node:
    def __init__(self, source: sql.Identifier, prop: str | None):
        self.source = source
        self.prop = prop
        self.snake_prop: str | None = None

        if self.prop is not None:
            self.snake_prop = "".join(
                "_" + c.lower() if c.isupper() else c for c in self.prop
            )

            # there's also sorts of weird edge cases here that don't come up in practice
            if (
                (naked := self.prop.lstrip("_"))
                and len(naked) > 0
                and naked[0].isupper()
            ):
                self.snake_prop = self.snake_prop.removeprefix("_")
