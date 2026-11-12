from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, LiteralString, cast

import duckdb
import psycopg
from psycopg import sql

if TYPE_CHECKING:
    from typing import TypeAlias


Conn: TypeAlias = duckdb.DuckDBPyConnection | psycopg.Connection


def literalize(stmt: sql.Composed) -> LiteralString:
    # There isn't an overlap in types between duckdb/psycopg for execute
    # This is defined twice rather than importing from a parent package
    return cast("LiteralString", stmt.as_string())


@dataclass
class TableCreationSql:
    output_path: str | None
    parent_path: str | None
    _columns: list[sql.Composable]
    _source_stmt: sql.SQL
    _source_table: sql.Identifier

    def stamp(
        self,
        create_table_preamble: sql.SQL,
        output_table: sql.Identifier,
        parent_table: sql.Identifier,
    ) -> sql.Composed:
        return (
            create_table_preamble.format(table=output_table)
            + sql.SQL("\nSELECT\n")
            + sql.SQL("\n    ,").join(self._columns)
            + self._source_stmt.format(
                source_table=self._source_table,
                parent_table=parent_table,
            )
            + sql.SQL("""
;ANALYZE {output_table} (__id);
""").format(output_table=output_table)
        )


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
