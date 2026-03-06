from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, NoReturn

if TYPE_CHECKING:
    from collections.abc import Callable

    import duckdb
    import psycopg
    from psycopg import sql
    from tqdm import tqdm


@dataclass
class ExpandContext:
    conn: duckdb.DuckDBPyConnection | psycopg.Connection
    source_table: sql.Identifier
    json_depth: int
    get_transform_table: Callable[[int], sql.Identifier]
    get_output_table: Callable[[str], tuple[str, sql.Identifier]]
    # This is necessary for Analyzing the table in pg before querying it
    # I don't love how this is implemented
    preprocess: Callable[
        [
            duckdb.DuckDBPyConnection | psycopg.Connection,
            sql.Identifier,
            list[sql.Identifier],
        ],
        None,
    ]
    # source_cte will go away when DuckDB implements CTAS RETURNING
    source_cte: Callable[[bool], str]
    scan_progress: tqdm[NoReturn]
    transform_progress: tqdm[NoReturn]

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
            self.preprocess,
            self.source_cte,
            self.scan_progress,
            self.transform_progress,
        )
