from collections.abc import Iterator
from datetime import datetime, timezone
from itertools import count
from typing import TYPE_CHECKING, Any, cast

import duckdb
from psycopg import sql

from ._prefix import Prefix
from ._typed_database import TypedDatabase

if TYPE_CHECKING:
    from typing_extensions import Self


class DuckDbDatabase(TypedDatabase[duckdb.DuckDBPyConnection]):
    def __init__(self, db: duckdb.DuckDBPyConnection) -> None:
        # See the notes below for why we're monkey patching DuckDB
        super().__init__(
            lambda _: cast(
                "duckdb.DuckDBPyConnection",
                _MonkeyDBPyConnection(db.cursor()),
            ),
        )

        with self._conn_factory(True) as cur:
            cur.execute(
                r"""
-- These are shims to be able to use the postgres native operations.
CREATE OR REPLACE FUNCTION jsonb_typeof(j) AS
    CASE coalesce(main.json_type(j), 'NULL')
        WHEN 'VARCHAR' THEN 'string'
        WHEN 'BIGINT' THEN 'number'
        WHEN 'DOUBLE' THEN 'number'
        WHEN 'UBIGINT' THEN 'number'
        WHEN 'OBJECT' THEN 'object'
        WHEN 'BOOLEAN' THEN 'boolean'
        WHEN 'ARRAY' THEN 'array'
        WHEN 'NULL' THEN 'null'
        ELSE main.json_type(j)
    END
;

CREATE OR REPLACE FUNCTION jsonb_object_keys(j) AS TABLE
    SELECT je.key as ld_key, id as "ordinality" FROM json_each(j) je ORDER BY je.id
;

CREATE OR REPLACE FUNCTION jsonb_array_elements(j) AS TABLE (
    SELECT value as ld_value, rowid + 1 AS "ordinality" FROM main.json_each(j)
);
""",
            )

    @property
    def _default_schema(self) -> str:
        return "main"

    @property
    def _create_raw_table_sql(self) -> sql.SQL:
        return sql.SQL("CREATE TABLE IF NOT EXISTS {table} (__id integer, jsonb text);")

    def ingest_records(
        self,
        prefix: str,
        records: Iterator[bytes],
    ) -> int:
        pfx = Prefix(prefix)
        download_started = datetime.now(timezone.utc)
        pkey = count(1)
        with self._conn_factory(False) as conn:
            self._prepare_raw_table(conn, pfx)

            insert_sql = (
                sql.SQL("INSERT INTO {table} VALUES(?, ?);")
                .format(table=pfx.raw_table.id)
                .as_string()
            )
            # duckdb has better performance bulk inserting in a transaction
            with conn.begin() as tx, tx.cursor() as cur:
                for r in records:
                    cur.execute(insert_sql, (next(pkey), r.decode()))

                total = next(pkey) - 1
                self._download_complete(conn, pfx, total, download_started)
                tx.commit()

        return total

    def source_table_cte_stmt(self, keep_source: bool) -> str:  # noqa: ARG002
        return "WITH ld_source AS (SELECT * FROM {source_table})"


# DuckDB has some strong opinions about cursors that are different than postgres
# https://github.com/duckdb/duckdb/issues/11018
class _MonkeyDBPyCursor:
    def __init__(self, cur: duckdb.DuckDBPyConnection) -> None:
        self._cur = cur

    # We're patching out the close on the cursor
    # This is so that temp tables aren't closed with the cursor
    def close(self) -> None:
        return None

    """
    # This exists to quickly print out any sql executed during tests
    # Uncomment in production at your own peril
    def execute(self, *args, **kwargs) -> duckdb.DuckDBPyConnection:
        print(args[0])
        return self._cur.execute(*args, **kwargs)

    """

    def __enter__(self) -> "Self":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]  # noqa: ANN001
        return None

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        return getattr(self._cur, name)


class _MonkeyDBPyConnection:
    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    # This fakes creating a cursor
    # DuckDB doesn't need them and they're each essentially a new connection
    def cursor(self) -> _MonkeyDBPyCursor:
        return _MonkeyDBPyCursor(self._conn)

    def __enter__(self) -> "Self":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]  # noqa: ANN001
        return self.close()

    def close(self) -> None:
        return self._conn.close()

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        return getattr(self._conn, name)
