from collections.abc import Callable, Iterator

import duckdb
from psycopg import sql

from . import Prefix, TypedDatabase


class DuckDbDatabase(TypedDatabase[duckdb.DuckDBPyConnection]):
    def _rollback(self, conn: duckdb.DuckDBPyConnection) -> None:
        pass

    @property
    def _missing_table_error(self) -> type[Exception]:
        return duckdb.CatalogException

    @property
    def _create_raw_table_sql(self) -> sql.SQL:
        return sql.SQL("CREATE TABLE IF NOT EXISTS {table} (__id integer, jsonb text);")

    def ingest_records(
        self,
        prefix: Prefix,
        on_processed: Callable[[], bool],
        records: Iterator[tuple[int, bytes]],
    ) -> None:
        with self._conn_factory() as conn:
            self._prepare_raw_table(conn, prefix)

            insert_sql = (
                sql.SQL("INSERT INTO {table} VALUES(?, ?);")
                .format(
                    table=prefix.raw_table_name,
                )
                .as_string()
            )
            with conn.cursor() as cur:
                for pkey, r in records:
                    cur.execute(insert_sql, (pkey, r.decode()))
                    if not on_processed():
                        break
