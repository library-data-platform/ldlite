from collections.abc import Iterator
from itertools import count

import duckdb
from psycopg import sql

from . import Prefix, TypedDatabase


class DuckDbDatabase(TypedDatabase[duckdb.DuckDBPyConnection]):
    def _rollback(self, conn: duckdb.DuckDBPyConnection) -> None:
        pass

    @property
    def _default_schema(self) -> str:
        return "main"

    @property
    def _missing_table_error(self) -> type[Exception]:
        return duckdb.CatalogException

    @property
    def _create_raw_table_sql(self) -> sql.SQL:
        return sql.SQL("CREATE TABLE IF NOT EXISTS {table} (__id integer, jsonb text);")

    def ingest_records(
        self,
        prefix: Prefix,
        records: Iterator[bytes],
    ) -> int:
        pkey = count(1)
        with self._conn_factory() as conn:
            self._prepare_raw_table(conn, prefix)

            insert_sql = (
                sql.SQL("INSERT INTO {table} VALUES(?, ?);")
                .format(
                    table=prefix.raw_table_identifier,
                )
                .as_string()
            )
            # duckdb has better performance bulk inserting in a transaction
            with conn.begin() as tx, tx.cursor() as cur:
                for r in records:
                    cur.execute(insert_sql, (next(pkey), r.decode()))
                tx.commit()

        return next(pkey) - 1
