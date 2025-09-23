from collections.abc import Iterator
from itertools import count

import psycopg
from psycopg import sql

from . import Prefix, TypedDatabase


class PostgresDatabase(TypedDatabase[psycopg.Connection]):
    def __init__(self, dsn: str):
        # RawCursor lets us use $1, $2, etc to use the
        # same sql between duckdb and postgres
        super().__init__(lambda: psycopg.connect(dsn, cursor_factory=psycopg.RawCursor))

    def _rollback(self, conn: psycopg.Connection) -> None:
        conn.rollback()

    @property
    def _missing_table_error(self) -> type[Exception]:
        return psycopg.errors.UndefinedTable

    @property
    def _create_raw_table_sql(self) -> sql.SQL:
        return sql.SQL(
            "CREATE TABLE IF NOT EXISTS {table} (__id integer, jsonb jsonb);",
        )

    def ingest_records(
        self,
        prefix: Prefix,
        records: Iterator[bytes],
    ) -> int:
        pkey = count(1)
        with self._conn_factory() as conn:
            self._prepare_raw_table(conn, prefix)

            with (
                conn.cursor() as cur,
                cur.copy(
                    sql.SQL(
                        "COPY {table} (__id, jsonb) FROM STDIN (FORMAT BINARY)",
                    ).format(table=prefix.raw_table_name),
                ) as copy,
            ):
                # postgres jsonb is always version 1
                # and it always goes in front
                jver = (1).to_bytes(1, "big")
                for r in records:
                    rb = bytearray()
                    rb.extend(jver)
                    rb.extend(r)
                    copy.write_row((next(pkey).to_bytes(4, "big"), rb))

            conn.commit()
        return next(pkey) - 1
