from collections.abc import Callable, Iterator

import psycopg
from psycopg import sql

from . import Prefix, TypedDatabase


class PostgresDatabase(TypedDatabase[psycopg.Connection]):
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
        on_processed: Callable[[], bool],
        records: Iterator[tuple[int, bytes]],
    ) -> None:
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
                for pkey, r in records:
                    rb = bytearray()
                    rb.extend(jver)
                    rb.extend(r)
                    copy.write_row((pkey.to_bytes(4, "big"), rb))
                    if not on_processed():
                        break

            conn.commit()
