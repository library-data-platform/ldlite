from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from itertools import count
from typing import TYPE_CHECKING

import psycopg
from psycopg import sql

from ._prefix import Prefix
from ._typed_database import TypedDatabase

if TYPE_CHECKING:
    from collections.abc import Iterator


class PostgresDatabase(TypedDatabase[psycopg.Connection]):
    def __init__(self, dsn: str):
        try:
            # RawCursor lets us use $1, $2, etc to use the
            # same sql between duckdb and postgres
            super().__init__(
                lambda transact: psycopg.connect(
                    dsn,
                    cursor_factory=psycopg.RawCursor,
                    autocommit=not transact,
                ),
            )
        except psycopg.errors.UniqueViolation:
            # postgres throws a couple of errors when multiple threads try to create
            # the same resource even if CREATE IF NOT EXISTS was used
            # if we get it then something else created the ldlite_system
            # resources and it is ok to not commit
            pass
        except psycopg.errors.InternalError_ as e:
            if str(e) != "tuple concurrently updated":
                raise

    @property
    def _default_schema(self) -> str:
        return "public"

    @property
    def _create_raw_table_sql(self) -> sql.SQL:
        return sql.SQL(
            "CREATE TABLE IF NOT EXISTS {table} (__id integer, jsonb jsonb);",
        )

    @contextmanager
    def _begin(self, conn: psycopg.Connection) -> Iterator[None]:
        with conn.transaction():
            yield

    def ingest_records(
        self,
        prefix: str,
        records: Iterator[bytes],
    ) -> int:
        pfx = Prefix(prefix)
        download_started = datetime.now(timezone.utc)
        pkey = count(1)
        with self._conn_factory(True) as conn:
            self._prepare_raw_table(conn, pfx)

            with (
                conn.cursor() as cur,
                cur.copy(
                    sql.SQL(
                        "COPY {table} (__id, jsonb) FROM STDIN (FORMAT BINARY)",
                    ).format(table=pfx.raw_table.id),
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

            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("ANALYZE {table} (jsonb);").format(table=pfx.raw_table.id),
                )

            total = next(pkey) - 1
            self._download_complete(conn, pfx, total, download_started)
            conn.commit()

        return next(pkey) - 1
