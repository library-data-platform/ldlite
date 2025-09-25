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

    @staticmethod
    def _setup_jfuncs(conn: psycopg.Connection) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
CREATE OR REPLACE FUNCTION ldlite_system.jtype_of(j JSONB) RETURNS TEXT AS $$
BEGIN
    RETURN jsonb_typeof(j);
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ldlite_system.jextract(j JSONB, p TEXT) RETURNS JSONB AS $$
BEGIN
    RETURN CASE
        WHEN ldlite_system.jtype_of(j->p) = 'string' THEN
            CASE
                WHEN lower(j->>p) = 'null' THEN 'null'::JSONB
                ELSE j->p
            END
        WHEN ldlite_system.jtype_of(j->p) = 'array' THEN
            CASE
                WHEN jsonb_array_length(jsonb_path_query_array(j->p, '$[*] ? (@ != null)')) = 0 THEN 'null'::JSONB
                ELSE jsonb_path_query_array(j->p, '$[*] ? (@ != null)')
            END
        WHEN ldlite_system.jtype_of(j->p) = 'object' THEN
            CASE
                WHEN j->>p = '{}' THEN 'null'::JSONB
                ELSE j->p
            END
        ELSE j->p
    END;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ldlite_system.jextract_string(j JSONB, p TEXT) RETURNS TEXT AS $$
BEGIN
    RETURN ldlite_system.jextract(j, p) ->> 0;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ldlite_system.jobject_keys(j JSONB) RETURNS SETOF TEXT AS $$
BEGIN
    RETURN QUERY SELECT jsonb_object_keys(j);
END
$$ LANGUAGE plpgsql;

""",  # noqa: E501
            )

    @property
    def _default_schema(self) -> str:
        return "public"

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
                    ).format(table=prefix.raw_table_identifier),
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
