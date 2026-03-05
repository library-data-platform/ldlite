from collections.abc import Iterator
from itertools import count

import psycopg
from psycopg import sql

from ._prefix import Prefix
from ._typed_database import TypedDatabase


class PostgresDatabase(TypedDatabase[psycopg.Connection]):
    def __init__(self, dsn: str):
        try:
            # RawCursor lets us use $1, $2, etc to use the
            # same sql between duckdb and postgres
            super().__init__(
                lambda: psycopg.connect(
                    dsn,
                    cursor_factory=psycopg.RawCursor,
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

    @staticmethod
    def _setup_jfuncs(conn: psycopg.Connection) -> None:
        with conn.cursor() as cur:
            cur.execute(
                r"""
CREATE OR REPLACE FUNCTION ldlite_system.jtype_of(j JSONB) RETURNS TEXT AS $$
SELECT jsonb_typeof(j);
$$
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE;

CREATE OR REPLACE FUNCTION ldlite_system.jextract(j JSONB, p TEXT) RETURNS JSONB AS $$
SELECT
    CASE jsonb_typeof(val)
        WHEN 'string' THEN
            CASE
                WHEN lower(val #>> '{}') IN ('null', '') THEN 'null'::jsonb
                ELSE val
            END
        WHEN 'array' THEN
            CASE
                WHEN jsonb_array_length(val) = 0 THEN 'null'::jsonb
                WHEN NOT EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(val) AS e(elem)
                    WHERE elem = 'null'::jsonb
                    LIMIT 1
                ) THEN val
                ELSE COALESCE(
                    (
                        SELECT jsonb_agg(e)
                        FROM jsonb_array_elements(val) AS a(e)
                        WHERE e <> 'null'::jsonb
                    ),
                    'null'::jsonb
                )
            END
        WHEN 'object' THEN
            CASE
                WHEN val = '{}'::jsonb THEN 'null'::jsonb
                ELSE val
            END
        ELSE val
    END
FROM (
    -- This is somewhat of a hack.
    -- There isn't a really good way to get the element unchanged
    -- which works for duckdb and postgres AND CRUCIALLY
    -- has a similar syntax to everything else so that we don't
    -- have to have special cases for exploding the array and it
    -- can share all the same type checking / statement generation code.
    -- We're pretending that postgres supports -> '$' style syntax like duckdb.
    SELECT CASE WHEN p = '$' THEN j ELSE j->p END AS val
) s;
$$
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
STRICT;

CREATE OR REPLACE FUNCTION ldlite_system.jextract_string(j JSONB, p TEXT) RETURNS TEXT AS $$
SELECT ldlite_system.jextract(j, p) #>> '{}'
$$
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE;

CREATE OR REPLACE FUNCTION ldlite_system.jobject_keys(j JSONB) RETURNS SETOF TEXT AS $$
SELECT jsonb_object_keys(j);
$$
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE;

CREATE OR REPLACE FUNCTION ldlite_system.jis_uuid(j JSONB) RETURNS BOOLEAN AS $$
SELECT
    CASE
        WHEN jsonb_typeof(j) = 'string' THEN
        (
            WITH v AS (SELECT $1 #>> '{}' AS s)
            SELECT
                LENGTH(s) = 36
                -- Hyphens at canonical positions: 9,14,19,24
                AND substr(s,  9,1) = '-'
                AND substr(s, 14,1) = '-'
                AND substr(s, 19,1) = '-'
                AND substr(s, 24,1) = '-'

                -- Version M at pos 15 must be 1..5
                AND substr(s, 15,1) BETWEEN '1' AND '5'

                -- Variant N at pos 20 must be 8,9,a,b,A,B
                AND substr(s, 20,1) IN ('8','9','a','b','A','B')

                -- All other non-hyphen characters must be hex [0-9a-fA-F]
                AND (
                -- positions 1..8
                    (substr(s,  1,1) BETWEEN '0' AND '9' OR substr(s,  1,1) BETWEEN 'a' AND 'f' OR substr(s,  1,1) BETWEEN 'A' AND 'F') AND
                    (substr(s,  2,1) BETWEEN '0' AND '9' OR substr(s,  2,1) BETWEEN 'a' AND 'f' OR substr(s,  2,1) BETWEEN 'A' AND 'F') AND
                    (substr(s,  3,1) BETWEEN '0' AND '9' OR substr(s,  3,1) BETWEEN 'a' AND 'f' OR substr(s,  3,1) BETWEEN 'A' AND 'F') AND
                    (substr(s,  4,1) BETWEEN '0' AND '9' OR substr(s,  4,1) BETWEEN 'a' AND 'f' OR substr(s,  4,1) BETWEEN 'A' AND 'F') AND
                    (substr(s,  5,1) BETWEEN '0' AND '9' OR substr(s,  5,1) BETWEEN 'a' AND 'f' OR substr(s,  5,1) BETWEEN 'A' AND 'F') AND
                    (substr(s,  6,1) BETWEEN '0' AND '9' OR substr(s,  6,1) BETWEEN 'a' AND 'f' OR substr(s,  6,1) BETWEEN 'A' AND 'F') AND
                    (substr(s,  7,1) BETWEEN '0' AND '9' OR substr(s,  7,1) BETWEEN 'a' AND 'f' OR substr(s,  7,1) BETWEEN 'A' AND 'F') AND
                    (substr(s,  8,1) BETWEEN '0' AND '9' OR substr(s,  8,1) BETWEEN 'a' AND 'f' OR substr(s,  8,1) BETWEEN 'A' AND 'F') AND

                    -- positions 10..13
                    (substr(s, 10,1) BETWEEN '0' AND '9' OR substr(s, 10,1) BETWEEN 'a' AND 'f' OR substr(s, 10,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 11,1) BETWEEN '0' AND '9' OR substr(s, 11,1) BETWEEN 'a' AND 'f' OR substr(s, 11,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 12,1) BETWEEN '0' AND '9' OR substr(s, 12,1) BETWEEN 'a' AND 'f' OR substr(s, 12,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 13,1) BETWEEN '0' AND '9' OR substr(s, 13,1) BETWEEN 'a' AND 'f' OR substr(s, 13,1) BETWEEN 'A' AND 'F') AND

                    -- positions 16..18 (pos 15 is version, already checked)
                    (substr(s, 16,1) BETWEEN '0' AND '9' OR substr(s, 16,1) BETWEEN 'a' AND 'f' OR substr(s, 16,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 17,1) BETWEEN '0' AND '9' OR substr(s, 17,1) BETWEEN 'a' AND 'f' OR substr(s, 17,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 18,1) BETWEEN '0' AND '9' OR substr(s, 18,1) BETWEEN 'a' AND 'f' OR substr(s, 18,1) BETWEEN 'A' AND 'F') AND

                    -- positions 21..23 (pos 20 is variant, already checked)
                    (substr(s, 21,1) BETWEEN '0' AND '9' OR substr(s, 21,1) BETWEEN 'a' AND 'f' OR substr(s, 21,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 22,1) BETWEEN '0' AND '9' OR substr(s, 22,1) BETWEEN 'a' AND 'f' OR substr(s, 22,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 23,1) BETWEEN '0' AND '9' OR substr(s, 23,1) BETWEEN 'a' AND 'f' OR substr(s, 23,1) BETWEEN 'A' AND 'F') AND

                    -- positions 25..36
                    (substr(s, 25,1) BETWEEN '0' AND '9' OR substr(s, 25,1) BETWEEN 'a' AND 'f' OR substr(s, 25,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 26,1) BETWEEN '0' AND '9' OR substr(s, 26,1) BETWEEN 'a' AND 'f' OR substr(s, 26,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 27,1) BETWEEN '0' AND '9' OR substr(s, 27,1) BETWEEN 'a' AND 'f' OR substr(s, 27,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 28,1) BETWEEN '0' AND '9' OR substr(s, 28,1) BETWEEN 'a' AND 'f' OR substr(s, 28,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 29,1) BETWEEN '0' AND '9' OR substr(s, 29,1) BETWEEN 'a' AND 'f' OR substr(s, 29,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 30,1) BETWEEN '0' AND '9' OR substr(s, 30,1) BETWEEN 'a' AND 'f' OR substr(s, 30,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 31,1) BETWEEN '0' AND '9' OR substr(s, 31,1) BETWEEN 'a' AND 'f' OR substr(s, 31,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 32,1) BETWEEN '0' AND '9' OR substr(s, 32,1) BETWEEN 'a' AND 'f' OR substr(s, 32,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 33,1) BETWEEN '0' AND '9' OR substr(s, 33,1) BETWEEN 'a' AND 'f' OR substr(s, 33,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 34,1) BETWEEN '0' AND '9' OR substr(s, 34,1) BETWEEN 'a' AND 'f' OR substr(s, 34,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 35,1) BETWEEN '0' AND '9' OR substr(s, 35,1) BETWEEN 'a' AND 'f' OR substr(s, 35,1) BETWEEN 'A' AND 'F') AND
                    (substr(s, 36,1) BETWEEN '0' AND '9' OR substr(s, 36,1) BETWEEN 'a' AND 'f' OR substr(s, 36,1) BETWEEN 'A' AND 'F')
                )
            FROM v
        )
        ELSE FALSE
    END;
$$
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE;

CREATE OR REPLACE FUNCTION ldlite_system.jis_datetime(j JSONB) RETURNS BOOLEAN AS $$
SELECT
    CASE
        WHEN jsonb_typeof(j) = 'string' THEN
        (
            WITH v AS (SELECT $1 #>> '{}' AS s)
            SELECT
                -- Length must be exactly 29 characters
                length(s) = 29

                -- Fixed punctuation positions
                AND substr(s, 5, 1)  = '-'
                AND substr(s, 8, 1)  = '-'
                AND substr(s, 11, 1) = 'T'
                AND substr(s, 14, 1) = ':'
                AND substr(s, 17, 1) = ':'
                AND substr(s, 20, 1) = '.'
                AND substr(s, 24, 1) = '+'
                AND substr(s, 27, 1) = ':'

                -- YYYY
                AND substr(s, 1, 1) BETWEEN '0' AND '9'
                AND substr(s, 2, 1) BETWEEN '0' AND '9'
                AND substr(s, 3, 1) BETWEEN '0' AND '9'
                AND substr(s, 4, 1) BETWEEN '0' AND '9'

                -- MM
                AND substr(s, 6, 1) BETWEEN '0' AND '9'
                AND substr(s, 7, 1) BETWEEN '0' AND '9'

                -- DD
                AND substr(s, 9, 1) BETWEEN '0' AND '9'
                AND substr(s, 10, 1) BETWEEN '0' AND '9'

                -- HH
                AND substr(s, 12, 1) BETWEEN '0' AND '9'
                AND substr(s, 13, 1) BETWEEN '0' AND '9'

                -- mm
                AND substr(s, 15, 1) BETWEEN '0' AND '9'
                AND substr(s, 16, 1) BETWEEN '0' AND '9'

                -- SS
                AND substr(s, 18, 1) BETWEEN '0' AND '9'
                AND substr(s, 19, 1) BETWEEN '0' AND '9'

                -- mmm
                AND substr(s, 21, 1) BETWEEN '0' AND '9'
                AND substr(s, 22, 1) BETWEEN '0' AND '9'
                AND substr(s, 23, 1) BETWEEN '0' AND '9'

                -- Timezone HH
                AND substr(s, 25, 1) BETWEEN '0' AND '9'
                AND substr(s, 26, 1) BETWEEN '0' AND '9'

                -- Timezone MM
                AND substr(s, 28, 1) BETWEEN '0' AND '9'
                AND substr(s, 29, 1) BETWEEN '0' AND '9'
            FROM v
        )
        ELSE FALSE
    END;
$$
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE;

CREATE OR REPLACE FUNCTION ldlite_system.jis_float(j JSONB) RETURNS BOOLEAN AS $$
SELECT
    CASE
        WHEN jsonb_typeof(j) = 'number' THEN scale((j)::numeric) > 0
        ELSE FALSE
    END;
$$
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE;

CREATE OR REPLACE FUNCTION ldlite_system.jis_null(j JSONB) RETURNS BOOLEAN AS $$
SELECT COALESCE(j = 'null'::jsonb, TRUE);
$$
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE;

CREATE OR REPLACE FUNCTION ldlite_system.jexplode(j JSONB) RETURNS TABLE (ld_value JSONB) AS $$
SELECT * FROM jsonb_array_elements(j);
$$
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE;
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
        prefix: str,
        records: Iterator[bytes],
    ) -> int:
        pfx = Prefix(prefix)
        pkey = count(1)
        with self._conn_factory() as conn:
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

            conn.commit()
        return next(pkey) - 1

    def preprocess_source_table(
        self,
        conn: psycopg.Connection,
        table_name: sql.Identifier,
        column_names: list[sql.Identifier],
    ) -> None:
        if len(column_names) == 0:
            return

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("ANALYZE {table_name} ({column_name})").format(
                    table_name=table_name,
                    column_name=sql.SQL(",").join(column_names),
                ),
            )

    def source_table_cte_stmt(self, keep_source: bool) -> str:
        if keep_source:
            return "WITH ld_source AS (SELECT * FROM {source_table})"
        return "WITH ld_source AS (DELETE FROM {source_table} RETURNING *)"
