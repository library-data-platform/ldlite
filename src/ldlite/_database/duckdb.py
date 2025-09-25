from collections.abc import Iterator
from itertools import count

import duckdb
from psycopg import sql

from . import Prefix, TypedDatabase


class DuckDbDatabase(TypedDatabase[duckdb.DuckDBPyConnection]):
    @staticmethod
    def _setup_jfuncs(conn: duckdb.DuckDBPyConnection) -> None:
        with conn.cursor() as cur:
            cur.execute(
                r"""
CREATE OR REPLACE FUNCTION ldlite_system.jtype_of(j) AS
    CASE main.json_type(j)
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

CREATE OR REPLACE FUNCTION ldlite_system.jextract(j, p) AS
    CASE ldlite_system.jtype_of(main.json_extract(j, p))
        WHEN 'string' THEN
            CASE
                WHEN lower(main.json_extract_string(j, p)) = 'null' THEN 'null'::JSON
                WHEN length(main.json_extract_string(j, p)) = 0 THEN 'null'::JSON
                ELSE main.json_extract(j, p)
            END
        WHEN 'object' THEN
            CASE
                WHEN main.json_extract_string(j, p) = '{}' THEN 'null'::JSON
                ELSE main.json_extract(j, p)
            END
        WHEN 'array' THEN
            CASE
                WHEN length(list_filter((main.json_extract(j, p))::JSON[], lambda x: x != 'null'::JSON)) = 0 THEN 'null'::JSON
                ELSE list_filter((main.json_extract(j, p))::JSON[], lambda x: x != 'null'::JSON)
            END
        ELSE main.json_extract(j, p)
    END
;

CREATE OR REPLACE FUNCTION ldlite_system.jextract_string(j, p) AS
    main.json_extract_string(ldlite_system.jextract(j, p), '$')
;

CREATE OR REPLACE FUNCTION ldlite_system.jobject_keys(j) AS
    unnest(main.json_keys(j))
;

CREATE OR REPLACE FUNCTION ldlite_system.jis_uuid(j) AS
    CASE ldlite_system.jtype_of(j)
        WHEN 'string' THEN regexp_full_match(main.json_extract_string(j, '$'), '^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[1-5][a-fA-F0-9]{3}-[89abAB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}$')
        ELSE FALSE
    END
;

CREATE OR REPLACE FUNCTION ldlite_system.jis_datetime(j) AS
    CASE ldlite_system.jtype_of(j)
        WHEN 'string' THEN try_strptime(main.json_extract_string(j, '$'), '%Y-%m-%dT%H:%M:%S.%g%z') IS NOT NULL
        ELSE FALSE
    END
;

""",  # noqa: E501
            )

    @property
    def _default_schema(self) -> str:
        return "main"

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
