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
                """
CREATE OR REPLACE FUNCTION ldlite_system.jextract(j, p) AS
    main.json_extract(j, p)
;

CREATE OR REPLACE FUNCTION ldlite_system.jextract_string(j, p) AS
    main.json_extract_string(j, p)
;

CREATE OR REPLACE FUNCTION ldlite_system.jobject_keys(j) AS
    unnest(main.json_keys(j))
;

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
