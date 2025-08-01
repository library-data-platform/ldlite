"""LDLite is a lightweight reporting tool for FOLIO services.

It is part of the Library Data Platform project and provides basic LDP
functions without requiring the platform to be installed.

LDLite functions include retrieving data from a FOLIO instance, transforming
the data, and storing the data in a reporting database for further querying.

To install LDLite or upgrade to the latest version:

    python -m pip install --upgrade ldlite

Example:
    # Import and initialize LDLite.
    import ldlite
    ld = ldlite.LDLite()

    # Connect to a database.
    db = ld.connect_db(filename='ldlite.db')

    # Connect to FOLIO.
    ld.connect_folio(url='https://folio-etesting-snapshot-kong.ci.folio.org',
                     tenant='diku',
                     user='diku_admin',
                     password='admin')

    # Send a CQL query and store the results in table "g", "g_j", etc.
    ld.query(table='g', path='/groups')

    # Print the result tables.
    ld.select(table='g')
    ld.select(table='g_j')
    # etc.

"""

from __future__ import annotations

import sqlite3
import sys
from typing import TYPE_CHECKING, NoReturn, cast

import duckdb
import psycopg2
from tqdm import tqdm

from ._csv import to_csv
from ._jsonx import Attr, drop_json_tables, transform_json
from ._select import select
from ._sqlx import DBType, as_postgres, autocommit, encode_sql_str, json_type, sqlid
from ._xlsx import to_xlsx
from .folio import FolioClient, FolioParams

if TYPE_CHECKING:
    from _typeshed import dbapi


class LDLite:
    """LDLite contains the primary functionality for reporting."""

    def __init__(self) -> None:
        """Creates an instance of LDLite.

        Example:
            import ldlite

            ld = ldlite.LDLite()

        """
        self._verbose = False
        self._quiet = False
        self.dbtype: DBType = DBType.UNDEFINED
        self.db: dbapi.DBAPIConnection | None = None
        self._folio: FolioClient | None = None
        self.page_size = 1000
        self._okapi_timeout = 60
        self._okapi_max_retries = 2

    def _set_page_size(self, page_size: int) -> None:
        self.page_size = page_size

    def connect_db(
        self,
        filename: str | None = None,
    ) -> duckdb.DuckDBPyConnection:
        """Connects to an embedded database for storing data.

        The optional *filename* designates a local file containing the
        database or where the database will be created if it does not exist.
        If *filename* is not specified, the database will be stored in memory
        and will not be persisted to disk.

        This method returns a connection to the database which can be used to
        submit SQL queries.

        Example:
            db = ld.connect_db(filename='ldlite.db')

        """
        return self._connect_db_duckdb(filename)

    def _connect_db_duckdb(
        self,
        filename: str | None = None,
    ) -> duckdb.DuckDBPyConnection:
        """Connects to an embedded DuckDB database for storing data.

        The optional *filename* designates a local file containing the DuckDB
        database or where the database will be created if it does not exist.
        If *filename* is not specified, the database will be stored in memory
        and will not be persisted to disk.

        This method returns a connection to the database which can be used to
        submit SQL queries.

        Example:
            db = ld.connect_db_duckdb(filename='ldlite.db')

        """
        self.dbtype = DBType.DUCKDB
        fn = filename if filename is not None else ":memory:"
        db = duckdb.connect(database=fn)
        self.db = cast("dbapi.DBAPIConnection", duckdb.connect(database=fn))
        return db

    def connect_db_postgresql(self, dsn: str) -> psycopg2.extensions.connection:
        """Connects to a PostgreSQL database for storing data.

        The data source name is specified by *dsn*.  This method returns a
        connection to the database which can be used to submit SQL queries.
        The returned connection defaults to autocommit mode.

        Example:
            db = ld.connect_db_postgresql(dsn='dbname=ld host=localhost user=ldlite')

        """
        self.dbtype = DBType.POSTGRES
        db = psycopg2.connect(dsn)
        self.db = cast("dbapi.DBAPIConnection", db)
        autocommit(self.db, self.dbtype, True)
        return db

    def experimental_connect_db_sqlite(
        self,
        filename: str | None = None,
    ) -> sqlite3.Connection:
        """Deprecated; this will be removed in the next major release of LDLite.

        Connects to an embedded SQLite database for storing data.

        The optional *filename* designates a local file containing the SQLite
        database or where the database will be created if it does not exist.
        If *filename* is not specified, the database will be stored in memory
        and will not be persisted to disk.

        This method returns a connection to the database which can be used to
        submit SQL queries.

        Example:
            db = ld.connect_db_sqlite(filename='ldlite.db')

        """
        self.dbtype = DBType.SQLITE
        fn = filename if filename is not None else ":memory:"
        self.db = sqlite3.connect(fn)
        autocommit(self.db, self.dbtype, True)
        return self.db

    def _check_folio(self) -> None:
        if self._folio is None:
            msg = "connection to folio not configured: use connect_folio()"
            raise RuntimeError(msg)

    def _check_db(self) -> None:
        if self.db is None:
            msg = "no database connection: use connect_db() or connect_db_postgresql()"
            raise RuntimeError(msg)

    def connect_folio(self, url: str, tenant: str, user: str, password: str) -> None:
        """Connects to a FOLIO instance with a user name and password.

        The *url*, *tenant*, *user*, and *password* settings are FOLIO-specific
        connection parameters.

        Example:
            ld.connect_folio(url='https://folio-etesting-snapshot-kong.ci.folio.org',
                             tenant='diku',
                             user='diku_admin',
                             password='admin')

        """
        if not url.startswith("https://"):
            msg = 'url must begin with "https://"'
            raise ValueError(msg)
        self._folio = FolioClient(FolioParams(url, tenant, user, password))

    def drop_tables(self, table: str) -> None:
        """Drops a specified table and any accompanying tables.

        A table called *table*_jtable is used to retrieve the names of the
        tables created by JSON transformation.

        Example:
            ld.drop_tables('g')

        """
        if self.db is None:
            self._check_db()
            return
        autocommit(self.db, self.dbtype, True)
        schema_table = table.strip().split(".")
        if len(schema_table) < 1 or len(schema_table) > 2:
            raise ValueError("invalid table name: " + table)
        self._check_db()
        cur = self.db.cursor()
        try:
            cur.execute("DROP TABLE IF EXISTS " + sqlid(table))
        except (RuntimeError, psycopg2.Error):
            pass
        finally:
            cur.close()
        drop_json_tables(self.db, table)

    def set_folio_max_retries(self, max_retries: int) -> None:
        """Sets the maximum number of retries for FOLIO requests.

        This method changes the configured maximum number of retries which is
        initially set to 2.  The *max_retries* parameter is the new value.

        Note that a request is only retried if a timeout occurs.

        Example:
            ld.set_folio_max_retries(5)

        """
        self._set_okapi_max_retries(max_retries)

    def _set_okapi_max_retries(self, max_retries: int) -> None:
        self._okapi_max_retries = max_retries

    def set_folio_timeout(self, timeout: int) -> None:
        """Sets the timeout for connections to FOLIO.

        This method changes the configured timeout which is initially set to 60
        seconds.  The *timeout* parameter is the new timeout in seconds.

        Example:
            ld.set_folio_timeout(300)

        """
        self._set_okapi_timeout(timeout)

    def _set_okapi_timeout(self, timeout: int) -> None:
        self._okapi_timeout = timeout

    def query(  # noqa: C901, PLR0912, PLR0913, PLR0915
        self,
        table: str,
        path: str,
        query: str | None = None,
        json_depth: int = 3,
        limit: int | None = None,
        transform: bool | None = None,
    ) -> list[str]:
        """Submits a query to a FOLIO module, and transforms and stores the result.

        The retrieved result is stored in *table* within the reporting
        database.  the *table* name may include a schema name;
        however, if the database is SQLite, which does not support
        schemas, the schema name will be added to the table name as a
        prefix.

        The *path* parameter is the request path.

        If *query* is a string, it is assumed to be a CQL or similar
        query and is encoded as query=*query*.  If *query* is a
        dictionary, it is interpreted as a set of query parameters.
        Each value of the dictionary must be either a string or a list
        of strings.  If a string, it is encoded as key=value.  If a
        list of strings, it is encoded as key=value1&key=value2&...

        By default JSON data are transformed into one or more tables
        that are created in addition to *table*.  New tables overwrite
        any existing tables having the same name.  If *json_depth* is
        specified within the range 0 < *json_depth* < 5, this
        determines how far into nested JSON data the transformation
        will descend.  (The default is 3.)  If *json_depth* is
        specified as 0, JSON data are not transformed.

        If *limit* is specified, then only up to *limit* records are
        retrieved.

        The *transform* parameter is no longer supported and will be
        removed in the future.  Instead, specify *json_depth* as 0 to
        disable JSON transformation.

        This method returns a list of newly created tables, or raises
        ValueError or RuntimeError.

        Example:
            ld.query(table='g', path='/groups')

        """
        if transform is not None:
            msg = (
                "transform is no longer supported: "
                "use json_depth=0 to disable JSON transformation"
            )
            raise ValueError(msg)
        if query == "cql.allRecords=1 sortby id":
            # this is everywhere in the docs but isn't necessary with the new client
            query = None
        schema_table = table.split(".")
        if len(schema_table) != 1 and len(schema_table) != 2:
            raise ValueError("invalid table name: " + table)
        if json_depth is None or json_depth < 0 or json_depth > 4:
            raise ValueError("invalid value for json_depth: " + str(json_depth))
        if self._folio is None:
            self._check_folio()
            return []
        if self.db is None:
            self._check_db()
            return []
        if len(schema_table) == 2 and self.dbtype == DBType.SQLITE:
            table = schema_table[0] + "_" + schema_table[1]
            schema_table = [table]
        if not self._quiet:
            print("ldlite: querying: " + path, file=sys.stderr)
        drop_json_tables(self.db, table)
        autocommit(self.db, self.dbtype, False)
        try:
            cur = self.db.cursor()
            try:
                if len(schema_table) == 2:
                    cur.execute("CREATE SCHEMA IF NOT EXISTS " + sqlid(schema_table[0]))
                cur.execute("DROP TABLE IF EXISTS " + sqlid(table))
                cur.execute(
                    "CREATE TABLE "
                    + sqlid(table)
                    + "(__id integer, jsonb "
                    + json_type(self.dbtype)
                    + ")",
                )
            finally:
                cur.close()
            self.db.commit()
            # First get total number of records
            records = self._folio.iterate_records(
                path,
                self._okapi_timeout,
                self._okapi_max_retries,
                self.page_size,
                query=query,
            )

            (total_records, _) = next(records)
            total = total_records if total_records is not None else 0
            if self._verbose:
                print("ldlite: estimated row count: " + str(total), file=sys.stderr)
            # Read result pages
            pbar = None
            pbartotal = 0
            if not self._quiet:
                if total == -1:
                    pbar = tqdm(
                        desc="reading",
                        leave=False,
                        mininterval=3,
                        smoothing=0,
                        colour="#A9A9A9",
                        bar_format="{desc} {elapsed} {bar}{postfix}",
                    )
                else:
                    pbar = tqdm(
                        desc="reading",
                        total=total,
                        leave=False,
                        mininterval=3,
                        smoothing=0,
                        colour="#A9A9A9",
                        bar_format="{desc} {bar}{postfix}",
                    )
            cur = self.db.cursor()
            try:
                count = 0
                for pkey, d in records:
                    cur.execute(
                        "INSERT INTO "
                        + sqlid(table)
                        + " VALUES("
                        + str(pkey)
                        + ","
                        + encode_sql_str(self.dbtype, d)
                        + ")",
                    )
                    count += 1
                    if pbar is not None:
                        if pbartotal + 1 > total:
                            pbartotal = total
                            pbar.update(total - pbartotal)
                        else:
                            pbartotal += 1
                            pbar.update(1)
                    if limit is not None and count == limit:
                        break
            finally:
                cur.close()
            if pbar is not None:
                pbar.close()
            self.db.commit()
            newtables = [table]
            newattrs = {}
            if json_depth > 0:
                jsontables, jsonattrs = transform_json(
                    self.db,
                    self.dbtype,
                    table,
                    count,
                    self._quiet,
                    json_depth,
                )
                newtables += jsontables
                newattrs = jsonattrs
                for t in newattrs:
                    newattrs[t]["__id"] = Attr("__id", "bigint")
                newattrs[table] = {"__id": Attr("__id", "bigint")}
        finally:
            autocommit(self.db, self.dbtype, True)
        # Create indexes (for postgres)
        # This broke in the 0.34.0 release but is probably better broken
        if False:
            index_total = sum(map(len, newattrs.values()))
            if not self._quiet:
                pbar = tqdm(
                    desc="indexing",
                    total=index_total,
                    leave=False,
                    mininterval=3,
                    smoothing=0,
                    colour="#A9A9A9",
                    bar_format="{desc} {bar}{postfix}",
                )
                pbartotal = 0
            for t, attrs in newattrs.items():
                for attr in attrs.values():
                    cur = self.db.cursor()
                    try:
                        cur.execute(
                            "CREATE INDEX ON "
                            + sqlid(t)
                            + " ("
                            + sqlid(attr.name)
                            + ")",
                        )
                    except (RuntimeError, psycopg2.Error):
                        pass
                    finally:
                        cur.close()
                    if pbar is not None:
                        pbartotal += 1
                        pbar.update(1)
            if pbar is not None:
                pbar.close()
        # Return table names
        if not self._quiet:
            print("ldlite: created tables: " + ", ".join(newtables), file=sys.stderr)
        return newtables

    def quiet(self, enable: bool) -> None:
        """Configures suppression of progress messages.

        If *enable* is True, progress messages are suppressed; if False, they
        are not suppressed.

        Example:
            ld.quiet(True)

        """
        if enable and self._verbose:
            msg = '"verbose" and "quiet" modes cannot both be enabled'
            raise ValueError(msg)
        self._quiet = enable

    def select(
        self,
        table: str,
        columns: list[str] | None = None,
        limit: int | None = None,
    ) -> None:
        """Prints rows of a table in the reporting database.

        By default all rows and columns of *table* are printed to standard
        output.  If *columns* is specified, then only the named columns are
        printed.  If *limit* is specified, then only up to *limit* rows are
        printed.

        Examples:
            ld.select(table='loans', limit=10)
            ld.select(table='loans', columns=['id', 'item_id', 'loan_date'])

        """
        if self.db is None:
            self._check_db()
            return

        f = sys.stdout
        if self._verbose:
            print("ldlite: reading from table: " + table, file=sys.stderr)
        autocommit(self.db, self.dbtype, False)
        try:
            select(self.db, self.dbtype, table, columns, limit, f)
            if (pgdb := as_postgres(self.db, self.dbtype)) is not None:
                pgdb.rollback()
        finally:
            autocommit(self.db, self.dbtype, True)

    def export_csv(self, filename: str, table: str, header: bool = True) -> None:
        """Export a table in the reporting database to a CSV file.

        All rows of *table* are exported to *filename*, or *filename*.csv if
        *filename* does not have an extension.

        If *header* is True (the default), the CSV file will begin with a
        header line containing the column names.

        Example:
            ld.to_csv(table='g', filename='g.csv')

        """
        if self.db is None:
            self._check_db()
            return

        autocommit(self.db, self.dbtype, False)
        try:
            to_csv(self.db, self.dbtype, table, filename, header)
            if (pgdb := as_postgres(self.db, self.dbtype)) is not None:
                pgdb.rollback()
        finally:
            autocommit(self.db, self.dbtype, True)

    def to_csv(self) -> NoReturn:  # pragma: nocover
        """Deprecated; use export_csv()."""
        msg = "to_csv() is no longer supported: use export_csv()"
        raise ValueError(msg)

    def export_excel(
        self,
        filename: str,
        table: str,
        header: bool = True,
    ) -> None:  # pragma: nocover
        """Deprecated; this will be removed in the next major release of LDLite.

        Export a table in the reporting database to an Excel file.

        All rows of *table* are exported to *filename*, or *filename*.xlsx if
        *filename* does not have an extension.

        If *header* is True (the default), the worksheet will begin with a row
        containing the column names.

        Example:
            ld.export_excel(table='g', filename='g')

        """
        if self.db is None:
            self._check_db()
            return

        autocommit(self.db, self.dbtype, False)
        try:
            to_xlsx(self.db, self.dbtype, table, filename, header)
            if (pgdb := as_postgres(self.db, self.dbtype)) is not None:
                pgdb.rollback()
        finally:
            autocommit(self.db, self.dbtype, True)

    def to_xlsx(self) -> NoReturn:  # pragma: nocover
        """Deprecated; use export_excel()."""
        msg = "to_xlsx() is no longer supported: use export_excel()"
        raise ValueError(msg)

    def verbose(self, enable: bool) -> None:
        """Configures verbose output.

        If *enable* is True, verbose output is enabled; if False, it is
        disabled.

        Example:
            ld.verbose(True)

        """
        if enable and self._quiet:
            msg = '"verbose" and "quiet" modes cannot both be enabled'
            raise ValueError(msg)
        self._verbose = enable


if __name__ == "__main__":
    pass
