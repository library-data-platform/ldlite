"""
LDLite is a lightweight reporting tool for Okapi-based services.  It is part of
the Library Data Platform project and provides basic LDP functions without
requiring the platform to be installed.

LDLite functions include extracting data from an Okapi instance, transforming
the data for reporting purposes, and storing the data in an analytic database
for further querying.

To install LDLite or upgrade to the latest version:

    python -m pip install --upgrade ldlite

Example:

    # Import and initialize LDLite.
    import ldlite
    ld = ldlite.LDLite()

    # Connect to a database.
    db = ld.connect_db(filename='ldlite.db')

    # Connect to Okapi.
    ld.connect_okapi(url='https://folio-snapshot-okapi.dev.folio.org',
                     tenant='diku',
                     user='diku_admin',
                     password='admin')

    # Send a CQL query and store the results in table "g", "g_j", etc.
    ld.query(table='g', path='/groups', query='cql.allRecords=1 sortby id')

    # Print the result tables.
    ld.select(table='g')
    ld.select(table='g_j')
    # etc.

"""

import json
import sys
from warnings import warn

import duckdb
import pandas
import psycopg2
import requests
from tqdm import tqdm

from ._csv import _to_csv
from ._jsonx import _transform_json
from ._jsonx import _drop_json_tables
from ._select import _select
from ._sqlx import _encode_sql_str
from ._sqlx import _autocommit
from ._sqlx import _sqlid
from ._sqlx import _varchar_type

class LDLite:

    def __init__(self):
        """Creates an instance of LDLite.

        Example:

            import ldlite

            ld = ldlite.LDLite()

        """
        self.page_size = 1000
        self._verbose = False
        self._quiet = False
        self.dbtype = 0
        self.db = None
        self.login_token = None

    def _set_page_size(self, page_size):
        self.page_size = page_size

    def connect_db(self, filename):
        """Connects to an embedded database for storing data.

        The *filename* specifies a local file containing the database or where
        the database will be created if it does not exist.  By default LDLite
        uses DuckDB to provide embedded analytic database features.  This
        function returns a connection to the database which can be used to
        submit SQL queries.

        Example:

            db = ld.connect_db(filename='ldlite.db')

        """
        self.dbtype = 1
        self.db = duckdb.connect(database=filename)
        return self.db

    def connect_db_postgresql(self, dsn):
        """Connects to a PostgreSQL database for storing data.

        The data source name is specified by *dsn*.  This function returns a
        connection to the database which can be used to submit SQL queries.
        The returned connection defaults to autocommit mode.

        Example:

            db = ld.connect_db_postgresql(dsn='dbname=ldlite host=localhost user=ldlite')

        """
        self.dbtype = 2
        self.db = psycopg2.connect(dsn)
        _autocommit(self.db, self.dbtype, True)
        return self.db

    def connect_db_redshift(self, dsn):
        """Connects to a Redshift database for storing data.

        The data source name is specified by *dsn*.  This function returns a
        connection to the database which can be used to submit SQL queries.
        The returned connection defaults to autocommit mode.

        Example:

            db = ld.connect_db_redshift(dsn='dbname=ldlite host=localhost user=ldlite')

        """
        self.dbtype = 3
        self.db = psycopg2.connect(dsn)
        _autocommit(self.db, self.dbtype, True)
        return self.db

    def _login(self):
        if self._verbose:
            print('ldlite: logging in to okapi', file=sys.stderr)
        hdr = { 'X-Okapi-Tenant': self.okapi_tenant,
                'Content-Type': 'application/json' }
        data = { 'username': self.okapi_user,
                'password': self.okapi_password }
        resp = requests.post(self.okapi_url+'/authn/login', headers=hdr, data=json.dumps(data))
        self.login_token = resp.headers['x-okapi-token']

    def _check_okapi(self):
        if self.login_token is None:
            raise RuntimeError('connection to okapi not configured: use connect_okapi()')

    def _check_db(self):
        if self.db is None:
            raise RuntimeError('database connection not configured: use connect_db() or connect_db_postgresql()')

    def connect_okapi(self, url, tenant, user, password):
        """Connects to an Okapi instance.

        The *url*, *tenant*, *user*, and *password* settings are Okapi-specific
        connection parameters.

        Example:

            ld.connect_okapi(url='https://folio-snapshot-okapi.dev.folio.org',
                             tenant='diku',
                             user='diku_admin',
                             password='admin')

        """
        if not url.startswith('https://'):
            raise ValueError('url must begin with "https://"')
        self.okapi_url = url.rstrip('/')
        self.okapi_tenant = tenant
        self.okapi_user = user
        self.okapi_password = password
        self._login()

    def drop_all_tables(self, table):
        """Drops a specified table and any accompanying tables that were output from JSON transformation.

        A table called *table*_jtable is used to retrieve the names of the
        tables created by JSON transformation.

        This function returns a list of all of the dropped tables.

        Example:

            ld.drop_all_tables('g')

        """
        schema_table = table.strip().split('.')
        if len(schema_table) < 1 or len(schema_table) > 2:
            raise ValueError('invalid table name: ' + table)
        self._check_db()
        _autocommit(self.db, self.dbtype, False)
        cur = self.db.cursor()
        try:
            cur.execute('DROP TABLE IF EXISTS ' + _sqlid(table))
        finally:
            cur.close()
        tables = [table]
        tables += _drop_json_tables(self.db, self.dbtype, table)
        self.db.commit()
        _autocommit(self.db, self.dbtype, True)
        return tables

    def query(self, table, path, query, json_depth=3, transform=None):
        """Submits a CQL query to an Okapi module, and transforms and stores the result.

        The *path* parameter is the request path, and *query* is the CQL query.
        The result is stored in *table* within the analytic database.

        By default JSON data are transformed into one or more tables that are
        created in addition to *table*.  New tables overwrite any existing
        tables having the same name.  If *json_depth* is specified within the
        range 0 < *json_depth* < 5, this determines how far into nested JSON
        data the transformation will descend.  (The default is 3.)  If
        *json_depth* is specified as 0, JSON data are not transformed.

        The *transform* parameter is no longer supported and will be removed in
        the future.  Instead, specify *json_depth* as 0 to disable JSON
        transformation.

        A list of newly created tables is returned by this function.

        Example:

            ld.query(table='g', path='/groups', query='cql.allRecords=1 sortby id')

        """
        if transform != None:
            raise ValueError('transform is no longer supported: use json_depth=0 to disable JSON transformation')
        schema_table = table.strip().split('.')
        if len(schema_table) < 1 or len(schema_table) > 2:
            raise ValueError('invalid table name: ' + table)
        if json_depth is None or json_depth < 0 or json_depth > 4:
            raise ValueError('invalid value for json_depth: ' + str(json_depth))
        self._check_okapi()
        self._check_db()
        if not self._quiet:
            print('ldlite: querying: '+path, file=sys.stderr)
        _autocommit(self.db, self.dbtype, False)
        cur = self.db.cursor()
        try:
            if len(schema_table) == 2:
                cur.execute('CREATE SCHEMA IF NOT EXISTS ' + _sqlid(schema_table[0]))
            cur.execute('DROP TABLE IF EXISTS ' + _sqlid(table))
            cur.execute('CREATE TABLE ' + _sqlid(table) + '(__id integer, jsonb ' + _varchar_type(self.dbtype) + ')')
        finally:
            cur.close()
        # First get total number of records
        hdr = { 'X-Okapi-Tenant': self.okapi_tenant,
                'X-Okapi-Token': self.login_token }
        resp = requests.get(self.okapi_url+path+'?offset=0&limit=1&query='+query, headers=hdr)
        if resp.status_code == 401:
            # Retry
            self._login()
            hdr = { 'X-Okapi-Tenant': self.okapi_tenant,
                    'X-Okapi-Token': self.login_token }
            resp = requests.get(self.okapi_url+path+'?offset=0&limit=1&query='+query, headers=hdr)
        if resp.status_code != 200:
            resp.raise_for_status()
        try:
            j = resp.json()
        except Exception as e:
            raise RuntimeError('received server response: ' + resp.text) from e
        if 'totalRecords' in j:
            total_records = j['totalRecords']
        else:
            total_records = -1
        total = total_records if total_records is not None else 0
        if self._verbose:
            print('ldlite: estimated row count: '+str(total), file=sys.stderr)
        # Read result pages
        count = 0
        page = 0
        if not self._quiet:
            if total == -1:
                pbar = tqdm(desc='reading', leave=False, mininterval=1, smoothing=0, colour='#A9A9A9', bar_format='{desc} {elapsed} {bar}{postfix}')
            else:
                pbar = tqdm(desc='reading', total=total, leave=False, mininterval=1, smoothing=0, colour='#A9A9A9', bar_format='{desc} {bar}{postfix}')
            pbartotal = 0
        cur = self.db.cursor()
        try:
            while True:
                offset = page * self.page_size
                limit = self.page_size
                resp = requests.get(self.okapi_url+path+'?offset='+str(offset)+'&limit='+str(limit)+'&query='+query, headers=hdr)
                try:
                    j = resp.json()
                except Exception as e:
                    raise RuntimeError('received server response: ' + resp.text) from e
                if isinstance(j, dict):
                    data = list(j.values())[0]
                else:
                    data = j
                lendata = len(data)
                if lendata == 0:
                    break
                for d in data:
                    cur.execute('INSERT INTO ' + _sqlid(table) + ' VALUES(' + str(count+1) + ',' + _encode_sql_str(self.dbtype, json.dumps(d, indent=4)) + ')')
                    count += 1
                    if not self._quiet:
                        if pbartotal + 1 > total:
                            pbartotal = total
                            pbar.update(total - pbartotal)
                        else:
                            pbartotal += 1
                            pbar.update(1)
                page += 1
        finally:
            cur.close()
        if not self._quiet:
            pbar.close()
        deleted_tables = set(_drop_json_tables(self.db, self.dbtype, table))
        newtables = [table]
        if json_depth > 0:
            newtables += _transform_json(self.db, self.dbtype, table, count, self._quiet, json_depth, deleted_tables)
        self.db.commit()
        if not self._quiet:
            print('ldlite: created tables: '+', '.join(newtables), file=sys.stderr)
        _autocommit(self.db, self.dbtype, True)
        return newtables

    def quiet(self, enable):
        """Configures suppression of progress messages.

        If *enable* is True, progress messages are suppressed; if False, they
        are not suppressed.

        Example:

            ld.quiet(True)

        """
        if enable is None:
            raise ValueError('quiet(None) is invalid')
        if enable and self._verbose:
            raise ValueError('"verbose" and "quiet" modes cannot both be enabled')
        self._quiet = enable

    def select(self, table, columns=None, limit=None):
        """Prints rows of a table in the analytic database.

        By default all rows and columns of *table* are printed to standard
        output.  If *columns* is specified, then only the named columns are
        printed.  If *limit* is specified, then only up to *limit* rows are
        printed.

        Examples:

            ld.select(table='loans', limit=10)
            ld.select(table='loans', columns=['id', 'item_id', 'loan_date'])

        """
        self._check_db()
        # f = sys.stdout if file is None else file
        f = sys.stdout
        if self._verbose:
            print('ldlite: reading from table: '+table, file=sys.stderr)
        _autocommit(self.db, self.dbtype, False)
        _select(self.db, self.dbtype, table, columns, limit, f)
        if self.dbtype == 2 or self.dbtype == 3:
            self.db.rollback()
        _autocommit(self.db, self.dbtype, True)

    def to_csv(self, filename, table):
        """Export a table in the analytic database to a CSV file.

        All rows of *table* are exported to *filename*.

        Example:

            ld.to_csv(table='g', filename='g.csv')

        """
        self._check_db()
        _autocommit(self.db, self.dbtype, False)
        _to_csv(self.db, self.dbtype, table, filename)
        if self.dbtype == 2 or self.dbtype == 3:
            self.db.rollback()
        _autocommit(self.db, self.dbtype, True)

    def _verbose(self, enable):
        """Configures verbose output.

        If *enable* is True, verbose output is enabled; if False, it is
        disabled.

        Example:

            ld.verbose(True)

        """
        if enable is None:
            raise ValueError('verbose(None) is invalid')
        if enable and self._quiet:
            raise ValueError('"verbose" and "quiet" modes cannot both be enabled')
        self._verbose = enable

if __name__ == '__main__':
    pass

