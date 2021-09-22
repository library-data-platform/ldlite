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

import duckdb
import pandas
import psycopg2
import requests
from tqdm import tqdm

from ._csv import _to_csv
from ._jsonx import _transform_json
from ._select import _select
from ._sqlx import _escape_sql
from ._sqlx import _autocommit

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

    def _set_page_size(self, page_size):
        self.page_size = page_size

    def connect_db(self, filename):
        """Connects to an embedded analytic database.

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
        """Connects to an analytic PostgreSQL database.

        PostgreSQL can be used as an alternative to the default embedded
        database.  The data source name is specified by *dsn*.  This function
        returns a connection to the database which can be used to submit SQL
        queries.  The returned connection defaults to autocommit mode.

        Example:

            db = ld.connect_db_postgresql(dsn='dbname=ldlite host=localhost user=ldlite')

        """
        self.dbtype = 2
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
        return resp.headers['x-okapi-token']

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
        self.okapi_url = url.rstrip('/')
        self.okapi_tenant = tenant
        self.okapi_user = user
        self.okapi_password = password
        # Test connection
        _ = self._login()

    def query(self, table, path, query, transform=True):
        """Submits a CQL query to an Okapi module, and transforms and stores the result.

        The *path* parameter is the request path, and *query* is the CQL query.
        The result is stored in *table* within the analytic database.  If
        *transform* is True (the default), JSON data are transformed into one
        or more tables that are created in addition to *table*.  New tables add
        a suffix "_j" to *table* and overwrite any existing tables with the
        same name.  A list of newly created tables is returned by this
        function.

        Example:

            ld.query(table='g', path='/groups', query='cql.allRecords=1 sortby id')

        """
        _autocommit(self.db, self.dbtype, True)
        token = self._login()
        cur = self.db.cursor()
        cur.execute('DROP TABLE IF EXISTS '+table)
        cur = self.db.cursor()
        cur.execute('CREATE TABLE '+table+'(__id integer, jsonb varchar)')
        hdr = { 'X-Okapi-Tenant': self.okapi_tenant,
                'X-Okapi-Token': token }
        # First get total number of records
        resp = requests.get(self.okapi_url+path+'?offset=0&limit=1&query='+query, headers=hdr)
        if resp.status_code != 200:
            resp.raise_for_status()
        try:
            j = resp.json()
        except Exception as e:
            raise RuntimeError(resp.text)
        if 'totalRecords' in j:
            total_records = j['totalRecords']
        else:
            total_records = -1
        total = total_records if total_records is not None else 0
        if self._verbose:
            print('ldlite: estimated row count: '+str(total), file=sys.stderr)
        # Read result pages
        if not self._quiet:
            print('ldlite: querying: '+path, file=sys.stderr)
        count = 0
        page = 0
        if not self._quiet:
            if total == -1:
                pbar = tqdm(desc='reading', leave=False, mininterval=1, smoothing=0, colour='#A9A9A9', bar_format='{desc} {elapsed} {bar}{postfix}')
            else:
                pbar = tqdm(desc='reading', total=total, leave=False, mininterval=1, smoothing=0, colour='#A9A9A9', bar_format='{desc} {bar}{postfix}')
            pbartotal = 0
        while True:
            offset = page * self.page_size
            limit = self.page_size
            resp = requests.get(self.okapi_url+path+'?offset='+str(offset)+'&limit='+str(limit)+'&query='+query, headers=hdr)
            j = resp.json()
            if isinstance(j, dict):
                data = list(j.values())[0]
            else:
                data = j
            lendata = len(data)
            if lendata == 0:
                break
            for d in data:
                cur = self.db.cursor()
                cur.execute('INSERT INTO '+table+' VALUES ('+str(count+1)+', \''+_escape_sql(json.dumps(d, indent=4))+'\')')
                count += 1
                if not self._quiet:
                    if pbartotal + 1 > total:
                        pbartotal = total
                        pbar.update(total - pbartotal)
                    else:
                        pbartotal += 1
                        pbar.update(1)
            page += 1
        if not self._quiet:
            pbar.close()
        newtables = [table]
        if transform:
            newtables += _transform_json(self.db, table, count, self._quiet)
        if not self._quiet:
            print('ldlite: created tables: '+', '.join(newtables), file=sys.stderr)
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

    def select(self, table, limit=None):
        """Prints rows of a table in the analytic database.

        By default all rows of *table* are printed to standard output.  If
        *limit* is specified, then only up to *limit* rows are printed.

        Example:

            ld.select(table='g')

        """
        _autocommit(self.db, self.dbtype, True)
        # f = sys.stdout if file is None else file
        f = sys.stdout
        if self._verbose:
            print('ldlite: reading from table: '+table, file=sys.stderr)
        _select(self.db, table, limit, f)

    def to_csv(self, filename, table):
        """Export a table in the analytic database to a CSV file.

        All rows of *table* are exported to *filename*.

        Example:

            ld.to_csv(table='g', filename='g.csv')

        """
        _autocommit(self.db, self.dbtype, True)
        _to_csv(self.db, table, filename)

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

