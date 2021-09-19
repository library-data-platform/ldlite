import json
import sys

import pandas
import requests
from tabulate import tabulate
from tqdm import tqdm

def _escape_sql(sql):
    n = ""
    for c in sql:
        if c == "'":
            n += "''"
        else:
            n += c
    return n

class LDLite:

    def __init__(self):
        """Creates an instance of LDLite."""
        self.page_size = 1000
        self.debug = False
        self.quiet = False

    def _set_page_size(self, page_size):
        self.page_size = page_size

    def config_okapi(self, url, tenant, user, password):
        """Configures the connection to Okapi.

        The *url*, *tenant*, *user*, and *password* settings are Okapi-specific
        connection parameters.
        """
        self.okapi_url = url
        self.okapi_tenant = tenant
        self.okapi_user = user
        self.okapi_password = password

    def config_db(self, db):
        """Configures the analytic database.

        The *db* parameter must be an existing database connection.
        """
        self.db = db

    def _login(self):
        if self.debug:
            print("ldlite: logging in to okapi", file=sys.stderr)
        hdr = { 'X-Okapi-Tenant': self.okapi_tenant,
                'Content-Type': 'application/json' }
        data = { 'username': self.okapi_user,
                'password': self.okapi_password }
        resp = requests.post(self.okapi_url+'/authn/login', headers=hdr, data=json.dumps(data))
        return resp.headers['x-okapi-token']

    def _transform_json(self, table, total):
        cur = self.db.cursor()
        cur.execute("SELECT jsonb FROM "+table)
        attrset = set()
        while True:
            row = cur.fetchone()
            if row == None:
                break
            if row[0] is None or row[0] == "":
                d = {}
            else:
                d = json.loads(row[0])
            for a in d.keys():
                attrset.add(a)
        self.db.execute("DROP TABLE IF EXISTS "+table+"_j")
        self.db.execute("CREATE TABLE "+table+"_j(__id integer)")
        for a in attrset:
            self.db.execute("ALTER TABLE \""+table+"_j\" ADD COLUMN \""+a+"\" varchar")
        cur = self.db.cursor()
        cur.execute("SELECT __id, jsonb FROM "+table)
        if not self.quiet:
            pbar = tqdm(total=total)
            pbartotal = 0
        while True:
            row = cur.fetchone()
            if row == None:
                break
            if row[1] is None or row[1] == "":
                d = {}
            else:
                d = json.loads(row[1])
            attrs = []
            values = []
            for a in d.keys():
                attrs.append("\""+a+"\"")
                values.append("'"+_escape_sql(str(d[a]))+"'")
            if len(attrs) != 0:
                q = "INSERT INTO \""+table+"_j\" ("
                q += "__id,"
                q += ",".join(attrs)
                q += ")VALUES("
                q += str(row[0])+","
                q += ",".join(values)
                q += ")"
                self.db.execute(q)
            if not self.quiet:
                pbartotal += 1
                pbar.update(1)
        if not self.quiet:
            pbar.close()

    def query(self, table: str, path: str, query: str, transform: str =True):
        """Submits a CQL query to an Okapi module.

        The *path* parameter is the request path, and *query* is the CQL query.
        The result is stored in *table* within the analytic database.  If
        *transform* is True, JSON data are transformed into one or more tables
        that are created in addition to *table*.
        """
        token = self._login()
        self.db.execute("DROP TABLE IF EXISTS "+table)
        self.db.execute("CREATE TABLE "+table+"(__id integer, jsonb varchar)")
        hdr = { 'X-Okapi-Tenant': self.okapi_tenant,
                'X-Okapi-Token': token }
        # First get total number of records
        resp = requests.get(self.okapi_url+path+'?offset=0&limit=1&query='+query, headers=hdr)
        j = resp.json()
        total_records = j["totalRecords"]
        total = total_records if total_records is not None else 0
        if self.debug:
            print("ldlite: estimated row count: "+str(total), file=sys.stderr)
        # Read result pages
        if not self.quiet:
            print("ldlite: reading results", file=sys.stderr)
        count = 0
        page = 0
        if not self.quiet:
            pbar = tqdm(total=total, bar_format="{l_bar}{bar}| [{elapsed}<{remaining}, {rate_fmt}{postfix}]")
            pbartotal = 0
        while True:
            offset = page * self.page_size
            limit = self.page_size
            resp = requests.get(self.okapi_url+path+'?offset='+str(offset)+'&limit='+str(limit)+'&query='+query, headers=hdr)
            j = resp.json()
            data = list(j.values())[0]
            lendata = len(data)
            if lendata == 0:
                break
            for d in data:
                self.db.execute("INSERT INTO "+table+" VALUES ("+str(count+1)+", '"+_escape_sql(json.dumps(d, indent=4))+"')")
                count += 1
                if not self.quiet:
                    if pbartotal + 1 > total:
                        pbartotal = total
                        pbar.update(total - pbartotal)
                    else:
                        pbartotal += 1
                        pbar.update(1)
            page += 1
        if not self.quiet:
            pbar.close()
        if transform:
            if not self.quiet:
                print("ldlite: transforming data", file=sys.stderr)
            self._transform_json(table, count)

    def select(self, table: str, limit: int):
        """Prints rows of a table in the analytic database.

        Up to *limit* rows of *table* are printed to standard output.
        """
        q = "SELECT * FROM \""+table+"\" LIMIT "+str(limit)
        self.db.execute(q)
        hdr = []
        for a in self.db.description:
            hdr.append(a[0])
        if self.debug:
            print("ldlite: reading from table: "+table, file=sys.stderr)
        print(tabulate(self.db.fetchall(), headers=hdr, tablefmt='fancy_grid'))

    def to_csv(self, filename: str, table: str, limit: int):
        """Export a table in the analytic database to a CSV file.

        Up to *limit* rows of *table* are exported to *filename* in CSV format.
        """
        q = "SELECT * FROM \""+table+"\" LIMIT "+str(limit)
        if self.debug:
            print("ldlite: reading from table: "+table, file=sys.stderr)
        df = self.db.execute(q).fetchdf()
        if self.debug:
            print("ldlite: exporting CSV to file: "+filename, file=sys.stderr)
        df.to_csv(filename, encoding="utf-8", index=False)

    def set_debug(self, enable: bool):
        """Configures debugging output.

        If *enable* is True, debugging output is enabled; otherwise it is
        disabled.
        """
        if enable and self.quiet:
            raise ValueError("\"debug\" and \"quiet\" modes cannot both be enabled")
        self.debug = enable

    def set_quiet(self, enable: bool):
        """Configures suppression of progress output.

        If *enable* is True, progress output is suppressed; otherwise it is not
        suppressed.
        """
        if enable and self.debug:
            raise ValueError("\"debug\" and \"quiet\" modes cannot both be enabled")
        self.quiet = enable

if __name__ == '__main__':
    pass

