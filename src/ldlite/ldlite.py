import json

import requests
from tabulate import tabulate

def escape_sql(sql):
    n = ""
    for c in sql:
        if c == "'":
            n += "''"
        else:
            n += c
    return n

class LDLite:

    def __init__(self):
        self.debug = False

    def config_okapi(self, url, tenant, user, password):
        self.okapi_url = url
        self.okapi_tenant = tenant
        self.okapi_user = user
        self.okapi_password = password

    def config_db(self, db):
        self.db = db

    def login(self):
        if self.debug:
            print('ldlite: login: '+self.okapi_user+': '+self.okapi_url, file=sys.stderr)
        hdr = { 'X-Okapi-Tenant': self.okapi_tenant,
                'Content-Type': 'application/json' }
        data = { 'username': self.okapi_user,
                'password': self.okapi_password }
        resp = requests.post(self.okapi_url+'/authn/login', headers=hdr, data=json.dumps(data))
        return resp.headers['x-okapi-token']

    def _transform_json(self, table):
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
                values.append("'"+escape_sql(str(d[a]))+"'")
            if len(attrs) != 0:
                q = "INSERT INTO \""+table+"_j\" ("
                q += ",".join(attrs)
                q += ")VALUES("
                q += ",".join(values)
                q += ")"
                self.db.execute(q)

    def query(self, table, path, query):
        token = self.login()
        self.db.execute("DROP TABLE IF EXISTS "+table)
        self.db.execute("CREATE TABLE "+table+"(__id integer, jsonb varchar)")
        page_size = 1000
        hdr = { 'X-Okapi-Tenant': self.okapi_tenant,
                'X-Okapi-Token': token }
        row = 1
        page = 0
        while True:
            if self.debug:
                print('ldlite: reading page '+str(page), file=sys.stderr)
            offset = page * page_size
            limit = page_size
            resp = requests.get(self.okapi_url+path+'?offset='+str(offset)+'&limit='+str(limit)+'&query='+query, headers=hdr)
            j = resp.json()
            data = list(j.values())[0]
            if len(data) == 0:
                break
            for d in data:
                self.db.execute("INSERT INTO "+table+" VALUES ("+str(row)+", '"+escape_sql(json.dumps(d, indent=4))+"')")
                row += 1
            page += 1
        self._transform_json(table)

    def select(self, table, limit):
        q = "SELECT * FROM "+table
        if limit is not None:
            q += " LIMIT "+str(limit)
        self.db.execute(q)
        hdr = []
        for a in self.db.description:
            hdr.append(a[0])
        print(tabulate(self.db.fetchall(), headers=hdr, tablefmt='fancy_grid'))


def init():
    return LDLite()

if __name__ == '__main__':
    pass

