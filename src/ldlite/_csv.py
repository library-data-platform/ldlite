from ._sqlx import _server_cursor
from ._sqlx import _sqlid

def _escape_csv(field):
    b = ''
    for f in field:
        if f == '"':
            b += '""'
        else:
            b += f
    return b

def _to_csv(db, dbtype, table, filename):
    cur = _server_cursor(db, dbtype)
    try:
        cur.execute('SELECT * FROM '+_sqlid(table))
        with open(filename, 'w') as f:
            while True:
                row = cur.fetchone()
                if row == None:
                    break
                s = ''
                for i, data in enumerate(row):
                    d = '' if data is None else data
                    if i != 0:
                        s += ','
                    if cur.description[i][1] == 'NUMBER':
                        s += str(d)
                    else:
                        s += '"'+_escape_csv(str(d))+'"'
                print(s, file=f)
    finally:
        cur.close()

