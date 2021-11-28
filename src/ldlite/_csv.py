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


def _to_csv(db, dbtype, table, filename, header):
    # Read attributes
    attrs = []
    cur = db.cursor()
    try:
        cur.execute('SELECT * FROM ' + _sqlid(table) + ' LIMIT 1')
        for a in cur.description:
            attrs.append((a[0], a[1]))
    finally:
        cur.close()
    # Write data
    cur = _server_cursor(db, dbtype)
    try:
        cols = ','.join([_sqlid(a[0]) for a in attrs])
        cur.execute('SELECT ' + cols + ' FROM ' + _sqlid(table) + ' ORDER BY ' + ','.join(
            [str(i + 1) for i in range(len(attrs))]))
        fn = filename if '.' in filename else filename + '.csv'
        with open(fn, 'w') as f:
            if header:
                print(','.join(['"' + a[0] + '"' for a in attrs]), file=f)
            while True:
                row = cur.fetchone()
                if row is None:
                    break
                s = ''
                for i, data in enumerate(row):
                    d = '' if data is None else data
                    if i != 0:
                        s += ','
                    if attrs[i][1] == 'NUMBER' or attrs[i][1] == 20 or attrs[i][1] == 23:
                        s += str(d)
                    else:
                        s += '"' + _escape_csv(str(d)) + '"'
                print(s, file=f)
    finally:
        cur.close()
