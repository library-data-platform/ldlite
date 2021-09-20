from ._sqlx import _sqlid

def _to_csv(db, table, filename):
    cur = db.cursor()
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
                    s += '"'+str(d)+'"'
            print(s, file=f)

