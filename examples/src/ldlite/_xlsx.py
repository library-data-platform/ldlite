import xlsxwriter

from ._sqlx import _server_cursor
from ._sqlx import _sqlid


def _to_xlsx(db, dbtype, table, filename, header):
    # Read attributes
    attrs = []
    width = []
    cur = db.cursor()
    try:
        cur.execute('SELECT * FROM ' + _sqlid(table) + ' LIMIT 1')
        for a in cur.description:
            attrs.append((a[0], a[1]))
            width.append(len(a[0]))
    finally:
        cur.close()
    cols = ','.join([_sqlid(a[0]) for a in attrs])
    query = 'SELECT ' + cols + ' FROM ' + _sqlid(table) + ' ORDER BY ' + ','.join(
        [str(i + 1) for i in range(len(attrs))])
    # Scan
    cur = _server_cursor(db, dbtype)
    try:
        cur.execute(query)
        while True:
            row = cur.fetchone()
            if row is None:
                break
            for i, data in enumerate(row):
                lines = ['']
                if data is not None:
                    lines = str(data).splitlines()
                for j, l in enumerate(lines):
                    len_l = len(l)
                    if len_l > width[i]:
                        width[i] = len_l
    finally:
        cur.close()
    # Write data
    cur = _server_cursor(db, dbtype)
    try:
        cur.execute(query)
        fn = filename if '.' in filename else filename + '.xlsx'
        workbook = xlsxwriter.Workbook(fn, {'constant_memory': True})
        try:
            worksheet = workbook.add_worksheet()
            for i, w in enumerate(width):
                worksheet.set_column(i, i, w + 2)
            if header:
                worksheet.freeze_panes(1, 0)
                for i, a in enumerate(attrs):
                    fmt = workbook.add_format()
                    fmt.set_bold()
                    fmt.set_align('center')
                    worksheet.write(0, i, a[0], fmt)
            row_i = 1 if header else 0
            datafmt = workbook.add_format()
            datafmt.set_align('top')
            while True:
                row = cur.fetchone()
                if row is None:
                    break
                for i, data in enumerate(row):
                    worksheet.write(row_i, i, data, datafmt)
                row_i += 1
        finally:
            workbook.close()
    finally:
        cur.close()
