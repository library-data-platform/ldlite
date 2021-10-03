import xlsxwriter

from ._sqlx import _server_cursor
from ._sqlx import _sqlid

def _to_xlsx(db, dbtype, table, filename, header):
    # Read attributes
    attrs = []
    cur = db.cursor()
    try:
        cur.execute('SELECT * FROM ' + _sqlid(table) + ' LIMIT 1')
        for a in cur.description:
            attrs.append( (a[0], a[1]) )
    finally:
        cur.close()
    # Write data
    cur = _server_cursor(db, dbtype)
    try:
        cols = ','.join([_sqlid(a[0]) for a in attrs])
        cur.execute('SELECT ' + cols + ' FROM ' + _sqlid(table) + ' ORDER BY ' + ','.join([str(i + 1) for i in range(len(attrs))]))
        workbook = xlsxwriter.Workbook(filename, {'constant_memory': True})
        try:
            worksheet = workbook.add_worksheet()
            worksheet.set_column(0, len(attrs) - 1, 40)
            if header:
                worksheet.freeze_panes(1, 0)
                for i, a in enumerate(attrs):
                    fmt = workbook.add_format()
                    fmt.set_align('center')
                    fmt.set_bold()
                    worksheet.write(0, i, a[0], fmt)
            row_i = 1 if header else 0
            while True:
                row = cur.fetchone()
                if row == None:
                    break
                for i, data in enumerate(row):
                    worksheet.write(row_i, i, data)
                row_i += 1
        finally:
            workbook.close()
    finally:
        cur.close()

