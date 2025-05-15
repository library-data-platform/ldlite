from typing import Any

import xlsxwriter

from ._sqlx import DBType, server_cursor, sqlid


def to_xlsx(  # noqa: C901, PLR0912, PLR0915
    db: Any, dbtype: DBType, table: str, filename: str, header: list[str]
) -> None:
    # Read attributes
    attrs = []
    width = []
    cur = db.cursor()
    try:
        cur.execute("SELECT * FROM " + sqlid(table) + " LIMIT 1")
        for a in cur.description:
            attrs.append((a[0], a[1]))
            width.append(len(a[0]))
    finally:
        cur.close()
    cols = ",".join([sqlid(a[0]) for a in attrs])
    query = (
        "SELECT "
        + cols
        + " FROM "
        + sqlid(table)
        + " ORDER BY "
        + ",".join([str(i + 1) for i in range(len(attrs))])
    )
    # Scan
    cur = server_cursor(db, dbtype)
    try:
        cur.execute(query)
        while True:
            row = cur.fetchone()
            if row is None:
                break
            for i, data in enumerate(row):
                lines = [""]
                if data is not None:
                    lines = str(data).splitlines()
                for _, ln in enumerate(lines):
                    width[i] = max(width[i], len(ln))
    finally:
        cur.close()
    # Write data
    cur = server_cursor(db, dbtype)
    try:
        cur.execute(query)
        fn = filename if "." in filename else filename + ".xlsx"
        workbook = xlsxwriter.Workbook(fn, {"constant_memory": True})
        try:
            worksheet = workbook.add_worksheet()
            for i, w in enumerate(width):
                worksheet.set_column(i, i, w + 2)
            if header:
                worksheet.freeze_panes(1, 0)
                for i, a in enumerate(attrs):
                    fmt = workbook.add_format()
                    fmt.set_bold()
                    fmt.set_align("center")
                    worksheet.write(0, i, a[0], fmt)
            row_i = 1 if header else 0
            datafmt = workbook.add_format()
            datafmt.set_align("top")
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
