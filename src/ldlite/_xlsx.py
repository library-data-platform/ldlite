from __future__ import annotations

from typing import TYPE_CHECKING

import xlsxwriter

from ._sqlx import DBType, server_cursor, sqlid

if TYPE_CHECKING:
    from _typeshed import dbapi


def to_xlsx(  # noqa: C901, PLR0912, PLR0915
    db: dbapi.DBAPIConnection,
    dbtype: DBType,
    table: str,
    filename: str,
    header: bool,
) -> None:
    # Read attributes
    attrs: list[tuple[str, dbapi.DBAPITypeCode]] = []
    width: list[int] = []
    cur = db.cursor()
    try:
        cur.execute("SELECT * FROM " + sqlid(table) + " LIMIT 1")
        if cur.description is not None:
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
                for i, attr in enumerate(attrs):
                    fmt = workbook.add_format()
                    fmt.set_bold()
                    fmt.set_align("center")
                    worksheet.write(0, i, attr[0], fmt)
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
