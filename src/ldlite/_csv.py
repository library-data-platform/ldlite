from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from ._sqlx import DBType, as_sqlite, server_cursor, sqlid

if TYPE_CHECKING:
    from _typeshed import dbapi


def _escape_csv(field: str) -> str:
    b = ""
    for f in field:
        if f == '"':
            b += '""'
        else:
            b += f
    return b


def to_csv(  # noqa: PLR0912
    db: dbapi.DBAPIConnection,
    dbtype: DBType,
    table: str,
    filename: str,
    header: bool,
) -> None:
    # Read attributes
    attrs: list[tuple[str, dbapi.DBAPITypeCode]] = []

    if sql3db := as_sqlite(db, dbtype):
        sql3cur = sql3db.cursor()
        try:
            sql3cur.execute("PRAGMA table_info(" + sqlid(table) + ")")
            attrs.extend([(a[1], a[2]) for a in sql3cur.fetchall()])
        finally:
            sql3cur.close()

    else:
        cur = server_cursor(db, dbtype)
        try:
            cur.execute("SELECT * FROM " + sqlid(table) + " LIMIT 1")
            cur.fetchall()
            if cur.description is not None:
                attrs.extend([(a[0], a[1]) for a in cur.description])
        finally:
            cur.close()

    # Write data
    cur = server_cursor(db, dbtype)
    try:
        cols = ",".join([sqlid(a[0]) for a in attrs])
        cur.execute(
            "SELECT "
            + cols
            + " FROM "
            + sqlid(table)
            + " ORDER BY "
            + ",".join([str(i + 1) for i in range(len(attrs))]),
        )
        fn = Path(filename if "." in filename else filename + ".csv")
        with fn.open("w") as f:
            if header:
                print(",".join(['"' + a[0] + '"' for a in attrs]), file=f)
            while True:
                row = cur.fetchone()
                if row is None:
                    break
                s = ""
                for i, data in enumerate(row):
                    d = "" if data is None else data
                    if i != 0:
                        s += ","
                    if attrs[i][1] in ["NUMBER", "bigint", 20, 23]:
                        s += str(d)
                    else:
                        s += '"' + _escape_csv(str(d)) + '"'
                print(s, file=f)
    finally:
        cur.close()
