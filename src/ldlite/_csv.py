from pathlib import Path
from typing import Any

from ._sqlx import DBType, server_cursor, sqlid


def _escape_csv(field: str) -> str:
    b = ""
    for f in field:
        if f == '"':
            b += '""'
        else:
            b += f
    return b


def to_csv(
    db: Any, dbtype: DBType, table: str, filename: str, header: list[str]
) -> None:
    # Read attributes
    attrs = []
    cur = db.cursor()
    try:
        cur.execute("SELECT * FROM " + sqlid(table) + " LIMIT 1")
        for a in cur.description:
            attrs.extend((a[0], a[1]))
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
            + ",".join([str(i + 1) for i in range(len(attrs))])
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
                    if (
                        attrs[i][1] == "NUMBER"
                        or attrs[i][1] == 20
                        or attrs[i][1] == 23
                    ):
                        s += str(d)
                    else:
                        s += '"' + _escape_csv(str(d)) + '"'
                print(s, file=f)
    finally:
        cur.close()
