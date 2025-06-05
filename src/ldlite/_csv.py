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
    attrs: list[tuple[str, dbapi.DBAPITypeCode]] = [("__id", "NUMBER")]

    if sql3db := as_sqlite(db, dbtype):
        sql3cur = sql3db.cursor()
        try:
            sql3cur.execute("PRAGMA table_info(" + sqlid(table) + ")")
            t_attrs = [(a[1], a[2]) for a in sql3cur.fetchall()[1:]]
            attrs.extend(sorted(t_attrs, key=lambda a: a[0]))
        finally:
            sql3cur.close()

    else:
        cur = server_cursor(db, dbtype)
        try:
            cur.execute("SELECT * FROM " + sqlid(table) + " LIMIT 1")
            cur.fetchall()
            if cur.description is not None:
                t_attrs = [(a[0], a[1]) for a in cur.description[1:]]
                attrs.extend(sorted(t_attrs, key=lambda a: a[0]))
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
            + ",".join([str(i + 2) for i in range(len(attrs[1:]))]),
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
                    if attrs[i][1] in [
                        "NUMBER",
                        "bigint",
                        "numeric",
                        20,
                        1700,
                    ]:
                        s += str(d).rstrip("0").rstrip(".")
                    elif attrs[i][1] in [
                        "boolean",
                        "bool",
                        16,
                    ]:
                        s += str(bool(d))

                    else:
                        s += '"' + _escape_csv(str(d)) + '"'
                print(s, file=f)
    finally:
        cur.close()
