from __future__ import annotations

import sys
from typing import Any, TextIO

from ._sqlx import DBType, server_cursor, sqlid


def _format_attr(attr: str, width: int) -> str:
    s = ""
    a = attr[0]
    len_a = len(a)
    shift_left = 1 if (len_a % 2 == 1 and width % 2 == 0 and len_a < width) else 0
    start = int(width / 2) - int(len_a / 2) - shift_left
    for _ in range(start):
        s += " "
    s += a
    for _ in range(width - start - len_a):
        s += " "
    return s


def _maxlen(lines: list[str]) -> int:
    m = 0
    for s in lines:
        m = max(m, len(s))
    return m


def _rstrip_lines(lines: list[str]) -> list[str]:
    newlines = []
    for s in lines:
        newlines.extend(s.rstrip())
    return newlines


def _format_value(value: list[str], dtype: str | int | Any) -> list[str]:
    if len(value) > 1:
        return value
    if dtype in {"bool", 16}:
        return ["t"] if value[0] == "True" else ["f"]
    return value


def _format_row(row: list[str], attrs: list[Any], width: list[int]) -> str:
    s = ""
    # Count number of lines
    rowlines = []
    maxlen = []
    maxlines = 1
    for i, data in enumerate(row):
        lines = [""]
        if data is not None:
            lines = _format_value(_rstrip_lines(str(data).splitlines()), attrs[i][1])
        maxlen.append(_maxlen(lines))
        rowlines.append(lines)
        lines_len = len(lines)
        maxlines = max(maxlines, lines_len)
    # Write lines
    for i in range(maxlines):
        for j, lines in enumerate(rowlines):
            lines_i = lines[i] if i < len(lines) else ""
            s += " " if j == 0 else "| "
            if attrs[j][1] == "NUMBER" or attrs[j][1] == 20 or attrs[j][1] == 23:
                start = width[j] - len(lines_i)
            else:
                start = 0
            for _ in range(start):
                s += " "
            s += lines_i
            for _ in range(width[j] - start - len(lines_i)):
                s += " "
            s += " "
        s += "\n"
    return s


def select(db: Any, dbtype: DBType, table: str, columns: list[str], limit: int, file: TextIO = sys.stdout) -> None:  # noqa: C901, PLR0912, PLR0913, PLR0915
    if columns is None or columns == []:
        colspec = "*"
    else:
        colspec = ",".join([sqlid(c) for c in columns])
    # Get attributes
    attrs = []
    width = []
    cur = db.cursor()
    try:
        cur.execute("SELECT " + colspec + " FROM " + sqlid(table) + " LIMIT 1")
        for a in cur.description:
            attrs.append((a[0], a[1]))
            width.append(len(a[0]))
    finally:
        cur.close()
    # Scan
    cur = server_cursor(db, dbtype)
    try:
        cur.execute("SELECT " + ",".join([sqlid(a[0]) for a in attrs]) + " FROM " + sqlid(table))
        while True:
            row = cur.fetchone()
            if row is None:
                break
            for i, v in enumerate(row):
                lines = [""]
                if v is not None:
                    lines = str(v).splitlines()
                for _, ln in enumerate(lines):
                    width[i] = max(width[i], len(ln.rstrip()))
    finally:
        cur.close()
    cur = server_cursor(db, dbtype)
    try:
        q = "SELECT " + ",".join([sqlid(a[0]) for a in attrs]) + " FROM " + sqlid(table)
        if limit is not None:
            q += " LIMIT " + str(limit)
        cur.execute(q)
        # Attribute names
        s = ""
        for i, v in enumerate(attrs):
            s += " " if i == 0 else "| "
            s += _format_attr(v, width[i])
            s += " "
        print(s, file=file)
        # Header bar
        s = ""
        for i in range(len(attrs)):
            s += "" if i == 0 else "+"
            s += "-"
            for _ in range(width[i]):
                s += "-"
            s += "-"
        print(s, file=file)
        # Data rows
        row_i = 0
        while True:
            row = cur.fetchone()
            if row is None:
                break
            s = _format_row(row, attrs, width)
            print(s, end="", file=file)
            row_i += 1
        print("(" + str(row_i) + " " + ("row" if row_i == 1 else "rows") + ")", file=file)
        print(file=file)
    finally:
        cur.close()
