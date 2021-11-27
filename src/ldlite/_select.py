import sys

from ._sqlx import _server_cursor
from ._sqlx import _sqlid


def _format_attr(attr, width):
    s = ''
    a = attr[0]
    len_a = len(a)
    shift_left = 1 if (len_a % 2 == 1 and width % 2 == 0 and len_a < width) else 0
    start = int(width / 2) - int(len_a / 2) - shift_left
    for i in range(0, start):
        s += ' '
    s += a
    for i in range(0, width - start - len_a):
        s += ' '
    return s


def _maxlen(lines):
    m = 0
    for s in lines:
        len_l = len(s)
        if len_l > m:
            m = len_l
    return m


def _rstrip_lines(lines):
    newlines = []
    for s in lines:
        newlines.append(s.rstrip())
    return newlines


def _format_value(value, dtype):
    if len(value) > 1:
        return value
    if dtype == 'bool' or dtype == 16:
        return ['t'] if value[0] == 'True' else ['f']
    else:
        return value


def _format_row(row, attrs, width):
    s = ''
    # Count number of lines
    rowlines = []
    maxlen = []
    maxlines = 1
    for i, data in enumerate(row):
        lines = ['']
        if data is not None:
            lines = _format_value(_rstrip_lines(str(data).splitlines()), attrs[i][1])
        maxlen.append(_maxlen(lines))
        rowlines.append(lines)
        lines_len = len(lines)
        if lines_len > maxlines:
            maxlines = lines_len
    # Write lines
    for i in range(0, maxlines):
        for j, lines in enumerate(rowlines):
            lines_i = lines[i] if i < len(lines) else ''
            s += ' ' if j == 0 else '| '
            if attrs[j][1] == 'NUMBER' or attrs[j][1] == 20 or attrs[j][1] == 23:
                start = width[j] - len(lines_i)
            else:
                start = 0
            for k in range(0, start):
                s += ' '
            s += lines_i
            for k in range(0, width[j] - start - len(lines_i)):
                s += ' '
            s += ' '
        s += '\n'
    return s


def _select(db, dbtype, table, columns, limit, file=sys.stdout):
    if columns is None or columns == []:
        colspec = '*'
    else:
        colspec = ','.join([_sqlid(c) for c in columns])
    # Get attributes
    attrs = []
    width = []
    cur = db.cursor()
    try:
        cur.execute('SELECT ' + colspec + ' FROM ' + _sqlid(table) + ' LIMIT 1')
        for a in cur.description:
            attrs.append((a[0], a[1]))
            width.append(len(a[0]))
    finally:
        cur.close()
    # Scan
    cur = _server_cursor(db, dbtype)
    try:
        cur.execute('SELECT ' + ','.join([_sqlid(a[0]) for a in attrs]) + ' FROM ' + _sqlid(table))
        while True:
            row = cur.fetchone()
            if row is None:
                break
            for i, v in enumerate(row):
                lines = ['']
                if v is not None:
                    lines = str(v).splitlines()
                for j, l in enumerate(lines):
                    len_l = len(l.rstrip())
                    if len_l > width[i]:
                        width[i] = len_l
    finally:
        cur.close()
    cur = _server_cursor(db, dbtype)
    try:
        q = 'SELECT ' + ','.join([_sqlid(a[0]) for a in attrs]) + ' FROM ' + _sqlid(table)
        if limit is not None:
            q += ' LIMIT ' + str(limit)
        cur.execute(q)
        # Attribute names
        s = ''
        for i, v in enumerate(attrs):
            s += ' ' if i == 0 else '| '
            s += _format_attr(attrs[i], width[i])
            s += ' '
        print(s, file=file)
        # Header bar
        s = ''
        for i in range(0, len(attrs)):
            s += '' if i == 0 else '+'
            s += '-'
            for j in range(0, width[i]):
                s += '-'
            s += '-'
        print(s, file=file)
        # Data rows
        row_i = 0
        while True:
            row = cur.fetchone()
            if row is None:
                break
            s = _format_row(row, attrs, width)
            print(s, end='', file=file)
            row_i += 1
        print('(' + str(row_i) + ' ' + ('row' if row_i == 1 else 'rows') + ')', file=file)
        print('', file=file)
    finally:
        cur.close()
