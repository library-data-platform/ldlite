def _strip_schema(table):
    st = table.split('.')
    if len(st) == 1:
        return table
    elif len(st) == 2:
        return st[1]
    else:
        raise ValueError('invalid table name: ' + table)


def _autocommit(db, dbtype, enable):
    if dbtype == 2 or dbtype == 3:
        db.rollback()
        db.set_session(autocommit=enable)


def _server_cursor(db, dbtype):
    if dbtype == 2 or dbtype == 3:
        return db.cursor(name='ldlite')
    else:
        return db.cursor()


def _sqlid(ident):
    sp = ident.split('.')
    if len(sp) == 1:
        return '"' + ident + '"'
    else:
        return '.'.join(['"' + s + '"' for s in sp])


def _varchar_type(dbtype):
    if dbtype == 3:
        return 'varchar(65535)'
    else:
        return 'varchar'


def _json_type(dbtype):
    if dbtype == 2:
        return 'jsonb'
    else:
        return 'varchar'


def _encode_sql_str(dbtype, s):
    if dbtype == 2:
        b = 'E\''
    else:
        b = '\''
    if dbtype == 1:
        for c in s:
            if c == '\'':
                b += '\'\''
            else:
                b += c
    if dbtype == 2 or dbtype == 3:
        for c in s:
            if c == '\'':
                b += '\'\''
            elif c == '\\':
                b += '\\\\'
            elif c == '\n':
                b += '\\n'
            elif c == '\r':
                b += '\\r'
            elif c == '\t':
                b += '\\t'
            elif c == '\f':
                b += '\\f'
            elif c == '\b':
                b += '\\b'
            else:
                b += c
    b += '\''
    return b


def _encode_sql(dbtype, data):
    if data is None:
        return 'NULL'
    elif isinstance(data, str):
        return _encode_sql_str(dbtype, data)
    elif isinstance(data, int):
        return str(data)
    elif isinstance(data, bool):
        return 'TRUE' if data else 'FALSE'
    else:
        return _encode_sql_str(dbtype, str(data))
