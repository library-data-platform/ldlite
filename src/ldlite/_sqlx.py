def _autocommit(db, dbtype, enable):
    if dbtype == 2:
        db.set_session(autocommit=enable)

def _sqlid(ident):
    sp = ident.split('.')
    if len(sp) == 1:
        return '"'+ident+'"'
    else:
        return '.'.join(['"'+s+'"' for s in sp])

def _escape_sql(sql):
    b = ''
    for c in sql:
        if c == '\'':
            b += '\'\''
        else:
            b += c
    return b

