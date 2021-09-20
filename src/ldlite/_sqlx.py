def _autocommit(db, dbtype, enable):
    if dbtype == 2:
        db.set_session(autocommit=enable)

def _sqlid(identifier):
    return '"'+identifier+'"'

def _escape_sql(sql):
    n = ''
    for c in sql:
        if c == '\'':
            n += '\'\''
        else:
            n += c
    return n

