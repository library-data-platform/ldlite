def _sqlid(identifier):
    return "\""+identifier+"\""

def _escape_sql(sql):
    n = ""
    for c in sql:
        if c == "'":
            n += "''"
        else:
            n += c
    return n

