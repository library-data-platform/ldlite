import json

import duckdb
import psycopg2
import uuid
from tqdm import tqdm
from ._camelcase import _decode_camel_case
from ._sqlx import _server_cursor
from ._sqlx import _encode_sql
from ._sqlx import _sqlid
from ._sqlx import _varchar_type


def _is_uuid(val):
    try:
        uuid.UUID(val)
        return True
    except ValueError:
        return False


class Attr:
    def __init__(self, name, datatype, order=None, data=None):
        self.name = name
        self.datatype = datatype
        self.order = order
        self.data = data

    def sqlattr(self, dbtype):
        return _sqlid(self.name) + ' ' + (_varchar_type(dbtype) if self.datatype == 'varchar' else self.datatype)

    def __repr__(self):
        if self.data is None:
            return '(name=' + self.name + ', datatype=' + self.datatype + ', order=' + str(self.order) + ')'
        else:
            return ('(name=' + self.name + ', datatype=' + self.datatype + ', order=' + str(self.order) + ', data=' +
                    str(self.data) + ')')


def _old_jtable(table):
    return table + '_jtable'


def _tcatalog(table):
    return table + '__tcatalog'


# noinspection DuplicatedCode
def _old_drop_json_tables(db, table):
    jtable_sql = _sqlid(_old_jtable(table))
    cur = db.cursor()
    try:
        cur.execute('SELECT table_name FROM ' + jtable_sql)
        while True:
            row = cur.fetchone()
            if row is None:
                break
            t = row[0]
            cur2 = db.cursor()
            try:
                cur2.execute('DROP TABLE IF EXISTS ' + _sqlid(t))
            except (RuntimeError, psycopg2.Error):
                pass
            finally:
                cur2.close()
    except (duckdb.CatalogException, RuntimeError, psycopg2.Error):
        pass
    finally:
        cur.close()
    cur = db.cursor()
    try:
        cur.execute('DROP TABLE IF EXISTS ' + jtable_sql)
    except (duckdb.CatalogException, RuntimeError, psycopg2.Error):
        pass
    finally:
        cur.close()


# noinspection DuplicatedCode
def _drop_json_tables(db, table):
    tcatalog_sql = _sqlid(_tcatalog(table))
    cur = db.cursor()
    try:
        cur.execute('SELECT table_name FROM ' + tcatalog_sql)
        while True:
            row = cur.fetchone()
            if row is None:
                break
            t = row[0]
            cur2 = db.cursor()
            try:
                cur2.execute('DROP TABLE IF EXISTS ' + _sqlid(t))
            except (duckdb.CatalogException, RuntimeError, psycopg2.Error):
                pass
            finally:
                cur2.close()
    except (duckdb.CatalogException, RuntimeError, psycopg2.Error):
        pass
    finally:
        cur.close()
    cur = db.cursor()
    try:
        cur.execute('DROP TABLE IF EXISTS ' + tcatalog_sql)
    except (RuntimeError, psycopg2.Error):
        pass
    finally:
        cur.close()
    _old_drop_json_tables(db, table)


def _table_name(parents):
    j = len(parents)
    while j > 0 and parents[j - 1][0] == 0:
        j -= 1
    table = ''
    i = 0
    while i < j:
        if i != 0:
            table += '__'
        table += parents[i][1]
        i += 1
    return table


def _compile_array_attrs(dbtype, parents, prefix, jarray, newattrs, depth, arrayattr, max_depth, quasikey):
    if depth > max_depth:
        return
    table = _table_name(parents)
    qkey = {}
    for k, a in quasikey.items():
        qkey[k] = Attr(a.name, a.datatype, order=1)
    if table not in newattrs:
        newattrs[table] = {}
    for k, a in quasikey.items():
        newattrs[table][k] = Attr(a.name, a.datatype, order=1)
    j_ord = Attr(prefix + 'o', 'integer', order=2)
    qkey[prefix + 'o'] = j_ord
    newattrs[table][prefix + 'o'] = j_ord
    for v in jarray:
        if isinstance(v, dict):
            _compile_attrs(dbtype, parents, prefix, v, newattrs, depth, max_depth, qkey)
        elif isinstance(v, list):
            # TODO
            continue
        elif isinstance(v, float) or isinstance(v, int):
            newattrs[table][arrayattr] = Attr(_decode_camel_case(arrayattr), 'numeric', order=3)
        else:
            newattrs[table][arrayattr] = Attr(_decode_camel_case(arrayattr), 'varchar', order=3)


def _compile_attrs(dbtype, parents, prefix, jdict, newattrs, depth, max_depth, quasikey):
    if depth > max_depth:
        return
    table = _table_name(parents)
    qkey = {}
    for k, a in quasikey.items():
        qkey[k] = Attr(a.name, a.datatype, order=1)
    arrays = []
    objects = []
    for k, v in jdict.items():
        if k is None or v is None:
            continue
        attr = prefix + k
        if isinstance(v, dict):
            if depth == max_depth:
                newattrs[table][attr] = Attr(_decode_camel_case(attr), 'varchar', order=3)
            else:
                objects.append((attr, v, k))
        elif isinstance(v, list):
            arrays.append((attr, v, k))
        elif isinstance(v, bool):
            a = Attr(_decode_camel_case(attr), 'boolean', order=3)
            qkey[attr] = a
            newattrs[table][attr] = a
        elif isinstance(v, float) or isinstance(v, int):
            a = Attr(_decode_camel_case(attr), 'numeric', order=3)
            qkey[attr] = a
            newattrs[table][attr] = a
        elif dbtype == 2 and _is_uuid(v):
            a = Attr(_decode_camel_case(attr), 'uuid', order=3)
            qkey[attr] = a
            newattrs[table][attr] = a
        else:
            a = Attr(_decode_camel_case(attr), 'varchar', order=3)
            qkey[attr] = a
            newattrs[table][attr] = a
    for b in objects:
        p = [(0, _decode_camel_case(b[2]))]
        _compile_attrs(dbtype, parents + p, _decode_camel_case(b[0]) + '__', b[1], newattrs, depth + 1, max_depth, qkey)
    for y in arrays:
        p = [(1, _decode_camel_case(y[2]))]
        _compile_array_attrs(dbtype, parents + p, _decode_camel_case(y[0]) + '__', y[1], newattrs, depth + 1, y[0], max_depth,
                             qkey)


def _transform_array_data(dbtype, prefix, cur, parents, jarray, newattrs, depth, row_ids, arrayattr, max_depth,
                          quasikey):
    if depth > max_depth:
        return
    table = _table_name(parents)
    for i, v in enumerate(jarray):
        if v is None:
            continue
        if isinstance(v, dict):
            qkey = {}
            for k, a in quasikey.items():
                qkey[k] = a
            qkey[prefix + 'o'] = Attr(prefix + 'o', 'integer', data=i + 1)
            _transform_data(dbtype, prefix, cur, parents, v, newattrs, depth, row_ids, max_depth, qkey)
            continue
        elif isinstance(v, list):
            # TODO
            continue
        a = newattrs[table][arrayattr]
        a.data = v
        if a.datatype == 'bigint':
            value = v
        elif a.datatype == 'numeric':
            value = v
        elif a.datatype == 'boolean':
            value = v
        else:
            value = v
        q = 'INSERT INTO ' + _sqlid(table) + '(__id'
        q += '' if len(quasikey) == 0 else ',' + ','.join([_sqlid(kv[1].name) for kv in quasikey.items()])
        q += ',' + prefix + 'o,' + _sqlid(a.name)
        q += ')VALUES(' + str(row_ids[table])
        q += '' if len(quasikey) == 0 else ',' + ','.join([_encode_sql(dbtype, kv[1].data) for kv in quasikey.items()])
        q += ',' + str(i + 1) + ',' + _encode_sql(dbtype, value) + ')'
        try:
            cur.execute(q)
        except (RuntimeError, psycopg2.Error) as e:
            raise RuntimeError('error executing SQL: ' + q) from e
        row_ids[table] += 1


def _compile_data(dbtype, prefix, cur, parents, jdict, newattrs, depth, row_ids, max_depth, quasikey):
    if depth > max_depth:
        return
    table = _table_name(parents)
    qkey = {}
    for k, a in quasikey.items():
        qkey[k] = a
    row = []
    arrays = []
    objects = []
    for k, v in jdict.items():
        if k is None:
            continue
        attr = prefix + k
        if isinstance(v, dict) and depth < max_depth:
            objects.append((attr, v, k))
        elif isinstance(v, list):
            arrays.append((attr, v, k))
        if attr not in newattrs[table]:
            continue
        aa = newattrs[table][attr]
        a = Attr(aa.name, aa.datatype, data=v)
        if a.datatype == 'bigint':
            qkey[attr] = a
            row.append((a.name, v))
        elif a.datatype == 'float':
            qkey[attr] = a
            row.append((a.name, v))
        elif a.datatype == 'boolean':
            qkey[attr] = a
            row.append((a.name, v))
        else:
            qkey[attr] = a
            if isinstance(v, dict):
                row.append((a.name, json.dumps(v, indent=4)))
            else:
                row.append((a.name, v))
    for b in objects:
        p = [(0, _decode_camel_case(b[2]))]
        row += _compile_data(dbtype, _decode_camel_case(b[0]) + '__', cur, parents + p, b[1], newattrs, depth + 1,
                             row_ids, max_depth, qkey)
    for y in arrays:
        p = [(1, _decode_camel_case(y[2]))]
        _transform_array_data(dbtype, _decode_camel_case(y[0]) + '__', cur, parents + p, y[1], newattrs, depth + 1,
                              row_ids, y[0], max_depth, qkey)
    return row


def _transform_data(dbtype, prefix, cur, parents, jdict, newattrs, depth, row_ids, max_depth, quasikey):
    if depth > max_depth:
        return
    table = _table_name(parents)
    row = []
    for k, a in quasikey.items():
        row.append((a.name, a.data))
    row += _compile_data(dbtype, prefix, cur, parents, jdict, newattrs, depth, row_ids, max_depth, quasikey)
    q = 'INSERT INTO ' + _sqlid(table) + '(__id,'
    q += ','.join([_sqlid(kv[0]) for kv in row])
    q += ')VALUES(' + str(row_ids[table]) + ','
    q += ','.join([_encode_sql(dbtype, kv[1]) for kv in row])
    q += ')'
    try:
        cur.execute(q)
    except (RuntimeError, psycopg2.Error) as e:
        raise RuntimeError('error executing SQL: ' + q) from e
    row_ids[table] += 1


def _transform_json(db, dbtype, table, total, quiet, max_depth):
    # Scan all fields for JSON data
    # First get a list of the string attributes
    str_attrs = []
    cur = db.cursor()
    try:
        cur.execute('SELECT * FROM ' + _sqlid(table) + ' LIMIT 1')
        for a in cur.description:
            if a[1] == 3802 or a[1] == 'STRING' or a[1] == 1043:
                str_attrs.append(a[0])
    finally:
        cur.close()
    # Scan data for JSON objects
    if len(str_attrs) == 0:
        return [], {}
    json_attrs = []
    json_attrs_set = set()
    newattrs = {}
    cur = _server_cursor(db, dbtype)
    try:
        cur.execute('SELECT ' + ','.join([_sqlid(a)+'::varchar' for a in str_attrs]) + ' FROM ' + _sqlid(table))
        pbar = None
        pbartotal = 0
        if not quiet:
            pbar = tqdm(desc='scanning', total=total, leave=False, mininterval=3, smoothing=0, colour='#A9A9A9',
                        bar_format='{desc} {bar}{postfix}')
        while True:
            row = cur.fetchone()
            if row is None:
                break
            for i, data in enumerate(row):
                if data is None:
                    continue
                ds = data.strip()
                if len(ds) == 0 or ds[0] != '{':
                    continue
                try:
                    jdict = json.loads(ds)
                except ValueError:
                    continue
                attr = str_attrs[i]
                if attr not in json_attrs_set:
                    json_attrs.append(attr)
                    json_attrs_set.add(attr)
                attr_index = json_attrs.index(attr)
                table_j = table + '__t' if attr_index == 0 else table + '__t' + str(attr_index + 1)
                if table_j not in newattrs:
                    newattrs[table_j] = {}
                _compile_attrs(dbtype, [(1, table_j)], '', jdict, newattrs, 1, max_depth, {})
            if not quiet:
                pbartotal += 1
                pbar.update(1)
        if not quiet:
            pbar.close()
    finally:
        cur.close()
    # Create table schemas
    cur = db.cursor()
    try:
        for t, attrs in newattrs.items():
            cur.execute('DROP TABLE IF EXISTS ' + _sqlid(t))
            cur.execute('CREATE TABLE ' + _sqlid(t) + '(__id bigint)')
            for k, a in attrs.items():
                if a.order == 1:
                    cur.execute('ALTER TABLE ' + _sqlid(t) + ' ADD COLUMN ' + a.sqlattr(dbtype))
            for k, a in attrs.items():
                if a.order == 2:
                    cur.execute('ALTER TABLE ' + _sqlid(t) + ' ADD COLUMN ' + a.sqlattr(dbtype))
            for k, a in attrs.items():
                if a.order == 3:
                    cur.execute('ALTER TABLE ' + _sqlid(t) + ' ADD COLUMN ' + a.sqlattr(dbtype))
    finally:
        cur.close()
    db.commit()
    # Set all row IDs to 1
    row_ids = {}
    for t in newattrs.keys():
        row_ids[t] = 1
    # Run transformation
    # Select only JSON columns
    if len(json_attrs) == 0:
        return [], {}
    cur = _server_cursor(db, dbtype)
    try:
        cur.execute('SELECT ' + ','.join([_sqlid(a)+'::varchar' for a in json_attrs]) + ' FROM ' + _sqlid(table))
        pbar = None
        pbartotal = 0
        if not quiet:
            pbar = tqdm(desc='transforming', total=total, leave=False, mininterval=3, smoothing=0, colour='#A9A9A9',
                        bar_format='{desc} {bar}{postfix}')
        cur2 = db.cursor()
        while True:
            row = cur.fetchone()
            if row is None:
                break
            for i, data in enumerate(row):
                if data is None:
                    continue
                d = data.strip()
                if len(d) == 0 or d[0] != '{':
                    continue
                try:
                    jdict = json.loads(d)
                except ValueError:
                    continue
                table_j = table + '__t' if i == 0 else table + '__t' + str(i + 1)
                _transform_data(dbtype, '', cur2, [(1, table_j)], jdict, newattrs, 1, row_ids, max_depth, {})
            if not quiet:
                pbartotal += 1
                pbar.update(1)
        if not quiet:
            pbar.close()
    finally:
        cur.close()
    db.commit()
    tcatalog = _tcatalog(table)
    cur = db.cursor()
    try:
        cur.execute('CREATE TABLE ' + _sqlid(tcatalog) + '(table_name ' + _varchar_type(dbtype) + ' NOT NULL)')
        for t in newattrs.keys():
            cur.execute('INSERT INTO ' + _sqlid(tcatalog) + ' VALUES(' + _encode_sql(dbtype, t) + ')')
    finally:
        cur.close()
    db.commit()
    return sorted(list(newattrs.keys()) + [tcatalog]), newattrs
