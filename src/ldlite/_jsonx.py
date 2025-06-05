from __future__ import annotations

import json
import sqlite3
import uuid
from typing import TYPE_CHECKING, Literal, Union

import duckdb
import psycopg2
from tqdm import tqdm

from ._camelcase import decode_camel_case
from ._sqlx import (
    DBType,
    cast_to_varchar,
    encode_sql,
    server_cursor,
    sqlid,
    varchar_type,
)

if TYPE_CHECKING:
    from _typeshed import dbapi

JsonValue = Union[float, int, str, bool, "Json", list["JsonValue"], None]
Json = dict[str, JsonValue]


def _is_uuid(val: str) -> bool:
    try:
        uuid.UUID(val)
    except ValueError:
        return False

    return True


class Attr:
    def __init__(
        self,
        name: str,
        datatype: Literal["varchar", "integer", "numeric", "boolean", "uuid", "bigint"],
        order: None | Literal[1, 2, 3] = None,
        data: JsonValue = None,
    ):
        self.name = name
        self.datatype: Literal[
            "varchar",
            "integer",
            "numeric",
            "boolean",
            "uuid",
            "bigint",
        ] = datatype
        self.order: None | Literal[1, 2, 3] = order
        self.data = data

    def sqlattr(self, dbtype: DBType) -> str:
        return (
            sqlid(self.name)
            + " "
            + (varchar_type(dbtype) if self.datatype == "varchar" else self.datatype)
        )

    def __repr__(self) -> str:
        if self.data is None:
            return (
                "(name="
                + self.name
                + ", datatype="
                + self.datatype
                + ", order="
                + str(self.order)
                + ")"
            )
        return (
            "(name="
            + self.name
            + ", datatype="
            + self.datatype
            + ", order="
            + str(self.order)
            + ", data="
            + str(self.data)
            + ")"
        )


def _old_jtable(table: str) -> str:
    return table + "_jtable"


def _tcatalog(table: str) -> str:
    return table + "__tcatalog"


# noinspection DuplicatedCode
def _old_drop_json_tables(db: dbapi.DBAPIConnection, table: str) -> None:
    jtable_sql = sqlid(_old_jtable(table))
    cur = db.cursor()
    try:
        cur.execute("SELECT table_name FROM " + jtable_sql)
        rows = list(cur.fetchall())
        for row in rows:
            t = row[0]
            cur2 = db.cursor()
            try:
                cur2.execute("DROP TABLE " + sqlid(t))
            except (psycopg2.Error, duckdb.CatalogException, sqlite3.OperationalError):
                continue
            finally:
                cur2.close()
    except (
        psycopg2.Error,
        sqlite3.OperationalError,
        duckdb.CatalogException,
    ):
        pass
    finally:
        cur.close()
    cur = db.cursor()
    try:
        cur.execute("DROP TABLE " + jtable_sql)
    except (
        psycopg2.Error,
        duckdb.CatalogException,
        sqlite3.OperationalError,
    ):
        pass
    finally:
        cur.close()


# noinspection DuplicatedCode
def drop_json_tables(db: dbapi.DBAPIConnection, table: str) -> None:
    tcatalog_sql = sqlid(_tcatalog(table))
    cur = db.cursor()
    try:
        cur.execute("SELECT table_name FROM " + tcatalog_sql)
        rows = list(cur.fetchall())
        for row in rows:
            t = row[0]
            cur2 = db.cursor()
            try:
                cur2.execute("DROP TABLE " + sqlid(t))
            except (psycopg2.Error, duckdb.CatalogException, sqlite3.OperationalError):
                continue
            finally:
                cur2.close()
    except (
        psycopg2.Error,
        duckdb.CatalogException,
        sqlite3.OperationalError,
    ):
        pass
    finally:
        cur.close()
    cur = db.cursor()
    try:
        cur.execute("DROP TABLE " + tcatalog_sql)
    except (
        psycopg2.Error,
        duckdb.CatalogException,
        sqlite3.OperationalError,
    ):
        pass
    finally:
        cur.close()
    _old_drop_json_tables(db, table)


def _table_name(parents: list[tuple[int, str]]) -> str:
    j = len(parents)
    while j > 0 and parents[j - 1][0] == 0:
        j -= 1
    table = ""
    i = 0
    while i < j:
        if i != 0:
            table += "__"
        table += parents[i][1]
        i += 1
    return table


def _compile_array_attrs(  # noqa: PLR0913
    dbtype: DBType,
    parents: list[tuple[int, str]],
    prefix: str,
    jarray: list[JsonValue],
    newattrs: dict[str, dict[str, Attr]],
    depth: int,
    arrayattr: str,
    max_depth: int,
    quasikey: dict[str, Attr],
) -> None:
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
    j_ord = Attr(prefix + "o", "integer", order=2)
    qkey[prefix + "o"] = j_ord
    newattrs[table][prefix + "o"] = j_ord
    for v in jarray:
        if isinstance(v, dict):
            _compile_attrs(dbtype, parents, prefix, v, newattrs, depth, max_depth, qkey)
        elif isinstance(v, list):
            # TODO: ???
            continue
        elif isinstance(v, (float, int)):
            newattrs[table][arrayattr] = Attr(
                decode_camel_case(arrayattr),
                "numeric",
                order=3,
            )
        else:
            newattrs[table][arrayattr] = Attr(
                decode_camel_case(arrayattr),
                "varchar",
                order=3,
            )


def _compile_attrs(  # noqa: C901, PLR0912, PLR0913
    dbtype: DBType,
    parents: list[tuple[int, str]],
    prefix: str,
    jdict: Json,
    newattrs: dict[str, dict[str, Attr]],
    depth: int,
    max_depth: int,
    quasikey: dict[str, Attr],
) -> None:
    if depth > max_depth:
        return
    table = _table_name(parents)
    qkey = {}
    for k, a in quasikey.items():
        qkey[k] = Attr(a.name, a.datatype, order=1)
    arrays: list[tuple[str, list[JsonValue], str]] = []
    objects: list[tuple[str, Json, str]] = []
    for k, v in jdict.items():
        if k is None or v is None:
            continue
        attr = prefix + k
        if isinstance(v, dict):
            if depth == max_depth:
                newattrs[table][attr] = Attr(
                    decode_camel_case(attr),
                    "varchar",
                    order=3,
                )
            else:
                objects.append((attr, v, k))
        elif isinstance(v, list):
            arrays.append((attr, v, k))
        elif isinstance(v, bool):
            a = Attr(decode_camel_case(attr), "boolean", order=3)
            qkey[attr] = a
            newattrs[table][attr] = a
        elif isinstance(v, (float, int)):
            a = Attr(decode_camel_case(attr), "numeric", order=3)
            qkey[attr] = a
            newattrs[table][attr] = a
        elif dbtype == DBType.POSTGRES and _is_uuid(v):
            a = Attr(decode_camel_case(attr), "uuid", order=3)
            qkey[attr] = a
            newattrs[table][attr] = a
        else:
            a = Attr(decode_camel_case(attr), "varchar", order=3)
            qkey[attr] = a
            newattrs[table][attr] = a
    for b in objects:
        p = [(0, decode_camel_case(b[2]))]
        _compile_attrs(
            dbtype,
            parents + p,
            decode_camel_case(b[0]) + "__",
            b[1],
            newattrs,
            depth + 1,
            max_depth,
            qkey,
        )
    for y in arrays:
        p = [(1, decode_camel_case(y[2]))]
        _compile_array_attrs(
            dbtype,
            parents + p,
            decode_camel_case(y[0]) + "__",
            y[1],
            newattrs,
            depth + 1,
            y[0],
            max_depth,
            qkey,
        )


def _transform_array_data(  # noqa: PLR0913
    dbtype: DBType,
    prefix: str,
    cur: dbapi.DBAPICursor,
    parents: list[tuple[int, str]],
    jarray: list[JsonValue],
    newattrs: dict[str, dict[str, Attr]],
    depth: int,
    row_ids: dict[str, int],
    arrayattr: str,
    max_depth: int,
    quasikey: dict[str, Attr],
) -> None:
    if depth > max_depth:
        return
    table = _table_name(parents)
    for i, v in enumerate(jarray):
        if v is None:
            continue
        if isinstance(v, dict):
            qkey = {k: quasikey[k] for k in quasikey}
            qkey[prefix + "o"] = Attr(prefix + "o", "integer", data=i + 1)
            _transform_data(
                dbtype,
                prefix,
                cur,
                parents,
                v,
                newattrs,
                depth,
                row_ids,
                max_depth,
                qkey,
            )
            continue
        if isinstance(v, list):
            # TODO: ???
            continue
        a = newattrs[table][arrayattr]
        a.data = v
        value = v
        q = "INSERT INTO " + sqlid(table) + "(__id"
        q += (
            ""
            if len(quasikey) == 0
            else "," + ",".join([sqlid(kv[1].name) for kv in quasikey.items()])
        )
        q += "," + prefix + "o," + sqlid(a.name)
        q += ")VALUES(" + str(row_ids[table])
        q += (
            ""
            if len(quasikey) == 0
            else ","
            + ",".join([encode_sql(dbtype, kv[1].data) for kv in quasikey.items()])
        )
        q += "," + str(i + 1) + "," + encode_sql(dbtype, value) + ")"
        try:
            cur.execute(q)
        except (RuntimeError, psycopg2.Error) as e:
            raise RuntimeError("error executing SQL: " + q) from e
        row_ids[table] += 1


def _compile_data(  # noqa: C901, PLR0912, PLR0913
    dbtype: DBType,
    prefix: str,
    cur: dbapi.DBAPICursor,
    parents: list[tuple[int, str]],
    jdict: Json,
    newattrs: dict[str, dict[str, Attr]],
    depth: int,
    row_ids: dict[str, int],
    max_depth: int,
    quasikey: dict[str, Attr],
) -> None | list[tuple[str, JsonValue]]:
    if depth > max_depth:
        return None
    table = _table_name(parents)
    qkey = {k: quasikey[k] for k in quasikey}
    row: list[tuple[str, JsonValue]] = []
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
        if a.datatype in {"bigint", "float", "boolean"}:
            qkey[attr] = a
            row.append((a.name, v))
        else:
            qkey[attr] = a
            if isinstance(v, dict):
                row.append((a.name, json.dumps(v, indent=4)))
            else:
                row.append((a.name, v))
    for b in objects:
        p = [(0, decode_camel_case(b[2]))]
        r = _compile_data(
            dbtype,
            decode_camel_case(b[0]) + "__",
            cur,
            parents + p,
            b[1],
            newattrs,
            depth + 1,
            row_ids,
            max_depth,
            qkey,
        )
        if r is not None:
            row += r
    for y in arrays:
        p = [(1, decode_camel_case(y[2]))]
        _transform_array_data(
            dbtype,
            decode_camel_case(y[0]) + "__",
            cur,
            parents + p,
            y[1],
            newattrs,
            depth + 1,
            row_ids,
            y[0],
            max_depth,
            qkey,
        )
    return row


def _transform_data(  # noqa: PLR0913
    dbtype: DBType,
    prefix: str,
    cur: dbapi.DBAPICursor,
    parents: list[tuple[int, str]],
    jdict: Json,
    newattrs: dict[str, dict[str, Attr]],
    depth: int,
    row_ids: dict[str, int],
    max_depth: int,
    quasikey: dict[str, Attr],
) -> None:
    if depth > max_depth:
        return
    table = _table_name(parents)
    row = [(a.name, a.data) for a in quasikey.values()]
    r = _compile_data(
        dbtype,
        prefix,
        cur,
        parents,
        jdict,
        newattrs,
        depth,
        row_ids,
        max_depth,
        quasikey,
    )
    if r is not None:
        row += r
    q = "INSERT INTO " + sqlid(table) + "(__id,"
    q += ",".join([sqlid(kv[0]) for kv in row])
    q += ")VALUES(" + str(row_ids[table]) + ","
    q += ",".join([encode_sql(dbtype, kv[1]) for kv in row])
    q += ")"
    try:
        cur.execute(q)
    except (RuntimeError, psycopg2.Error) as e:
        raise RuntimeError("error executing SQL: " + q) from e
    row_ids[table] += 1


def transform_json(  # noqa: C901, PLR0912, PLR0913, PLR0915
    db: dbapi.DBAPIConnection,
    dbtype: DBType,
    table: str,
    total: int,
    quiet: bool,
    max_depth: int,
) -> tuple[list[str], dict[str, dict[str, Attr]]]:
    # Scan all fields for JSON data
    # First get a list of the string attributes
    str_attrs: list[str] = []
    cur = db.cursor()
    try:
        cur.execute("SELECT * FROM " + sqlid(table) + " LIMIT 1")
        if cur.description is not None:
            str_attrs.extend([a[0] for a in cur.description])
    finally:
        cur.close()
    # Scan data for JSON objects
    if len(str_attrs) == 0:
        return [], {}
    json_attrs: list[str] = []
    json_attrs_set: set[str] = set()
    newattrs: dict[str, dict[str, Attr]] = {}
    cur = server_cursor(db, dbtype)
    try:
        cur.execute(
            "SELECT "
            + ",".join([cast_to_varchar(sqlid(a), dbtype) for a in str_attrs])
            + " FROM "
            + sqlid(table),
        )
        pbar = None
        pbartotal = 0
        if not quiet:
            pbar = tqdm(
                desc="scanning",
                total=total,
                leave=False,
                mininterval=3,
                smoothing=0,
                colour="#A9A9A9",
                bar_format="{desc} {bar}{postfix}",
            )
        while True:
            row = cur.fetchone()
            if row is None:
                break
            for i, data in enumerate(row):
                if data is None:
                    continue
                ds = data.strip()
                if len(ds) == 0 or ds[0] != "{":
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
                table_j = (
                    table + "__t"
                    if attr_index == 0
                    else table + "__t" + str(attr_index + 1)
                )
                if table_j not in newattrs:
                    newattrs[table_j] = {}
                _compile_attrs(
                    dbtype,
                    [(1, table_j)],
                    "",
                    jdict,
                    newattrs,
                    1,
                    max_depth,
                    {},
                )
            if pbar is not None:
                pbartotal += 1
                pbar.update(1)
        if pbar is not None:
            pbar.close()
    finally:
        cur.close()
    # Create table schemas
    cur = db.cursor()
    try:
        for t, attrs in newattrs.items():
            cur.execute("DROP TABLE IF EXISTS " + sqlid(t))
            cur.execute("CREATE TABLE " + sqlid(t) + "(__id bigint)")
            for a in attrs.values():
                if a.order == 1:
                    cur.execute(
                        "ALTER TABLE " + sqlid(t) + " ADD COLUMN " + a.sqlattr(dbtype),
                    )
            for a in attrs.values():
                if a.order == 2:
                    cur.execute(
                        "ALTER TABLE " + sqlid(t) + " ADD COLUMN " + a.sqlattr(dbtype),
                    )
            for a in attrs.values():
                if a.order == 3:
                    cur.execute(
                        "ALTER TABLE " + sqlid(t) + " ADD COLUMN " + a.sqlattr(dbtype),
                    )
    finally:
        cur.close()
    db.commit()
    # Set all row IDs to 1
    row_ids = {}
    for t in newattrs:
        row_ids[t] = 1
    # Run transformation
    # Select only JSON columns
    if len(json_attrs) == 0:
        return [], {}
    cur = server_cursor(db, dbtype)
    try:
        cur.execute(
            "SELECT "
            + ",".join([cast_to_varchar(sqlid(a), dbtype) for a in json_attrs])
            + " FROM "
            + sqlid(table),
        )
        pbar = None
        pbartotal = 0
        if not quiet:
            pbar = tqdm(
                desc="transforming",
                total=total,
                leave=False,
                mininterval=3,
                smoothing=0,
                colour="#A9A9A9",
                bar_format="{desc} {bar}{postfix}",
            )
        cur2 = db.cursor()
        while True:
            row = cur.fetchone()
            if row is None:
                break
            for i, data in enumerate(row):
                if data is None:
                    continue
                d = data.strip()
                if len(d) == 0 or d[0] != "{":
                    continue
                try:
                    jdict = json.loads(d)
                except ValueError:
                    continue
                table_j = table + "__t" if i == 0 else table + "__t" + str(i + 1)
                _transform_data(
                    dbtype,
                    "",
                    cur2,
                    [(1, table_j)],
                    jdict,
                    newattrs,
                    1,
                    row_ids,
                    max_depth,
                    {},
                )
            if pbar is not None:
                pbartotal += 1
                pbar.update(1)
        if pbar is not None:
            pbar.close()
    except (
        RuntimeError,
        psycopg2.Error,
        sqlite3.OperationalError,
        duckdb.CatalogException,
    ) as e:
        raise RuntimeError("running JSON transform: " + str(e)) from e
    finally:
        cur.close()
    db.commit()
    tcatalog = _tcatalog(table)
    cur = db.cursor()
    try:
        cur.execute(
            "CREATE TABLE "
            + sqlid(tcatalog)
            + "(table_name "
            + varchar_type(dbtype)
            + " NOT NULL)",
        )
        for t in newattrs:
            cur.execute(
                "INSERT INTO "
                + sqlid(tcatalog)
                + " VALUES("
                + encode_sql(dbtype, t)
                + ")",
            )
    except (
        RuntimeError,
        psycopg2.Error,
        sqlite3.OperationalError,
        duckdb.CatalogException,
    ) as e:
        raise RuntimeError("writing table catalog for JSON transform: " + str(e)) from e
    finally:
        cur.close()
    db.commit()
    return sorted([*list(newattrs.keys()), tcatalog]), newattrs
