from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import duckdb
import psycopg
import pytest
from pytest_cases import parametrize, parametrize_with_cases

if TYPE_CHECKING:
    from _typeshed import dbapi


def _db() -> str:
    db = "db" + str(uuid4()).split("-")[0]
    print(db)  # noqa: T201
    return db


@dataclass
class Assertion:
    statement: str

    expect: int | str | list[tuple[str | int, ...]] | None = None
    exp_pg: int | str | list[tuple[str | int, ...]] | None = None
    exp_duck: int | str | list[tuple[str | int, ...]] | None = None

    def expected(self, db: str) -> int | str | list[tuple[str | int, ...]] | None:
        if db == "postgres":
            return self.exp_pg or self.expect
        if db == "duckdb":
            return self.exp_duck or self.expect
        return self.expect


@dataclass
class ExpansionTC:
    records: list[bytes]
    assertions: list[Assertion]
    json_depth: int = 999
    keep_raw: bool = True


def case_typed_columns() -> ExpansionTC:
    return ExpansionTC(
        records=[
            b"""
{
    "id": "id1",
    "numeric": 1,
    "text": "value",
    "boolean": false,
    "uuid": "88888888-8888-1888-8888-888888888888"
}
""",
            b"""
{
    "id": "id2",
    "numeric": 2,
    "text": "00000000-0000-1000-A000-000000000000",
    "boolean": false,
    "uuid": "11111111-1111-1111-8111-111111111111"
}
""",
        ],
        assertions=[
            Assertion(
                f"""
SELECT DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'prefix__t' AND COLUMN_NAME = '{a[0]}'
""",
                exp_pg=a[0],
                exp_duck=a[1],
            )
            for a in [
                ("numeric", "DECIMAL(18,3)"),
                ("text", "VARCHAR"),
                ("uuid", "UUID"),
                ("boolean", "BOOLEAN"),
            ]
        ],
    )


@parametrize(
    "assertion",
    [
        ("all_null", None, None),
        ("nullable_numeric", "numeric", "DECIMAL(18,3)"),
        ("nullable_uuid", "uuid", "UUID"),
        ("nullable_bool", "boolean", "BOOLEAN"),
        ("nullable_object__id", "numeric", "DECIMAL(18,3)"),
        ("nullable_array", "numeric", "DECIMAL(18,3)"),
        ("sortof_nullable_array__id", "numeric", "DECIMAL(18,3)"),
    ],
    idgen="prop={assertion[0]}",
)
def case_null(assertion: tuple[str, str | None, str | None]) -> ExpansionTC:
    return ExpansionTC(
        records=[
            b"""
{
    "all_null": null,
    "nullable_numeric": null,
    "nullable_bool": null,
    "nullable_uuid": null,
    "nullable_object": null,
    "nullable_array": [],
    "sortof_nullable_array": [{ "id": 5 }]
}
""",
            b"""
{
    "all_null": null,
    "nullable_numeric": 5,
    "nullable_uuid": null,
    "nullable_bool": false,
    "nullable_object": { "id": 5 },
    "nullable_array": null,
    "sortof_nullable_array": [{}, {}]
}
""",
            b"""
{
    "all_null": null,
    "nullable_numeric": null,
    "nullable_bool": true,
    "nullable_uuid": "0b03c888-102b-18e9-afb7-85e22229ca4d",
    "nullable_object": { "id": null},
    "nullable_array": [null, 5, null],
    "sortof_nullable_array": [{ "id": 5 }]
}
""",
        ],
        assertions=[
            Assertion(
                f"""
SELECT DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME LIKE 'prefix__t%' AND COLUMN_NAME = '{assertion[0]}'
""",
                exp_pg=assertion[1],
                exp_duck=assertion[2],
            )
            for _ in range(15)
        ],
    )


@parametrize(
    "prop",
    [
        "some_uuid",
        "some_numeric",
        "some_object",
        "some_array",
    ],
)
def case_mixed_types(prop: str) -> ExpansionTC:
    return ExpansionTC(
        records=[
            b"""
{
    "some_uuid": "0b03c888-102b-18e9-afb7-85e22229ca4d",
    "some_numeric": 5,
    "some_object": { "id": 5 },
    "some_array": [5]
}
""",
            b"""
{
    "some_uuid": "not-uuid",
    "some_numeric": "not-numeric",
    "some_object": "not-object",
    "some_array": "not-array"
}
""",
            b"""
{
    "some_uuid": "0b03c888-102b-18e9-afb7-85e22229ca4d",
    "some_numeric": 5,
    "some_object": { "id": 5 },
    "some_array": [5]
}
""",
        ],
        assertions=[
            Assertion(
                f"""
SELECT DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'prefix__t' AND COLUMN_NAME = '{prop}'
""",
                exp_pg="text",
                exp_duck="VARCHAR",
            )
            for _ in range(15)
        ],
    )


def case_basic_array() -> ExpansionTC:
    return ExpansionTC(
        records=[
            b"""
{
    "id": "id1",
    "list1": ["a1", "b1", "c1"],
    "list2": [1]
}
""",
            b"""
{
    "id": "id2",
    "list1": ["a2", "b2", "c2"],
    "list2": [2]
}
""",
        ],
        assertions=[
            Assertion("""SELECT COUNT(*) FROM tests.prefix__t__list1""", expect=6),
            Assertion("""SELECT COUNT(*) FROM tests.prefix__t__list2""", expect=2),
            Assertion(
                """SELECT * FROM tests.prefix__tcatalog ORDER BY table_name""",
                expect=[
                    ("prefix__t",),
                    ("prefix__t__list1",),
                    ("prefix__t__list2",),
                ],
            ),
            Assertion(
                """
SELECT id, list1
FROM tests.prefix__t__list1
ORDER BY id, list1
                """,
                expect=[
                    ("id1", "a1"),
                    ("id1", "b1"),
                    ("id1", "c1"),
                    ("id2", "a2"),
                    ("id2", "b2"),
                    ("id2", "c2"),
                ],
            ),
            Assertion(
                """
SELECT id, list1__o
FROM tests.prefix__t__list1
ORDER BY id, list1__o
                """,
                expect=[
                    ("id1", 1),
                    ("id1", 2),
                    ("id1", 3),
                    ("id2", 1),
                    ("id2", 2),
                    ("id2", 3),
                ],
            ),
            *[
                Assertion(
                    f"""
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'prefix__t__{a[0]}'
ORDER BY ORDINAL_POSITION
""",
                    exp_duck=[
                        ("__id", "INTEGER"),
                        ("id", "VARCHAR"),
                        (f"{a[0]}__o", "SMALLINT"),
                        (f"{a[0]}", a[1]),
                    ],
                    exp_pg=[
                        ("__id", "integer"),
                        ("id", "text"),
                        (f"{a[0]}__o", "smallint"),
                        (f"{a[0]}", a[2]),
                    ],
                )
                for a in [
                    ("list1", "VARCHAR", "text"),
                    ("list2", "DECIMAL(18,3)", "numeric"),
                ]
            ],
        ],
    )


def case_nested_arrays() -> ExpansionTC:
    return ExpansionTC(
        records=[
            b"""
{
    "id": "id1",
    "sub": [{ "id": "sub1id1"}, { "id": "sub2id1" }]
}
""",
            b"""
{
    "id": "id2",
    "sub": [{ "id": "sub1id2" }, { "id": "sub2id2" }]
}
""",
        ],
        assertions=[
            Assertion("""SELECT COUNT(*) FROM tests.prefix__t__sub""", expect=4),
            Assertion(
                """SELECT * FROM tests.prefix__tcatalog ORDER BY table_name""",
                expect=[
                    ("prefix__t",),
                    ("prefix__t__sub",),
                ],
            ),
            Assertion(
                """
SELECT id, sub__id
FROM tests.prefix__t__sub
ORDER BY id, sub__id
                """,
                expect=[
                    ("id1", "sub1id1"),
                    ("id1", "sub2id1"),
                    ("id2", "sub1id2"),
                    ("id2", "sub2id2"),
                ],
            ),
            Assertion(
                """
SELECT id, sub__o
FROM tests.prefix__t__sub
ORDER BY id, sub__o
                """,
                expect=[
                    ("id1", 1),
                    ("id1", 2),
                    ("id2", 1),
                    ("id2", 2),
                ],
            ),
            Assertion(
                """
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'prefix__t__sub'
ORDER BY ORDINAL_POSITION
""",
                exp_duck=[
                    ("__id", "INTEGER"),
                    ("id", "VARCHAR"),
                    ("sub__o", "SMALLINT"),
                    ("sub__id", "VARCHAR"),
                ],
                exp_pg=[
                    ("__id", "integer"),
                    ("id", "text"),
                    ("sub__o", "smallint"),
                    ("sub__id", "text"),
                ],
            ),
        ],
    )


def case_basic_object() -> ExpansionTC:
    return ExpansionTC(
        records=[
            b"""{"id": "id1", "camelValue": "value1"}""",
            b"""{"id": "id2", "camelValue": "value2"}""",
        ],
        assertions=[
            Assertion("SELECT COUNT(*) FROM tests.prefix__t;", 2),
            Assertion(
                """SELECT * FROM tests.prefix__tcatalog ORDER BY table_name""",
                expect=[
                    ("prefix__t",),
                ],
            ),
            Assertion("SELECT id FROM tests.prefix__t WHERE __id = 1", "id1"),
            Assertion(
                "SELECT camel_value FROM tests.prefix__t WHERE __id = 1",
                "value1",
            ),
            Assertion("SELECT id FROM tests.prefix__t WHERE __id = 2", "id2"),
            Assertion(
                "SELECT camel_value FROM tests.prefix__t WHERE __id = 2",
                "value2",
            ),
        ],
    )


def case_nested_objects() -> ExpansionTC:
    return ExpansionTC(
        records=[
            b"""
{
    "id": "id1",
    "value": "value1",
    "intEr": {
        "mediate": {"id": "snake"}
    },
    "sub": {
        "id": "sub_id1",
        "subSub": {"id": "subsub_id1"},
        "subSib": {"id": "subsib_id1"}
    },
    "sib": {
        "id": "sib_id1",
        "sibSub": {"id": "sibsub_id1"},
        "sibSib": {"id": "sibsib_id1"}
    }
}
""",
            b"""
{
    "id": "id2",
    "value": "value2",
    "intEr": {
        "mediate": {"id": "case"}
    },
    "sub": {
        "id": "sub_id2",
        "subSub": {"id": "subsub_id2"},
        "subSib": {"id": "subsib_id2"}
    },
    "sib": {
        "id": "sib_id2",
        "sibSub": {"id": "sibsub_id2"},
        "sibSib": {"id": "sibsib_id2"}
    }
}
""",
        ],
        assertions=[
            Assertion("SELECT COUNT(*) FROM tests.prefix__t;", 2),
            Assertion(
                """SELECT * FROM tests.prefix__tcatalog ORDER BY table_name""",
                expect=[
                    ("prefix__t",),
                ],
            ),
            Assertion("SELECT id FROM tests.prefix__t WHERE __id = 1", "id1"),
            Assertion("SELECT sub__id FROM tests.prefix__t WHERE __id = 1", "sub_id1"),
            Assertion(
                "SELECT int_er__mediate__id FROM tests.prefix__t WHERE __id = 1",
                "snake",
            ),
            Assertion(
                "SELECT sub__sub_sub__id FROM tests.prefix__t WHERE __id = 1",
                "subsub_id1",
            ),
            Assertion(
                "SELECT sub__sub_sib__id FROM tests.prefix__t WHERE __id = 1",
                "subsib_id1",
            ),
            Assertion("SELECT id FROM tests.prefix__t WHERE __id = 2", "id2"),
            Assertion("SELECT sib__id FROM tests.prefix__t WHERE __id = 2", "sib_id2"),
            Assertion(
                "SELECT int_er__mediate__id FROM tests.prefix__t WHERE __id = 2",
                "case",
            ),
            Assertion(
                "SELECT sib__sib_sub__id FROM tests.prefix__t WHERE __id = 2",
                "sibsub_id2",
            ),
            Assertion(
                "SELECT sib__sib_sib__id FROM tests.prefix__t WHERE __id = 2",
                "sibsib_id2",
            ),
        ],
    )


def case_json_depth() -> ExpansionTC:
    return ExpansionTC(
        json_depth=3,
        records=[
            b"""
{
    "id": "id1",
    "depth2Obj": {
        "id": "id2",
        "depth3Obj": {
            "id": "id3",
            "depth4Obj": {
                "id": "id4"
            },
            "depth4Arr": ["id4"]
        },
        "depth3Arr": ["id3"]
    },
    "depth2Arr": [
        {
            "id": "id2",
            "depth3Arr": [
                {
                    "id": "id3",
                    "depth4Arr": ["id4"]
                }
            ],
            "depth3Obj": {
                "id": "id3",
                "depth4Obj": { "id": "id4" },
                "depth4Arr": ["id4"]
            }
        }
    ],
    "depth2Obj2": { "id": "id2" },
    "depth2Obj3": { "id": "id2" },
    "depth2Obj4": { "id": "id2" }
}""",
        ],
        assertions=[
            Assertion("""SELECT "depth2_obj4__id" FROM tests.prefix__t""", "id2"),
            Assertion(
                """SELECT * FROM tests.prefix__tcatalog ORDER BY table_name""",
                expect=[
                    ("prefix__t",),
                    ("prefix__t__depth2_arr",),
                    ("prefix__t__depth2_arr__depth3_arr",),
                    ("prefix__t__depth2_obj__depth3_arr",),
                ],
            ),
            Assertion(
                """
SELECT "depth2_obj__depth3_obj__id"
FROM tests.prefix__t
""",
                "id3",
            ),
            Assertion(
                """
SELECT "depth2_obj__depth3_arr"
FROM tests.prefix__t__depth2_obj__depth3_arr
""",
                "id3",
            ),
            Assertion(
                """
SELECT "depth2_arr__depth3_arr__id"
FROM tests.prefix__t__depth2_arr__depth3_arr
""",
                "id3",
            ),
            Assertion(
                """
SELECT "depth2_arr__depth3_obj__id"
FROM tests.prefix__t__depth2_arr
""",
                "id3",
            ),
            Assertion(
                """
SELECT COUNT(DISTINCT DATA_TYPE), ANY_VALUE(DATA_TYPE)
FROM INFORMATION_SCHEMA.COLUMNS
WHERE COLUMN_NAME IN (
    'depth2_obj__depth3_obj__depth4_obj'
    ,'depth2_obj__depth3_obj__depth4_arr'
    ,'depth2_arr__depth3_arr__depth4_arr'
    ,'depth2_arr__depth3_obj__depth4_arr'
    ,'depth2_arr__depth3_obj__depth4_obj'
)
""",
                exp_duck=[(1, "JSON")],
                exp_pg=[(1, "jsonb")],
            ),
        ],
    )


def case_keep_raw() -> ExpansionTC:
    return ExpansionTC(
        keep_raw=True,
        records=[
            b"""{ "id": "id1" }""",
            b"""{ "id": "id2" }""",
        ],
        assertions=[
            Assertion(
                """
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'prefix'
""",
                1,
            ),
        ],
    )


def case_dont_keep_raw() -> ExpansionTC:
    return ExpansionTC(
        keep_raw=False,
        records=[
            b"""{ "id": "id1" }""",
            b"""{ "id": "id2" }""",
        ],
        assertions=[
            Assertion(
                """
SELECT COUNT(*)
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = 'prefix'
""",
                0,
            ),
        ],
    )


def _assert(
    conn: "dbapi.DBAPIConnection",
    db: str,
    tc: ExpansionTC,
) -> None:
    with closing(conn.cursor()) as cur:
        for a in tc.assertions:
            cur.execute(a.statement.format())

            if isinstance(expected := a.expected(db), list):
                for e in expected:
                    assert cur.fetchone() == e
                assert cur.fetchone() is None
            else:
                actual = cur.fetchone()
                if a.expected(db) is None:
                    assert actual is None
                else:
                    assert actual is not None
                    assert actual[0] == a.expected(db)


@parametrize_with_cases("tc", cases=".")
def test_duckdb(tc: ExpansionTC) -> None:
    from ldlite import LDLite

    dsn = f":memory:{_db()}"

    ld = LDLite()
    ld.connect_db(dsn)
    assert ld.database_experimental is not None

    ld.database_experimental.ingest_records("tests.prefix", iter(tc.records))
    ld.database_experimental.expand_prefix("tests.prefix", tc.json_depth, tc.keep_raw)

    with duckdb.connect(dsn) as conn:
        _assert(cast("dbapi.DBAPIConnection", conn), "duckdb", tc)


@parametrize_with_cases("tc", cases=".")
def test_postgres(pg_dsn: None | Callable[[str], str], tc: ExpansionTC) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    from ldlite import LDLite

    dsn = pg_dsn(_db())

    ld = LDLite()
    ld.connect_db_postgresql(dsn)
    assert ld.database_experimental is not None

    ld.database_experimental.ingest_records("tests.prefix", iter(tc.records))
    ld.database_experimental.expand_prefix("tests.prefix", tc.json_depth, tc.keep_raw)

    with psycopg.connect(dsn, cursor_factory=psycopg.RawCursor) as conn:
        _assert(cast("dbapi.DBAPIConnection", conn), "postgres", tc)
