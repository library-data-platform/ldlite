from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast
from unittest import mock
from unittest.mock import MagicMock

import duckdb
import psycopg
import pytest
from psycopg import sql
from pytest_cases import parametrize, parametrize_with_cases

from .mock_response_case import Call, MockedResponseTestCase

if TYPE_CHECKING:
    from _typeshed import dbapi

    import ldlite


@dataclass(frozen=True)
class QueryTC(MockedResponseTestCase):
    expected_tables: list[str]
    expected_values: dict[str, tuple[list[str], list[tuple[Any, ...]]]]
    unexpected_columns: list[tuple[str, str]] | None = None
    expected_indexes: list[tuple[str, str]] | None = None


@parametrize(json_depth=range(1, 2))
def case_one_table(json_depth: int) -> QueryTC:
    return QueryTC(
        Call(
            "tests.prefix",
            json_depth=json_depth,
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                    },
                ],
            },
        ),
        expected_tables=["prefix", "prefix__t", "prefix__tcatalog"],
        expected_values={
            "prefix__t": (
                ["id", "value"],
                [("b096504a-3d54-4664-9bf5-1b872466fd66", "value")],
            ),
            "prefix__tcatalog": (["table_name"], [("tests.prefix__t",)]),
        },
        expected_indexes=[
            ("prefix__t", "id"),
        ],
    )


@parametrize(json_depth=range(2, 3))
def case_two_tables(json_depth: int) -> QueryTC:
    return QueryTC(
        Call(
            "tests.prefix",
            json_depth=json_depth,
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                        "subObjects": [
                            {
                                "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                "value": "sub-value-1",
                            },
                            {
                                "id": "f5bda109-a719-4f72-b797-b9c22f45e4e1",
                                "value": "sub-value-2",
                            },
                        ],
                    },
                ],
            },
        ),
        expected_tables=[
            "prefix",
            "prefix__t",
            "prefix__t__sub_objects",
            "prefix__tcatalog",
        ],
        expected_values={
            "prefix__t": (
                ["id", "value"],
                [("b096504a-3d54-4664-9bf5-1b872466fd66", "value")],
            ),
            "prefix__t__sub_objects": (
                ["id", "sub_objects__id", "sub_objects__value"],
                [
                    (
                        "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "2b94c631-fca9-4892-a730-03ee529ffe2a",
                        "sub-value-1",
                    ),
                    (
                        "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "f5bda109-a719-4f72-b797-b9c22f45e4e1",
                        "sub-value-2",
                    ),
                ],
            ),
            "prefix__tcatalog": (
                ["table_name"],
                [("tests.prefix__t",), ("tests.prefix__t__sub_objects",)],
            ),
        },
        expected_indexes=[
            ("prefix__t", "id"),
            ("prefix__t__sub_objects", "id"),
            ("prefix__t__sub_objects", "sub_objects__id"),
        ],
    )


@parametrize(json_depth=range(1))
def case_table_no_expansion(json_depth: int) -> QueryTC:
    return QueryTC(
        Call(
            "tests.prefix",
            json_depth=json_depth,
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                        "subObjects": [
                            {
                                "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                "value": "sub-value",
                            },
                        ],
                    },
                ],
            },
        ),
        expected_tables=["prefix"],
        expected_values={},
    )


def case_table_underexpansion() -> QueryTC:
    return QueryTC(
        Call(
            "tests.prefix",
            json_depth=2,
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "subObjects": [
                            {
                                "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                "value": "sub-value",
                                "subSubObjects": [
                                    {
                                        "id": ("2b94c631-fca9-4892-a730-03ee529ffe2a"),
                                        "value": "sub-sub-value",
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        ),
        expected_tables=[
            "prefix",
            "prefix__t",
            "prefix__t__sub_objects",
            "prefix__tcatalog",
        ],
        expected_values={
            "prefix__t__sub_objects": (
                [
                    "id",
                    "sub_objects__id",
                    "sub_objects__value",
                ],
                [
                    (
                        "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "2b94c631-fca9-4892-a730-03ee529ffe2a",
                        "sub-value",
                    ),
                ],
            ),
            "prefix__tcatalog": (
                ["table_name"],
                [("tests.prefix__t",), ("tests.prefix__t__sub_objects",)],
            ),
        },
    )


@parametrize(json_depth=range(3, 4))
def case_three_tables(json_depth: int) -> QueryTC:
    return QueryTC(
        Call(
            "tests.prefix",
            json_depth=json_depth,
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                        "subObjects": [
                            {
                                "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                "value": "sub-value",
                                "subSubObjects": [
                                    {
                                        "id": ("2b94c631-fca9-4892-a730-03ee529ffe2a"),
                                        "value": "sub-sub-value",
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        ),
        expected_tables=[
            "prefix",
            "prefix__t",
            "prefix__t__sub_objects",
            "prefix__t__sub_objects__sub_sub_objects",
            "prefix__tcatalog",
        ],
        expected_values={
            "prefix__t__sub_objects__sub_sub_objects": (
                [
                    "id",
                    "sub_objects__id",
                    "sub_objects__sub_sub_objects__id",
                    "sub_objects__sub_sub_objects__value",
                ],
                [
                    (
                        "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "2b94c631-fca9-4892-a730-03ee529ffe2a",
                        "2b94c631-fca9-4892-a730-03ee529ffe2a",
                        "sub-sub-value",
                    ),
                ],
            ),
            "prefix__tcatalog": (
                ["table_name"],
                [
                    ("tests.prefix__t",),
                    ("tests.prefix__t__sub_objects",),
                    ("tests.prefix__t__sub_objects__sub_sub_objects",),
                ],
            ),
        },
        expected_indexes=[
            ("prefix__t", "id"),
            ("prefix__t__sub_objects", "id"),
            ("prefix__t__sub_objects", "sub_objects__id"),
            ("prefix__t__sub_objects__sub_sub_objects", "id"),
            ("prefix__t__sub_objects__sub_sub_objects", "sub_objects__id"),
            (
                "prefix__t__sub_objects__sub_sub_objects",
                "sub_objects__sub_sub_objects__id",
            ),
        ],
    )


def case_nested_object() -> QueryTC:
    return QueryTC(
        Call(
            "tests.prefix",
            json_depth=2,
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                        "subObject": {
                            "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "value": "sub-value",
                        },
                    },
                ],
            },
        ),
        expected_tables=["prefix", "prefix__t", "prefix__tcatalog"],
        expected_values={
            "prefix__t": (
                ["id", "value", "sub_object__id", "sub_object__value"],
                [
                    (
                        "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value",
                        "2b94c631-fca9-4892-a730-03ee529ffe2a",
                        "sub-value",
                    ),
                ],
            ),
            "prefix__tcatalog": (
                ["table_name"],
                [("tests.prefix__t",)],
            ),
        },
        expected_indexes=[
            ("prefix__t", "id"),
            ("prefix__t", "sub_object__id"),
        ],
    )


def case_doubly_nested_object() -> QueryTC:
    return QueryTC(
        Call(
            "tests.prefix",
            json_depth=3,
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                        "subObject": {
                            "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "value": "sub-value",
                            "subSubObject": {
                                "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                "value": "sub-sub-value",
                            },
                        },
                    },
                ],
            },
        ),
        expected_tables=["prefix", "prefix__t", "prefix__tcatalog"],
        expected_values={
            "prefix__t": (
                [
                    "id",
                    "value",
                    "sub_object__id",
                    "sub_object__sub_sub_object__id",
                    "sub_object__sub_sub_object__value",
                ],
                [
                    (
                        "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value",
                        "2b94c631-fca9-4892-a730-03ee529ffe2a",
                        "2b94c631-fca9-4892-a730-03ee529ffe2a",
                        "sub-sub-value",
                    ),
                ],
            ),
            "prefix__tcatalog": (
                ["table_name"],
                [("tests.prefix__t",)],
            ),
        },
        expected_indexes=[
            ("prefix__t", "id"),
            ("prefix__t", "sub_object__id"),
            ("prefix__t", "sub_object__sub_sub_object__id"),
        ],
    )


def case_nested_object_underexpansion() -> QueryTC:
    return QueryTC(
        Call(
            "tests.prefix",
            json_depth=1,
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                        "subObject": {
                            "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "value": "sub-value",
                        },
                    },
                ],
            },
        ),
        expected_tables=["prefix", "prefix__t", "prefix__tcatalog"],
        expected_values={
            "prefix__t": (
                ["id", "value", "sub_object"],
                [
                    (
                        "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value",
                        '{"id":"2b94c631-fca9-4892-a730-03ee529ffe2a",'
                        '"value":"sub-value"}',
                    ),
                ],
            ),
            "prefix__tcatalog": (
                ["table_name"],
                [("tests.prefix__t",)],
            ),
        },
    )


def case_tables_and_lists() -> QueryTC:
    return QueryTC(
        Call(
            "tests.prefix",
            json_depth=3,
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "subObjects": [
                            {
                                "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            },
                            {
                                "id": "f5bda109-a719-4f72-b797-b9c22f45e4e1",
                            },
                        ],
                        "excludedFromLists": {
                            "id": "b0f3e260-8eb7-4dd2-b9c6-be74e00229a8",
                        },
                        "subObject": {
                            "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "subSubObject": {
                                "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            },
                            "subObjects": [
                                {
                                    "id": "15f1d05d-6a54-4704-9d53-90718a8339b1",
                                },
                                {
                                    "id": "37de4f71-cc99-4e8c-90c6-5a17f168f21d",
                                },
                            ],
                        },
                    },
                ],
            },
        ),
        expected_tables=[
            "prefix",
            "prefix__t",
            "prefix__t__sub_objects",
            "prefix__t__sub_object__sub_objects",
            "prefix__tcatalog",
        ],
        expected_values={
            "prefix__t": (
                ["id", "sub_object__id", "sub_object__sub_sub_object__id"],
                [
                    (
                        "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "2b94c631-fca9-4892-a730-03ee529ffe2a",
                        "2b94c631-fca9-4892-a730-03ee529ffe2a",
                    ),
                ],
            ),
            "prefix__t__sub_objects": (
                ["id", "sub_objects__id"],
                [
                    (
                        "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "2b94c631-fca9-4892-a730-03ee529ffe2a",
                    ),
                    (
                        "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "f5bda109-a719-4f72-b797-b9c22f45e4e1",
                    ),
                ],
            ),
            "prefix__t__sub_object__sub_objects": (
                [
                    "id",
                    "sub_object__id",
                    "sub_object__sub_objects__id",
                ],
                [
                    (
                        "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "2b94c631-fca9-4892-a730-03ee529ffe2a",
                        "15f1d05d-6a54-4704-9d53-90718a8339b1",
                    ),
                    (
                        "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "2b94c631-fca9-4892-a730-03ee529ffe2a",
                        "37de4f71-cc99-4e8c-90c6-5a17f168f21d",
                    ),
                ],
            ),
            "prefix__tcatalog": (
                ["table_name"],
                [
                    ("tests.prefix__t",),
                    ("tests.prefix__t__sub_object__sub_objects",),
                    ("tests.prefix__t__sub_objects",),
                ],
            ),
        },
        unexpected_columns=[
            ("prefix__t__sub_objects", "sub_object__id"),
            ("prefix__t__sub_object__sub_objects", "excluded_from_lists__id"),
        ],
    )


def case_id_generation() -> QueryTC:
    return QueryTC(
        Call(
            "tests.prefix",
            json_depth=4,
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "subObjects": [
                            {
                                "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                "subSubObjects": [
                                    {
                                        "id": ("2b94c631-fca9-4892-a730-03ee529ffe2a"),
                                    },
                                    {
                                        "id": ("8516a913-8bf7-55a4-ab71-417aba9171c9"),
                                    },
                                ],
                            },
                            {
                                "id": "b5d8cdc4-9441-487c-90cf-0c7ec97728eb",
                                "subSubObjects": [
                                    {
                                        "id": ("13a24cc8-a15c-4158-abbd-4abf25c8815a"),
                                    },
                                    {
                                        "id": ("37344879-09ce-4cd8-976f-bf1a57c0cfa6"),
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
        ),
        expected_tables=[
            "prefix",
            "prefix__t",
            "prefix__t__sub_objects",
            "prefix__t__sub_objects__sub_sub_objects",
            "prefix__tcatalog",
        ],
        expected_values={
            "prefix__t__sub_objects": (
                ["__id", "id", "sub_objects__o"],
                [
                    (
                        "1",
                        "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "1",
                    ),
                    (
                        "2",
                        "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "2",
                    ),
                ],
            ),
            "prefix__t__sub_objects__sub_sub_objects": (
                ["sub_objects__o", "sub_objects__sub_sub_objects__o"],
                [
                    ("1", "1"),
                    ("1", "2"),
                    ("2", "1"),
                    ("2", "2"),
                ],
            ),
        },
    )


def case_indexing_id_like() -> QueryTC:
    return QueryTC(
        Call(
            "tests.prefix",
            json_depth=4,
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "otherId": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "anIdButWithADifferentEnding": (
                            "b096504a-3d54-4664-9bf5-1b872466fd66"
                        ),
                    },
                ],
            },
        ),
        expected_tables=[
            "prefix",
            "prefix__t",
            "prefix__tcatalog",
        ],
        expected_values={},
        expected_indexes=[
            ("prefix__t", "id"),
            ("prefix__t", "other_id"),
            ("prefix__t", "an_id_but_with_a_different_ending"),
        ],
    )


@parametrize(json_depth=range(1, 2))
def case_drop_raw(json_depth: int) -> QueryTC:
    return QueryTC(
        Call(
            "tests.prefix",
            json_depth=json_depth,
            keep_raw=False,
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                    },
                ],
            },
        ),
        expected_tables=["prefix__t", "prefix__tcatalog"],
        expected_values={
            "prefix__t": (
                ["id", "value"],
                [("b096504a-3d54-4664-9bf5-1b872466fd66", "value")],
            ),
            "prefix__tcatalog": (["table_name"], [("tests.prefix__t",)]),
        },
        expected_indexes=[
            ("prefix__t", "id"),
        ],
    )


# this case should be testing the FolioClient class
# but it isn't setup to mock the data properly right now
def case_null_records() -> QueryTC:
    return QueryTC(
        Call(
            "tests.prefix",
            json_depth=1,
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                    },
                    None,
                ],
            },
        ),
        expected_tables=["prefix", "prefix__t", "prefix__tcatalog"],
        expected_values={},
        expected_indexes=[
            ("prefix__t", "id"),
        ],
    )


def case_erm_keys() -> QueryTC:
    return QueryTC(
        Call(
            "tests.prefix",
            json_depth=3,
            returns={
                "pageSize": 30,
                "page": 1,
                "totalPages": 10,
                "meta": {"updated": "by"},
                "total": 285,
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                    },
                ],
            },
        ),
        expected_tables=["prefix", "prefix__t", "prefix__tcatalog"],
        expected_values={
            "prefix__t": (
                ["id", "value"],
                [("b096504a-3d54-4664-9bf5-1b872466fd66", "value")],
            ),
            "prefix__tcatalog": (["table_name"], [("tests.prefix__t",)]),
        },
        expected_indexes=[
            ("prefix__t", "id"),
        ],
    )


def _arrange(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    tc: QueryTC,
) -> "ldlite.LDLite":
    from ldlite import LDLite

    uut = LDLite()
    tc.patch_request_get(uut, httpx_post_mock, client_get_mock)
    uut.connect_folio("https://doesnt.matter", "", "", "")
    return uut


def _act(uut: "ldlite.LDLite", tc: QueryTC) -> None:
    for call in tc.calls_list:
        uut.query(
            table=call.prefix,
            path="/patched",
            json_depth=call.json_depth,
            keep_raw=call.keep_raw,
        )


def _assert(
    conn: "dbapi.DBAPIConnection",
    tc: QueryTC,
) -> None:
    with closing(conn.cursor()) as cur:
        cur.execute(
            """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='tests'
                """,
        )
        assert sorted([r[0] for r in cur.fetchall()]) == sorted(tc.expected_tables)

        for table, (cols, values) in tc.expected_values.items():
            cur.execute(
                sql.SQL('SELECT {cols} FROM {table} ORDER BY {cols} COLLATE "C";')
                .format(
                    cols=sql.SQL(", ").join(
                        [
                            sql.SQL(
                                "REGEXP_REPLACE({col}::text, '\\s+', '', 'g')",
                            ).format(col=sql.Identifier(c))
                            for c in cols
                        ],
                    ),
                    table=sql.Identifier("tests", table),
                )
                .as_string(),
            )
            for v in values:
                assert cur.fetchone() == v, str(v)

            assert cur.fetchone() is None

        if tc.unexpected_columns is not None:
            for exp in tc.unexpected_columns:
                cur.execute(
                    """
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_name=$1 and column_name=$2
                """,
                    exp,
                )
                assert cur.fetchone() == (0,), str(exp)

        if tc.expected_indexes is not None:
            for exp in tc.expected_indexes:
                cur.execute(
                    """
                    SELECT * FROM pg_indexes
                    WHERE tablename = $1 and indexdef LIKE $2;
                    """,
                    (exp[0], "%" + exp[1] + "%"),
                )
                assert cur.fetchone() is not None, str(exp)

            indexed_tables = {exp[0] for exp in tc.expected_indexes}
            where = ",".join([f"${n}" for n in range(1, len(indexed_tables) + 1)])
            cur.execute(
                f"""
                    SELECT tablename, indexdef FROM pg_indexes
                    WHERE tablename IN ({where})
                    ORDER BY tablename;
                    """,
                list(indexed_tables),
            )
            actual_indexes = cur.fetchall()
            expected_indexes = tc.expected_indexes
            assert len(actual_indexes) == len(expected_indexes)


@mock.patch("httpx_folio.auth.httpx.post")
@mock.patch("httpx_folio.factories.httpx.Client.get")
@parametrize_with_cases("tc", cases=".")
def test_duckdb(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    tc: QueryTC,
) -> None:
    uut = _arrange(client_get_mock, httpx_post_mock, tc)
    dsn = f":memory:{tc.db}"
    uut.connect_db(dsn)

    _act(uut, tc)

    with duckdb.connect(dsn) as conn:
        _assert(cast("dbapi.DBAPIConnection", conn), tc)


@mock.patch("httpx_folio.auth.httpx.post")
@mock.patch("httpx_folio.factories.httpx.Client.get")
@parametrize_with_cases("tc", cases=".")
def test_postgres(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    pg_dsn: None | Callable[[str], str],
    tc: QueryTC,
) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    uut = _arrange(client_get_mock, httpx_post_mock, tc)
    dsn = pg_dsn(tc.db)
    uut.connect_db_postgresql(dsn)

    _act(uut, tc)

    with psycopg.connect(dsn, cursor_factory=psycopg.RawCursor) as conn:
        _assert(cast("dbapi.DBAPIConnection", conn), tc)
