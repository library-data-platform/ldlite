from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast
from unittest import mock
from unittest.mock import MagicMock

import duckdb
import psycopg
import pytest
from pytest_cases import parametrize, parametrize_with_cases

from .mock_response_case import Call, MockedResponseTestCase

if TYPE_CHECKING:
    from _typeshed import dbapi

    import ldlite


@dataclass(frozen=True)
class LoadHistoryTC(MockedResponseTestCase):
    expected_loads: dict[str, tuple[str | None, int]]


@parametrize(query=[None, "poline.id=*A"])
def case_one_load(query: str | None) -> LoadHistoryTC:
    return LoadHistoryTC(
        Call(
            "prefix",
            query=query,
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                    },
                    {
                        "id": "b096504a-9999-4664-9bf5-1b872466fd66",
                        "value": "value-2",
                    },
                ],
            },
        ),
        expected_loads={"prefix": (query, 2)},
    )


def case_schema_load() -> LoadHistoryTC:
    return LoadHistoryTC(
        Call(
            "schema.prefix",
            returns={
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                    },
                    {
                        "id": "b096504a-9999-4664-9bf5-1b872466fd66",
                        "value": "value-2",
                    },
                ],
            },
        ),
        expected_loads={"schema.prefix": (None, 2)},
    )


def case_two_loads() -> LoadHistoryTC:
    return LoadHistoryTC(
        [
            Call(
                "prefix",
                returns={
                    "purchaseOrders": [
                        {
                            "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                            "value": "value",
                        },
                    ],
                },
            ),
            Call(
                "prefix",
                query="a query",
                returns={
                    "purchaseOrders": [
                        {
                            "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                            "value": "value",
                        },
                        {
                            "id": "b096504a-9999-4664-9bf5-1b872466fd66",
                            "value": "value-2",
                        },
                    ],
                },
            ),
        ],
        expected_loads={"prefix": ("a query", 2)},
    )


def _arrange(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    tc: LoadHistoryTC,
) -> "ldlite.LDLite":
    from ldlite import LDLite

    uut = LDLite()
    tc.patch_request_get(uut, httpx_post_mock, client_get_mock)
    uut.connect_folio("https://doesnt.matter", "", "", "")
    return uut


def _act(uut: "ldlite.LDLite", tc: LoadHistoryTC) -> None:
    for call in tc.calls_list:
        uut.query(table=call.prefix, path="/patched", query=call.query)


def _assert(
    conn: "dbapi.DBAPIConnection",
    tc: LoadHistoryTC,
) -> None:
    with closing(conn.cursor()) as cur:
        cur.execute('SELECT COUNT(*) FROM "ldlite_system"."load_history"')
        assert (ud := cur.fetchone()) is not None
        assert ud[0] == len(tc.expected_loads)

        for tn, (q, t) in tc.expected_loads.items():
            cur.execute(
                'SELECT * FROM "ldlite_system"."load_history" WHERE "table_name" = $1',
                (tn,),
            )
            assert (d := cur.fetchone()) is not None
            assert d[1] == q
            assert d[7] == t
            assert d[6] > d[5] > d[4] > d[3] > d[2]


@mock.patch("httpx_folio.auth.httpx.post")
@mock.patch("httpx_folio.factories.httpx.Client.get")
@parametrize_with_cases("tc", cases=".")
def test_duckdb(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    tc: LoadHistoryTC,
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
    tc: LoadHistoryTC,
) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    uut = _arrange(client_get_mock, httpx_post_mock, tc)
    dsn = pg_dsn(tc.db)
    uut.connect_db_postgresql(dsn)

    _act(uut, tc)

    with psycopg.connect(dsn, cursor_factory=psycopg.RawCursor) as conn:
        _assert(cast("dbapi.DBAPIConnection", conn), tc)
