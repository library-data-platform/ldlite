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
class DropTablesTC(MockedResponseTestCase):
    drop: str
    expected_tables: list[str]


@parametrize(keep_raw=[True, False])
def case_one_table(keep_raw: bool) -> DropTablesTC:
    return DropTablesTC(
        Call(
            "prefix",
            returns={"purchaseOrders": [{"id": "1"}]},
            keep_raw=keep_raw,
        ),
        drop="prefix",
        expected_tables=[],
    )


@parametrize(keep_raw=[True, False])
def case_two_tables(keep_raw: bool) -> DropTablesTC:
    return DropTablesTC(
        Call(
            "prefix",
            returns={
                "purchaseOrders": [
                    {
                        "id": "1",
                        "subObjects": [{"id": "2"}, {"id": "3"}],
                    },
                ],
            },
            keep_raw=keep_raw,
        ),
        drop="prefix",
        expected_tables=[],
    )


@parametrize(keep_raw=[True, False])
def case_separate_table(keep_raw: bool) -> DropTablesTC:
    expected_tables = [
        "notdropped__t",
        "notdropped__tcatalog",
    ]
    if keep_raw:
        expected_tables = ["notdropped", *expected_tables]

    return DropTablesTC(
        [
            Call(
                "prefix",
                returns={"purchaseOrders": [{"id": "1"}]},
                keep_raw=keep_raw,
            ),
            Call(
                "notdropped",
                returns={"purchaseOrders": [{"id": "1"}]},
                keep_raw=keep_raw,
            ),
        ],
        drop="prefix",
        expected_tables=expected_tables,
    )


def _arrange(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    tc: DropTablesTC,
) -> "ldlite.LDLite":
    from ldlite import LDLite

    uut = LDLite()
    tc.patch_request_get(uut, httpx_post_mock, client_get_mock)
    uut.connect_folio("https://doesnt.matter", "", "", "")
    return uut


def _act(uut: "ldlite.LDLite", tc: DropTablesTC) -> None:
    uut.drop_tables(tc.drop)
    for call in tc.calls_list:
        uut.query(table=call.prefix, path="/patched", keep_raw=call.keep_raw)
    uut.drop_tables(tc.drop)


def _assert(
    conn: "dbapi.DBAPIConnection",
    res_schema: str,  # TODO: have schema be part of tc
    tc: DropTablesTC,
) -> None:
    with closing(conn.cursor()) as cur:
        cur.execute(
            """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema=$1
                """,
            (res_schema,),
        )
        assert sorted([r[0] for r in cur.fetchall()]) == sorted(tc.expected_tables)

        cur.execute('SELECT COUNT(*) FROM "ldlite_system"."load_history"')
        assert (ud := cur.fetchone()) is not None
        assert ud[0] == len(tc.calls_list) - 1
        cur.execute(
            'SELECT COUNT(*) FROM "ldlite_system"."load_history" '
            'WHERE "table_name" = $1',
            (tc.drop,),
        )
        assert (d := cur.fetchone()) is not None
        assert d[0] == 0


@mock.patch("httpx_folio.auth.httpx.post")
@mock.patch("httpx_folio.factories.httpx.Client.get")
@parametrize_with_cases("tc", cases=".")
def test_duckdb(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    tc: DropTablesTC,
) -> None:
    uut = _arrange(client_get_mock, httpx_post_mock, tc)
    dsn = f":memory:{tc.db}"
    uut.connect_db(dsn)

    _act(uut, tc)

    with duckdb.connect(dsn) as conn:
        _assert(cast("dbapi.DBAPIConnection", conn), "main", tc)


@mock.patch("httpx_folio.auth.httpx.post")
@mock.patch("httpx_folio.factories.httpx.Client.get")
@parametrize_with_cases("tc", cases=".")
def test_postgres(
    client_get_mock: MagicMock,
    httpx_post_mock: MagicMock,
    pg_dsn: None | Callable[[str], str],
    tc: DropTablesTC,
) -> None:
    if pg_dsn is None:
        pytest.skip("Specify the pg host using --pg-host to run")

    uut = _arrange(client_get_mock, httpx_post_mock, tc)
    dsn = pg_dsn(tc.db)
    uut.connect_db_postgresql(dsn)

    _act(uut, tc)

    with psycopg.connect(dsn, cursor_factory=psycopg.RawCursor) as conn:
        _assert(cast("dbapi.DBAPIConnection", conn), "public", tc)
