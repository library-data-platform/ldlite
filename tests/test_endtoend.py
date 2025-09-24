from collections.abc import Callable
from contextlib import closing
from dataclasses import astuple, dataclass
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest
from httpx_folio.factories import FolioParams, default_client_factory
from httpx_folio.query import QueryParams, QueryType
from pytest_cases import parametrize, parametrize_with_cases

if TYPE_CHECKING:
    import ldlite


@dataclass(frozen=True)
class NonSrsCase:
    snapshot_ok: bool
    path: str
    query: str | dict[str, str] | None


class NonSrsCases:
    def case_no_id_col(self) -> NonSrsCase:
        return NonSrsCase(False, "/finance/ledger-rollovers-logs", None)

    def case_finicky_sorting(self) -> NonSrsCase:
        return NonSrsCase(False, "/notes", "title=Key Permissions")

    def case_id_descending(self) -> NonSrsCase:
        return NonSrsCase(True, "/invoice/invoices", "vendorId==e0* sortBy id desc")

    def case_non_id_sort(self) -> NonSrsCase:
        return NonSrsCase(True, "/groups", "cql.allRecords=1 sortBy group desc")


class TestIntegration:
    def _nonsrs_arrange(
        self,
        folio_params: tuple[bool, FolioParams],
        tc: NonSrsCase,
    ) -> "ldlite.LDLite":
        if not tc.snapshot_ok and folio_params[0]:
            pytest.skip(
                "Specify an environment having data with --folio-base-url to run",
            )

        from ldlite import LDLite

        uut = LDLite()
        uut.page_size = 3
        uut.connect_folio(*astuple(folio_params[1]))
        return uut

    def _nonsrs_assert(
        self,
        uut: "ldlite.LDLite",
        folio_params: tuple[bool, FolioParams],
        tc: NonSrsCase,
    ) -> None:
        with default_client_factory(folio_params[1])() as client:
            res = client.get(
                tc.path,
                params=QueryParams(cast("QueryType", tc.query)).stats(),
            )
            res.raise_for_status()

            expected = res.json()["totalRecords"]
            assert expected > 3

        if uut.db is None:
            pytest.fail("No active database connection.")

        with closing(uut.db.cursor()) as cur:
            cur.execute("SELECT COUNT(*) FROM (SELECT DISTINCT * FROM test__t) t;")
            actual = cast("tuple[int]", cur.fetchone())[0]

            assert actual == expected

    @parametrize_with_cases("tc", cases=NonSrsCases)
    def test_nonsrs_duckdb(
        self,
        folio_params: tuple[bool, FolioParams],
        tc: NonSrsCase,
    ) -> None:
        uut = self._nonsrs_arrange(folio_params, tc)
        uut.connect_db()

        uut.query(table="test", path=tc.path, query=tc.query)
        self._nonsrs_assert(uut, folio_params, tc)

    @parametrize_with_cases("tc", cases=NonSrsCases)
    def test_nonsrs_postgres(
        self,
        folio_params: tuple[bool, FolioParams],
        pg_dsn: None | Callable[[str], str],
        tc: NonSrsCase,
    ) -> None:
        if pg_dsn is None:
            pytest.skip("Specify the pg host using --pg-host to run")

        uut = self._nonsrs_arrange(folio_params, tc)
        db = "db" + str(uuid4()).split("-")[0]
        print(db)  # noqa: T201
        dsn = pg_dsn(db)
        uut.connect_db_postgresql(dsn)

        uut.query(table="test", path=tc.path, query=tc.query)
        self._nonsrs_assert(uut, folio_params, tc)

    @parametrize(
        srs=[
            "/source-storage/records",
            "/source-storage/stream/records",
            "/source-storage/source-records",
            "/source-storage/stream/source-records",
        ],
    )
    def test_endtoend_srs(
        self,
        folio_params: tuple[bool, FolioParams],
        srs: str,
    ) -> None:
        from ldlite import LDLite as uut

        ld = uut()
        db = ld.connect_db()

        ld.connect_folio(*astuple(folio_params[1]))
        ld.query(table="test", path=srs, limit=10)

        db.execute("SELECT COUNT(DISTINCT COLUMNS(*)) FROM test__t;")
        actual = cast("tuple[int]", db.fetchone())[0]

        # snapshot a variable number of records
        assert actual >= 1
        if folio_params[0]:
            assert actual <= 10
        else:
            assert actual == 10
