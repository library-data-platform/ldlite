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


SrsCases = [
    "/source-storage/records",
    "/source-storage/stream/records",
    "/source-storage/source-records",
    "/source-storage/stream/source-records",
]


class TestIntegration:
    def _arrange(
        self,
        folio_params: tuple[bool, FolioParams],
        snapshot_ok: bool = True,
    ) -> "ldlite.LDLite":
        if not snapshot_ok and folio_params[0]:
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
        uut = self._arrange(folio_params, tc.snapshot_ok)
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

        uut = self._arrange(folio_params, tc.snapshot_ok)
        db = "db" + str(uuid4()).split("-")[0]
        print(db)  # noqa: T201
        dsn = pg_dsn(db)
        uut.connect_db_postgresql(dsn)

        uut.query(table="test", path=tc.path, query=tc.query)

        self._nonsrs_assert(uut, folio_params, tc)

    def _srs_assert(self, uut: "ldlite.LDLite", is_snapshot: bool) -> None:
        if uut.db is None:
            pytest.fail("No active database connection.")

        with closing(uut.db.cursor()) as cur:
            cur.execute("SELECT COUNT(*) FROM (SELECT DISTINCT * FROM test__t) t;")
            actual = cast("tuple[int]", cur.fetchone())[0]

            # snapshot has a variable number of records
            assert actual >= 1
            if is_snapshot:
                assert actual <= 10
            else:
                assert actual == 10

    @parametrize(path=SrsCases)
    def test_srs_duckdb(
        self,
        folio_params: tuple[bool, FolioParams],
        path: str,
    ) -> None:
        uut = self._arrange(folio_params)
        uut.connect_db()

        uut.query(table="test", path=path, limit=10)

        self._srs_assert(uut, folio_params[0])

    @parametrize(path=SrsCases)
    def test_srs_postgres(
        self,
        folio_params: tuple[bool, FolioParams],
        pg_dsn: None | Callable[[str], str],
        path: str,
    ) -> None:
        if pg_dsn is None:
            pytest.skip("Specify the pg host using --pg-host to run")

        uut = self._arrange(folio_params)
        db = "db" + str(uuid4()).split("-")[0]
        print(db)  # noqa: T201
        dsn = pg_dsn(db)
        uut.connect_db_postgresql(dsn)

        uut.query(table="test", path=path, limit=10)

        self._srs_assert(uut, folio_params[0])
