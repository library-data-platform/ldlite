from __future__ import annotations

from dataclasses import dataclass

from pytest_cases import parametrize


@dataclass(frozen=True)
class NonSRSQueryTestCase:
    query: str | dict[str, str] | None = None
    page_size: int = 1000
    expected_total_query: str = "cql.allRecords=1"
    expected_values_query: str = "id>00000000-0000-0000-0000-000000000000"
    expected_filters_query: str = "id>00000000-0000-0000-0000-000000000000"


class NonSRSQueryTestCases:
    def case_no_query(self) -> NonSRSQueryTestCase:
        return NonSRSQueryTestCase()

    def case_with_query(self) -> NonSRSQueryTestCase:
        return NonSRSQueryTestCase(
            query="some_query",
            expected_total_query="some_query",
            expected_values_query=(
                "id>00000000-0000-0000-0000-000000000000 and (some_query)"
            ),
        )

    def custom_page_size(self) -> NonSRSQueryTestCase:
        return NonSRSQueryTestCase(page_size=50)

    @parametrize(
        q=[
            "cql.allRecords=1",
            "cql.allRecords=1 sortBy id",
            "cql.allRecords=1 sortBy id asc",
            "cql.allRecords=1 sortBy id desc",
            "cql.allRecords=1 sortby id",
            "cql.allRecords=1 sortby id asc",
            "cql.allRecords=1 sortby id desc",
            "cql.allRecords=1 SORTBY id ASC",
            "cql.allRecords=1 SORTBY id DESC",
        ],
    )
    def case_default_query(self, q: str) -> NonSRSQueryTestCase:
        return NonSRSQueryTestCase(query=q)

    @parametrize(
        q=[
            "some_query sortBy some_column",
            "some_query sortBy some_column asc",
            "some_query sortBy some_column desc",
            "some_query sortby some_column",
            "some_query sortby some_column asc",
            "some_query sortby some_column desc",
            "some_query SORTBY some_column ASC",
            "some_query SORTBY some_column DESC",
        ],
    )
    def case_sort_query(self, q: str) -> NonSRSQueryTestCase:
        return NonSRSQueryTestCase(
            query=q,
            expected_total_query="some_query",
            expected_values_query=(
                "id>00000000-0000-0000-0000-000000000000 and (some_query)"
            ),
        )
