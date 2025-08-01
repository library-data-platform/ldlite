from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NonSRSQueryTestCase:
    query: str | dict[str, str] | None = None
    page_size: int = 1000
    expected_total_query: str = "cql.allRecords=1"
    expected_values_query: str = "id>00000000-0000-0000-0000-000000000000"


class NonSRSQueryTestCases:
    def case_no_query(self) -> NonSRSQueryTestCase:
        return NonSRSQueryTestCase()

    def custom_page_size(self) -> NonSRSQueryTestCase:
        return NonSRSQueryTestCase(page_size=50)
