from dataclasses import dataclass

from pytest_cases import parametrize

from .base import EndToEndTestCase


@dataclass(frozen=True)
class DropTablesCase(EndToEndTestCase):
    drop: str
    expected_tables: list[str]
    keep_raw: bool


class DropTablesCases:
    @parametrize(keep_raw=[True, False])
    def case_one_table(self, keep_raw: bool) -> DropTablesCase:
        return DropTablesCase(
            drop="prefix",
            values={"prefix": [{"purchaseOrders": [{"id": "1"}]}]},
            expected_tables=[],
            keep_raw=keep_raw,
        )

    @parametrize(keep_raw=[True, False])
    def case_two_tables(self, keep_raw: bool) -> DropTablesCase:
        return DropTablesCase(
            drop="prefix",
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "id": "1",
                                "subObjects": [{"id": "2"}, {"id": "3"}],
                            },
                        ],
                    },
                ],
            },
            expected_tables=[],
            keep_raw=keep_raw,
        )

    @parametrize(keep_raw=[True, False])
    def case_separate_table(self, keep_raw: bool) -> DropTablesCase:
        expected_tables = [
            "notdropped__t",
            "notdropped__tcatalog",
        ]
        if keep_raw:
            expected_tables = ["notdropped", *expected_tables]

        return DropTablesCase(
            drop="prefix",
            values={
                "prefix": [{"purchaseOrders": [{"id": "1"}]}],
                "notdropped": [{"purchaseOrders": [{"id": "1"}]}],
            },
            expected_tables=expected_tables,
            keep_raw=keep_raw,
        )
