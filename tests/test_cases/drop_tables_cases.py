from dataclasses import dataclass

from pytest_cases import parametrize

from .base import Call, MockedResponseTestCase


@dataclass(frozen=True)
class DropTablesCase(MockedResponseTestCase):
    drop: str
    expected_tables: list[str]


class DropTablesCases:
    @parametrize(keep_raw=[True, False])
    def case_one_table(self, keep_raw: bool) -> DropTablesCase:
        return DropTablesCase(
            Call(
                "prefix",
                returns={"purchaseOrders": [{"id": "1"}]},
                keep_raw=keep_raw,
            ),
            drop="prefix",
            expected_tables=[],
        )

    @parametrize(keep_raw=[True, False])
    def case_two_tables(self, keep_raw: bool) -> DropTablesCase:
        return DropTablesCase(
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
    def case_separate_table(self, keep_raw: bool) -> DropTablesCase:
        expected_tables = [
            "notdropped__t",
            "notdropped__tcatalog",
        ]
        if keep_raw:
            expected_tables = ["notdropped", *expected_tables]

        return DropTablesCase(
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
