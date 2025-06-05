from dataclasses import dataclass

from .base import TestCase


@dataclass(frozen=True)
class DropTablesCase(TestCase):
    drop: str
    expected_tables: list[str]


class DropTablesCases:
    def case_one_table(self) -> DropTablesCase:
        return DropTablesCase(
            drop="prefix",
            values={"prefix": [{"purchaseOrders": [{"id": "1"}]}]},
            expected_tables=[],
        )

    def case_two_tables(self) -> DropTablesCase:
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
        )

    def case_separate_table(self) -> DropTablesCase:
        return DropTablesCase(
            drop="prefix",
            values={
                "prefix": [{"purchaseOrders": [{"id": "1"}]}],
                "notdropped": [{"purchaseOrders": [{"id": "1"}]}],
            },
            expected_tables=[
                "notdropped",
                "notdropped__t",
                "notdropped__tcatalog",
            ],
        )
