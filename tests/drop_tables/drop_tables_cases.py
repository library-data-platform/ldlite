from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4


@dataclass(frozen=True)
class DropTablesCase:
    db: str
    drop: str
    values: dict[str, list[dict[str, Any]]]
    expected_tables: list[str]

    def patch_request_get(self, _request_get_mock: MagicMock) -> None:
        side_effects = []
        for values in self.values.values():
            total_mock = MagicMock()
            total_mock.status_code = 200
            total_mock.json.return_value = {}

            value_mocks = []
            for v in values:
                value_mock = MagicMock()
                value_mock.status_code = 200
                value_mock.json.return_value = v
                value_mocks.append(value_mock)

            end_mock = MagicMock()
            end_mock.status_code = 200
            end_mock.json.return_value = {"empty": []}

            side_effects.extend([total_mock, *value_mocks, end_mock])

        _request_get_mock.side_effect = side_effects


class DropTablesCases:
    @classmethod
    def _db(cls) -> str:
        db = "db" + str(uuid4()).split("-")[0]
        print(db)  # noqa: T201
        return db

    def case_one_table(self) -> DropTablesCase:
        return DropTablesCase(
            self._db(),
            "prefix",
            {"prefix": [{"purchaseOrders": [{"id": "1"}]}]},
            [],
        )

    def case_two_tables(self) -> DropTablesCase:
        return DropTablesCase(
            self._db(),
            "prefix",
            {
                "prefix": [
                    {"purchaseOrders": [{"id": "1", "subObjects": [{"id": "2"}]}]},
                ],
            },
            [],
        )

    def case_separate_table(self) -> DropTablesCase:
        return DropTablesCase(
            self._db(),
            "prefix",
            {
                "prefix": [{"purchaseOrders": [{"id": "1"}]}],
                "notdropped": [{"purchaseOrders": [{"id": "1"}]}],
            },
            ["notdropped", "notdropped__t", "notdropped__tcatalog"],
        )
