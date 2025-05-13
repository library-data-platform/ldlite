from uuid import uuid4
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

@dataclass(frozen=True)
class QueryCase:
    db: str
    values: list[dict[str, Any]]
    expected_tables: set[str]
    expected_values: dict[str, tuple[list[str], list[tuple[str,...]]]]

    def patch__request_get(self, _request_get_mock: MagicMock) -> None:
        total_mock = MagicMock()
        total_mock.status_code = 200
        total_mock.json.return_value = {}

        value_mocks = []
        for v in self.values:
            value_mock = MagicMock()
            value_mock.status_code = 200
            value_mock.json.return_value = v
            value_mocks.append(value_mock)

        end_mock = MagicMock()
        end_mock.status_code = 200
        end_mock.json.return_value = { "empty": [] }

        _request_get_mock.side_effect = [total_mock, *value_mocks, end_mock]


class QueryTestCases:
    def case_one_table(self) -> QueryCase:
        test_id = "b096504a-3d54-4664-9bf5-1b872466fd66"
        return QueryCase(
            "db" + str(uuid4()).split("-")[0],
            [{
                "purchaseOrders": [
                    {
                        "id": test_id,
                        "value": "value",
                    }
                ]
            }],
            {"t"},
            {"t": (["id", "value"], [(test_id, "value")])},
        )

