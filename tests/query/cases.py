from uuid import uuid4
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

from pytest_cases import parametrize

@dataclass(frozen=True)
class QueryCase:
    db: str
    json_depth: int
    values: list[dict[str, Any]]
    expected_tables: list[str]
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
    @classmethod
    def _db(cls) -> str:
        db = "db" + str(uuid4()).split("-")[0]
        print(db)
        return db

    @parametrize(json_depth=range(1,2))
    def case_one_table(self, json_depth: int) -> QueryCase:
        return QueryCase(
            self._db(),
            json_depth,
            [{
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                    }
                ]
            }],
            ["t"],
            {"t": (["id", "value"], [("b096504a-3d54-4664-9bf5-1b872466fd66", "value")])},
        )

    @parametrize(json_depth=range(2,3))
    def case_two_tables(self, json_depth: int) -> QueryCase:
        return QueryCase(
            self._db(),
            json_depth,
            [{
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                        "subObjects": [{
                            "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "value": "sub-value",
                        }]
                    }
                ]
            }],
            ["t", "t__sub_objects"],
            {
                "t": (["id", "value"], [("b096504a-3d54-4664-9bf5-1b872466fd66", "value")]),
                "t__sub_objects": (["id", "sub_objects__id", "sub_objects__value"], [("b096504a-3d54-4664-9bf5-1b872466fd66", "2b94c631-fca9-4892-a730-03ee529ffe2a", "sub-value")])
            },
        )
