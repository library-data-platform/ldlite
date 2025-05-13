import json
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
    expected_values: dict[str, tuple[list[str], list[tuple[Any,...]]]]

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
            ["t", "tcatalog"],
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
                        "subObjects": [
                            {
                                "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                "value": "sub-value-1",
                            },
                            {
                                "id": "f5bda109-a719-4f72-b797-b9c22f45e4e1",
                                "value": "sub-value-2",
                            }
                        ]
                    }
                ]
            }],
            ["t", "tcatalog", "t__sub_objects"],
            {
                "t": (["id", "value"], [("b096504a-3d54-4664-9bf5-1b872466fd66", "value")]),
                "t__sub_objects": (
                    ["id", "sub_objects__id", "sub_objects__value"],
                    [
                        ("b096504a-3d54-4664-9bf5-1b872466fd66", "2b94c631-fca9-4892-a730-03ee529ffe2a", "sub-value-1"),
                        ("b096504a-3d54-4664-9bf5-1b872466fd66", "f5bda109-a719-4f72-b797-b9c22f45e4e1", "sub-value-2"),
                    ]
                )
            },
        )

    @parametrize(json_depth=range(0,1))
    def case_table_no_expansion(self, json_depth) -> QueryCase:
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
            [],
            {}
        )

    def case_table_underexpansion(self) -> QueryCase:
        return QueryCase(
            self._db(),
            2,
            [{
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "subObjects": [ {
                            "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "value": "sub-value",
                            "subSubObjects": [
                            {
                                "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                "value": "sub-sub-value",
                            }],
                        }],
                    },
                ],
            }],
            ["t", "tcatalog", "t__sub_objects"],
            {
                "t__sub_objects": (
                    ["*"],
                    [
                        (1, "b096504a-3d54-4664-9bf5-1b872466fd66", 1, "2b94c631-fca9-4892-a730-03ee529ffe2a", "sub-value"),
                    ]
                )
            },
        )

    @parametrize(json_depth=range(3,4))
    def case_three_tables(self, json_depth: int) -> QueryCase:
        return QueryCase(
            self._db(),
            json_depth,
            [{
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                        "subObjects": [
                            {
                                "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                "value": "sub-value",
                                "subSubObjects": [
                                {
                                    "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                    "value": "sub-sub-value",
                                }],
                            }
                        ]
                    }
                ]
            }],
            ["t", "tcatalog", "t__sub_objects", "t__sub_objects__sub_sub_objects"],
            {
                "t__sub_objects__sub_sub_objects": (
                    ["id", "sub_objects__id", "sub_objects__sub_sub_objects__id", "sub_objects__sub_sub_objects__value"],
                    [
                        ("b096504a-3d54-4664-9bf5-1b872466fd66", "2b94c631-fca9-4892-a730-03ee529ffe2a", "2b94c631-fca9-4892-a730-03ee529ffe2a", "sub-sub-value"),
                    ]
                )
            },
        )

    def case_nested_object(self) -> QueryCase:
        return QueryCase(
            self._db(),
            2,
            [{
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                        "subObject": {
                            "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "value": "sub-value",
                        }
                    }
                ]
            }],
            ["t", "tcatalog"],
            {"t": (
                ["id", "value", "sub_object__id", "sub_object__value"], 
                [
                    ("b096504a-3d54-4664-9bf5-1b872466fd66", "value", "2b94c631-fca9-4892-a730-03ee529ffe2a", "sub-value")
                ])
            },
        )

    def case_doubly_nested_object(self) -> QueryCase:
        return QueryCase(
            self._db(),
            3,
            [{
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                        "subObject": {
                            "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "value": "sub-value",
                            "subSubObject":
                            {
                                "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                "value": "sub-sub-value",
                            },
                        }
                    }
                ]
            }],
            ["t", "tcatalog"],
            {"t": (
                ["id", "value", "sub_object__id", "sub_object__sub_sub_object__id", "sub_object__sub_sub_object__value"], 
                [
                    ("b096504a-3d54-4664-9bf5-1b872466fd66", "value", "2b94c631-fca9-4892-a730-03ee529ffe2a", "2b94c631-fca9-4892-a730-03ee529ffe2a", "sub-sub-value")
                ])
            },
        )

    def case_nested_object_underexpansion(self) -> QueryCase:
        return QueryCase(
            self._db(),
            1,
            [{
                "purchaseOrders": [
                    {
                        "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                        "value": "value",
                        "subObject": {
                            "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "value": "sub-value",
                        }
                    }
                ]
            }],
            ["t", "tcatalog"],
            {"t": (
                ["id", "value", "sub_object"], 
                [
                    ("b096504a-3d54-4664-9bf5-1b872466fd66", "value", json.dumps({"id": "2b94c631-fca9-4892-a730-03ee529ffe2a", "value": "sub-value"}, indent=4))
                ])
            },
        )

