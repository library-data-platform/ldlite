from dataclasses import dataclass

from pytest_cases import parametrize

from .base import Call, MockedResponseTestCase


@dataclass(frozen=True)
class LoadHistoryCase(MockedResponseTestCase):
    expected_loads: dict[str, tuple[str | None, int]]


class LoadHistoryTestCases:
    @parametrize(query=[None, "poline.id=*A"])
    def case_one_load(self, query: str | None) -> LoadHistoryCase:
        return LoadHistoryCase(
            Call(
                "prefix",
                query=query,
                returns={
                    "purchaseOrders": [
                        {
                            "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                            "value": "value",
                        },
                        {
                            "id": "b096504a-9999-4664-9bf5-1b872466fd66",
                            "value": "value-2",
                        },
                    ],
                },
            ),
            expected_loads={"prefix": (query, 2)},
        )

    def case_schema_load(self) -> LoadHistoryCase:
        return LoadHistoryCase(
            Call(
                "schema.prefix",
                returns={
                    "purchaseOrders": [
                        {
                            "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                            "value": "value",
                        },
                        {
                            "id": "b096504a-9999-4664-9bf5-1b872466fd66",
                            "value": "value-2",
                        },
                    ],
                },
            ),
            expected_loads={"schema.prefix": (None, 2)},
        )

    def case_two_loads(self) -> LoadHistoryCase:
        return LoadHistoryCase(
            [
                Call(
                    "prefix",
                    returns={
                        "purchaseOrders": [
                            {
                                "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "value": "value",
                            },
                        ],
                    },
                ),
                Call(
                    "prefix",
                    query="a query",
                    returns={
                        "purchaseOrders": [
                            {
                                "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "value": "value",
                            },
                            {
                                "id": "b096504a-9999-4664-9bf5-1b872466fd66",
                                "value": "value-2",
                            },
                        ],
                    },
                ),
            ],
            expected_loads={"prefix": ("a query", 2)},
        )
