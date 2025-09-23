from dataclasses import dataclass

from pytest_cases import parametrize

from .base import EndToEndTestCase


@dataclass(frozen=True)
class LoadHistoryCase(EndToEndTestCase):
    queries: dict[str, list[str | None | dict[str, str]]]
    expected_loads: dict[str, tuple[str | None, int]]


class LoadHistoryTestCases:
    @parametrize(query=[None, "poline.id=*A"])
    def case_one_load(self, query: str | None) -> LoadHistoryCase:
        return LoadHistoryCase(
            values={
                "prefix": [
                    {
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
                ],
            },
            queries={"prefix": [query]},
            expected_loads={
                "public.prefix": (query, 2),
            },
        )

    def case_two_loads(self) -> LoadHistoryCase:
        return LoadHistoryCase(
            values={
                "prefix": [
                    [
                        {
                            "purchaseOrders": [
                                {
                                    "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                    "value": "value",
                                },
                            ],
                        },
                    ],
                    [
                        {
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
                    ],
                ],
            },
            queries={"prefix": [None, "a query"]},
            expected_loads={
                "public.prefix": ("a query", 2),
            },
        )
