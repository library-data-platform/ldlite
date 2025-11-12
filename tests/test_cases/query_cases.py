from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pytest_cases import parametrize

from .base import EndToEndTestCase


@dataclass(frozen=True)
class QueryCase(EndToEndTestCase):
    json_depth: int
    expected_tables: list[str]
    expected_values: dict[str, tuple[list[str], list[tuple[Any, ...]]]]
    expected_indexes: list[tuple[str, str]] | None = None
    keep_raw: bool = True


class QueryTestCases:
    @parametrize(json_depth=range(1, 2))
    def case_one_table(self, json_depth: int) -> QueryCase:
        return QueryCase(
            json_depth=json_depth,
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "value": "value",
                            },
                        ],
                    },
                ],
            },
            expected_tables=["prefix", "prefix__t", "prefix__tcatalog"],
            expected_values={
                "prefix__t": (
                    ["id", "value"],
                    [("b096504a-3d54-4664-9bf5-1b872466fd66", "value")],
                ),
                "prefix__tcatalog": (["table_name"], [("prefix__t",)]),
            },
            expected_indexes=[
                ("prefix", "__id"),
                ("prefix__t", "__id"),
                ("prefix__t", "id"),
            ],
        )

    @parametrize(json_depth=range(2, 3))
    def case_two_tables(self, json_depth: int) -> QueryCase:
        return QueryCase(
            json_depth=json_depth,
            values={
                "prefix": [
                    {
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
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
            expected_tables=[
                "prefix",
                "prefix__t",
                "prefix__t__sub_objects",
                "prefix__tcatalog",
            ],
            expected_values={
                "prefix__t": (
                    ["id", "value"],
                    [("b096504a-3d54-4664-9bf5-1b872466fd66", "value")],
                ),
                "prefix__t__sub_objects": (
                    ["id", "sub_objects__id", "sub_objects__value"],
                    [
                        (
                            "b096504a-3d54-4664-9bf5-1b872466fd66",
                            "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "sub-value-1",
                        ),
                        (
                            "b096504a-3d54-4664-9bf5-1b872466fd66",
                            "f5bda109-a719-4f72-b797-b9c22f45e4e1",
                            "sub-value-2",
                        ),
                    ],
                ),
                "prefix__tcatalog": (
                    ["table_name"],
                    [("prefix__t",), ("prefix__t__sub_objects",)],
                ),
            },
            expected_indexes=[
                ("prefix", "__id"),
                ("prefix__t", "__id"),
                ("prefix__t", "id"),
                ("prefix__t__sub_objects", "__id"),
                ("prefix__t__sub_objects", "id"),
                ("prefix__t__sub_objects", "sub_objects__o"),
                ("prefix__t__sub_objects", "sub_objects__id"),
            ],
        )

    @parametrize(json_depth=range(1))
    def case_table_no_expansion(self, json_depth: int) -> QueryCase:
        return QueryCase(
            json_depth=json_depth,
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "value": "value",
                                "subObjects": [
                                    {
                                        "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                        "value": "sub-value",
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
            expected_tables=["prefix"],
            expected_values={},
        )

    def case_table_underexpansion(self) -> QueryCase:
        return QueryCase(
            json_depth=2,
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "subObjects": [
                                    {
                                        "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                        "value": "sub-value",
                                        "subSubObjects": [
                                            {
                                                "id": (
                                                    "2b94c631-fca9-4892-a730-03ee529ffe2a"
                                                ),
                                                "value": "sub-sub-value",
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
            expected_tables=[
                "prefix",
                "prefix__t",
                "prefix__t__sub_objects",
                "prefix__tcatalog",
            ],
            expected_values={
                "prefix__t__sub_objects": (
                    ["*"],
                    [
                        (
                            1,
                            "b096504a-3d54-4664-9bf5-1b872466fd66",
                            1,
                            "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "sub-value",
                        ),
                    ],
                ),
                "prefix__tcatalog": (
                    ["table_name"],
                    [("prefix__t",), ("prefix__t__sub_objects",)],
                ),
            },
        )

    @parametrize(json_depth=range(3, 4))
    def case_three_tables(self, json_depth: int) -> QueryCase:
        return QueryCase(
            json_depth=json_depth,
            values={
                "prefix": [
                    {
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
                                                "id": (
                                                    "2b94c631-fca9-4892-a730-03ee529ffe2a"
                                                ),
                                                "value": "sub-sub-value",
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
            expected_tables=[
                "prefix",
                "prefix__t",
                "prefix__t__sub_objects",
                "prefix__t__sub_objects__sub_sub_objects",
                "prefix__tcatalog",
            ],
            expected_values={
                "prefix__t__sub_objects__sub_sub_objects": (
                    [
                        "id",
                        "sub_objects__id",
                        "sub_objects__sub_sub_objects__id",
                        "sub_objects__sub_sub_objects__value",
                    ],
                    [
                        (
                            "b096504a-3d54-4664-9bf5-1b872466fd66",
                            "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "sub-sub-value",
                        ),
                    ],
                ),
                "prefix__tcatalog": (
                    ["table_name"],
                    [
                        ("prefix__t",),
                        ("prefix__t__sub_objects",),
                        ("prefix__t__sub_objects__sub_sub_objects",),
                    ],
                ),
            },
            expected_indexes=[
                ("prefix", "__id"),
                ("prefix__t", "__id"),
                ("prefix__t", "id"),
                ("prefix__t__sub_objects", "__id"),
                ("prefix__t__sub_objects", "id"),
                ("prefix__t__sub_objects", "sub_objects__o"),
                ("prefix__t__sub_objects", "sub_objects__id"),
                ("prefix__t__sub_objects__sub_sub_objects", "__id"),
                ("prefix__t__sub_objects__sub_sub_objects", "id"),
                ("prefix__t__sub_objects__sub_sub_objects", "sub_objects__o"),
                ("prefix__t__sub_objects__sub_sub_objects", "sub_objects__id"),
                (
                    "prefix__t__sub_objects__sub_sub_objects",
                    "sub_objects__sub_sub_objects__o",
                ),
                (
                    "prefix__t__sub_objects__sub_sub_objects",
                    "sub_objects__sub_sub_objects__id",
                ),
            ],
        )

    def case_nested_object(self) -> QueryCase:
        return QueryCase(
            json_depth=2,
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "value": "value",
                                "subObject": {
                                    "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                    "value": "sub-value",
                                },
                            },
                        ],
                    },
                ],
            },
            expected_tables=["prefix", "prefix__t", "prefix__tcatalog"],
            expected_values={
                "prefix__t": (
                    ["id", "value", "sub_object__id", "sub_object__value"],
                    [
                        (
                            "b096504a-3d54-4664-9bf5-1b872466fd66",
                            "value",
                            "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "sub-value",
                        ),
                    ],
                ),
                "prefix__tcatalog": (
                    ["table_name"],
                    [("prefix__t",)],
                ),
            },
            expected_indexes=[
                ("prefix", "__id"),
                ("prefix__t", "__id"),
                ("prefix__t", "id"),
                ("prefix__t", "sub_object__id"),
            ],
        )

    def case_doubly_nested_object(self) -> QueryCase:
        return QueryCase(
            json_depth=3,
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "value": "value",
                                "subObject": {
                                    "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                    "value": "sub-value",
                                    "subSubObject": {
                                        "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                        "value": "sub-sub-value",
                                    },
                                },
                            },
                        ],
                    },
                ],
            },
            expected_tables=["prefix", "prefix__t", "prefix__tcatalog"],
            expected_values={
                "prefix__t": (
                    [
                        "id",
                        "value",
                        "sub_object__id",
                        "sub_object__sub_sub_object__id",
                        "sub_object__sub_sub_object__value",
                    ],
                    [
                        (
                            "b096504a-3d54-4664-9bf5-1b872466fd66",
                            "value",
                            "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "2b94c631-fca9-4892-a730-03ee529ffe2a",
                            "sub-sub-value",
                        ),
                    ],
                ),
                "prefix__tcatalog": (
                    ["table_name"],
                    [("prefix__t",)],
                ),
            },
            expected_indexes=[
                ("prefix", "__id"),
                ("prefix__t", "__id"),
                ("prefix__t", "id"),
                ("prefix__t", "sub_object__id"),
                ("prefix__t", "sub_object__sub_sub_object__id"),
            ],
        )

    def case_nested_object_underexpansion(self) -> QueryCase:
        return QueryCase(
            json_depth=1,
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "value": "value",
                                "subObject": {
                                    "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                    "value": "sub-value",
                                },
                            },
                        ],
                    },
                ],
            },
            expected_tables=["prefix", "prefix__t", "prefix__tcatalog"],
            expected_values={
                "prefix__t": (
                    ["id", "value", "sub_object"],
                    [
                        (
                            "b096504a-3d54-4664-9bf5-1b872466fd66",
                            "value",
                            json.dumps(
                                {
                                    "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                    "value": "sub-value",
                                },
                                indent=4,
                            ),
                        ),
                    ],
                ),
                "prefix__tcatalog": (
                    ["table_name"],
                    [("prefix__t",)],
                ),
            },
        )

    def case_id_generation(self) -> QueryCase:
        return QueryCase(
            json_depth=4,
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "subObjects": [
                                    {
                                        "id": "2b94c631-fca9-4892-a730-03ee529ffe2a",
                                        "subSubObjects": [
                                            {
                                                "id": (
                                                    "2b94c631-fca9-4892-a730-03ee529ffe2a"
                                                ),
                                            },
                                            {
                                                "id": (
                                                    "8516a913-8bf7-55a4-ab71-417aba9171c9"
                                                ),
                                            },
                                        ],
                                    },
                                    {
                                        "id": "b5d8cdc4-9441-487c-90cf-0c7ec97728eb",
                                        "subSubObjects": [
                                            {
                                                "id": (
                                                    "13a24cc8-a15c-4158-abbd-4abf25c8815a"
                                                ),
                                            },
                                            {
                                                "id": (
                                                    "37344879-09ce-4cd8-976f-bf1a57c0cfa6"
                                                ),
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            },
            expected_tables=[
                "prefix",
                "prefix__t",
                "prefix__t__sub_objects",
                "prefix__t__sub_objects__sub_sub_objects",
                "prefix__tcatalog",
            ],
            expected_values={
                "prefix__t__sub_objects": (
                    ["__id", "id", "sub_objects__o", "sub_objects__id"],
                    [
                        (
                            1,
                            "b096504a-3d54-4664-9bf5-1b872466fd66",
                            1,
                            "2b94c631-fca9-4892-a730-03ee529ffe2a",
                        ),
                        (
                            2,
                            "b096504a-3d54-4664-9bf5-1b872466fd66",
                            2,
                            "b5d8cdc4-9441-487c-90cf-0c7ec97728eb",
                        ),
                    ],
                ),
                "prefix__t__sub_objects__sub_sub_objects": (
                    ["__id", "sub_objects__o", "sub_objects__sub_sub_objects__o"],
                    [
                        (1, 1, 1),
                        (2, 1, 2),
                        (3, 2, 1),
                        (4, 2, 2),
                    ],
                ),
            },
        )

    def case_indexing_id_like(self) -> QueryCase:
        return QueryCase(
            json_depth=4,
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "otherId": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "anIdButWithADifferentEnding": (
                                    "b096504a-3d54-4664-9bf5-1b872466fd66"
                                ),
                            },
                        ],
                    },
                ],
            },
            expected_tables=[
                "prefix",
                "prefix__t",
                "prefix__tcatalog",
            ],
            expected_values={},
            expected_indexes=[
                ("prefix", "__id"),
                ("prefix__t", "__id"),
                ("prefix__t", "id"),
                ("prefix__t", "other_id"),
                ("prefix__t", "an_id_but_with_a_different_ending"),
            ],
        )

    @parametrize(json_depth=range(1, 2))
    def case_drop_raw(self, json_depth: int) -> QueryCase:
        return QueryCase(
            json_depth=json_depth,
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "value": "value",
                            },
                        ],
                    },
                ],
            },
            expected_tables=["prefix__t", "prefix__tcatalog"],
            expected_values={
                "prefix__t": (
                    ["id", "value"],
                    [("b096504a-3d54-4664-9bf5-1b872466fd66", "value")],
                ),
                "prefix__tcatalog": (["table_name"], [("prefix__t",)]),
            },
            expected_indexes=[
                ("prefix__t", "__id"),
                ("prefix__t", "id"),
            ],
            keep_raw=False,
        )

    # this case should be testing the FolioClient class
    # but it isn't setup to mock the data properly right now
    def case_null_records(self) -> QueryCase:
        return QueryCase(
            json_depth=1,
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "value": "value",
                            },
                            None,
                        ],
                    },
                ],
            },
            expected_tables=["prefix", "prefix__t", "prefix__tcatalog"],
            expected_values={},
            expected_indexes=[
                ("prefix", "__id"),
                ("prefix__t", "__id"),
                ("prefix__t", "id"),
            ],
        )

    # https://github.com/library-data-platform/ldlite/issues/52
    def case_erm_keys(self) -> QueryCase:
        return QueryCase(
            json_depth=3,
            values={
                "prefix": [
                    {
                        "pageSize": 30,
                        "page": 1,
                        "totalPages": 10,
                        "meta": {"updated": "by"},
                        "total": 285,
                        "purchaseOrders": [
                            {
                                "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "value": "value",
                            },
                        ],
                    },
                ],
            },
            expected_tables=["prefix", "prefix__t", "prefix__tcatalog"],
            expected_values={
                "prefix__t": (
                    ["id", "value"],
                    [("b096504a-3d54-4664-9bf5-1b872466fd66", "value")],
                ),
                "prefix__tcatalog": (["table_name"], [("prefix__t",)]),
            },
            expected_indexes=[
                ("prefix", "__id"),
                ("prefix__t", "__id"),
                ("prefix__t", "id"),
            ],
        )

    # https://github.com/library-data-platform/ldlite/issues/54
    def case_mixed_uuid(self) -> QueryCase:
        return QueryCase(
            json_depth=3,
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "id": "aaaa",
                                "value": "value",
                            },
                            {
                                "id": "b096504a-3d54-4664-9bf5-1b872466fd66",
                                "value": 15,
                            },
                        ],
                    },
                ],
            },
            expected_tables=["prefix", "prefix__t", "prefix__tcatalog"],
            expected_values={
                "prefix__t": (
                    ["id", "value"],
                    [
                        ("aaaa", "value"),
                        ("b096504a-3d54-4664-9bf5-1b872466fd66", "15"),
                    ],
                ),
                "prefix__tcatalog": (["table_name"], [("prefix__t",)]),
            },
            expected_indexes=[
                ("prefix", "__id"),
                ("prefix__t", "__id"),
                ("prefix__t", "id"),
            ],
        )
