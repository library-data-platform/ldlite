from dataclasses import dataclass
from pathlib import Path

from .base import TestCase

_SAMPLE_PATH = Path() / "tests" / "test_cases" / "to_csv_samples"


@dataclass(frozen=True)
class ToCsvCase(TestCase):
    expected_csvs: list[tuple[str, Path]]


class ToCsvCases:
    def case_basic(self) -> ToCsvCase:
        return ToCsvCase(
            values={"prefix": [{"purchaseOrders": [{"val": "value"}]}]},
            expected_csvs=[("prefix__t", _SAMPLE_PATH / "basic.csv")],
        )

    def case_datatypes(self) -> ToCsvCase:
        return ToCsvCase(
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "string": "string",
                                "integer": 1,
                                "numeric": 1.1,
                                "boolean": True,
                                "uuid": "6a31a12a-9570-405c-af20-6abf2992859c",
                            },
                        ],
                    },
                ],
            },
            expected_csvs=[("prefix__t", _SAMPLE_PATH / "datatypes.csv")],
        )

    def case_escaped_chars(self) -> ToCsvCase:
        return ToCsvCase(
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "comma": "Double, double toil and trouble",
                                "doubleQuote": 'Cry "Havoc!" a horse',
                                "newLine": """To be
or not
to be""",
                                "singleQuote": "Cry 'Havoc!' a horse",
                            },
                            {
                                "comma": "Z",
                                "doubleQuote": "Z",
                                "newLine": "Z",
                                "singleQuote": "Z",
                            },
                        ],
                    },
                ],
            },
            expected_csvs=[("prefix__t", _SAMPLE_PATH / "escaped_chars.csv")],
        )

    def case_sorting(self) -> ToCsvCase:
        return ToCsvCase(
            values={
                "prefix": [
                    {
                        "purchaseOrders": [
                            {
                                "C": "YY",
                                "B": "XX",
                                "A": "ZZ",
                            },
                            {
                                "C": "Y",
                                "B": "XX",
                                "A": "ZZ",
                            },
                            {
                                "C": "Y",
                                "B": "X",
                                "A": "Z",
                            },
                        ],
                    },
                ],
            },
            expected_csvs=[("prefix__t", _SAMPLE_PATH / "sorting.csv")],
        )
