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
            values={"prefix": [{"purchaseOrders": [{"id": "1"}]}]},
            expected_csvs=[("prefix__t", _SAMPLE_PATH / "basic.csv")],
        )
