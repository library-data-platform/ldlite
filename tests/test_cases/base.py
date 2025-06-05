from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock
from uuid import uuid4

if TYPE_CHECKING:
    import ldlite


@dataclass(frozen=True)
class TestCase:
    values: dict[str, list[dict[str, Any]]]

    @cached_property
    def db(self) -> str:
        db = "db" + str(uuid4()).split("-")[0]
        print(db)  # noqa: T201
        return db

    def patch_request_get(
        self,
        ld: "ldlite.LDLite",
        _request_get_mock: MagicMock,
    ) -> None:
        # _check_okapi() hack
        ld.login_token = "token"
        ld.okapi_url = "url"
        # leave tqdm out of it
        ld.quiet(enable=True)

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
