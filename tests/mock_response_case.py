import json
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING
from unittest.mock import MagicMock
from uuid import uuid4

if TYPE_CHECKING:
    import ldlite


@dataclass(frozen=True)
class Call:
    prefix: str
    returns: "ldlite._jsonx.Json | list[ldlite._jsonx.Json]"

    # duplicate of LDLite.query default params
    query: str | dict[str, str] | None = None
    json_depth: int = 3
    limit: int | None = None
    keep_raw: bool = True

    @property
    def returns_list(self) -> list["ldlite._jsonx.Json"]:
        if isinstance(self.returns, list):
            return self.returns

        return [self.returns]


@dataclass(frozen=True)
class MockedResponseTestCase:
    calls: Call | list[Call]

    @property
    def calls_list(self) -> list[Call]:
        if isinstance(self.calls, list):
            return self.calls

        return [self.calls]

    @cached_property
    def db(self) -> str:
        db = "db" + str(uuid4()).split("-")[0]
        print(db)  # noqa: T201
        return db

    def patch_request_get(
        self,
        ld: "ldlite.LDLite",
        httpx_post_mock: MagicMock,
        client_get_mock: MagicMock,
    ) -> None:
        # leave tqdm out of it
        ld.quiet(enable=True)

        httpx_post_mock.return_value.cookies.__getitem__.return_value = "token"

        side_effects = []
        for call in self.calls_list:
            key = next(iter(call.returns_list[0].keys()))
            total_mock = MagicMock()
            total_mock.text = f'{{"{key}": [{{"id": ""}}], "totalRecords": 100000}}'

            value_mocks = []
            for v in call.returns_list:
                value_mock = MagicMock()
                value_mock.text = json.dumps(v)
                value_mocks.append(value_mock)

            end_mock = MagicMock()
            end_mock.text = f'{{"{key}": [] }}'

            side_effects.extend([total_mock, *value_mocks, end_mock])

        client_get_mock.side_effect = side_effects
