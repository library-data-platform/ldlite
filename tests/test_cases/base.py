import json
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock
from uuid import uuid4

if TYPE_CHECKING:
    import ldlite


@dataclass(frozen=True)
class EndToEndTestCase:
    values: dict[str, list[dict[str, Any]]]

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
        # iteration hack
        ld.page_size = 1
        # leave tqdm out of it
        ld.quiet(enable=True)

        httpx_post_mock.return_value.cookies.__getitem__.return_value = "token"

        side_effects = []
        for i, values in enumerate(self.values.values()):
            key = next(iter(values[0].keys()))
            total_mock = MagicMock()
            if i % 2 == 0:
                total_mock.text = f'{{"{key}": [{{"id": ""}}], "totalRecords": 100000}}'
            else:
                total_mock.text = f'{{"totalRecords": 100000, "{key}": [{{"id": ""}}]}}'

            value_mocks = []
            for v in values:
                value_mock = MagicMock()
                value_mock.text = json.dumps(v)
                value_mocks.append(value_mock)

            end_mock = MagicMock()
            end_mock.text = f'{{"{key}": [] }}'

            side_effects.extend([total_mock, *value_mocks, end_mock])

        client_get_mock.side_effect = side_effects
