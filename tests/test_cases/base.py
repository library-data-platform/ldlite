import json
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock
from uuid import uuid4

if TYPE_CHECKING:
    import ldlite


@dataclass(frozen=True)
class EndToEndTestCase:
    values: dict[str, list[dict[str, Any]] | list[list[dict[str, Any]]]]

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
        for vsource in self.values.values():
            list_values = (
                [cast("list[dict[str, Any]]", vsource)]
                if isinstance(vsource[0], dict)
                else cast("list[list[dict[str, Any]]]", vsource)
            )

            key = next(iter(list_values[0][0].keys()))
            total_mock = MagicMock()
            total_mock.text = f'{{"{key}": [{{"id": ""}}], "totalRecords": 100000}}'

            for values in list_values:
                value_mocks = []
                for v in values:
                    value_mock = MagicMock()
                    value_mock.text = json.dumps(v)
                    value_mocks.append(value_mock)

                end_mock = MagicMock()
                end_mock.text = f'{{"{key}": [] }}'

                side_effects.extend([total_mock, *value_mocks, end_mock])

        client_get_mock.side_effect = side_effects
