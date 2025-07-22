from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import httpx
from httpx_retries import Retry, RetryTransport

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator
    from types import TracebackType

    from typing_extensions import Self


@dataclass
class FolioParams:
    """Connection parameters for FOLIO.

    base_url and tenant can be found in Settings > Software versions.
    """

    """The service url for FOLIO."""
    base_url: str
    """The FOLIO tenant. ECS setups are not currently supported."""
    tenant: str
    """The user to query FOLIO. LDlite will have the same permissions as this user."""
    username: str
    """The user's FOLIO password."""
    password: str
    """Timeout for requests."""
    timeout: float = 60.0
    """The number of times a failed request will be retried."""
    retries: int = 2
    """The number of FOLIO records to retrieve in a single request."""
    page_size: int = 10000


class _RefreshTokenAuth(httpx.Auth):
    requires_response_body = True

    def __init__(self, params: FolioParams):
        self.hdr = {"x-okapi-tenant": params.tenant}
        self.auth_url = params.base_url + "/authn/login-with-expiry"
        self.auth_body = {
            "username": params.username,
            "password": params.password,
        }

        self._do_auth()

    def auth_flow(
        self,
        request: httpx.Request,
    ) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers.update(self.hdr)
        response = yield request

        if response.status_code == 401:
            self._do_auth()
            request.headers.update(self.hdr)
            yield request

    def _do_auth(self) -> None:
        if "x-okapi-token" in self.hdr:
            del self.hdr["x-okapi-token"]

        res = httpx.post(
            self.auth_url,
            headers=self.hdr,
            json=self.auth_body,
        )
        res.raise_for_status()

        self.hdr["x-okapi-token"] = res.cookies["folioAccessToken"]


class FolioClient:
    """Client for reliably and performantly fetching FOLIO records."""

    def __init__(self, params: FolioParams):
        self._params = params
        self._client: httpx.Client | None = None

    def _get_client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self._params.base_url,
            auth=_RefreshTokenAuth(self._params),
            transport=RetryTransport(
                retry=Retry(
                    total=self._params.retries,
                    backoff_factor=0.5,
                ),
            ),
            timeout=self._params.timeout,
        )

    def __enter__(self) -> Self:
        self._client = self._get_client()
        return self

    def __exit__(
        self,
        type_: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        if self._client is not None:
            self._client.close()
        self._client = None

        return False

    def iterate_records(
        self,
        path: str,
    ) -> Iterator[tuple[int, str]]:
        """Iterates all records for a given path.

        Returns:
            A tuple of the autoincrementing key + the json for each record.
            The first result will be the total.
        """
        dispose = self._client is None
        client = self._client or self._get_client()

        try:
            res = client.get(
                path,
                params={
                    "query": "cql.allRecords=1 sortBy id asc",
                    "limit": 1,
                },
            )
            res.raise_for_status()
            j = res.json()
            yield (int(j["totalRecords"]), "")

            key = j.keys()[0]
            records = 1
            last_id = j[key][0]["id"]
            pkey = 0
            while records > 0:
                res = client.get(
                    path,
                    params={
                        "query": f'id>="{last_id}" sortBy id asc',
                        "limit": self._params.page_size,
                    },
                )
                res.raise_for_status()
                j = res.json()
                for r in j[key]:
                    yield (pkey, json.dumps(r))
                    pkey += 1

                records = j[key]
                last_id = j[key][-1]["id"]

        finally:
            if dispose:
                client.close()
