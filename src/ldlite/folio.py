"""Utilities for connecting to FOLIO."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import count
from typing import TYPE_CHECKING

import httpx
import orjson
from httpx_retries import Retry, RetryTransport

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator


@dataclass(frozen=True)
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


class _RefreshTokenAuth(httpx.Auth):
    requires_response_body = True

    def __init__(self, params: FolioParams):
        self._params = params
        self._hdr = _RefreshTokenAuth._do_auth(self._params)

    def auth_flow(
        self,
        request: httpx.Request,
    ) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers.update(self._hdr)
        response = yield request

        if response.status_code == 401:
            self._hdr = _RefreshTokenAuth._do_auth(self._params)
            request.headers.update(self._hdr)
            yield request

    @staticmethod
    def _do_auth(params: FolioParams) -> dict[str, str]:
        hdr = {"x-okapi-tenant": params.tenant}
        res = httpx.post(
            params.base_url + "/authn/login-with-expiry",
            headers=hdr,
            json={
                "username": params.username,
                "password": params.password,
            },
        )
        res.raise_for_status()

        hdr["x-okapi-token"] = res.cookies["folioAccessToken"]
        return hdr


class FolioClient:
    """Client for reliably and performantly fetching FOLIO records."""

    def __init__(self, params: FolioParams):
        """Initializes and tests the Folio connection."""
        self._base_url = params.base_url
        self._auth = _RefreshTokenAuth(params)

    def iterate_records(
        self,
        path: str,
        timeout: float,
        retries: int,
        page_size: int,
        query: str | None = None,
    ) -> Iterator[tuple[int, str | bytes]]:
        """Iterates all records for a given path.

        Returns:
            A tuple of the autoincrementing key + the json for each record.
            The first result will be the total record count.
        """
        is_src = path.startswith("/source-storage")

        with httpx.Client(
            base_url=self._base_url,
            auth=self._auth,
            transport=RetryTransport(retry=Retry(total=retries, backoff_factor=0.5)),
            timeout=timeout,
        ) as client:
            path = path if not is_src else "/source-storage/source-records"
            q = query if query is not None else "cql.allRecords=1"
            res = client.get(
                path,
                # ERM endpoints use perPage and stats
                # Additional filtering for ERM endpoints is ignored
                params={"query": q, "limit": 1, "perPage": 1, "stats": True},
            )
            res.raise_for_status()
            j = orjson.loads(res.text)
            r = int(j["totalRecords"])
            yield (r, b"")

            if r == 0:
                return

            path = path if not is_src else "/source-storage/stream/source-records"

            # key is useless but also harmless for source-storage
            key = next(iter(j.keys()))
            last_id = "00000000-0000-0000-0000-000000000000"
            pkey = count(start=1)
            page = 0
            while True:
                iter_query = f"id>{last_id}"
                # Additional filtering for ERM endpoints is ignored
                q = query + " " + iter_query if query is not None else iter_query
                p = httpx.QueryParams(
                    {
                        "sort": "id;asc",
                        "filters": iter_query,
                        "query": q + " sortBy id asc",
                        "limit": page_size,
                        "perPage": page_size,
                        "stats": True,
                    },
                )
                if is_src:
                    # source-storage doesn't support id based so we fallback to offset
                    p.add("offset", page_size * page)
                res = client.get(
                    path,
                    params=p,
                )
                res.raise_for_status()

                if is_src:
                    # the source-storage streaming endpoint is a newline-delimited json
                    j = res.text.splitlines()
                    yield from [(next(pkey), r) for r in j]
                    if len(j) < page_size:
                        return
                else:
                    j = orjson.loads(res.text)[key]
                    yield from [(next(pkey), orjson.dumps(r)) for r in j]

                    if len(j) < page_size:
                        return

                    last_id = j[-1]["id"]
