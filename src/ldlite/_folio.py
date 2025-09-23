from collections.abc import Iterator
from itertools import count
from typing import cast

import orjson
from httpx_folio.factories import (
    BasicClientOptions,
    FolioParams,
    default_client_factory,
)
from httpx_folio.query import QueryParams, QueryType

_SOURCESTATS = {
    "/source-storage/records": "/source-storage/records",
    "/source-storage/stream/records": "/source-storage/records",
    "/source-storage/source-records": "/source-storage/source-records",
    "/source-storage/stream/source-records": "/source-storage/source-records",
    # This endpoint is in the docs but not actually in FOLIO?
    # "/source-storage/stream/marc-record-identifiers": "???",
}
_SOURCESTREAM = {
    "/source-storage/records": "/source-storage/stream/records",
    "/source-storage/stream/records": "/source-storage/stream/records",
    "/source-storage/source-records": "/source-storage/stream/source-records",
    "/source-storage/stream/source-records": "/source-storage/stream/source-records",
}


class FolioClient:
    def __init__(self, params: FolioParams):
        self._client_factory = default_client_factory(params)

    def iterate_records(
        self,
        path: str,
        timeout: float,
        retries: int,
        page_size: int,
        query: QueryType | None = None,
    ) -> tuple[int, Iterator[bytes]]:
        is_srs = path.lower() in _SOURCESTATS
        # this is Java's max size of int because we want all the source records
        params = QueryParams(query, 2_147_483_647 - 1 if is_srs else page_size)

        client_opts = BasicClientOptions(retries=retries, timeout=timeout)
        with self._client_factory(client_opts) as client:
            res = client.get(
                path if not is_srs else _SOURCESTATS[path.lower()],
                params=params.stats(),
            )
            res.raise_for_status()
            j = orjson.loads(res.text)
            r = int(j["totalRecords"])

        if r == 0:
            return (0, iter([]))

        if is_srs:
            return (r, self._iterate_records_srs(client_opts, path, params))

        key = cast("str", next(iter(j.keys())))
        r1 = j[key][0]
        if (
            nonid_key := cast("str", next(iter(r1.keys()))) if "id" not in r1 else None
        ) or not params.can_page_by_id():
            return (
                r,
                self._iterate_records_offset(
                    client_opts,
                    path,
                    params,
                    key,
                    nonid_key,
                ),
            )

        return (
            r,
            self._iterate_records_id(
                client_opts,
                path,
                params,
                key,
            ),
        )

    def _iterate_records_srs(
        self,
        client_opts: BasicClientOptions,
        path: str,
        params: QueryParams,
    ) -> Iterator[bytes]:
        with (
            self._client_factory(client_opts) as client,
            client.stream(
                "GET",
                _SOURCESTREAM[path.lower()],
                params=params.normalized(),
            ) as res,
        ):
            res.raise_for_status()
            record = ""
            for f in res.iter_lines():
                # HTTPX can return partial json fragments during iteration
                # if they contain "newline-ish" characters like U+2028
                record += f
                if len(f) == 0 or f[-1] != "}":
                    continue
                yield orjson.dumps(orjson.Fragment(record))
                record = ""

    def _iterate_records_offset(
        self,
        client_opts: BasicClientOptions,
        path: str,
        params: QueryParams,
        key: str,
        nonid_key: str | None,
    ) -> Iterator[bytes]:
        with self._client_factory(client_opts) as client:
            page = count(start=1)
            while True:
                res = client.get(
                    path,
                    params=params.offset_paging(page=next(page))
                    if nonid_key is None
                    else params.offset_paging(key=nonid_key, page=next(page)),
                )
                res.raise_for_status()

                last = None
                for r in (o for o in orjson.loads(res.text)[key] if o is not None):
                    last = r
                    yield orjson.dumps(r)

                if last is None:
                    return

    def _iterate_records_id(
        self,
        client_opts: BasicClientOptions,
        path: str,
        params: QueryParams,
        key: str,
    ) -> Iterator[bytes]:
        with self._client_factory(client_opts) as client:
            last_id: str | None = None
            while True:
                res = client.get(path, params=params.id_paging(last_id=last_id))
                res.raise_for_status()

                last = None
                for r in (o for o in orjson.loads(res.text)[key] if o is not None):
                    last = r
                    yield orjson.dumps(r)

                if last is None:
                    return

                last_id = last["id"]
