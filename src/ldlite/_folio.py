import logging
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

logger = logging.getLogger("ldlite.download")

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
            logger.debug("reading %s with %s", path, query)
            res = client.get(
                path if not is_srs else _SOURCESTATS[path.lower()],
                params=params.stats(),
            )
            res.raise_for_status()
            j = orjson.loads(res.text)
            r = int(j["totalRecords"])
            logger.debug("got %s records", r)

        if r == 0:
            logger.debug("bailing early because there are no records")
            return (0, iter([]))

        if is_srs:
            return (r, self._iterate_records_srs(client_opts, path, params))

        # folio records usually have additional keys besides the actual list
        # the items are usually first but not always
        # so we check for keys until we find a good one
        # totalRecords and erm as of right now are the only keys like this
        # but there may end up with more
        keys = iter(j.keys())
        while (json_key := next(keys)) and json_key in [
            "totalRecords",
            "pageSize",
            "page",
            "totalPages",
            "meta",
            "total",
        ]:
            continue
        logger.debug("selected %s as json key from options %s", json_key, j.keys())

        r1 = j[json_key][0]
        if (
            nonid_sort := cast("str", next(iter(r1.keys()))) if "id" not in r1 else None
        ) or not params.can_page_by_id(path=path):
            logger.warning("paging by offset using %s instead of by id", nonid_sort)
            return (
                r,
                self._iterate_records_offset(
                    client_opts,
                    path,
                    params,
                    json_key,
                    nonid_sort,
                ),
            )

        return (
            r,
            self._iterate_records_id(
                client_opts,
                path,
                params,
                json_key,
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
                    logger.warning("got fragment of srs record %f", f)
                    continue
                yield orjson.dumps(orjson.Fragment(record))
                record = ""

    def _iterate_records_offset(
        self,
        client_opts: BasicClientOptions,
        path: str,
        params: QueryParams,
        json_key: str,
        nonid_sort: str | None,
    ) -> Iterator[bytes]:
        with self._client_factory(client_opts) as client:
            page = count(start=1)
            while True:
                res = client.get(
                    path,
                    params=params.offset_paging(page=next(page))
                    if nonid_sort is None
                    else params.offset_paging(key=nonid_sort, page=next(page)),
                )
                res.raise_for_status()

                last = None
                for r in (o for o in orjson.loads(res.text)[json_key] if o is not None):
                    last = r
                    yield orjson.dumps(r)

                if last is None:
                    return

    def _iterate_records_id(
        self,
        client_opts: BasicClientOptions,
        path: str,
        params: QueryParams,
        json_key: str,
    ) -> Iterator[bytes]:
        with self._client_factory(client_opts) as client:
            last_id: str | None = None
            while True:
                res = client.get(path, params=params.id_paging(last_id=last_id))
                res.raise_for_status()

                last = None
                for r in (o for o in orjson.loads(res.text)[json_key] if o is not None):
                    last = r
                    yield orjson.dumps(r)

                if last is None:
                    return

                last_id = last["id"]
