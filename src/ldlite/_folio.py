"""Utilities for connecting to FOLIO."""

from __future__ import annotations

from itertools import count
from typing import TYPE_CHECKING

import orjson
from httpx_folio.factories import (
    BasicClientOptions,
    FolioParams,
    default_client_factory,
)
from httpx_folio.query import QueryParams, QueryType

if TYPE_CHECKING:
    from collections.abc import Iterator

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
    ) -> Iterator[tuple[int, bytes]]:
        """Iterates all records for a given path.

        Returns:
            A tuple of the autoincrementing key + the json for each record.
            The first result will be the total record count.
        """
        is_srs = path.lower() in _SOURCESTATS
        # this is Java's max size of int because we want all the source records
        params = QueryParams(query, 2_147_483_647 - 1 if is_srs else page_size)

        with self._client_factory(
            BasicClientOptions(
                retries=retries,
                timeout=timeout,
            ),
        ) as client:
            res = client.get(
                path if not is_srs else _SOURCESTATS[path.lower()],
                params=params.stats(),
            )
            res.raise_for_status()
            j = orjson.loads(res.text)
            r = int(j["totalRecords"])
            yield (r, b"")

            if r == 0:
                return

            pkey = count(start=1)
            if is_srs:
                # streaming is a more stable endpoint for source records
                with client.stream(
                    "GET",
                    _SOURCESTREAM[path.lower()],
                    params=params.normalized(),
                ) as res:
                    res.raise_for_status()
                    record = ""
                    for f in res.iter_lines():
                        # HTTPX can return partial json fragments during iteration
                        # if they contain "newline-ish" characters like U+2028
                        record += f
                        if len(f) == 0 or f[-1] != "}":
                            continue
                        yield (next(pkey), orjson.dumps(orjson.Fragment(record)))
                        record = ""
                    return

            key = next(iter(j.keys()))
            nonid_key = (
                # Grab the first key if there isn't an id column
                # because we need it to offset page properly
                next(iter(j[key][0].keys())) if "id" not in j[key][0] else None
            )

            last_id: str | None = None
            page = count(start=1)
            while True:
                if nonid_key is not None:
                    p = params.offset_paging(key=nonid_key, page=next(page))
                elif params.can_page_by_id():
                    p = params.id_paging(last_id=last_id)
                else:
                    p = params.offset_paging(page=next(page))

                res = client.get(path, params=p)
                res.raise_for_status()

                last = None
                for r in (o for o in orjson.loads(res.text)[key] if o is not None):
                    last = r
                    yield (next(pkey), orjson.dumps(r))

                if last is None:
                    return

                last_id = last.get(
                    "id",
                    "this value is unused because we're offset paging",
                )
