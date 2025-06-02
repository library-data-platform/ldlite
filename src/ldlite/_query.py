from __future__ import annotations

import copy


def query_dict(query: None | str | dict[str, str]) -> dict[str, str]:
    if isinstance(query, str):
        return {"query": query}
    if isinstance(query, dict):
        return copy.deepcopy(query)

    return {}
