import copy


def _query_dict(query):
    if query is None:
        return {}
    if isinstance(query, str):
        return {'query': query}
    elif isinstance(query, dict):
        return copy.deepcopy(query)
    else:
        raise ValueError('invalid query "' + str(query) + '"')
