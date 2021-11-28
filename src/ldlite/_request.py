import requests


def _request_get(url, params, headers, timeout, max_retries):
    r = 0
    while r < max_retries:
        try:
            return requests.get(url, params=params, headers=headers, timeout=timeout)
        except requests.exceptions.Timeout:
            pass
        r += 1
    return requests.get(url, params=params, headers=headers, timeout=timeout)

