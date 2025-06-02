import requests


def request_get(
    url: str,
    params: dict[str, str],
    headers: dict[str, str],
    timeout: int,
    max_retries: int,
) -> requests.Response:
    r = 0
    while r < max_retries:
        try:
            return requests.get(url, params=params, headers=headers, timeout=timeout)
        except requests.exceptions.Timeout:
            pass
        r += 1
    return requests.get(url, params=params, headers=headers, timeout=timeout)
