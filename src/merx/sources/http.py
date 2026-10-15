"""Fetch a URL. Non-200 responses and network errors raise; nothing is retried here
(retries belong to the orchestrator, where they are visible)."""

import urllib.request
from dataclasses import dataclass

USER_AGENT = "merx/0.1 (+https://github.com/ambarshukla/merx)"


@dataclass(frozen=True)
class FetchResult:
    url: str
    status: int
    headers: dict[str, str]
    content: bytes


def fetch(url: str, timeout_s: float = 60.0) -> FetchResult:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        if response.status != 200:
            raise RuntimeError(f"GET {url} returned HTTP {response.status}")
        return FetchResult(
            url=url,
            status=response.status,
            headers=dict(response.headers.items()),
            content=response.read(),
        )
