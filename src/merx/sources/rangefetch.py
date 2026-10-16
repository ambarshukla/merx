"""Fetch a date range from an API that will only answer a shorter one.

Both Great Britain sources cap how wide a window they will serve — Elexon at a week,
the carbon intensity API at about a month — while a partition here is a calendar month
padded at both ends. The window is therefore walked in chunks and merged into a single
document, so the raw zone keeps one whole, re-readable file per partition rather than
fragments whose meaning depends on how they happened to be split.

Chunks share an endpoint, so the same observation can arrive twice; a key function
decides what "the same" means for that source and the duplicate is dropped.
"""

import json
from collections.abc import Callable
from datetime import datetime, timedelta

Row = dict
KeyFn = Callable[[Row], tuple]


def fetch_in_chunks(
    get: Callable[[str], bytes],
    url_template: str,
    start: datetime,
    end: datetime,
    *,
    max_days: int,
    key: KeyFn,
    instant: Callable[[datetime], str] | None = None,
) -> bytes:
    """Walk ``start``..``end`` in ``max_days`` steps, merging each response's ``data``."""
    as_text = instant or (lambda when: when.strftime("%Y-%m-%dT%H:%MZ"))
    merged: list[Row] = []
    seen: set[tuple] = set()
    window = timedelta(days=max_days)
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + window, end)
        url = url_template.replace("{from}", as_text(chunk_start)).replace(
            "{to}", as_text(chunk_end)
        )
        for row in json.loads(get(url)).get("data", []):
            identity = key(row)
            if identity in seen:
                continue
            seen.add(identity)
            merged.append(row)
        chunk_start = chunk_end
    return json.dumps({"data": merged}).encode()
