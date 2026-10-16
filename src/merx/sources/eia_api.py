"""EIA's API (v2): a second channel for the same series as the spreadsheets.

Shape observed 2026-10-15/16 (``/v2/petroleum/pri/spt/data/``, API version 2.1.14):
``{"response": {"total": "10272", "dateFormat": "YYYY-MM-DD", "frequency": "daily",
"data": [{"period": "1986-01-02", "series": "RWTC", "value": "25.56", "units": "$/BBL",
...}]}, "request": {"command": ..., "params": {..., "api_key": ...}}, ...}``.
``total`` and ``value`` are strings; a page holds at most 5,000 rows.

Raw files: every page of one fetch, wrapped in one JSON document
``{"pages": [page, page, ...]}``. One change is made before landing: **the API echoes the
caller's key in ``request.params``, and it is removed**, so no key is ever stored. The
key itself comes from the environment and never appears in a stored URL.
"""

import json
from collections.abc import Callable
from datetime import date
from urllib.parse import urlencode

from merx.sources import SourceFormatError

PAGE_LENGTH = 5000


def page_url(base_url: str, series: str, frequency: str, offset: int, api_key: str) -> str:
    params = [
        ("api_key", api_key),
        ("frequency", frequency),
        ("data[0]", "value"),
        ("facets[series][]", series),
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "asc"),
        ("offset", str(offset)),
        ("length", str(PAGE_LENGTH)),
    ]
    return f"{base_url}?{urlencode(params)}"


def fetch_all(
    fetch: Callable[[str], bytes], base_url: str, series: str, frequency: str, api_key: str
) -> bytes:
    """Fetch every page and return them as one JSON document, with the echoed key removed."""
    pages: list[dict] = []
    collected, total = 0, None
    while total is None or collected < total:
        body = json.loads(fetch(page_url(base_url, series, frequency, collected, api_key)))
        params = body.get("request", {}).get("params", {})
        params.pop("api_key", None)
        response = body.get("response")
        if not isinstance(response, dict) or "data" not in response:
            raise SourceFormatError(f"no response.data in API reply: {str(body)[:200]}")
        total = int(response["total"])
        rows = len(response["data"])
        if rows == 0 and collected < total:
            raise SourceFormatError(f"API stopped at {collected} of {total} rows")
        pages.append(body)
        collected += rows
    return json.dumps({"pages": pages}, separators=(",", ":")).encode()


def parse(content: bytes, series: str) -> list[tuple[date, float | None]]:
    try:
        pages = json.loads(content)["pages"]
    except (ValueError, KeyError, TypeError) as e:
        raise SourceFormatError(f"not an API pages document: {e}") from e
    points: list[tuple[date, float | None]] = []
    units: set[str] = set()
    totals: set[int] = set()
    for page in pages:
        response = page["response"]
        totals.add(int(response["total"]))
        if response.get("dateFormat") != "YYYY-MM-DD":
            raise SourceFormatError(f"unexpected dateFormat {response.get('dateFormat')!r}")
        for row in response["data"]:
            if row.get("series") != series:
                raise SourceFormatError(
                    f"row for series {row.get('series')!r}, expected {series!r}"
                )
            try:
                valid_date = date.fromisoformat(row["period"])
            except (KeyError, ValueError) as e:
                raise SourceFormatError(f"bad period in {row}") from e
            raw = row.get("value")
            try:
                value = None if raw is None else float(raw)
            except ValueError:
                raise SourceFormatError(f"{valid_date}: value not a number: {raw!r}") from None
            units.add(row.get("units"))
            points.append((valid_date, value))
    if len(totals) > 1:
        raise SourceFormatError(f"pages disagree on the total: {sorted(totals)}")
    if totals and len(points) != totals.pop():
        raise SourceFormatError(f"{len(points)} rows received, the API reported a different total")
    if len(units) > 1:
        raise SourceFormatError(f"mixed units in one series: {sorted(map(str, units))}")
    return points
