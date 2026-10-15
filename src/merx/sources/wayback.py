"""Dated copies of a public file from the Internet Archive's Wayback Machine.

Sources overwrite their files in place, so their past versions are gone, unless a web
archive happened to keep a copy. The Wayback Machine's capture index (CDX) lists each
distinct copy it holds with its capture time; ``id_`` URLs return the bytes exactly as
captured. Used to backfill point-in-time history from before this project began.

Observed 2026-10-15: 15 distinct copies of EIA's weekly gas storage file (2015-2026),
36 of weekly crude stocks (2011-2026) and 51 of daily WTI spot (2010-2026). The archive
refuses some requests under load, so imports are polite (one request at a time, with a
pause) and resumable (copies already landed are skipped).
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlencode, urlparse

CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"


@dataclass(frozen=True)
class Capture:
    timestamp: str  # YYYYMMDDhhmmss, UTC
    digest: str

    @property
    def captured_at_utc(self) -> datetime:
        return datetime.strptime(self.timestamp, "%Y%m%d%H%M%S").replace(tzinfo=UTC)


def index_url(url: str) -> str:
    """The CDX query listing distinct successful captures of ``url``."""
    parsed = urlparse(url)
    params = {
        "url": parsed.netloc + parsed.path,
        "output": "json",
        "fl": "timestamp,digest",
        "filter": "statuscode:200",
        "collapse": "digest",  # one row per distinct content
    }
    return f"{CDX_ENDPOINT}?{urlencode(params)}"


def parse_index(content: bytes) -> list[Capture]:
    rows = json.loads(content or b"[]")
    if not rows:
        return []
    header, *data = rows
    if header != ["timestamp", "digest"]:
        raise ValueError(f"unexpected capture index header: {header}")
    return [Capture(timestamp=ts, digest=digest) for ts, digest in data]


def capture_url(url: str, capture: Capture) -> str:
    """The URL returning the captured bytes unmodified (``id_``: no archive banner)."""
    return f"https://web.archive.org/web/{capture.timestamp}id_/{url}"
