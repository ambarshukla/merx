"""From raw bytes to observations, per file format.

Each reader turns one raw file into (measure, valid_date, value) rows plus whatever
release information the file carries. Parsers in ``merx.sources`` know file layouts;
readers here apply a contract to a parser's output.
"""

import gzip
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

from merx.contracts import Contract
from merx.sources import cftc, eia_xls

Observation = tuple[str, date, float | None]


@dataclass(frozen=True)
class ReadResult:
    observations: list[Observation]
    release_date: date | None = None
    next_release_date: date | None = None
    # The provider's display names for the series in this file (names drift; codes do not).
    source_labels: tuple[str, ...] = ()


def _read_eia_hist_xls(content: bytes, contract: Contract) -> ReadResult:
    parsed = eia_xls.parse(content, contract.source.series_key)
    (measure,) = contract.measures
    return ReadResult(
        observations=[(measure, d, v) for d, v in parsed.points],
        release_date=parsed.release_date,
        next_release_date=parsed.next_release_date,
    )


def _read_cftc_disaggregated(content: bytes, contract: Contract) -> ReadResult:
    assert contract.source.columns is not None  # guaranteed by the contract schema
    parsed = cftc.parse(content, contract.source.series_key, contract.source.columns)
    return ReadResult(observations=parsed.observations, source_labels=tuple(parsed.market_names))


READERS: dict[str, Callable[[bytes, Contract], ReadResult]] = {
    "eia_hist_xls": _read_eia_hist_xls,
    "cftc_disaggregated": _read_cftc_disaggregated,
}


_GZIP_MAGIC = b"\x1f\x8b"


def read(content: bytes, contract: Contract) -> ReadResult:
    # A file can arrive gzip-compressed for transfer (seen 2026-10-15 in a Wayback Machine
    # copy of an EIA spreadsheet captured in 2022). The raw zone keeps the bytes as
    # received; the compression is undone here, before the format's own parser runs.
    if content.startswith(_GZIP_MAGIC):
        content = gzip.decompress(content)
    return READERS[contract.source.format](content, contract)
