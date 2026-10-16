"""From raw bytes to observations, per file format.

Each reader turns one raw file into (measure, valid_date, value) rows plus whatever
release information the file carries. Parsers in ``merx.sources`` know file layouts;
readers here apply a contract to a parser's output.
"""

import gzip
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from typing import NamedTuple

from merx.contracts import Contract
from merx.sources import carbon, cftc, eia_api, eia_xls, elexon, worldbank


class Observation(NamedTuple):
    """One number from a file.

    ``period`` numbers an observation within its date, for data that arrives more often
    than daily: Great Britain settles electricity in 48 half-hour periods a day, and the
    market keys on (date, period) rather than on a timestamp — which is what makes the
    46- and 50-period clock-change days expressible at all. Daily and coarser data uses
    period 0, meaning "the whole day". ``valid_time_utc`` is the period's start, carried
    for display and ordering; it is never the key.
    """

    measure: str
    valid_date: date
    value: float | None
    period: int = 0
    valid_time_utc: datetime | None = None


@dataclass(frozen=True)
class ReadResult:
    observations: list[Observation]
    release_date: date | None = None
    next_release_date: date | None = None
    # The provider's display names for the series in this file (names drift; codes do not).
    source_labels: tuple[str, ...] = ()


def _read_elexon_mid(content: bytes, contract: Contract, partition: str | None) -> ReadResult:
    assert contract.source.columns is not None  # guaranteed by the contract schema
    parsed = elexon.parse_mid(
        content, contract.source.series_key, contract.source.columns, month=partition
    )
    return ReadResult(
        observations=[
            Observation(m, d, v, period=p, valid_time_utc=start.replace(tzinfo=None))
            for m, d, p, start, v in parsed.observations
        ]
    )


def _read_carbon_intensity(content: bytes, contract: Contract, partition: str | None) -> ReadResult:
    assert contract.source.columns is not None  # guaranteed by the contract schema
    parsed = carbon.parse(content, contract.source.columns, month=partition)
    return ReadResult(
        observations=[
            Observation(m, d, v, period=p, valid_time_utc=start.replace(tzinfo=None))
            for m, d, p, start, v in parsed.observations
        ]
    )


def _read_worldbank_pink_sheet(content: bytes, contract: Contract) -> ReadResult:
    assert contract.source.columns is not None  # guaranteed by the contract schema
    parsed = worldbank.parse(content, contract.source.columns)
    # The file states its own units, so a repricing at the source fails here rather
    # than silently rescaling a series by a thousand.
    disagree = {
        measure: (stated, contract.unit_for(measure))
        for measure, stated in parsed.units.items()
        if stated != contract.unit_for(measure)
    }
    if disagree:
        raise worldbank.PinkSheetError(
            "the Pink Sheet's units disagree with the contract: "
            + ", ".join(
                f"{m}: file says {f!r}, contract says {c!r}" for m, (f, c) in disagree.items()
            )
        )
    return ReadResult(
        observations=[Observation(m, d, v) for m, d, v in parsed.observations],
        release_date=parsed.release_date,
    )


def _read_eia_hist_xls(content: bytes, contract: Contract) -> ReadResult:
    parsed = eia_xls.parse(content, contract.source.series_key)
    (measure,) = contract.measures
    return ReadResult(
        observations=[Observation(measure, d, v) for d, v in parsed.points],
        release_date=parsed.release_date,
        next_release_date=parsed.next_release_date,
    )


def _read_eia_api_v2(content: bytes, contract: Contract) -> ReadResult:
    (measure,) = contract.measures
    points = eia_api.parse(content, contract.source.series_key)
    return ReadResult(observations=[Observation(measure, d, v) for d, v in points])


def _read_cftc_disaggregated(content: bytes, contract: Contract) -> ReadResult:
    assert contract.source.columns is not None  # guaranteed by the contract schema
    parsed = cftc.parse(content, contract.source.series_key, contract.source.columns)
    return ReadResult(
        observations=[Observation(m, d, v) for m, d, v in parsed.observations],
        source_labels=tuple(parsed.market_names),
    )


# Readers that need to know which partition they are reading, because the source
# answers a date range and the response can overlap the neighbouring partitions.
_PARTITION_AWARE: dict[str, Callable[[bytes, Contract, str | None], ReadResult]] = {
    "elexon_mid": _read_elexon_mid,
    "carbon_intensity": _read_carbon_intensity,
}

READERS: dict[str, Callable[..., ReadResult]] = {
    "eia_hist_xls": _read_eia_hist_xls,
    "eia_api_v2": _read_eia_api_v2,
    "cftc_disaggregated": _read_cftc_disaggregated,
    "worldbank_pink_sheet": _read_worldbank_pink_sheet,
    **_PARTITION_AWARE,
}


_GZIP_MAGIC = b"\x1f\x8b"


def read(content: bytes, contract: Contract, partition: str | None = None) -> ReadResult:
    # A file can arrive gzip-compressed for transfer (seen 2026-10-15 in a Wayback Machine
    # copy of an EIA spreadsheet captured in 2022). The raw zone keeps the bytes as
    # received; the compression is undone here, before the format's own parser runs.
    if content.startswith(_GZIP_MAGIC):
        content = gzip.decompress(content)
    fmt = contract.source.format
    if fmt in _PARTITION_AWARE:
        return _PARTITION_AWARE[fmt](content, contract, partition)
    return READERS[fmt](content, contract)
