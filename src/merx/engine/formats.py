"""From raw bytes to observations, per file format.

Each reader turns one raw file into (measure, valid_date, value) rows plus whatever
release information the file carries. Parsers in ``merx.sources`` know file layouts;
readers here apply a contract to a parser's output.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

from merx.contracts import Contract
from merx.sources import eia_xls

Observation = tuple[str, date, float | None]


@dataclass(frozen=True)
class ReadResult:
    observations: list[Observation]
    release_date: date | None = None
    next_release_date: date | None = None


def _read_eia_hist_xls(content: bytes, contract: Contract) -> ReadResult:
    parsed = eia_xls.parse(content, contract.source.series_key)
    (measure,) = contract.measures
    return ReadResult(
        observations=[(measure, d, v) for d, v in parsed.points],
        release_date=parsed.release_date,
        next_release_date=parsed.next_release_date,
    )


READERS: dict[str, Callable[[bytes, Contract], ReadResult]] = {
    "eia_hist_xls": _read_eia_hist_xls,
}


def read(content: bytes, contract: Contract) -> ReadResult:
    return READERS[contract.source.format](content, contract)
