"""The GB grid's carbon intensity API: a forecast and an outturn for every half hour.

Two numbers for the same half hour, published by the same body — what the grid expected
and what actually happened. That makes a *forecast accuracy* control possible without
any modelling of our own: the error is already in the data, and it can be checked,
charted and reported like any other series.

The API publishes UTC half-hour windows and says nothing about settlement periods, so
the date and period are derived with the same rule Elexon settles on (merx.sources.gb),
which is what lets the two be compared at all.
"""

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime

from merx.sources.gb import settlement_period
from merx.sources.rangefetch import fetch_in_chunks


class CarbonFormatError(ValueError):
    """The file is not shaped the way the contract expects."""


@dataclass(frozen=True)
class ParsedIntensity:
    # (measure, settlement date, period, window start, value)
    observations: list[tuple[str, date, int, datetime, float | None]]


def parse(
    content: bytes,
    columns: dict[str, str],
    month: str | None = None,
) -> ParsedIntensity:
    """Rows for one ``YYYY-MM`` settlement month, with the periods derived from UTC.

    ``columns`` maps each measure onto a key of the ``intensity`` object, so a contract
    can take the forecast, the outturn, or both.
    """
    payload = json.loads(content)
    rows = payload.get("data")
    if rows is None:
        raise CarbonFormatError("no 'data' in the carbon intensity response")

    observations: list[tuple[str, date, int, datetime, float | None]] = []
    # A key missing from one half hour is a gap in the record; a key missing from every
    # half hour is the API having changed shape. Only the second is a format error, so
    # the two are counted apart rather than judged row by row. Observed 2026-10-16: 26
    # half hours in January 2025 carry no forecast at all, while every later month is
    # complete.
    present = dict.fromkeys(columns, 0)
    for row in rows:
        try:
            start = datetime.fromisoformat(row["from"].replace("Z", "+00:00")).astimezone(UTC)
            intensity = row["intensity"]
        except (KeyError, TypeError, ValueError) as exc:
            raise CarbonFormatError(f"unreadable carbon intensity row: {row!r}") from exc
        day, period = settlement_period(start)
        if month is not None and day.strftime("%Y-%m") != month:
            continue
        for measure, key in columns.items():
            value = intensity.get(key)
            present[measure] += key in intensity
            observations.append(
                (measure, day, period, start, None if value is None else float(value))
            )

    if not observations:
        raise CarbonFormatError("no carbon intensity rows" + (f" in {month}" if month else ""))
    absent = sorted(columns[m] for m, seen in present.items() if not seen)
    if absent:
        raise CarbonFormatError(
            f"{absent} missing from every half hour"
            + (f" in {month}" if month else "")
            + ": the intensity object has changed shape"
        )
    return ParsedIntensity(observations=observations)


MAX_RANGE_DAYS = 14  # The API refuses about a month; a fortnight is comfortably inside.


def fetch_range(
    get: Callable[[str], bytes], url_template: str, start: datetime, end: datetime
) -> bytes:
    """One month of half-hourly intensity, fetched a fortnight at a time."""
    return fetch_in_chunks(
        get,
        url_template,
        start,
        end,
        max_days=MAX_RANGE_DAYS,
        key=lambda row: (row.get("from"),),
    )
