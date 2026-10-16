"""Elexon's Insights API: Great Britain's electricity market data.

The dataset used here is **MID** (Market Index Data): the day-ahead reference price and
traded volume for each settlement period, as reported by the power exchanges. Elexon
publishes more than one exchange's index; a contract names the one it wants, because
mixing two exchanges' prices into one series would be a silent methodology change.

Elexon states the settlement date and period itself, so nothing is derived here — but
the start time is checked against them, because a provider that quietly changes its
convention should fail loudly rather than shift every value by half an hour.
"""

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime

from merx.sources.gb import settlement_period
from merx.sources.rangefetch import fetch_in_chunks


class ElexonFormatError(ValueError):
    """The file is not shaped the way the contract expects."""


@dataclass(frozen=True)
class ParsedMid:
    # (measure, settlement date, period, start time, value)
    observations: list[tuple[str, date, int, datetime, float | None]]
    provider: str


def _instant(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)


def parse_mid(
    content: bytes,
    data_provider: str,
    columns: dict[str, str],
    month: str | None = None,
) -> ParsedMid:
    """Rows for one exchange, optionally limited to one ``YYYY-MM`` settlement month.

    The month filter is what makes partitioned downloads safe: a request is padded by a
    day at each end so no settlement day is half-fetched, and the padding is then
    dropped so neighbouring partitions cannot both claim the same day.
    """
    payload = json.loads(content)
    rows = payload.get("data")
    if rows is None:
        raise ElexonFormatError("no 'data' in the Elexon response")

    observations: list[tuple[str, date, int, datetime, float | None]] = []
    seen_providers = set()
    for row in rows:
        seen_providers.add(row.get("dataProvider"))
        if row.get("dataProvider") != data_provider:
            continue
        try:
            day = date.fromisoformat(row["settlementDate"])
            period = int(row["settlementPeriod"])
            start = _instant(row["startTime"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ElexonFormatError(f"unreadable Elexon row: {row!r}") from exc

        derived_day, derived_period = settlement_period(start)
        if (derived_day, derived_period) != (day, period):
            raise ElexonFormatError(
                f"Elexon's settlement date and period ({day} period {period}) disagree with "
                f"its own start time {start.isoformat()}, which falls in {derived_day} "
                f"period {derived_period}"
            )
        if month is not None and day.strftime("%Y-%m") != month:
            continue
        for measure, column in columns.items():
            value = row.get(column)
            observations.append(
                (measure, day, period, start, None if value is None else float(value))
            )

    if not observations:
        raise ElexonFormatError(
            f"no rows for data provider {data_provider!r}"
            + (f" in {month}" if month else "")
            + f" (the file holds {sorted(p for p in seen_providers if p)})"
        )
    return ParsedMid(observations=observations, provider=data_provider)


MAX_RANGE_DAYS = 7  # Elexon refuses a wider window (observed 2026-10-16: 7 passes, 10 is a 400)


def fetch_range(
    get: Callable[[str], bytes], url_template: str, start: datetime, end: datetime
) -> bytes:
    """One month of MID, fetched a week at a time and merged into one document."""
    return fetch_in_chunks(
        get,
        url_template,
        start,
        end,
        max_days=MAX_RANGE_DAYS,
        key=lambda row: (
            row.get("dataProvider"),
            row.get("settlementDate"),
            row.get("settlementPeriod"),
        ),
    )
