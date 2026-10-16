"""Great Britain's electricity clock.

GB settles electricity in half-hour **settlement periods**, numbered from 1 within each
*local* day. Two days a year the local day is not 24 hours long, so it holds 46 or 50
periods instead of 48 — which is the reason merx keys sub-daily data on (date, period)
rather than on a timestamp (D-017).

Providers disagree about which they publish. Elexon states the settlement date and
period outright. The carbon intensity API publishes UTC half-hour windows instead, so
the date and period are derived here, from the same rule: local midnight starts
period 1.
"""

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

LONDON = ZoneInfo("Europe/London")
PERIOD_MINUTES = 30


def settlement_period(start_utc: datetime) -> tuple[date, int]:
    """The settlement date and period a half-hour beginning at ``start_utc`` belongs to.

    The period is counted in elapsed local time since local midnight, not in wall-clock
    arithmetic: on the day the clocks go back, 01:30 happens twice and the second one is
    a later period, which a naive hour-of-day calculation would collide with the first.
    """
    local = start_utc.astimezone(LONDON)
    midnight = datetime.combine(local.date(), datetime.min.time(), tzinfo=LONDON)
    elapsed = local.astimezone(UTC) - midnight.astimezone(UTC)
    return local.date(), int(elapsed // timedelta(minutes=PERIOD_MINUTES)) + 1


def periods_in_day(day: date) -> int:
    """How many settlement periods a given local day holds: 46, 48 or 50."""
    midnight = datetime.combine(day, datetime.min.time(), tzinfo=LONDON)
    next_midnight = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=LONDON)
    length = next_midnight.astimezone(UTC) - midnight.astimezone(UTC)
    return int(length // timedelta(minutes=PERIOD_MINUTES))
