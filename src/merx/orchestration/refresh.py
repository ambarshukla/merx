"""Run today's refreshes: the jobs whose schedule falls on today, in their own time zone.

A host that does not run Dagster's always-on scheduler (the daemon) can call this once a
day, after every source's release time, from a system timer. Each due job runs through
Dagster with the instance in ``DAGSTER_HOME``, so the runs and their checks appear in
Dagster's UI exactly as scheduled runs would. Partitioned jobs refresh one partition:
the month in progress for monthly sources, and the latest report's year for yearly ones
(``report_year``, which is not simply the calendar year around the new year). On Sundays
the quality lab is rerun too.

Only the day-of-week field of each cron schedule is used: every schedule in the
contracts names a minute, an hour and days of the week, and the timer runs after the
latest of those hours.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import dagster as dg

from merx.contracts import Contract
from merx.contracts.model import MonthlyPartitions
from merx.orchestration.factory import report_year, schedule_job_name
from merx.orchestration.lab import BAKEOFF_KEY


def cron_days(field: str) -> set[int]:
    """Days of the week in a cron field, as cron numbers (0 = Sunday)."""
    if field == "*":
        return set(range(7))
    days: set[int] = set()
    for part in field.split(","):
        if "-" in part:
            lo, hi = (int(x) for x in part.split("-"))
            days.update(range(lo, hi + 1))
        else:
            days.add(int(part))
    return {0 if d == 7 else d for d in days}  # cron allows 7 for Sunday too


@dataclass(frozen=True)
class DueJob:
    name: str
    partition_key: str | None


def due_jobs(contracts: list[Contract], now: datetime) -> list[DueJob]:
    due: dict[str, DueJob] = {}
    for c in contracts:
        local = now.astimezone(ZoneInfo(c.schedule.timezone))
        cron_weekday = (local.weekday() + 1) % 7  # Python: Monday = 0; cron: Sunday = 0
        if cron_weekday not in cron_days(c.schedule.cron.split()[4]):
            continue
        if c.source.partitions is not None:
            key = (
                local.strftime("%Y-%m")
                if isinstance(c.source.partitions, MonthlyPartitions)
                else report_year(local)
            )
            due[f"refresh_{c.id}"] = DueJob(f"refresh_{c.id}", key)
        else:
            name = schedule_job_name(c.schedule.cron, c.schedule.timezone)
            due[name] = DueJob(name, None)
    return sorted(due.values(), key=lambda j: j.name)


def refresh(
    defs: dg.Definitions,
    contracts: list[Contract],
    now: datetime | None = None,
    instance: dg.DagsterInstance | None = None,
) -> list[tuple[str, bool]]:
    now = now or datetime.now(UTC)
    instance = instance or dg.DagsterInstance.get()
    results = []
    for job in due_jobs(contracts, now):
        result = defs.resolve_job_def(job.name).execute_in_process(
            instance=instance, partition_key=job.partition_key, raise_on_error=False
        )
        results.append((job.name, result.success))
    if now.astimezone(UTC).weekday() == 6:  # Sunday: rescore the detectors
        lab = defs.resolve_implicit_global_asset_job_def().execute_in_process(
            instance=instance, asset_selection=[BAKEOFF_KEY], raise_on_error=False
        )
        results.append(("lab/detector_bakeoff", lab.success))
    return results
