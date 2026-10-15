"""Run a contract's checks against its staged table.

Each check returns an outcome with enough evidence (counts and example rows) for a person
to see why it failed without rerunning anything.
"""

from dataclasses import dataclass, field
from datetime import date

import duckdb

from merx.contracts.model import Check, FreshnessCheck, NotNullCheck, RangeCheck, UniqueCheck

_EXAMPLES = 20


@dataclass(frozen=True)
class CheckOutcome:
    passed: bool
    description: str
    metadata: dict = field(default_factory=dict)


def _not_null(con: duckdb.DuckDBPyConnection, table: str, check: NotNullCheck) -> CheckOutcome:
    null_dates = [
        d
        for (d,) in con.execute(
            f"SELECT valid_date FROM {table} WHERE {check.column} IS NULL ORDER BY valid_date"
        ).fetchall()
    ]
    allowed = set(check.allow)
    unexpected = [d for d in null_dates if d not in allowed]
    # An accepted gap that has since been filled is worth knowing about: the exception
    # in the contract can be removed.
    filled = sorted(allowed - set(null_dates))
    return CheckOutcome(
        passed=not unexpected,
        description=f"{len(unexpected)} unexpected gaps in {check.column}"
        + (f" ({len(null_dates) - len(unexpected)} accepted)" if allowed else ""),
        metadata={
            "unexpected_nulls": len(unexpected),
            "accepted_nulls": len(null_dates) - len(unexpected),
            "examples": [d.isoformat() for d in unexpected[:_EXAMPLES]],
            "accepted_gaps_now_filled": [d.isoformat() for d in filled],
        },
    )


def _unique(con: duckdb.DuckDBPyConnection, table: str, check: UniqueCheck) -> CheckOutcome:
    cols = ", ".join(check.columns)
    dupes = con.execute(
        f"SELECT {cols}, count(*) AS n FROM {table} GROUP BY ALL HAVING n > 1 ORDER BY {cols}"
    ).fetchall()
    return CheckOutcome(
        passed=not dupes,
        description=f"{len(dupes)} duplicated ({cols}) keys",
        metadata={
            "duplicate_keys": len(dupes),
            "examples": [[str(v) for v in row] for row in dupes[:_EXAMPLES]],
        },
    )


def _range(con: duckdb.DuckDBPyConnection, table: str, check: RangeCheck) -> CheckOutcome:
    outside = con.execute(
        f"SELECT valid_date, {check.column} FROM {table} "
        f"WHERE {check.column} < ? OR {check.column} > ? ORDER BY valid_date",
        [check.min, check.max],
    ).fetchall()
    return CheckOutcome(
        passed=not outside,
        description=f"{len(outside)} values outside [{check.min}, {check.max}]",
        metadata={
            "outside": len(outside),
            "min_allowed": check.min,
            "max_allowed": check.max,
            "examples": [[d.isoformat(), v] for d, v in outside[:_EXAMPLES]],
        },
    )


def _freshness(
    con: duckdb.DuckDBPyConnection, table: str, check: FreshnessCheck, today: date
) -> CheckOutcome:
    (latest,) = con.execute(
        f"SELECT max(valid_date) FROM {table} WHERE value IS NOT NULL"
    ).fetchone()
    if latest is None:
        return CheckOutcome(False, "no values at all", {"latest_valid_date": None})
    age = (today - latest).days
    return CheckOutcome(
        passed=age <= check.max_age_days,
        description=f"newest value is {age} days old (limit {check.max_age_days})",
        metadata={
            "latest_valid_date": latest.isoformat(),
            "age_days": age,
            "max_age_days": check.max_age_days,
            "as_of": today.isoformat(),
        },
    )


def run_check(
    con: duckdb.DuckDBPyConnection, table: str, check: Check, today: date
) -> CheckOutcome:
    match check:
        case NotNullCheck():
            return _not_null(con, table, check)
        case UniqueCheck():
            return _unique(con, table, check)
        case RangeCheck():
            return _range(con, table, check)
        case FreshnessCheck():
            return _freshness(con, table, check, today)
    raise TypeError(f"unknown check: {check!r}")
