"""Run a contract's checks against its staged table.

Each check returns an outcome with enough evidence (counts and example rows) for a person
to see why it failed without rerunning anything. Tables are in long form, so examples
name the measure as well as the date.
"""

from dataclasses import dataclass, field
from datetime import date

import duckdb

from merx.contracts.model import (
    BalanceCheck,
    Check,
    FreshnessCheck,
    NotNullCheck,
    NoWithdrawalsCheck,
    RangeCheck,
    UniqueCheck,
)
from merx.engine import withdrawals

_EXAMPLES = 20


@dataclass(frozen=True)
class CheckOutcome:
    passed: bool
    description: str
    metadata: dict = field(default_factory=dict)


def _not_null(con: duckdb.DuckDBPyConnection, table: str, check: NotNullCheck) -> CheckOutcome:
    nulls = con.execute(
        f"SELECT measure, valid_date FROM {table} WHERE {check.column} IS NULL "
        "ORDER BY valid_date, measure"
    ).fetchall()
    allowed = set(check.allow)
    unexpected = [(m, d) for m, d in nulls if d not in allowed]
    # An accepted gap that has since been filled is worth knowing about: the exception
    # in the contract can be removed.
    filled = sorted(allowed - {d for _, d in nulls})
    return CheckOutcome(
        passed=not unexpected,
        description=f"{len(unexpected)} unexpected gaps in {check.column}"
        + (f" ({len(nulls) - len(unexpected)} accepted)" if allowed else ""),
        metadata={
            "unexpected_nulls": len(unexpected),
            "accepted_nulls": len(nulls) - len(unexpected),
            "examples": [[m, d.isoformat()] for m, d in unexpected[:_EXAMPLES]],
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
    scope = "" if check.measure is None else "AND measure = ? "
    params = [check.min, check.max] + ([] if check.measure is None else [check.measure])
    outside = con.execute(
        f"SELECT measure, valid_date, {check.column} FROM {table} "
        f"WHERE ({check.column} < ? OR {check.column} > ?) {scope}"
        "ORDER BY valid_date, measure",
        params,
    ).fetchall()
    return CheckOutcome(
        passed=not outside,
        description=f"{len(outside)} values outside [{check.min}, {check.max}]",
        metadata={
            "outside": len(outside),
            "min_allowed": check.min,
            "max_allowed": check.max,
            "measure": check.measure or "all",
            "examples": [[m, d.isoformat(), v] for m, d, v in outside[:_EXAMPLES]],
        },
    )


def _freshness(
    con: duckdb.DuckDBPyConnection, table: str, check: FreshnessCheck, today: date
) -> CheckOutcome:
    # Every measure must be fresh, so the dataset is as fresh as its stalest measure.
    latest = con.execute(
        f"SELECT measure, max(valid_date) FROM {table} WHERE value IS NOT NULL "
        "GROUP BY measure ORDER BY 2, 1"
    ).fetchall()
    if not latest:
        return CheckOutcome(False, "no values at all", {"latest_valid_date": None})
    stalest_measure, stalest = latest[0]
    age = (today - stalest).days
    return CheckOutcome(
        passed=age <= check.max_age_days,
        description=f"newest value is {age} days old (limit {check.max_age_days})"
        + (f" for measure {stalest_measure}" if len(latest) > 1 else ""),
        metadata={
            "latest_valid_date": stalest.isoformat(),
            "stalest_measure": stalest_measure,
            "age_days": age,
            "max_age_days": check.max_age_days,
            "as_of": today.isoformat(),
        },
    )


def _balance(con: duckdb.DuckDBPyConnection, table: str, check: BalanceCheck) -> CheckOutcome:
    placeholders = ", ".join("?" for _ in check.parts)
    breaks = con.execute(
        f"""
        SELECT valid_date, total, parts_sum, n_parts FROM (
            SELECT valid_date,
                   sum(value) FILTER (WHERE measure = ?)                    AS total,
                   sum(value) FILTER (WHERE measure IN ({placeholders}))    AS parts_sum,
                   count(value) FILTER (WHERE measure IN ({placeholders}))  AS n_parts
            FROM {table} GROUP BY valid_date
        )
        WHERE total IS NULL OR n_parts <> ? OR abs(total - parts_sum) > ?
        ORDER BY valid_date
        """,
        [check.total, *check.parts, *check.parts, len(check.parts), check.tolerance],
    ).fetchall()
    return CheckOutcome(
        passed=not breaks,
        description=f"{len(breaks)} dates where {check.total} != sum of {len(check.parts)} parts",
        metadata={
            "breaks": len(breaks),
            "tolerance": check.tolerance,
            "examples": [
                [d.isoformat(), total, parts, n] for d, total, parts, n in breaks[:_EXAMPLES]
            ],
        },
    )


def _no_withdrawals(
    con: duckdb.DuckDBPyConnection, table: str, dataset_id: str | None
) -> CheckOutcome:
    if dataset_id is None:
        raise ValueError("the no_withdrawals check needs the dataset id")
    rows = withdrawals.missing(con, dataset_id, table)
    if rows is None:
        return CheckOutcome(True, "no history yet to compare against", {"missing": 0})
    unaccepted = [r for r in rows if not r[4]]
    return CheckOutcome(
        passed=not unaccepted,
        description=f"{len(unaccepted)} previously known values missing from this build"
        + (f" ({len(rows) - len(unaccepted)} accepted)" if len(rows) > len(unaccepted) else ""),
        metadata={
            "missing_unaccepted": len(unaccepted),
            "missing_accepted": len(rows) - len(unaccepted),
            "examples": [
                [m, d.isoformat(), v, seen.isoformat()]
                for m, d, v, seen, _ in unaccepted[:_EXAMPLES]
            ],
        },
    )


def run_check(
    con: duckdb.DuckDBPyConnection,
    table: str,
    check: Check,
    today: date,
    dataset_id: str | None = None,
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
        case BalanceCheck():
            return _balance(con, table, check)
        case NoWithdrawalsCheck():
            return _no_withdrawals(con, table, dataset_id)
    raise TypeError(f"unknown check: {check!r}")
