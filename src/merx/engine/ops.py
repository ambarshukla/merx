"""Operations records: every check result, and exceptions as data.

``ops.check_results`` keeps one row per check run, tied to the staged build it judged.

``ops.exceptions`` turns failing checks into work items. A failing check opens an
exception for (dataset, check) unless one is already open, in which case it is seen
again. A passing check closes the open exception. Each exception carries the dataset's
owner, when it opened and closed, how many runs saw it, and a root-cause class that a
person assigns. From these come the operating numbers: open exceptions, time to recover,
and the share of exceptions that repeat an earlier one.
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime

import duckdb

from merx.engine.checks import CheckOutcome

# The classes a person can assign when reviewing an exception.
ROOT_CAUSES = (
    "source_late",  # the source published later than its calendar
    "source_format_change",  # the file layout changed
    "source_data_error",  # the source published a wrong value
    "market_event",  # a real, extreme move: not an error
    "known_gap",  # the source has no value and never will
    "source_discontinued",  # the source still publishes the file but stopped the series
    "pipeline_bug",  # our code was wrong
    "rule_too_strict",  # the check's threshold was wrong
)


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.check_results (
            dataset_id     VARCHAR   NOT NULL,
            build_id       VARCHAR,
            check_name     VARCHAR   NOT NULL,
            severity       VARCHAR   NOT NULL,
            passed         BOOLEAN   NOT NULL,
            description    VARCHAR   NOT NULL,
            metadata_json  VARCHAR   NOT NULL,
            checked_at_utc TIMESTAMP NOT NULL
        )
        """
    )
    con.execute("CREATE SEQUENCE IF NOT EXISTS ops.exception_ids")
    # The root-cause classes are validated in code (assign_root_cause), not by a table
    # constraint, so the list can grow without migrating the table.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.exceptions (
            exception_id   BIGINT    PRIMARY KEY DEFAULT nextval('ops.exception_ids'),
            dataset_id     VARCHAR   NOT NULL,
            check_name     VARCHAR   NOT NULL,
            severity       VARCHAR   NOT NULL,
            owner          VARCHAR   NOT NULL,
            opened_at_utc  TIMESTAMP NOT NULL,
            last_seen_utc  TIMESTAMP NOT NULL,
            closed_at_utc  TIMESTAMP,
            occurrences    INTEGER   NOT NULL,
            first_description VARCHAR NOT NULL,
            last_description  VARCHAR NOT NULL,
            root_cause     VARCHAR,
            note           VARCHAR
        )
        """
    )


@dataclass(frozen=True)
class ExceptionChange:
    action: str  # "opened", "seen_again", "closed" or "none"
    exception_id: int | None


def record(
    con: duckdb.DuckDBPyConnection,
    *,
    dataset_id: str,
    owner: str,
    build_id: str | None,
    check_name: str,
    severity: str,
    outcome: CheckOutcome,
    at: datetime | None = None,
) -> ExceptionChange:
    """Log a check result and open, update or close the matching exception."""
    now = (at or datetime.now(UTC)).astimezone(UTC).replace(tzinfo=None)
    ensure_tables(con)
    con.execute(
        "INSERT INTO ops.check_results VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            dataset_id,
            build_id,
            check_name,
            severity,
            outcome.passed,
            outcome.description,
            json.dumps(outcome.metadata, default=str),
            now,
        ],
    )
    open_row = con.execute(
        "SELECT exception_id FROM ops.exceptions "
        "WHERE dataset_id = ? AND check_name = ? AND closed_at_utc IS NULL",
        [dataset_id, check_name],
    ).fetchone()
    if outcome.passed:
        if open_row is None:
            return ExceptionChange("none", None)
        con.execute(
            "UPDATE ops.exceptions SET closed_at_utc = ? WHERE exception_id = ?",
            [now, open_row[0]],
        )
        return ExceptionChange("closed", open_row[0])
    if open_row is not None:
        con.execute(
            "UPDATE ops.exceptions SET last_seen_utc = ?, occurrences = occurrences + 1, "
            "last_description = ? WHERE exception_id = ?",
            [now, outcome.description, open_row[0]],
        )
        return ExceptionChange("seen_again", open_row[0])
    (new_id,) = con.execute(
        "INSERT INTO ops.exceptions (dataset_id, check_name, severity, owner, opened_at_utc, "
        "last_seen_utc, occurrences, first_description, last_description) "
        "VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?) RETURNING exception_id",
        [
            dataset_id,
            check_name,
            severity,
            owner,
            now,
            now,
            outcome.description,
            outcome.description,
        ],
    ).fetchone()
    return ExceptionChange("opened", new_id)


def assign_root_cause(
    con: duckdb.DuckDBPyConnection, exception_id: int, root_cause: str, note: str = ""
) -> None:
    if root_cause not in ROOT_CAUSES:
        raise ValueError(f"unknown root cause {root_cause!r}; choose from {ROOT_CAUSES}")
    ensure_tables(con)
    updated = con.execute(
        "UPDATE ops.exceptions SET root_cause = ?, note = ? WHERE exception_id = ? "
        "RETURNING exception_id",
        [root_cause, note, exception_id],
    ).fetchall()
    if not updated:
        raise LookupError(f"no exception {exception_id}")


def latest_results(
    con: duckdb.DuckDBPyConnection, dataset_id: str, build_id: str
) -> dict[str, tuple[bool, str]]:
    """check name -> (passed, severity) of the most recent result for this build."""
    ensure_tables(con)
    rows = con.execute(
        "SELECT check_name, passed, severity FROM ops.check_results "
        "WHERE dataset_id = ? AND build_id = ? "
        "QUALIFY row_number() OVER (PARTITION BY check_name ORDER BY checked_at_utc DESC) = 1",
        [dataset_id, build_id],
    ).fetchall()
    return {name: (passed, severity) for name, passed, severity in rows}


def summary(con: duckdb.DuckDBPyConnection) -> dict[str, float | int | None]:
    """The operating numbers over every exception so far."""
    ensure_tables(con)
    total, open_now, closed, hours_to_recover, repeats = con.execute(
        """
        WITH e AS (
            SELECT *,
                   count(*) OVER (PARTITION BY dataset_id, check_name
                                  ORDER BY opened_at_utc ROWS UNBOUNDED PRECEDING) AS nth
            FROM ops.exceptions
        )
        SELECT count(*),
               count(*) FILTER (WHERE closed_at_utc IS NULL),
               count(closed_at_utc),
               median(epoch(closed_at_utc - opened_at_utc) / 3600.0),
               count(*) FILTER (WHERE nth > 1)
        FROM e
        """
    ).fetchone()
    return {
        "exceptions": total,
        "open": open_now,
        "closed": closed,
        "median_hours_to_recover": hours_to_recover,
        "repeat_share": (repeats / total) if total else None,
    }
