"""Values that were known once and are missing now.

A value is *missing* when its (measure, valid_date, period) appears anywhere in the dataset's
point-in-time history but not in the current staged build. A person can accept a
missing value, with a reason, in ``ops.accepted_withdrawals``; accepted values no longer
fail the ``no_withdrawals`` check. Acceptances are kept as data, not in the contract,
because a single source change can remove hundreds of values at once.
"""

from datetime import UTC, date, datetime

import duckdb


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ops.accepted_withdrawals (
            dataset_id      VARCHAR   NOT NULL,
            measure         VARCHAR   NOT NULL,
            valid_date      DATE      NOT NULL,
            period          INTEGER   NOT NULL DEFAULT 0,
            reason          VARCHAR   NOT NULL,
            accepted_at_utc TIMESTAMP NOT NULL,
            PRIMARY KEY (dataset_id, measure, valid_date, period)
        )
        """
    )
    # Acceptances recorded before sub-daily data existed have no period column. Adding
    # it keeps those decisions (they are all whole-day values, so period 0 is correct)
    # instead of asking a person to make them again.
    columns = {
        name
        for (name,) in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'ops' AND table_name = 'accepted_withdrawals'"
        ).fetchall()
    }
    if "period" not in columns:
        con.execute("ALTER TABLE ops.accepted_withdrawals ADD COLUMN period INTEGER DEFAULT 0")


def _has_history(con: duckdb.DuckDBPyConnection, dataset_id: str) -> bool:
    (n,) = con.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'pit' AND table_name = ?",
        [dataset_id],
    ).fetchone()
    return n > 0


def missing(
    con: duckdb.DuckDBPyConnection, dataset_id: str, staged_table: str
) -> list[tuple[str, date, int, float | None, datetime, bool]] | None:
    """Every (measure, valid_date, period) ever known but absent from the staged table,
    with its last known value, when that value was last current, and whether accepted.
    None when the dataset has no history yet."""
    if not _has_history(con, dataset_id):
        return None
    ensure_table(con)
    return con.execute(
        f"""
        WITH known AS (
            SELECT measure, valid_date, period,
                   last(value ORDER BY known_from_utc) AS last_value,
                   max(coalesce(known_to_utc, known_from_utc)) AS last_seen_utc
            FROM pit."{dataset_id}"
            GROUP BY measure, valid_date, period
        )
        SELECT k.measure, k.valid_date, k.period, k.last_value, k.last_seen_utc,
               a.dataset_id IS NOT NULL AS accepted
        FROM known k
        ANTI JOIN {staged_table} s USING (measure, valid_date, period)
        LEFT JOIN ops.accepted_withdrawals a
               ON a.dataset_id = ? AND a.measure = k.measure
              AND a.valid_date = k.valid_date AND a.period = k.period
        ORDER BY k.valid_date, k.measure
        """,
        [dataset_id],
    ).fetchall()


def accept(
    con: duckdb.DuckDBPyConnection,
    dataset_id: str,
    staged_table: str,
    reason: str,
    at: datetime | None = None,
) -> int:
    """Accept every currently missing, not yet accepted value of a dataset. Returns how
    many were accepted. The reason is required: an acceptance is a decision."""
    if not reason.strip():
        raise ValueError("a reason is required to accept withdrawn values")
    rows = missing(con, dataset_id, staged_table)
    if rows is None:
        return 0
    now = (at or datetime.now(UTC)).astimezone(UTC).replace(tzinfo=None)
    new = [
        (dataset_id, m, d, period, reason, now)
        for m, d, period, _, _, accepted in rows
        if not accepted
    ]
    if new:
        con.executemany("INSERT INTO ops.accepted_withdrawals VALUES (?, ?, ?, ?, ?, ?)", new)
    return len(new)
