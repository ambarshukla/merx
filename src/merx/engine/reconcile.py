"""Reconcile two sources for the same series, value by value.

Each (measure, valid_date) is compared between this dataset and the other one. Matches
are counted; every difference is stored in ``recon."<id>"`` with its class:

* ``value_differs``: both have the date, the values differ by more than the tolerance
  (a missing value against a present one counts as different);
* ``only_here``: this dataset has the date, the other does not, inside the other's range;
* ``only_there``: the other has it, this one does not, inside this one's range;
* ``outside_range``: the date lies beyond the other dataset's first or last date, which
  is usually just the two channels updating at different times.

The first three are breaks: real disagreements that someone should explain.

Two sources are not always on the same clock. A monthly benchmark from one institution
cannot be compared date-for-date with another's daily prices, so ``align:
monthly_mean`` averages the daily values within each month first. That comparison is a
different question from the two-channel one: channels of a single publisher should
agree exactly, while two institutions averaging "the price of WTI in September" will
differ by their method, and the tolerance is where that expectation is written down.
"""

from datetime import UTC, datetime

import duckdb

from merx.contracts import Contract
from merx.engine.stage import staged_table

BREAK_CLASSES = ("value_differs", "only_here", "only_there")


def recon_table(contract: Contract) -> str:
    return f'recon."{contract.id}"'


def _other_source(other_id: str, align: str, min_observations: int) -> str:
    """The SQL for the other side of the comparison, lined up with this dataset."""
    table = f'staged."{other_id}"'
    if align == "exact":
        return f"SELECT measure, valid_date, period, value FROM {table}"
    # A month is represented by its first day, which is how monthly sources date it.
    # Months too thinly observed to have a meaningful average are left out entirely
    # rather than compared against a handful of days.
    return f"""
        SELECT measure,
               date_trunc('month', valid_date)::DATE AS valid_date,
               0 AS period,
               avg(value) AS value
        FROM {table}
        WHERE value IS NOT NULL
        GROUP BY 1, 2
        HAVING count(*) >= {int(min_observations)}
    """


def reconcile(
    con: duckdb.DuckDBPyConnection,
    contract: Contract,
    other_id: str,
    tolerance: float,
    align: str = "exact",
    min_observations: int = 15,
) -> dict[str, int]:
    here = staged_table(contract)
    there = _other_source(other_id, align, min_observations)
    con.execute("CREATE SCHEMA IF NOT EXISTS recon")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {recon_table(contract)} AS
        WITH h AS (SELECT measure, valid_date, period, value FROM {here}),
             t AS ({there}),
             h_range AS (SELECT measure, min(valid_date) AS lo, max(valid_date) AS hi
                         FROM h GROUP BY measure),
             t_range AS (SELECT measure, min(valid_date) AS lo, max(valid_date) AS hi
                         FROM t GROUP BY measure),
             joined AS (
                SELECT coalesce(h.measure, t.measure) AS measure,
                       coalesce(h.valid_date, t.valid_date) AS valid_date,
                       h.value AS value_here, t.value AS value_there,
                       h.valid_date IS NOT NULL AS in_here,
                       t.valid_date IS NOT NULL AS in_there
                FROM h FULL OUTER JOIN t USING (measure, valid_date, period)
             )
        SELECT j.measure, j.valid_date, j.value_here, j.value_there,
               CASE
                 WHEN j.in_here AND j.in_there THEN
                   CASE WHEN j.value_here IS NULL AND j.value_there IS NULL THEN 'match'
                        WHEN j.value_here IS NULL OR j.value_there IS NULL THEN 'value_differs'
                        WHEN abs(j.value_here - j.value_there) <= ? THEN 'match'
                        ELSE 'value_differs' END
                 WHEN j.in_here AND j.valid_date BETWEEN tr.lo AND tr.hi THEN 'only_here'
                 WHEN j.in_there AND j.valid_date BETWEEN hr.lo AND hr.hi THEN 'only_there'
                 ELSE 'outside_range'
               END AS class
        FROM joined j
        LEFT JOIN h_range hr USING (measure)
        LEFT JOIN t_range tr USING (measure)
        ORDER BY j.measure, j.valid_date
        """,
        [tolerance],
    )
    counts = dict(
        con.execute(f"SELECT class, count(*) FROM {recon_table(contract)} GROUP BY 1").fetchall()
    )
    # Matches are only counted; the stored table keeps the differences.
    con.execute(f"DELETE FROM {recon_table(contract)} WHERE class = 'match'")
    classes = ("match", *BREAK_CLASSES, "outside_range")
    result = {c: counts.get(c, 0) for c in classes}
    # Keep every run's counts, so the matches (not stored row by row) stay visible.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS meta.reconciliations (
            dataset_id       VARCHAR   NOT NULL,
            other_dataset_id VARCHAR   NOT NULL,
            reconciled_at_utc TIMESTAMP NOT NULL,
            match INTEGER, value_differs INTEGER, only_here INTEGER,
            only_there INTEGER, outside_range INTEGER
        )
        """
    )
    con.execute(
        "INSERT INTO meta.reconciliations VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [contract.id, other_id, datetime.now(UTC).replace(tzinfo=None), *result.values()],
    )
    return result
