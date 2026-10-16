"""Reconcile two channels of the same series, value by value.

Each (measure, valid_date) is compared between this dataset and the other one. Matches
are counted; every difference is stored in ``recon."<id>"`` with its class:

* ``value_differs``: both have the date, the values differ by more than the tolerance
  (a missing value against a present one counts as different);
* ``only_here``: this dataset has the date, the other does not, inside the other's range;
* ``only_there``: the other has it, this one does not, inside this one's range;
* ``outside_range``: the date lies beyond the other dataset's first or last date, which
  is usually just the two channels updating at different times.

The first three are breaks: real disagreements that someone should explain.
"""

from datetime import UTC, datetime

import duckdb

from merx.contracts import Contract
from merx.engine.stage import staged_table

BREAK_CLASSES = ("value_differs", "only_here", "only_there")


def recon_table(contract: Contract) -> str:
    return f'recon."{contract.id}"'


def reconcile(
    con: duckdb.DuckDBPyConnection, contract: Contract, other_id: str, tolerance: float
) -> dict[str, int]:
    here, there = staged_table(contract), f'staged."{other_id}"'
    con.execute("CREATE SCHEMA IF NOT EXISTS recon")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {recon_table(contract)} AS
        WITH h AS (SELECT measure, valid_date, value FROM {here}),
             t AS (SELECT measure, valid_date, value FROM {there}),
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
                FROM h FULL OUTER JOIN t USING (measure, valid_date)
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
