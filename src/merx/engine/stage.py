"""Stage observations: one typed table per dataset, rebuilt from a single raw file.

``staged.<dataset_id>`` is replaced in one statement, so a rerun on the same raw file
produces the same table (idempotent) and a reader never sees half a load. The table is
in long form, one row per (measure, valid_date). Each row carries the raw file it came
from and when that file was fetched (its knowledge time), which is what the
point-in-time store will build on.
"""

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa

from merx.contracts import Contract
from merx.engine.formats import ReadResult
from merx.store import connect
from merx.store.raw import LandedFile

_RELEASE_LOG_DDL = """
CREATE TABLE IF NOT EXISTS meta.release_log (
    dataset_id         VARCHAR   NOT NULL,
    raw_path           VARCHAR   NOT NULL PRIMARY KEY,
    knowledge_time_utc TIMESTAMP NOT NULL,
    release_date       DATE      NOT NULL,
    next_release_date  DATE      NOT NULL
)
"""


@dataclass(frozen=True)
class StageResult:
    table: str
    rows: int
    null_values: int
    first_date: date
    last_date: date
    measures: list[str]


def staged_table(contract: Contract) -> str:
    return f'staged."{contract.id}"'


def stage(
    contract: Contract,
    instrument_id: str,
    parts: list[tuple[LandedFile, ReadResult]],
    data_dir: Path,
) -> StageResult:
    """Rebuild the staged table from one raw file, or from one file per partition."""
    rows_in: list[tuple[str, date, float | None, LandedFile]] = [
        (m, d, v, landed) for landed, read in parts for m, d, v in read.observations
    ]
    if not rows_in:
        paths = [landed.path for landed, _ in parts]
        raise ValueError(f"{contract.id}: raw files {paths} hold no observations")
    unexpected = {m for m, _, _, _ in rows_in} - set(contract.measures)
    if unexpected:
        raise ValueError(f"{contract.id}: file produced undeclared measures {sorted(unexpected)}")
    n = len(rows_in)
    table = pa.table(
        {
            "dataset_id": [contract.id] * n,
            "series_key": [contract.source.series_key] * n,
            "instrument_id": [instrument_id] * n,
            "measure": [m for m, _, _, _ in rows_in],
            "valid_date": pa.array([d for _, d, _, _ in rows_in], pa.date32()),
            "value": pa.array([v for _, _, v, _ in rows_in], pa.float64()),
            "unit": [contract.unit] * n,
            "knowledge_time_utc": pa.array(
                [f.fetched_at_utc for _, _, _, f in rows_in], pa.timestamp("us")
            ),
            "raw_sha256": [f.sha256 for _, _, _, f in rows_in],
            "raw_path": [f.path for _, _, _, f in rows_in],
        }
    )
    name = staged_table(contract)
    con = connect(data_dir)
    try:
        con.register("incoming", table)
        con.execute(
            f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM incoming ORDER BY measure, valid_date"
        )
        for landed, read in parts:
            if read.release_date is None or read.next_release_date is None:
                continue
            con.execute(_RELEASE_LOG_DDL)
            con.execute(
                "INSERT OR IGNORE INTO meta.release_log VALUES (?, ?, ?, ?, ?)",
                [
                    contract.id,
                    landed.path,
                    landed.fetched_at_utc,
                    read.release_date,
                    read.next_release_date,
                ],
            )
        rows, nulls, first, last = con.execute(
            f"SELECT count(*), count(*) - count(value), min(valid_date), max(valid_date) "
            f"FROM {name}"
        ).fetchone()
        measures = [
            m for (m,) in con.execute(f"SELECT DISTINCT measure FROM {name} ORDER BY 1").fetchall()
        ]
    finally:
        con.close()
    return StageResult(
        table=name,
        rows=rows,
        null_values=nulls,
        first_date=first,
        last_date=last,
        measures=measures,
    )
