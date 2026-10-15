"""Stage a parsed series: one typed table per dataset, rebuilt from a single raw file.

``staged.<dataset_id>`` is replaced in one statement, so a rerun on the same raw file
produces the same table (idempotent) and a reader never sees half a load. Each row
carries the raw file it came from and when that file was fetched (its knowledge time),
which is what the point-in-time store will build on.
"""

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa

from merx.contracts import Contract
from merx.sources.eia_xls import ParsedSeries
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


def staged_table(contract: Contract) -> str:
    return f'staged."{contract.id}"'


def stage(
    contract: Contract,
    instrument_id: str,
    landed: LandedFile,
    parsed: ParsedSeries,
    data_dir: Path,
) -> StageResult:
    if not parsed.points:
        raise ValueError(f"{contract.id}: parsed file {landed.path} has no observations")
    n = len(parsed.points)
    table = pa.table(
        {
            "dataset_id": [contract.id] * n,
            "series_key": [parsed.series_key] * n,
            "instrument_id": [instrument_id] * n,
            "valid_date": pa.array([d for d, _ in parsed.points], pa.date32()),
            "value": pa.array([v for _, v in parsed.points], pa.float64()),
            "unit": [contract.unit] * n,
            "knowledge_time_utc": pa.array([landed.fetched_at_utc] * n, pa.timestamp("us")),
            "raw_sha256": [landed.sha256] * n,
            "raw_path": [landed.path] * n,
        }
    )
    name = staged_table(contract)
    con = connect(data_dir)
    try:
        con.register("incoming", table)
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM incoming")
        con.execute(_RELEASE_LOG_DDL)
        con.execute(
            "INSERT OR IGNORE INTO meta.release_log VALUES (?, ?, ?, ?, ?)",
            [
                contract.id,
                landed.path,
                landed.fetched_at_utc,
                parsed.release_date,
                parsed.next_release_date,
            ],
        )
        rows, nulls, first, last = con.execute(
            f"SELECT count(*), count(*) - count(value), min(valid_date), max(valid_date) "
            f"FROM {name}"
        ).fetchone()
    finally:
        con.close()
    return StageResult(table=name, rows=rows, null_values=nulls, first_date=first, last_date=last)
