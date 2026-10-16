"""Stage observations: one typed table per dataset, rebuilt from a single raw file.

``staged.<dataset_id>`` is replaced in one statement, so a rerun on the same raw file
produces the same table (idempotent) and a reader never sees half a load. The table is
in long form, one row per (measure, valid_date). Each row carries the raw file it came
from and when that file was fetched (its knowledge time), which is what the
point-in-time store will build on.

Every row carries a ``period``: 0 for daily-and-coarser data, meaning "the whole day",
and 1..n for data settled in numbered periods within a day. Keeping it non-null (rather
than null for daily data) means every key, join and grouping in the engine works the
same way for both, since in SQL a null never equals a null.
"""

import hashlib
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from merx.contracts import Contract
from merx.engine.formats import Observation, ReadResult
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


_BUILDS_DDL = """
CREATE TABLE IF NOT EXISTS meta.stage_builds (
    dataset_id     VARCHAR   NOT NULL,
    build_id       VARCHAR   NOT NULL,
    staged_at_utc  TIMESTAMP NOT NULL,
    rows           BIGINT    NOT NULL,
    raw_files      INTEGER   NOT NULL
)
"""


@dataclass(frozen=True)
class StageResult:
    table: str
    build_id: str
    rows: int
    null_values: int
    first_date: date
    last_date: date
    measures: list[str]


def staged_table(contract: Contract) -> str:
    return f'staged."{contract.id}"'


def build_id_for(dataset_id: str, raw_paths: list[str]) -> str:
    """A fingerprint of exactly which raw files a staged table was built from. Checks and
    the publish gate refer to it, so a result always belongs to one specific build."""
    text = "\n".join([dataset_id, *sorted(raw_paths)])
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def current_build(con: duckdb.DuckDBPyConnection, dataset_id: str) -> str | None:
    """The build the staged table currently holds."""
    con.execute(_BUILDS_DDL)
    row = con.execute(
        "SELECT build_id FROM meta.stage_builds WHERE dataset_id = ? "
        "ORDER BY staged_at_utc DESC LIMIT 1",
        [dataset_id],
    ).fetchone()
    return None if row is None else row[0]


def stage(
    contract: Contract,
    instrument_id: str,
    parts: list[tuple[LandedFile, ReadResult]],
    data_dir: Path,
) -> StageResult:
    """Rebuild the staged table from one raw file, or from one file per partition."""
    rows_in: list[tuple[Observation, LandedFile]] = [
        (obs, landed) for landed, read in parts for obs in read.observations
    ]
    if not rows_in:
        paths = [landed.path for landed, _ in parts]
        raise ValueError(f"{contract.id}: raw files {paths} hold no observations")
    unexpected = {obs.measure for obs, _ in rows_in} - set(contract.measures)
    if unexpected:
        raise ValueError(f"{contract.id}: file produced undeclared measures {sorted(unexpected)}")
    n = len(rows_in)
    build_id = build_id_for(contract.id, [landed.path for landed, _ in parts])
    table = pa.table(
        {
            "dataset_id": [contract.id] * n,
            "series_key": [contract.source.series_key] * n,
            "instrument_id": [instrument_id] * n,
            "measure": [o.measure for o, _ in rows_in],
            "valid_date": pa.array([o.valid_date for o, _ in rows_in], pa.date32()),
            "period": pa.array([o.period for o, _ in rows_in], pa.int32()),
            "valid_time_utc": pa.array([o.valid_time_utc for o, _ in rows_in], pa.timestamp("us")),
            "value": pa.array([o.value for o, _ in rows_in], pa.float64()),
            "unit": [contract.unit_for(o.measure) for o, _ in rows_in],
            "knowledge_time_utc": pa.array(
                [f.fetched_at_utc for _, f in rows_in], pa.timestamp("us")
            ),
            "raw_sha256": [f.sha256 for _, f in rows_in],
            "raw_path": [f.path for _, f in rows_in],
            "build_id": [build_id] * n,
        }
    )
    name = staged_table(contract)
    con = connect(data_dir)
    try:
        con.register("incoming", table)
        con.execute(
            f"CREATE OR REPLACE TABLE {name} AS "
            "SELECT * FROM incoming ORDER BY measure, valid_date, period"
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
        con.execute(_BUILDS_DDL)
        con.execute(
            "INSERT INTO meta.stage_builds VALUES (?, ?, ?, ?, ?)",
            [contract.id, build_id, datetime.now(UTC).replace(tzinfo=None), rows, len(parts)],
        )
    finally:
        con.close()
    return StageResult(
        table=name,
        build_id=build_id,
        rows=rows,
        null_values=nulls,
        first_date=first,
        last_date=last,
        measures=measures,
    )
