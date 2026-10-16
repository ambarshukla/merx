"""Point-in-time history: every version of every value, and when it was known.

Each value has two times. Its *valid date* is the date it describes. Its *knowledge
time* is when it became known: the fetch time of the raw file it first appeared in (or,
for an archived copy, the archive's capture time). Replaying every raw file of a dataset
in knowledge-time order gives, for each (measure, valid date), a sequence of versions:

    value, known_from_utc, known_to_utc, ended_by

``known_to_utc`` is when the version stopped being current, because a later file
changed the value (``revised``) or no longer contained it (``withdrawn``); it is empty
for the version that is current now. From this table, the dataset *as known at* any
moment can be rebuilt, and every revision can be listed.

Files of a partitioned dataset are compared only with earlier files of the same
partition. Values are compared exactly, and a missing value (NULL) is a value like any
other, so a gap that is later filled is a revision.
"""

from dataclasses import dataclass
from datetime import date, datetime

import duckdb
import pyarrow as pa

from merx.contracts import Contract
from merx.engine import formats
from merx.store.raw import RawZone


@dataclass
class _Open:
    value: float | None
    known_from: datetime
    raw_path: str
    origin: str
    # The start of the period this value belongs to, for sub-daily data. A property of
    # the (date, period) key rather than of the version, but carried here because the
    # key is what the replay walks.
    valid_time: datetime | None = None


def pit_table(contract: Contract) -> str:
    return f'pit."{contract.id}"'


def replay(contract: Contract, zone: RawZone) -> list[tuple]:
    """Rows of (measure, valid_date, period, valid_time, value, known_from, known_to,
    ended_by, raw_path, origin), from every raw file in knowledge-time order."""
    rows: list[tuple] = []
    current: dict[tuple[str | None, str, date, int], _Open] = {}
    for landed in zone.history(contract.id):
        observations = formats.read(zone.read(landed), contract, landed.partition).observations
        at = landed.fetched_at_utc
        seen: set[tuple[str | None, str, date, int]] = set()
        for obs in observations:
            key = (landed.partition, obs.measure, obs.valid_date, obs.period)
            seen.add(key)
            old = current.get(key)
            if old is not None and old.value == obs.value:
                continue
            if old is not None:
                rows.append(_row(key, old, at, "revised"))
            current[key] = _Open(obs.value, at, landed.path, landed.origin, obs.valid_time_utc)
        for key in [k for k in current if k[0] == landed.partition and k not in seen]:
            rows.append(_row(key, current.pop(key), at, "withdrawn"))
    rows.extend(_row(key, version, None, None) for key, version in current.items())
    return rows


def _row(key, version: _Open, known_to: datetime | None, ended_by: str | None) -> tuple:
    _, measure, valid_date, period = key
    return (
        measure,
        valid_date,
        period,
        version.valid_time,
        version.value,
        version.known_from,
        known_to,
        ended_by,
        version.raw_path,
        version.origin,
    )


def build(con: duckdb.DuckDBPyConnection, contract: Contract, zone: RawZone) -> dict:
    """Rebuild ``pit."<id>"`` from every raw file, and summarise it."""
    rows = replay(contract, zone)
    if not rows:
        raise ValueError(f"{contract.id}: no raw files to build history from")
    columns = [
        ("measure", pa.string()),
        ("valid_date", pa.date32()),
        ("period", pa.int32()),
        ("valid_time_utc", pa.timestamp("us")),
        ("value", pa.float64()),
        ("known_from_utc", pa.timestamp("us")),
        ("known_to_utc", pa.timestamp("us")),
        ("ended_by", pa.string()),
        ("raw_path", pa.string()),
        ("origin", pa.string()),
    ]
    table = pa.table({c: pa.array([r[i] for r in rows], t) for i, (c, t) in enumerate(columns)})
    con.execute("CREATE SCHEMA IF NOT EXISTS pit")
    con.register("incoming", table)
    con.execute(
        f"CREATE OR REPLACE TABLE {pit_table(contract)} AS "
        "SELECT * FROM incoming ORDER BY measure, valid_date, period, known_from_utc"
    )
    con.unregister("incoming")
    versions, keys, revised, withdrawn, first_known = con.execute(
        f"""
        SELECT count(*),
               count(DISTINCT (measure, valid_date, period)),
               count(DISTINCT (measure, valid_date, period)) FILTER (WHERE ended_by = 'revised'),
               count(*) FILTER (WHERE ended_by = 'withdrawn'),
               min(known_from_utc)
        FROM {pit_table(contract)}
        """
    ).fetchone()
    return {
        "versions": versions,
        "values": keys,
        "revised_values": revised,
        "withdrawn_versions": withdrawn,
        "first_known_utc": first_known,
        "raw_files": len(zone.history(contract.id)),
    }


def as_known_at(
    con: duckdb.DuckDBPyConnection, contract: Contract, at: datetime
) -> list[tuple[str, date, float | None]]:
    """The dataset as it was known at ``at`` (UTC): (measure, valid_date, period, value)."""
    return con.execute(
        f"SELECT measure, valid_date, period, value FROM {pit_table(contract)} "
        "WHERE known_from_utc <= ? AND (known_to_utc IS NULL OR known_to_utc > ?) "
        "ORDER BY measure, valid_date, period",
        [at, at],
    ).fetchall()


def revisions(con: duckdb.DuckDBPyConnection, contract: Contract) -> list[tuple]:
    """Every value that has been revised: (measure, valid_date, first value, latest value,
    number of versions, when it first changed)."""
    return con.execute(
        f"""
        SELECT measure, valid_date, period,
               -- first/last keep NULLs; arg_min/arg_max would skip a missing value
               first(value ORDER BY known_from_utc) AS first_value,
               last(value ORDER BY known_from_utc) AS latest_value,
               count(*) AS versions,
               min(known_to_utc) AS first_changed_utc
        FROM {pit_table(contract)}
        GROUP BY measure, valid_date, period
        HAVING count(*) > 1
        ORDER BY measure, valid_date, period
        """
    ).fetchall()
