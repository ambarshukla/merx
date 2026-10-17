"""Read-only queries behind the two consoles.

Every query opens the database read-only and closes it straight away. DuckDB lets one
process write a database file at a time, so while a pipeline run is writing, a read can
briefly fail to open the file; it is retried a few times before giving up.
"""

import bisect
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb

from merx.contracts import Contract, check_name
from merx.reference import Reference
from merx.store import DB_FILE

_RETRIES = 10
_PAUSE_S = 0.3


class DatabaseBusy(RuntimeError):
    """The pipeline held the database for longer than the retries allow."""


@dataclass(frozen=True)
class Store:
    data_dir: Path

    def query(self, sql: str, params: list | None = None) -> list[tuple]:
        path = self.data_dir / DB_FILE
        if not path.exists():
            return []
        for attempt in range(_RETRIES):
            try:
                con = duckdb.connect(str(path), read_only=True)
            except duckdb.IOException:
                if attempt == _RETRIES - 1:
                    raise DatabaseBusy("the database is being written; try again") from None
                time.sleep(_PAUSE_S)
                continue
            try:
                return con.execute(sql, params or []).fetchall()
            finally:
                con.close()
        return []

    def tables(self) -> set[tuple[str, str]]:
        return set(self.query("SELECT table_schema, table_name FROM information_schema.tables"))

    def has(self, schema: str, table: str) -> bool:
        return (schema, table) in self.tables()

    def columns(self, schema: str, table: str) -> set[str]:
        """The columns a table actually has.

        The website opens the database **read-only**, so it cannot migrate anything: it
        has to read whatever shape it finds. Most tables are rebuilt on every run and
        catch up by themselves, but a dataset blocked at the publish gate keeps its last
        good publication for as long as it stays blocked — which can be indefinitely.
        Where a query mentions a column added later, it asks first.
        """
        return {
            name
            for (name,) in self.query(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ?",
                [schema, table],
            )
        }


def _iso(v):
    if isinstance(v, datetime | date):
        return v.isoformat()
    return v


def rows_as_dicts(rows: list[tuple], columns: list[str]) -> list[dict]:
    return [dict(zip(columns, map(_iso, row), strict=True)) for row in rows]


# --- ops ------------------------------------------------------------------------------


def dataset_status(store: Store, contracts: list[Contract]) -> list[dict]:
    tables = store.tables()

    def latest(sql: str) -> dict:
        return {r[0]: r[1:] for r in store.query(sql)}

    builds = (
        latest(
            "SELECT dataset_id, staged_at_utc, rows FROM meta.stage_builds "
            "QUALIFY row_number() OVER (PARTITION BY dataset_id ORDER BY staged_at_utc DESC)=1"
        )
        if ("meta", "stage_builds") in tables
        else {}
    )
    pubs = (
        latest(
            "SELECT dataset_id, status, failed_checks, attempted_at_utc FROM meta.publications "
            "QUALIFY row_number() OVER "
            "(PARTITION BY dataset_id ORDER BY attempted_at_utc DESC)=1"
        )
        if ("meta", "publications") in tables
        else {}
    )
    open_exc = (
        latest(
            "SELECT dataset_id, count(*) FROM ops.exceptions WHERE closed_at_utc IS NULL GROUP BY 1"
        )
        if ("ops", "exceptions") in tables
        else {}
    )
    raw = (
        latest(
            "SELECT dataset_id, count(*), sum(bytes), "
            "max(fetched_at_utc) FILTER (WHERE origin = 'direct') "
            "FROM meta.raw_manifest GROUP BY 1"
        )
        if ("meta", "raw_manifest") in tables
        else {}
    )
    out = []
    for c in contracts:
        last_date = None
        if ("staged", c.id) in tables:
            (last_date,) = store.query(f'SELECT max(valid_date) FROM staged."{c.id}"')[0]
        staged_at, rows = builds.get(c.id, (None, None))
        status, failed, attempted = pubs.get(c.id, (None, [], None))
        files, size, fetched = raw.get(c.id, (0, 0, None))
        out.append(
            {
                "id": c.id,
                "title": c.title,
                "domain": c.domain,
                "tier": c.tier,
                "owner": c.owner,
                "frequency": c.frequency,
                "unit": c.unit,
                "rows": rows,
                "last_valid_date": _iso(last_date),
                "staged_at_utc": _iso(staged_at),
                "last_fetched_utc": _iso(fetched),
                "raw_files": files,
                "raw_bytes": size,
                "publication": status,
                "failed_checks": list(failed or []),
                "last_attempt_utc": _iso(attempted),
                "open_exceptions": open_exc.get(c.id, (0,))[0],
                "reconciled_with": c.reconcile_with.dataset if c.reconcile_with else None,
            }
        )
    return out


def exceptions(store: Store) -> list[dict]:
    if not store.has("ops", "exceptions"):
        return []
    columns = [
        "id", "dataset", "check", "severity", "owner", "opened_at_utc", "last_seen_utc",
        "closed_at_utc", "occurrences", "root_cause", "note", "last_description",
    ]  # fmt: skip
    rows = store.query(
        "SELECT exception_id, dataset_id, check_name, severity, owner, opened_at_utc, "
        "last_seen_utc, closed_at_utc, occurrences, root_cause, note, last_description "
        "FROM ops.exceptions ORDER BY closed_at_utc IS NOT NULL, opened_at_utc DESC"
    )
    return rows_as_dicts(rows, columns)


def checks(store: Store, contract: Contract) -> list[dict]:
    if not (store.has("ops", "check_results") and store.has("meta", "stage_builds")):
        return []
    rows = store.query(
        """
        SELECT check_name, severity, passed, description, metadata_json, checked_at_utc
        FROM ops.check_results
        WHERE dataset_id = ? AND build_id = (
            SELECT build_id FROM meta.stage_builds WHERE dataset_id = ?
            ORDER BY staged_at_utc DESC LIMIT 1)
        QUALIFY row_number() OVER (PARTITION BY check_name ORDER BY checked_at_utc DESC) = 1
        """,
        [contract.id, contract.id],
    )
    order = {check_name(c): i for i, c in enumerate(contract.checks)}
    result = rows_as_dicts(
        rows, ["check", "severity", "passed", "description", "metadata", "checked_at_utc"]
    )
    return sorted(result, key=lambda r: order.get(r["check"], 99))


def reconciliation(store: Store, contract: Contract) -> dict | None:
    if contract.reconcile_with is None or not store.has("recon", contract.id):
        return None
    counts = dict(store.query(f'SELECT class, count(*) FROM recon."{contract.id}" GROUP BY 1'))
    sample = store.query(
        f'SELECT valid_date, class, value_here, value_there FROM recon."{contract.id}" '
        "ORDER BY valid_date DESC LIMIT 30"
    )
    return {
        "other": contract.reconcile_with.dataset,
        "counts": counts,
        "latest_breaks": rows_as_dicts(sample, ["valid_date", "class", "here", "there"]),
    }


def missing(store: Store, contract: Contract) -> dict:
    """Values the history knows and the current staged build lacks, and how many of
    them a person has accepted."""
    if not (store.has("pit", contract.id) and store.has("staged", contract.id)):
        return {"missing": 0, "accepted": 0, "examples": []}
    # The key is (measure, valid_date, period), except in tables written before period
    # existed. Matching on the columns both sides actually have keeps the comparison
    # correct either way: for daily data period is always 0, so leaving it out of the
    # key changes nothing.
    key = ["measure", "valid_date"]
    if {"period"} <= store.columns("pit", contract.id) & store.columns("staged", contract.id):
        key.append("period")
    keys = ", ".join(key)

    accepted_join, accepted_col = "", "false"
    if store.has("ops", "accepted_withdrawals"):
        on = " AND ".join(
            f"a.{c} = k.{c}" for c in key if c in store.columns("ops", "accepted_withdrawals")
        )
        accepted_join = f"LEFT JOIN ops.accepted_withdrawals a ON a.dataset_id = ? AND {on}"
        accepted_col = "a.dataset_id IS NOT NULL"
    rows = store.query(
        f"""
        WITH k AS (SELECT DISTINCT {keys} FROM pit."{contract.id}")
        SELECT k.measure, k.valid_date, {accepted_col}
        FROM k ANTI JOIN staged."{contract.id}" s USING ({keys})
        {accepted_join}
        ORDER BY k.valid_date DESC
        """,
        [contract.id] if accepted_join else [],
    )
    return {
        "missing": len(rows),
        "accepted": sum(1 for r in rows if r[2]),
        "examples": [[m, d.isoformat(), bool(a)] for m, d, a in rows[:20]],
    }


def lab_headline(store: Store) -> list[dict]:
    if not store.has("lab", "scores"):
        return []
    rows = store.query(
        """
        SELECT s.detector_id,
               sum(caught) / sum(defects),
               sum(true_flags) / nullif(sum(true_flags + induced_false_flags), 0),
               a.apy
        FROM lab.scores s
        JOIN (SELECT detector_id, sum(flags) / sum(years) AS apy
              FROM lab.real_data_alerts GROUP BY 1) a USING (detector_id)
        WHERE s.primary_setting
        GROUP BY s.detector_id, a.apy ORDER BY 2 DESC
        """
    )
    return rows_as_dicts(rows, ["detector", "recall", "precision", "alerts_per_year"])


# --- client (published data only) -------------------------------------------------------


def _check_descriptions(store: Store) -> dict[tuple[str, str, str], str]:
    """What each check said, keyed by (dataset, build, check)."""
    if not store.has("ops", "check_results"):
        return {}
    return {
        (dataset_id, build_id, name): description
        for dataset_id, build_id, name, description in store.query(
            """
            SELECT dataset_id, build_id, check_name, description FROM ops.check_results
            QUALIFY row_number() OVER (PARTITION BY dataset_id, build_id, check_name
                                       ORDER BY checked_at_utc DESC) = 1
            """
        )
    }


def _publications(store: Store) -> tuple[dict[str, tuple], dict[str, tuple]]:
    """The newest attempt per dataset, and the newest one that actually published.

    They differ whenever the gate is holding newer data back: the rows on display come
    from the older, successful publication, so the two have to be reported separately.
    """
    if not store.has("meta", "publications"):
        return {}, {}
    rows = store.query(
        "SELECT dataset_id, status, attempted_at_utc, build_id, failed_checks "
        "FROM meta.publications ORDER BY attempted_at_utc"
    )
    latest, published = {}, {}
    for dataset_id, status, at, build_id, failed in rows:
        latest[dataset_id] = (status, at, build_id, list(failed))
        if status in ("pass", "warn"):
            published[dataset_id] = (status, at, build_id, list(failed))
    return latest, published


def _reasons(descriptions: dict, dataset_id: str, entry: tuple | None) -> list[dict]:
    """The checks a publication attempt recorded as failing, with what they said.

    The names come from the attempt itself, not from re-reading the checks now: a check
    can fail later against the same raw files (``no_withdrawals`` compares with a history
    that keeps growing), and that must not be reported as a reason these rows were
    published with a warning.
    """
    if entry is None:
        return []
    _status, _at, build_id, failed = entry
    return [
        {"check": name, "description": descriptions.get((dataset_id, build_id, name), "")}
        for name in failed
    ]


def published_catalog(
    store: Store, contracts: list[Contract], reference: Reference | None = None
) -> list[dict]:
    tables = store.tables()
    instruments = reference.instruments if reference else {}
    attempts, publications = _publications(store)
    descriptions = _check_descriptions(store)
    out = []
    for c in contracts:
        if ("published", c.id) not in tables:
            continue
        (status, published_at, first, last, n) = store.query(
            f"SELECT any_value(quality_status), max(published_at_utc), min(valid_date), "
            f'max(valid_date), count(*) FROM published."{c.id}"'
        )[0]
        measures = [m for (m,) in store.query(
            f'SELECT DISTINCT measure FROM published."{c.id}" ORDER BY 1'
        )]  # fmt: skip
        revised = 0
        if ("pit", c.id) in tables:
            (revised,) = store.query(
                f'SELECT count(DISTINCT (measure, valid_date)) FROM pit."{c.id}" '
                "WHERE ended_by IS NOT NULL"
            )[0]
        instrument = instruments.get(c.instrument)
        out.append(
            {
                "id": c.id,
                "title": c.title,
                "domain": c.domain,
                # The sector and the sort of data this is, for the catalogue's menu.
                "sector": instrument.sector if instrument else c.domain,
                "data_type": instrument.data_type if instrument else "spot",
                "instrument": c.instrument,
                "instrument_name": instrument.name if instrument else c.instrument,
                "unit": c.unit,
                "units": {m: c.unit_for(m) for m in measures},
                "frequency": c.frequency,
                "sub_daily": c.sub_daily,
                "measures": measures,
                "quality_status": status,
                "published_at_utc": _iso(published_at),
                "first_date": _iso(first),
                "last_date": _iso(last),
                "points": n,
                "changed_values_in_history": revised,
                # Why this publication is marked "warn": the checks it failed.
                "quality_reasons": _reasons(descriptions, c.id, publications.get(c.id)),
                # The newest attempt can be blocked while an older publication stays up,
                # so what blocked it is reported separately from the rows on display.
                "latest_attempt": attempts.get(c.id, (None,))[0],
                "latest_attempt_utc": _iso(attempts.get(c.id, (None, None))[1]),
                "latest_attempt_reasons": _reasons(descriptions, c.id, attempts.get(c.id)),
            }
        )
    return out


def published_series(
    store: Store, contract: Contract, measure: str, start: date | None, end: date | None
) -> list[list]:
    # Below a day there are many values per date, so the x axis is the period's start
    # time and the ordering needs the period. At a day or coarser there is one value
    # per date, so neither column is mentioned — which also means this reads a table
    # published before those columns existed. A blocked dataset keeps its last good
    # publication indefinitely, so such tables are not a transient state.
    x = "strftime(valid_time_utc, '%Y-%m-%dT%H:%M')" if contract.sub_daily else "valid_date"
    order = "valid_date, period" if contract.sub_daily else "valid_date"
    rows = store.query(
        f'SELECT {x}, value FROM published."{contract.id}" '
        f"WHERE measure = ? AND valid_date BETWEEN ? AND ? ORDER BY {order}",
        [measure, start or date(1900, 1, 1), end or date(2999, 12, 31)],
    )
    # Sub-daily x values arrive from the query as text already; dates do not.
    return [[d if isinstance(d, str) else d.isoformat(), v] for d, v in rows]


def series_as_known_at(store: Store, contract: Contract, measure: str, at: datetime) -> list[list]:
    if not store.has("pit", contract.id):
        return []
    # The history stores a period but not its start time, so a sub-daily series is
    # dated here by the period's start only when the column is present; otherwise the
    # date alone is returned, which the chart reads as the start of that day.
    available = store.columns("pit", contract.id)
    sub_daily = contract.sub_daily and "period" in available
    x = (
        "strftime(valid_time_utc, '%Y-%m-%dT%H:%M')"
        if sub_daily and "valid_time_utc" in available
        else "valid_date"
    )
    order = "valid_date, period" if sub_daily else "valid_date"
    rows = store.query(
        f'SELECT {x}, value FROM pit."{contract.id}" '
        "WHERE measure = ? AND known_from_utc <= ? "
        f"AND (known_to_utc IS NULL OR known_to_utc > ?) ORDER BY {order}",
        [measure, at, at],
    )
    return [[d if isinstance(d, str) else d.isoformat(), v] for d, v in rows]


def revisions(store: Store, contract: Contract, measure: str, limit: int = 200) -> list[dict]:
    if not store.has("pit", contract.id):
        return []
    rows = store.query(
        f"""
        SELECT valid_date,
               first(value ORDER BY known_from_utc) AS first_value,
               last(value ORDER BY known_from_utc) AS latest_value,
               count(*) AS versions,
               min(known_to_utc) AS first_changed_utc,
               bool_or(ended_by = 'withdrawn') AS withdrawn,
               bool_or(known_to_utc IS NULL) AS present_now
        FROM pit."{contract.id}" WHERE measure = ?
        GROUP BY valid_date HAVING count(*) > 1 OR bool_or(ended_by = 'withdrawn')
        ORDER BY valid_date DESC LIMIT ?
        """,
        [measure, limit],
    )
    return rows_as_dicts(
        rows,
        ["valid_date", "first_value", "latest_value", "versions", "first_changed_utc",
         "withdrawn", "present_now"],
    )  # fmt: skip


# --- two sources: one series through two channels ----------------------------------------


def reconciled_pairs(store: Store, contracts: list[Contract]) -> list[dict]:
    """Datasets reconciled with a second channel, with the latest counts."""
    by_id = {c.id: c for c in contracts}
    latest = {}
    if store.has("meta", "reconciliations"):
        latest = {
            r[0]: r[1:]
            for r in store.query(
                "SELECT dataset_id, match, value_differs, only_here, only_there, "
                "outside_range, reconciled_at_utc FROM meta.reconciliations "
                "QUALIFY row_number() OVER "
                "(PARTITION BY dataset_id ORDER BY reconciled_at_utc DESC) = 1"
            )
        }
    out = []
    for c in contracts:
        if c.reconcile_with is None:
            continue
        other = by_id[c.reconcile_with.dataset]
        counts = latest.get(c.id)
        out.append(
            {
                "id": c.id,
                "title": c.title,
                "other": other.id,
                "other_title": other.title,
                "unit": c.unit,
                "counts": None
                if counts is None
                else dict(
                    zip(
                        [
                            "match",
                            "value_differs",
                            "only_here",
                            "only_there",
                            "outside_range",
                            "reconciled_at_utc",
                        ],
                        map(_iso, counts),
                        strict=True,
                    )
                ),  # fmt: skip
            }
        )
    return out


def two_sources(store: Store, contract: Contract, other: Contract) -> dict:
    """Both channels' published series, every difference grouped in plain classes, and
    when the history saw values removed."""
    m = contract.measures[0]

    def series(c: Contract) -> list[list]:
        if not store.has("published", c.id):
            return []
        return [
            [d.isoformat(), v]
            for d, v in store.query(
                f'SELECT valid_date, value FROM published."{c.id}" '
                "WHERE measure = ? ORDER BY valid_date",
                [m],
            )
        ]

    groups: dict[str, list] = {
        "dropped": [], "api_only": [], "api_fill": [], "here_only": [], "differs": [],
    }  # fmt: skip
    timeline: list[dict] = []
    if store.has("recon", contract.id):
        withdrawn = set()
        if store.has("pit", contract.id):
            withdrawn = {
                d
                for (d,) in store.query(
                    f'SELECT DISTINCT valid_date FROM pit."{contract.id}" '
                    "WHERE ended_by = 'withdrawn' AND measure = ?",
                    [m],
                )
            }
        there_values = dict(
            store.query(
                f'SELECT valid_date, value FROM staged."{other.id}" WHERE measure = ?', [m]
            )
        ) if store.has("staged", other.id) else {}  # fmt: skip
        there_dates = sorted(there_values)

        for d, cls, here, there in store.query(
            f'SELECT valid_date, class, value_here, value_there FROM recon."{contract.id}" '
            "WHERE measure = ? ORDER BY valid_date",
            [m],
        ):
            row = [d.isoformat(), here, there]
            if cls == "value_differs":
                groups["differs"].append(row)
            elif cls == "only_here":
                groups["here_only"].append(row)
            elif cls == "only_there":
                if d in withdrawn:
                    groups["dropped"].append(row)
                else:
                    i = bisect.bisect_left(there_dates, d) - 1
                    prev = there_values[there_dates[i]] if i >= 0 else None
                    groups["api_fill" if prev == there else "api_only"].append(row)
        if store.has("pit", contract.id) and store.has("meta", "raw_manifest"):
            for known_to, n, first, last in store.query(
                f"SELECT known_to_utc, count(*), min(valid_date), max(valid_date) "
                f"FROM pit.\"{contract.id}\" WHERE ended_by = 'withdrawn' AND measure = ? "
                "GROUP BY 1 ORDER BY 1",
                [m],
            ):
                (before,) = store.query(
                    "SELECT max(fetched_at_utc) FROM meta.raw_manifest "
                    "WHERE dataset_id = ? AND fetched_at_utc < ?",
                    [contract.id, known_to],
                )[0]
                timeline.append(
                    {
                        "removed_between": [_iso(before), _iso(known_to)],
                        "values": n,
                        "first_date": _iso(first),
                        "last_date": _iso(last),
                    }
                )
    labels = {
        "dropped": "Dropped from the spreadsheet: known before, missing now, still in the API",
        "api_only": "Only ever in the API",
        "api_fill": "Only in the API, repeating the previous day's value (a holiday fill)",
        "here_only": "Only in the spreadsheet",
        "differs": "In both, with different values",
    }
    return {
        "id": contract.id,
        "title": contract.title,
        "other": other.id,
        "other_title": other.title,
        "unit": contract.unit,
        "measure": m,
        "here": series(contract),
        "there": series(other),
        "groups": [
            {"key": k, "label": labels[k], "count": len(v), "rows": v} for k, v in groups.items()
        ],
        "removals": timeline,
    }


# --- provenance: where a dataset's numbers actually came from ---------------------------

# What each source format is, in words, for people who will never read the parser.
_FORMAT = {
    "eia_hist_xls": "Excel spreadsheet (one series per file)",
    "eia_api_v2": "JSON API, version 2",
    "cftc_disaggregated": "ZIP archive holding the disaggregated Commitments of Traders report",
    "elexon_mid": "JSON API, market index data",
    "carbon_intensity": "JSON API, carbon intensity",
    "worldbank_pink_sheet": "Excel spreadsheet (many series; one column is read)",
}

_FREQUENCY_WORDS = {
    "half_hourly": "every half hour",
    "hourly": "every hour",
    "daily": "every weekday",
    "weekly": "weekly",
    "monthly": "monthly",
    "annual": "yearly",
}

_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def schedule_in_words(contract: Contract) -> str:
    """A contract's cron schedule as a sentence. Only the fields the contracts use."""
    minute, hour, _dom, _mon, dow = contract.schedule.cron.split()
    if dow == "*":
        when = "every day"
    else:
        names = [_DAYS[(int(d) - 1) % 7] for part in dow.split(",") for d in _expand(part)]
        when = "every " + ("weekday" if names == _DAYS[:5] else ", ".join(names))
    return f"{when} at {int(hour):02d}:{int(minute):02d} {contract.schedule.timezone}"


def _expand(part: str) -> list[str]:
    if "-" not in part:
        return [part]
    lo, hi = (int(x) for x in part.split("-"))
    return [str(d) for d in range(lo, hi + 1)]


def raw_files(store: Store, contract: Contract, limit: int = 8) -> dict:
    """Every download kept for this dataset: how many, how large, and the newest few.

    The raw zone is the evidence behind every published number, so the client page can
    show it rather than asking anyone to take the lineage on trust. Names are shown as
    the file is stored, which is how it would be found on disk.
    """
    empty = {"count": 0, "bytes": 0, "first_utc": None, "by_origin": {}, "newest": []}
    if not store.has("meta", "raw_manifest"):
        return empty
    (count, total, first) = store.query(
        "SELECT count(*), coalesce(sum(bytes), 0), min(fetched_at_utc) "
        "FROM meta.raw_manifest WHERE dataset_id = ?",
        [contract.id],
    )[0]
    # Downloads this project made and copies imported from the archive are both kept,
    # but they are different evidence: an archived copy is dated by when the archive
    # captured it, which is years before this project existed. Counting them together
    # would make the raw zone look older than it is.
    by_origin = {
        origin: {"count": n, "bytes": size, "first_utc": _iso(since)}
        for origin, n, size, since in store.query(
            "SELECT origin, count(*), coalesce(sum(bytes), 0), min(fetched_at_utc) "
            "FROM meta.raw_manifest WHERE dataset_id = ? GROUP BY 1 ORDER BY 1",
            [contract.id],
        )
    }
    rows = store.query(
        "SELECT path, url, fetched_at_utc, bytes, sha256, http_status, last_modified, "
        "partition, origin, archive_url FROM meta.raw_manifest WHERE dataset_id = ? "
        "ORDER BY fetched_at_utc DESC LIMIT ?",
        [contract.id, limit],
    )
    newest = rows_as_dicts(
        rows,
        ["path", "url", "fetched_at_utc", "bytes", "sha256", "http_status",
         "last_modified", "partition", "origin", "archive_url"],
    )  # fmt: skip
    for f in newest:
        f["name"] = f["path"].rsplit("/", 1)[-1]
        f["fetched_at_utc"] = _iso(f["fetched_at_utc"])
        # The full digest is not useful on a page; enough of it to compare by eye is.
        f["sha256_short"] = (f.pop("sha256") or "")[:16]
    return {
        "count": count,
        "bytes": total,
        "first_utc": _iso(first),
        "by_origin": by_origin,
        "newest": newest,
    }


def provenance(store: Store, contract: Contract, reference: Reference) -> dict:
    """Where this dataset comes from: publisher, terms, request, files, schedule.

    Everything here is already true of the pipeline — it is read from the contract and
    from the raw zone's manifest rather than written out by hand, so it cannot drift
    away from what the engine actually does.
    """
    source = contract.source
    provider = reference.providers.get(source.provider)
    partitions = source.partitions
    return {
        "dataset": contract.id,
        "provider": {
            "id": source.provider,
            "name": provider.name if provider else source.provider,
            "access": provider.access if provider else "",
            "terms": provider.terms if provider else "",
            "terms_url": provider.terms_url if provider else "",
            "attribution": provider.attribution if provider else "",
        },
        "request": {
            # The template is the honest answer for a partitioned source: no single URL
            # covers the dataset, and the placeholders are the point.
            "url": source.url,
            "format": _FORMAT.get(source.format, source.format),
            "format_id": source.format,
            "series_key": source.series_key,
            "needs_key": source.api_key_env is not None,
            "partitioned_by": partitions.by if partitions is not None else None,
            "partitions_from": str(partitions.start) if partitions is not None else None,
            "columns": source.columns,
        },
        "schedule": {
            "in_words": schedule_in_words(contract),
            "cron": contract.schedule.cron,
            "timezone": contract.schedule.timezone,
            "frequency": _FREQUENCY_WORDS.get(contract.frequency, contract.frequency),
        },
        "archive_backfill": source.archive_backfill,
        "archive_provider": reference.providers["internet_archive"].name
        if source.archive_backfill and "internet_archive" in reference.providers
        else None,
        "tier": contract.tier,
        "files": raw_files(store, contract),
    }
