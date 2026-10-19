"""Point-in-time history: replaying raw files, "as known at" queries, archive imports."""

import json
from datetime import UTC, date, datetime
from pathlib import Path

import dagster as dg
import duckdb
import pytest
from conftest import eia_workbook
from test_pipeline import FileFetcher

from merx.config import CONTRACTS_DIR
from merx.contracts import load_contract
from merx.engine import pit
from merx.orchestration.factory import build_assets
from merx.orchestration.resources import DataStore
from merx.reference import load_reference
from merx.sources import wayback
from merx.store import connect
from merx.store.raw import RawZone

STORAGE = load_contract(CONTRACTS_DIR / "eia_us_gas_storage.yaml")
KEY = STORAGE.source.series_key
D1, D2, D3 = date(2024, 9, 20), date(2024, 9, 27), date(2024, 10, 4)
T1, T2, T3 = (datetime(2024, 10, day, 15, tzinfo=UTC) for day in (3, 10, 17))


def naive(t: datetime) -> datetime:
    return t.astimezone(UTC).replace(tzinfo=None)


def land(zone: RawZone, points, at: datetime, origin: str = "direct") -> None:
    zone.land(
        dataset_id=STORAGE.id,
        provider="eia",
        url=STORAGE.source.url,
        content=eia_workbook(KEY, points),
        http_status=200,
        headers={},
        fetched_at_utc=at,
        origin=origin,
        archive_url=f"https://web.archive.org/web/{at:%Y%m%d%H%M%S}id_/x"
        if origin == "archive"
        else None,
    )


def three_vintages(tmp_path: Path) -> tuple[RawZone, duckdb.DuckDBPyConnection]:
    zone = RawZone(tmp_path)
    land(zone, [(D1, 3492.0), (D2, 3547.0)], T1)
    land(zone, [(D1, 3492.0), (D2, 3548.0), (D3, 3600.0)], T2)  # D2 revised, D3 new
    land(zone, [(D1, 3492.0), (D3, 3600.0)], T3)  # D2 withdrawn
    con = connect(tmp_path)
    pit.build(con, STORAGE, zone)
    return zone, con


def test_replay_records_each_version_and_how_it_ended(tmp_path):
    _, con = three_vintages(tmp_path)
    rows = con.execute(
        "SELECT valid_date, value, known_from_utc, known_to_utc, ended_by "
        'FROM pit."eia_us_gas_storage" ORDER BY valid_date, known_from_utc'
    ).fetchall()
    assert rows == [
        (D1, 3492.0, naive(T1), None, None),
        (D2, 3547.0, naive(T1), naive(T2), "revised"),
        (D2, 3548.0, naive(T2), naive(T3), "withdrawn"),
        (D3, 3600.0, naive(T2), None, None),
    ]


def test_as_known_at_rebuilds_the_dataset_at_any_moment(tmp_path):
    _, con = three_vintages(tmp_path)

    def at(t):
        return [(d, v) for _, d, _, v in pit.as_known_at(con, STORAGE, naive(t))]

    assert at(datetime(2024, 10, 1, tzinfo=UTC)) == []  # before anything was known
    assert at(datetime(2024, 10, 5, tzinfo=UTC)) == [(D1, 3492.0), (D2, 3547.0)]
    assert at(T2) == [(D1, 3492.0), (D2, 3548.0), (D3, 3600.0)]
    assert at(datetime(2026, 1, 1, tzinfo=UTC)) == [(D1, 3492.0), (D3, 3600.0)]


def test_revisions_lists_changed_values(tmp_path):
    _, con = three_vintages(tmp_path)
    # Daily data sits in period 0, "the whole day".
    assert pit.revisions(con, STORAGE) == [("working_gas", D2, 0, 3547.0, 3548.0, 2, naive(T2))]


def test_a_gap_filled_later_is_a_revision(tmp_path):
    zone = RawZone(tmp_path)
    land(zone, [(D1, 3492.0), (D2, None)], T1)
    land(zone, [(D1, 3492.0), (D2, 3547.0)], T2)
    con = connect(tmp_path)
    summary = pit.build(con, STORAGE, zone)
    assert summary["revised_values"] == 1
    assert pit.revisions(con, STORAGE)[0][3:5] == (None, 3547.0)


def test_identical_downloads_add_no_versions(tmp_path):
    zone = RawZone(tmp_path)
    for t in (T1, T2, T3):
        land(zone, [(D1, 3492.0), (D2, 3547.0)], t)
    con = connect(tmp_path)
    summary = pit.build(con, STORAGE, zone)
    assert (summary["versions"], summary["values"], summary["raw_files"]) == (2, 2, 3)


# --- the archive import ---------------------------------------------------------------


def test_capture_index_is_parsed_and_queried_without_the_scheme():
    url = wayback.index_url("https://www.eia.gov/dnav/ng/hist_xls/X.xls")
    assert "url=www.eia.gov%2Fdnav%2Fng%2Fhist_xls%2FX.xls" in url
    assert "collapse=digest" in url
    captures = wayback.parse_index(
        json.dumps([["timestamp", "digest"], ["20170801120000", "AAA"]]).encode()
    )
    assert captures == [wayback.Capture("20170801120000", "AAA")]
    assert captures[0].captured_at_utc == datetime(2017, 8, 1, 12, tzinfo=UTC)
    assert wayback.parse_index(b"[]") == []


def test_archive_import_backfills_history_and_never_feeds_staging(tmp_path):
    url = STORAGE.source.url
    old = wayback.Capture("20170801120000", "A")
    later = wayback.Capture("20190511120000", "B")
    files = {}
    for capture, value in ((old, 2888.0), (later, 2878.0)):  # a real-looking revision
        path = tmp_path / f"{capture.timestamp}.xls"
        # An archived copy states a release date at or before its own capture; a later
        # one would mean the capture stamp is wrong, which is its own case (D-029).
        path.write_bytes(
            eia_workbook(
                KEY,
                [(date(2017, 6, 30), value)],
                release_date=f"{capture.captured_at_utc:%m/%d/%Y}",
            )
        )
        files[wayback.capture_url(url, capture)] = str(path)
    index = tmp_path / "index.json"
    index.write_text(
        json.dumps([["timestamp", "digest"], [old.timestamp, "A"], [later.timestamp, "B"]])
    )
    files[wayback.index_url(url)] = str(index)
    today = tmp_path / "today.xls"
    today.write_bytes(eia_workbook(KEY, [(date(2017, 6, 30), 2878.0), (date(2026, 10, 9), 3500.0)]))
    files[url] = str(today)

    assets = build_assets(STORAGE, load_reference())
    resources = {
        "store": DataStore(data_dir=str(tmp_path / "data")),
        "fetcher": FileFetcher(files=files),
    }
    result = dg.materialize(assets, resources=resources, raise_on_error=False)
    assert result.success
    zone = RawZone(tmp_path / "data")
    assert [f.origin for f in zone.history(STORAGE.id)] == ["archive", "archive", "direct"]
    assert zone.latest(STORAGE.id).origin == "direct"  # staging reads our own download

    con = connect(tmp_path / "data")
    try:
        assert pit.revisions(con, STORAGE) == [
            ("working_gas", date(2017, 6, 30), 0, 2888.0, 2878.0, 2, naive(later.captured_at_utc))
        ]
        known_2018 = pit.as_known_at(con, STORAGE, datetime(2018, 1, 1))
        assert known_2018 == [("working_gas", date(2017, 6, 30), 0, 2888.0)]
    finally:
        con.close()

    # Resumable: a second import lands nothing new.
    again = dg.materialize(
        assets,
        resources=resources,
        selection=dg.AssetSelection.assets(dg.AssetKey(["archive", STORAGE.id])),
    )
    landed_now = [
        e.materialization.metadata["landed_now"].value
        for e in again.get_asset_materialization_events()
    ]
    assert landed_now == [0]


def test_a_gzip_compressed_copy_is_read_like_the_plain_file(tmp_path):
    import gzip

    from merx.engine import formats

    plain = eia_workbook(KEY, [(D1, 3492.0)])
    assert formats.read(gzip.compress(plain), STORAGE) == formats.read(plain, STORAGE)


def test_replay_stops_when_a_correction_would_break_knowledge_order(tmp_path):
    """The release-date correction (D-029) only moves a knowledge time later, so it can
    in principle push one file past the next. Nothing in the real data does, and guessing
    an order would invent revisions, so the rebuild stops instead."""
    zone = RawZone(tmp_path)
    # An archived copy stamped early but stating a release date after the next file:
    # replaying them in fetch order would be replaying them out of knowledge order.
    zone.land(
        dataset_id=STORAGE.id,
        provider="eia",
        url=STORAGE.source.url,
        content=eia_workbook(KEY, [(D1, 3492.0)], release_date="12/31/2024"),
        http_status=200,
        headers={},
        fetched_at_utc=datetime(2024, 1, 1, 12),
        origin="archive",
        archive_url="https://web.archive.org/web/20240101120000id_/x",
    )
    land(zone, [(D1, 3493.0)], datetime(2024, 6, 1, 12, tzinfo=UTC))
    con = connect(tmp_path)
    try:
        with pytest.raises(ValueError, match="replay needs knowledge order"):
            pit.build(con, STORAGE, zone)
    finally:
        con.close()
