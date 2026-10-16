"""End to end through Dagster: contract -> raw file landed -> staged table -> checks.

The network is replaced by a fetcher that serves synthetic files from disk.
"""

from datetime import date, timedelta
from pathlib import Path

import dagster as dg
import pytest
from conftest import core_assets, eia_workbook

from merx.config import CONTRACTS_DIR
from merx.contracts import load_contract, load_contracts
from merx.orchestration.definitions import build_definitions
from merx.orchestration.factory import schedule_job_name
from merx.orchestration.resources import DataStore, HttpFetcher
from merx.reference import load_reference
from merx.sources import wayback
from merx.sources.http import FetchResult
from merx.store import connect

CONTRACT = load_contract(CONTRACTS_DIR / "eia_wti_spot.yaml")


class FileFetcher(HttpFetcher):
    """Serves each URL from a local file instead of the network. A web-archive index that
    is not listed answers "no archived copies"."""

    files: dict[str, str]
    polite_delay_s: float = 0.0

    def fetch(self, url: str) -> FetchResult:
        if url not in self.files and url.startswith(wayback.CDX_ENDPOINT):
            return FetchResult(url=url, status=200, headers={}, content=b"[]")
        content = Path(self.files[url]).read_bytes()
        return FetchResult(url=url, status=200, headers={"ETag": '"t"'}, content=content)


def recent_points(missing: bool = False) -> list[tuple[date, float | None]]:
    today = date.today()
    return [
        (today - timedelta(days=4), 61.2),
        (today - timedelta(days=3), -36.98),
        (today - timedelta(days=2), None if missing else 60.0),
        (today - timedelta(days=1), 62.5),
    ]


def run(tmp_path: Path, points) -> dg.ExecuteInProcessResult:
    source = tmp_path / "source.xls"
    source.write_bytes(eia_workbook("RWTC", points))
    reference = load_reference()
    return dg.materialize(
        core_assets(CONTRACT, reference),
        resources={
            "store": DataStore(data_dir=str(tmp_path / "data")),
            "fetcher": FileFetcher(files={CONTRACT.source.url: str(source)}),
        },
        raise_on_error=False,
    )


def check_results(result: dg.ExecuteInProcessResult) -> dict[str, bool]:
    return {e.check_name: e.passed for e in result.get_asset_check_evaluations()}


def query(tmp_path: Path, sql: str):
    con = connect(tmp_path / "data")
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_clean_file_lands_stages_and_passes_every_check(tmp_path):
    result = run(tmp_path, recent_points())
    assert result.success
    assert check_results(result) == {
        "not_null_value": True,
        "unique_measure_valid_date": True,
        "range_value": True,
        "freshness": True,
        "no_withdrawals": True,
        "anomaly_seasonal_suite": True,
    }
    rows = query(
        tmp_path,
        'SELECT instrument_id, unit, value FROM staged."eia_wti_spot" ORDER BY valid_date',
    )
    assert [r[2] for r in rows] == [61.2, -36.98, 60.0, 62.5]
    assert {(r[0], r[1]) for r in rows} == {("WTI_CUSHING_SPOT", "USD/bbl")}


def test_rerun_keeps_every_download_and_rebuilds_the_same_table(tmp_path):
    run(tmp_path, recent_points())
    run(tmp_path, recent_points())
    assert query(tmp_path, "SELECT count(*) FROM meta.raw_manifest") == [(2,)]
    assert query(tmp_path, "SELECT count(DISTINCT sha256) FROM meta.raw_manifest") == [(1,)]
    assert query(tmp_path, 'SELECT count(*) FROM staged."eia_wti_spot"') == [(4,)]
    # The staged rows point at the newest download.
    assert query(
        tmp_path,
        'SELECT count(*) FROM staged."eia_wti_spot" s '
        "WHERE s.raw_path = (SELECT max(path) FROM meta.raw_manifest)",
    ) == [(4,)]


def test_release_dates_are_logged_per_download(tmp_path):
    run(tmp_path, recent_points())
    assert query(tmp_path, "SELECT release_date, next_release_date FROM meta.release_log") == [
        (date(2026, 10, 15), date(2026, 10, 21))
    ]


def test_missing_value_fails_the_not_null_check(tmp_path):
    result = run(tmp_path, recent_points(missing=True))
    assert check_results(result)["not_null_value"] is False
    # The missing value is kept as a NULL row, not dropped.
    assert query(tmp_path, 'SELECT count(*) FROM staged."eia_wti_spot" WHERE value IS NULL') == [
        (1,)
    ]


def test_wrong_file_fails_at_parse_and_stages_nothing(tmp_path):
    source = tmp_path / "source.xls"
    source.write_bytes(eia_workbook("RBRTE", recent_points()))
    result = dg.materialize(
        core_assets(CONTRACT),
        resources={
            "store": DataStore(data_dir=str(tmp_path / "data")),
            "fetcher": FileFetcher(files={CONTRACT.source.url: str(source)}),
        },
        raise_on_error=False,
    )
    assert not result.success
    # The raw file was still landed (it is evidence), but no staged table exists.
    assert query(tmp_path, "SELECT count(*) FROM meta.raw_manifest") == [(1,)]
    with pytest.raises(Exception, match="does not exist"):
        query(tmp_path, 'SELECT * FROM staged."eia_wti_spot"')


def test_repository_definitions_cover_every_contract(tmp_path):
    defs = build_definitions(data_dir=tmp_path)
    contracts = load_contracts(load_reference())
    keys = {k.to_user_string() for k in defs.resolve_all_asset_keys()}
    for c in contracts:
        assert {f"raw/{c.id}", f"staged/{c.id}"} <= keys
    # One schedule per distinct (cron, timezone) for ordinary contracts; a schedule of its
    # own for each partitioned contract, which picks the partition to refresh.
    expected = {
        f"refresh_{c.id}_schedule"
        if c.source.partitions
        else schedule_job_name(c.schedule.cron, c.schedule.timezone) + "_schedule"
        for c in contracts
    }
    assert {s.name for s in defs.schedules} == expected
    assert "refresh_30_18_x_x_1to5_new_york_schedule" in expected
    assert "refresh_cftc_wti_positions_schedule" in expected
