"""``merx backfill``: fetch older partitions, then rebuild the tables once."""

from datetime import date
from pathlib import Path

import dagster as dg
import pytest
from test_cftc import WTI_NEW_NAME, cftc_zip, positions
from test_pipeline import FileFetcher

from merx.cli import main
from merx.config import CONTRACTS_DIR
from merx.contracts import load_contract
from merx.orchestration import backfill as bf
from merx.orchestration.definitions import build_definitions
from merx.store import connect

CFTC = load_contract(CONTRACTS_DIR / "cftc_wti_positions.yaml")  # yearly partitions
WTI = load_contract(CONTRACTS_DIR / "eia_wti_spot.yaml")  # one file


# --- choosing the partitions ---------------------------------------------------------


def test_a_single_file_dataset_has_nothing_to_backfill():
    with pytest.raises(bf.NotPartitioned, match="single file"):
        bf.partitions(WTI, None, None)


def test_a_range_is_inclusive_and_ordered():
    keys = bf.partitions(CFTC, "2012", "2015")
    assert keys == ["2012", "2013", "2014", "2015"]
    assert bf.partitions(CFTC, None, None)[0] == "2010"  # the contract's first year


def test_a_partition_the_contract_does_not_have_is_refused():
    with pytest.raises(ValueError, match="no partition '1999'"):
        bf.partitions(CFTC, "1999", None)


def test_reconciled_datasets_rebuild_their_comparison_too():
    names = [k.to_user_string() for k in bf.downstream_keys(CFTC)]
    assert names == ["staged/cftc_wti_positions", "published/cftc_wti_positions",
                     "pit/cftc_wti_positions"]  # fmt: skip
    assert "reconcile/eia_wti_spot" in [k.to_user_string() for k in bf.downstream_keys(WTI)]


# --- running it ----------------------------------------------------------------------


def files_for(tmp_path: Path, years: dict[str, bytes]) -> dict[str, str]:
    urls = {}
    for year, content in years.items():
        path = tmp_path / f"{year}.zip"
        path.write_bytes(content)
        urls[CFTC.source.url.replace("{year}", year)] = str(path)
    return urls


def test_two_partitions_are_fetched_then_staged_once(tmp_path):
    years = {
        "2025": cftc_zip([("067651", date(2025, 10, 7), WTI_NEW_NAME, positions())]),
        "2026": cftc_zip([("067651", date(2026, 10, 6), WTI_NEW_NAME, positions(2))]),
    }
    defs = build_definitions(
        data_dir=tmp_path / "data", fetcher=FileFetcher(files=files_for(tmp_path, years))
    )
    with dg.instance_for_test() as instance:
        steps = bf.backfill(defs, CFTC, ["2025", "2026"], instance=instance)
    assert [(s.name, s.ok) for s in steps] == [("2025", True), ("2026", True), ("rebuild", True)]

    con = connect(tmp_path / "data")
    try:
        # One staged table built from both files, and the raw zone kept both.
        assert con.execute("SELECT count(*) FROM meta.raw_manifest").fetchone() == (2,)
        dates = con.execute(
            'SELECT DISTINCT valid_date FROM staged."cftc_wti_positions" ORDER BY 1'
        ).fetchall()
        assert dates == [(date(2025, 10, 7),), (date(2026, 10, 6),)]
    finally:
        con.close()


def test_a_partition_the_source_cannot_serve_does_not_stop_the_others(tmp_path):
    years = {"2026": cftc_zip([("067651", date(2026, 10, 6), WTI_NEW_NAME, positions())])}
    defs = build_definitions(
        data_dir=tmp_path / "data", fetcher=FileFetcher(files=files_for(tmp_path, years))
    )
    with dg.instance_for_test() as instance:
        steps = bf.backfill(defs, CFTC, ["2024", "2026"], instance=instance)
    # 2024 has no file behind it here, so it fails; 2026 is still fetched and staged.
    assert [(s.name, s.ok) for s in steps] == [("2024", False), ("2026", True), ("rebuild", True)]


# --- the command ---------------------------------------------------------------------


def test_dry_run_lists_the_partitions_without_fetching(capsys):
    assert main(["backfill", "cftc_wti_positions", "--from", "2024", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "2024" in out and "2025" in out
    assert "staged, published, history" in out


def test_the_command_refuses_a_dataset_that_has_no_partitions():
    with pytest.raises(SystemExit, match="single file"):
        main(["backfill", "eia_wti_spot", "--dry-run"])


def test_the_command_refuses_an_unknown_dataset():
    with pytest.raises(SystemExit, match="unknown dataset"):
        main(["backfill", "nope", "--dry-run"])
