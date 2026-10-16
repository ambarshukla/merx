"""The no_withdrawals check: values known before must not silently disappear."""

from datetime import date, timedelta
from pathlib import Path

import dagster as dg
import pytest
from conftest import eia_workbook
from test_pipeline import FileFetcher

from merx import cli
from merx.config import CONTRACTS_DIR
from merx.contracts import load_contract
from merx.engine import withdrawals
from merx.orchestration.factory import build_assets
from merx.orchestration.resources import DataStore
from merx.reference import load_reference
from merx.store import connect

WTI = load_contract(CONTRACTS_DIR / "eia_wti_spot.yaml")  # tier 1


def days(n: int) -> list[date]:
    today = date.today()
    return [today - timedelta(days=n - i) for i in range(n)]


def run(tmp_path: Path, points) -> dg.ExecuteInProcessResult:
    source = tmp_path / "wti.xls"
    source.write_bytes(eia_workbook("RWTC", points))
    return dg.materialize(
        build_assets(WTI, load_reference()),
        resources={
            "store": DataStore(data_dir=str(tmp_path / "data")),
            "fetcher": FileFetcher(files={WTI.source.url: str(source)}),
        },
        raise_on_error=False,
    )


def check(result: dg.ExecuteInProcessResult) -> dg.AssetCheckEvaluation:
    (evaluation,) = [
        e for e in result.get_asset_check_evaluations() if e.check_name == "no_withdrawals"
    ]
    return evaluation


def published(tmp_path: Path):
    con = connect(tmp_path / "data")
    try:
        return con.execute(
            "SELECT status, failed_checks FROM meta.publications ORDER BY attempted_at_utc"
        ).fetchall()
    finally:
        con.close()


def test_first_build_has_no_history_and_passes(tmp_path):
    result = run(tmp_path, [(d, 60.0 + i) for i, d in enumerate(days(5))])
    assert check(result).passed
    assert check(result).description == "no history yet to compare against"


def test_a_vanished_value_fails_blocks_and_can_be_accepted(tmp_path):
    full = [(d, 60.0 + i) for i, d in enumerate(days(5))]
    assert run(tmp_path, full).success

    without_one = full[:2] + full[3:]  # the source silently drops one day
    blocked = run(tmp_path, without_one)
    assert not check(blocked).passed
    assert check(blocked).metadata["examples"].value[0][:3] == [
        "price",
        full[2][0].isoformat(),
        full[2][1],
    ]
    assert published(tmp_path)[-1] == ("blocked", ["no_withdrawals"])

    con = connect(tmp_path / "data")
    try:
        with pytest.raises(ValueError, match="reason is required"):
            withdrawals.accept(con, WTI.id, 'staged."eia_wti_spot"', "  ")
        assert withdrawals.accept(con, WTI.id, 'staged."eia_wti_spot"', "source removed it") == 1
    finally:
        con.close()

    accepted = run(tmp_path, without_one)
    assert check(accepted).passed
    assert check(accepted).metadata["missing_accepted"].value == 1
    assert published(tmp_path)[-1] == ("pass", [])


def test_a_value_that_comes_back_is_no_longer_missing(tmp_path):
    full = [(d, 60.0 + i) for i, d in enumerate(days(5))]
    run(tmp_path, full)
    run(tmp_path, full[:2] + full[3:])
    restored = run(tmp_path, full)
    assert check(restored).passed


def test_cli_lists_missing_values_and_accepts_them(tmp_path, monkeypatch, capsys):
    full = [(d, 60.0 + i) for i, d in enumerate(days(5))]
    run(tmp_path, full)
    run(tmp_path, full[1:])
    monkeypatch.setenv("MERX_DATA_DIR", str(tmp_path / "data"))
    cli.main(["missing", "eia_wti_spot"])
    assert "1 missing, 0 accepted" in capsys.readouterr().out
    cli.main(["accept-withdrawals", "eia_wti_spot", "--reason", "test"])
    assert "accepted 1 missing values" in capsys.readouterr().out
    cli.main(["exceptions"])
    out = capsys.readouterr().out
    assert "no_withdrawals" in out and "oil-data-steward" in out
