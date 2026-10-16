"""The publish gate and exceptions as data."""

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import dagster as dg
import duckdb
import pytest
from conftest import eia_workbook
from test_pipeline import FileFetcher, recent_points

from merx.config import CONTRACTS_DIR
from merx.contracts import check_name, load_contract
from merx.engine import ops
from merx.engine.checks import CheckOutcome
from merx.engine.publish import decide
from merx.orchestration.factory import build_assets
from merx.orchestration.resources import DataStore
from merx.reference import load_reference
from merx.store import connect

WTI = load_contract(CONTRACTS_DIR / "eia_wti_spot.yaml")  # tier 1
FUTURES = load_contract(CONTRACTS_DIR / "eia_wti_futures_c1.yaml")  # tier 3


def all_pass(contract) -> dict[str, tuple[bool, str]]:
    return {check_name(c): (True, c.severity) for c in contract.checks}


# --- the gate rule ------------------------------------------------------------------


def test_tier1_publishes_when_every_check_passes():
    assert decide(WTI, all_pass(WTI)).status == "pass"


def test_tier1_blocks_on_a_failed_error_check():
    results = all_pass(WTI) | {"not_null_value": (False, "error")}
    decision = decide(WTI, results)
    assert (decision.publish, decision.status, decision.failed) == (
        False,
        "blocked",
        ["not_null_value"],
    )


def test_tier1_publishes_with_warning_on_a_failed_warn_check():
    results = all_pass(WTI) | {"freshness": (False, "warn")}
    assert decide(WTI, results).status == "warn"


def test_lower_tiers_publish_with_warning_even_on_error_checks():
    results = all_pass(FUTURES) | {"range_value": (False, "error")}
    decision = decide(FUTURES, results)
    assert (decision.publish, decision.status) == (True, "warn")


def test_an_unchecked_build_is_never_published():
    results = all_pass(WTI)
    del results["freshness"]
    decision = decide(WTI, results)
    assert (decision.publish, decision.status, decision.missing) == (
        False,
        "unchecked",
        ["freshness"],
    )


# --- exceptions as data -------------------------------------------------------------

T0 = datetime(2026, 10, 1, 12, tzinfo=UTC)


def record(con, passed: bool, hours: float, check: str = "freshness"):
    return ops.record(
        con,
        dataset_id="d",
        owner="oil-data-steward",
        build_id="b",
        check_name=check,
        severity="error",
        outcome=CheckOutcome(passed, "ok" if passed else "stale"),
        at=T0 + timedelta(hours=hours),
    )


def test_exception_lifecycle_and_operating_numbers():
    con = duckdb.connect()
    assert record(con, True, 0).action == "none"
    first = record(con, False, 1)
    assert first.action == "opened"
    assert record(con, False, 2) == ops.ExceptionChange("seen_again", first.exception_id)
    assert record(con, True, 5) == ops.ExceptionChange("closed", first.exception_id)
    again = record(con, False, 30)  # the same check fails again: a repeat
    assert again.action == "opened" and again.exception_id != first.exception_id

    (occurrences,) = con.execute(
        "SELECT occurrences FROM ops.exceptions WHERE exception_id = ?", [first.exception_id]
    ).fetchone()
    assert occurrences == 2
    s = ops.summary(con)
    assert (s["exceptions"], s["open"], s["closed"]) == (2, 1, 1)
    assert s["median_hours_to_recover"] == pytest.approx(4.0)
    assert s["repeat_share"] == pytest.approx(0.5)
    assert con.execute("SELECT count(*) FROM ops.check_results").fetchone() == (5,)


def test_root_cause_must_be_a_known_class():
    con = duckdb.connect()
    change = record(con, False, 0)
    with pytest.raises(ValueError, match="unknown root cause"):
        ops.assign_root_cause(con, change.exception_id, "gremlins")
    ops.assign_root_cause(con, change.exception_id, "source_late", "EIA holiday")
    assert con.execute("SELECT root_cause, note FROM ops.exceptions").fetchone() == (
        "source_late",
        "EIA holiday",
    )
    with pytest.raises(LookupError):
        ops.assign_root_cause(con, 999, "source_late")


# --- end to end through Dagster ------------------------------------------------------


def run_all(tmp_path: Path, contract, points, key: str) -> dg.ExecuteInProcessResult:
    source = tmp_path / f"{contract.id}.xls"
    source.write_bytes(eia_workbook(key, points))
    return dg.materialize(
        [*build_assets(contract, load_reference())],
        resources={
            "store": DataStore(data_dir=str(tmp_path / "data")),
            "fetcher": FileFetcher(files={contract.source.url: str(source)}),
        },
        raise_on_error=False,
    )


def query(tmp_path: Path, sql: str):
    con = connect(tmp_path / "data")
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_tier1_failure_blocks_then_recovery_publishes(tmp_path):
    blocked = run_all(tmp_path, WTI, recent_points(missing=True), "RWTC")
    assert not blocked.success
    assert query(tmp_path, "SELECT status, failed_checks FROM meta.publications") == [
        ("blocked", ["not_null_value"])
    ]
    with pytest.raises(duckdb.CatalogException):
        query(tmp_path, 'SELECT * FROM published."eia_wti_spot"')
    assert query(
        tmp_path, "SELECT check_name, closed_at_utc IS NULL, owner FROM ops.exceptions"
    ) == [("not_null_value", True, "oil-data-steward")]

    fixed = run_all(tmp_path, WTI, recent_points(), "RWTC")
    assert fixed.success
    assert query(tmp_path, 'SELECT DISTINCT quality_status FROM published."eia_wti_spot"') == [
        ("pass",)
    ]
    assert query(tmp_path, "SELECT closed_at_utc IS NOT NULL FROM ops.exceptions") == [(True,)]
    # The published rows are exactly the current staged build.
    assert query(
        tmp_path,
        'SELECT count(*) FROM published."eia_wti_spot" p '
        "WHERE p.build_id = (SELECT build_id FROM meta.stage_builds "
        "ORDER BY staged_at_utc DESC LIMIT 1)",
    ) == [(4,)]


def test_tier3_stale_data_is_published_with_a_warning(tmp_path):
    old = [(date(2024, 4, 1) + timedelta(days=i), 80.0 + i) for i in range(4)]
    result = run_all(tmp_path, FUTURES, old, "RCLC1")
    assert result.success
    assert query(
        tmp_path, 'SELECT DISTINCT quality_status FROM published."eia_wti_futures_c1"'
    ) == [("warn",)]
    assert query(tmp_path, "SELECT status, failed_checks FROM meta.publications") == [
        ("warn", ["freshness"])
    ]
