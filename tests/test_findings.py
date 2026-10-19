"""What the controls caught, read back out of the records.

A finding is never written down. It is derived from the exceptions, the point-in-time
history and the reconciliation tables, so it appears when a control catches something and
goes away when the thing is fixed. These tests pin that: a clean store has no findings,
and a store with a withdrawal has exactly one, carrying a date that really does replay
the values that went missing.
"""

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import dagster as dg
import pytest
from conftest import core_assets, eia_workbook
from fastapi.testclient import TestClient
from test_pipeline import FileFetcher

from merx.api import create_app, data
from merx.config import CONTRACTS_DIR
from merx.contracts import load_contract
from merx.engine import ops
from merx.orchestration.resources import DataStore
from merx.store import connect

WTI = load_contract(CONTRACTS_DIR / "eia_wti_spot.yaml")


def run(data_dir: Path, points) -> None:
    source = data_dir.parent / "wti.xls"
    source.write_bytes(eia_workbook("RWTC", points))
    dg.materialize(
        core_assets(WTI),
        resources={
            "store": DataStore(data_dir=str(data_dir)),
            "fetcher": FileFetcher(files={WTI.source.url: str(source)}),
        },
        raise_on_error=False,
    )


@pytest.fixture
def points():
    today = date.today()
    return [(today - timedelta(days=12 - i), 60.0 + i) for i in range(12)]


def test_a_clean_store_reports_nothing(tmp_path, points):
    """No finding is invented: a pipeline that has caught nothing says nothing."""
    run(tmp_path / "data", points)
    client = TestClient(create_app(tmp_path / "data"))
    assert client.get("/api/client/findings").json() == []


def test_an_empty_store_reports_nothing(tmp_path):
    client = TestClient(create_app(tmp_path / "data"))
    assert client.get("/api/client/findings").json() == []


def withdraw(tmp_path, points) -> tuple[TestClient, datetime, date]:
    """Publish a series, then republish it with three values gone, and own the loss.

    The instant between the two runs is handed back: a test that happens inside one
    second cannot use a date to tell "before" from "after", but the history is stamped
    to the microsecond and the replay takes a full timestamp. So is the day the loss was
    noticed, because comparing with ``date.today()`` afterwards fails a run that crosses
    midnight.
    """
    data_dir = tmp_path / "data"
    run(data_dir, points)
    between = datetime.now(UTC).replace(tzinfo=None)
    run(data_dir, points[:4] + points[7:])
    noticed = datetime.now(UTC).date()
    con = connect(data_dir)
    try:
        ops.ensure_tables(con)
        (exception_id,) = con.execute(
            "SELECT exception_id FROM ops.exceptions WHERE check_name = 'no_withdrawals'"
        ).fetchone()
        ops.assign_root_cause(con, exception_id, "source_data_error", "the source dropped them")
    finally:
        con.close()
    return TestClient(create_app(data_dir)), between, noticed


def replay(client: TestClient, at) -> int:
    return len(
        client.get(
            "/api/client/series/eia_wti_spot/as-known-at",
            params={"measure": "price", "at": at.isoformat()},
        ).json()["points"]
    )


def test_a_withdrawal_becomes_a_finding(tmp_path, points):
    client, _, noticed = withdraw(tmp_path, points)
    (finding,) = client.get("/api/client/findings").json()
    assert finding["kind"] == "withdrawn"
    assert finding["number"] == "3"
    assert finding["dataset"] == "eia_wti_spot"
    assert finding["caught_by"] == "no_withdrawals"
    assert [s["count"] for s in finding["series"]] == [3]
    # The date offered is the day the removal was noticed, replayed as at its midnight.
    assert finding["asof"] == noticed.isoformat()


def test_the_replay_the_finding_points_at_really_does_show_the_lost_values(tmp_path, points):
    client, between, _ = withdraw(tmp_path, points)
    assert replay(client, between) == 12, "before the removal the history held every value"
    assert replay(client, datetime(2999, 1, 1)) == 9, "and now it holds three fewer"
    # Tier 1: the gate refused the shorter build, so users still see the last good one.
    published = client.get("/api/client/series/eia_wti_spot?measure=price").json()["points"]
    assert len(published) == 12


def test_a_finding_goes_away_when_the_loss_is_accepted(tmp_path, points):
    """Closing the exception closes the finding: the panel tracks the records, not a list."""
    client, _, _ = withdraw(tmp_path, points)
    assert len(client.get("/api/client/findings").json()) == 1
    con = connect(tmp_path / "data")
    try:
        con.execute("UPDATE ops.exceptions SET closed_at_utc = now() WHERE 1 = 1")
    finally:
        con.close()
    assert client.get("/api/client/findings").json() == []


def test_a_withdrawal_nobody_has_classified_yet_is_not_claimed_as_a_finding(tmp_path, points):
    """Until a person says the source was wrong, it is an open question, not a result."""
    data_dir = tmp_path / "data"
    run(data_dir, points)
    run(data_dir, points[:4] + points[7:])
    client = TestClient(create_app(data_dir))
    assert client.get("/api/client/findings").json() == []


def test_the_largest_removal_is_the_one_linked_to(tmp_path, points):
    """Series lose values more than once; the date must point at the big one."""
    store = data.Store(tmp_path / "data")
    run(tmp_path / "data", points)
    run(tmp_path / "data", points[:11])  # one value goes
    run(tmp_path / "data", points[:4])  # then seven more
    asof, count = data._before_the_largest_removal(store, "eia_wti_spot")
    assert count == 7, "the bigger of the two removals is the one worth showing"
    assert asof is not None


def test_the_count_is_every_value_lost_not_just_the_biggest_loss(tmp_path, points):
    """A series loses values more than once, and all of them count.

    The replay date comes from the largest removal, because that is the one worth
    showing. The number must not: reporting the largest event's size would quietly drop
    every value lost in the smaller ones. On the live site that was the difference
    between 3,401 and 3,414.
    """
    data_dir = tmp_path / "data"
    run(data_dir, points)
    run(data_dir, points[:11])  # one value goes
    run(data_dir, points[:4])  # then seven more
    con = connect(data_dir)
    try:
        ops.ensure_tables(con)
        for (exception_id,) in con.execute(
            "SELECT exception_id FROM ops.exceptions WHERE check_name = 'no_withdrawals'"
        ).fetchall():
            ops.assign_root_cause(con, exception_id, "source_data_error", "dropped")
    finally:
        con.close()
    client = TestClient(create_app(data_dir))
    (finding,) = client.get("/api/client/findings").json()
    assert finding["number"] == "8", "one value plus seven, not seven"
    assert [s["count"] for s in finding["series"]] == [8]
    # The date still points at the larger of the two removals.
    asof, largest = data._before_the_largest_removal(data.Store(data_dir), "eia_wti_spot")
    assert largest == 7
    assert finding["asof"] == asof
