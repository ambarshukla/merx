"""The consoles' API: ops sees everything; the client sees only published data."""

from datetime import date, timedelta
from pathlib import Path

import dagster as dg
import pytest
from conftest import core_assets, eia_workbook
from fastapi.testclient import TestClient
from test_pipeline import FileFetcher

from merx.api import create_app
from merx.config import CONTRACTS_DIR
from merx.contracts import load_contract
from merx.orchestration.resources import DataStore

WTI = load_contract(CONTRACTS_DIR / "eia_wti_spot.yaml")


def run(data: Path, points) -> None:
    source = data.parent / "wti.xls"
    source.write_bytes(eia_workbook("RWTC", points))
    dg.materialize(
        core_assets(WTI),
        resources={
            "store": DataStore(data_dir=str(data)),
            "fetcher": FileFetcher(files={WTI.source.url: str(source)}),
        },
        raise_on_error=False,
    )


@pytest.fixture
def points():
    today = date.today()
    return [(today - timedelta(days=5 - i), 60.0 + i) for i in range(5)]


@pytest.fixture
def client(tmp_path, points) -> TestClient:
    run(tmp_path / "data", points)
    return TestClient(create_app(tmp_path / "data"))


def test_ops_lists_every_contract_with_its_status(client):
    rows = {d["id"]: d for d in client.get("/api/ops/datasets").json()}
    assert "eia_us_gas_storage" in rows  # known from its contract, never run here
    assert rows["eia_us_gas_storage"]["publication"] is None
    wti = rows["eia_wti_spot"]
    assert (wti["publication"], wti["rows"], wti["tier"]) == ("pass", 5, 1)
    assert wti["reconciled_with"] == "eia_api_wti_spot"


def test_ops_dataset_detail_shows_every_check(client):
    detail = client.get("/api/ops/datasets/eia_wti_spot").json()
    assert {"no_withdrawals", "anomaly_seasonal_suite"} <= {c["check"] for c in detail["checks"]}
    assert all(c["passed"] for c in detail["checks"])
    assert detail["missing"]["missing"] == 0
    assert client.get("/api/ops/datasets/nope").status_code == 404


def test_client_sees_only_published_datasets(client, points):
    catalog = client.get("/api/client/datasets").json()
    assert [c["id"] for c in catalog] == ["eia_wti_spot"]
    assert catalog[0]["quality_status"] == "pass"
    series = client.get("/api/client/series/eia_wti_spot").json()
    assert series["points"] == [[d.isoformat(), v] for d, v in points]
    assert client.get("/api/client/series/eia_us_gas_storage").status_code == 404
    assert client.get("/api/client/series/eia_wti_spot?measure=volume").status_code == 404


def test_client_history_and_a_held_back_update(tmp_path, points):
    data = tmp_path / "data"
    run(data, points)
    run(data, points[:2] + points[3:])  # a value vanishes: the gate blocks the update
    client = TestClient(create_app(data))
    (wti,) = client.get("/api/client/datasets").json()
    assert (wti["quality_status"], wti["latest_attempt"]) == ("pass", "blocked")
    # The last publication that passed is still what clients see.
    assert len(client.get("/api/client/series/eia_wti_spot").json()["points"]) == 5
    revisions = client.get("/api/client/series/eia_wti_spot/revisions").json()
    assert [(r["valid_date"], r["present_now"]) for r in revisions] == [
        (points[2][0].isoformat(), False)
    ]
    at = client.get(
        "/api/client/series/eia_wti_spot/as-known-at", params={"at": "2999-01-01T00:00:00"}
    ).json()
    # The history records the source as it is now, without the vanished value, even
    # though the published layer still holds the last build that passed.
    assert len(at["points"]) == 4


def test_pages_and_assets_are_served(client):
    assert "operations console" in client.get("/ops").text
    assert "published commodity data" in client.get("/client").text
    assert "lineChart" in client.get("/static/chart.js").text
    assert client.get("/", follow_redirects=False).status_code in (302, 307)


def test_an_empty_data_directory_is_read_without_being_created(tmp_path):
    client = TestClient(create_app(tmp_path / "nothing"))
    assert client.get("/api/client/datasets").json() == []
    assert client.get("/api/ops/exceptions").json() == []
    assert not (tmp_path / "nothing").exists()


def test_pages_answer_head_requests_too(client):
    # Link previews and uptime monitors check a page with HEAD before fetching it.
    for path in ("/client", "/sources", "/ops"):
        assert client.head(path).status_code == 200
