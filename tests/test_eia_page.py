"""One publisher's own page: what merx takes from EIA, and the storage identity.

The five regions EIA publishes are the parts of the Lower 48 total, but each is rounded
to whole Bcf and so is the total, so the parts do not add up to the whole. These tests
pin that the page measures the gap rather than asserting there is none.
"""

from datetime import date, timedelta
from pathlib import Path

import dagster as dg
from conftest import core_assets, eia_workbook
from fastapi.testclient import TestClient
from test_pipeline import FileFetcher

from merx.api import create_app
from merx.api.data import STORAGE_REGIONS, STORAGE_TOTAL
from merx.config import CONTRACTS_DIR
from merx.contracts import Contract, load_contract
from merx.orchestration.resources import DataStore

TOTAL = load_contract(CONTRACTS_DIR / f"{STORAGE_TOTAL}.yaml")
REGIONS = [load_contract(CONTRACTS_DIR / f"{d}.yaml") for d, _ in STORAGE_REGIONS]
# 100 + 200 + 300 + 40 + 60 = 700: regions that do add up, to vary from.
PARTS = [100.0, 200.0, 300.0, 40.0, 60.0]


def weeks(n: int) -> list[date]:
    """n consecutive week-ending Fridays, the last recent enough to pass freshness."""
    last = date.today() - timedelta(days=(date.today().weekday() - 4) % 7 + 7)
    return [last - timedelta(weeks=i) for i in reversed(range(n))]


def exact_split(dates: list[date]) -> dict[str, list[float]]:
    values = {c.id: [p] * len(dates) for c, p in zip(REGIONS, PARTS, strict=True)}
    values[TOTAL.id] = [sum(PARTS)] * len(dates)
    return values


def publish(
    data_dir: Path, values: dict[str, list[float]], dates: list[date], which: list[Contract]
) -> None:
    """Run each contract from its own synthetic spreadsheet, as the real ones are."""
    for contract in which:
        source = data_dir.parent / f"{contract.id}.xls"
        source.write_bytes(
            eia_workbook(
                contract.source.series_key,
                list(zip(dates, values[contract.id], strict=True)),
            )
        )
        dg.materialize(
            core_assets(contract),
            resources={
                "store": DataStore(data_dir=str(data_dir)),
                "fetcher": FileFetcher(files={contract.source.url: str(source)}),
            },
            raise_on_error=False,
        )


def storage_of(data_dir: Path) -> dict | None:
    return TestClient(create_app(data_dir)).get("/api/eia/overview").json()["storage"]


def test_the_page_is_served():
    assert TestClient(create_app(Path("nonexistent"))).get("/eia").status_code == 200


def test_overview_names_the_publisher_and_counts_only_its_datasets(tmp_path):
    body = TestClient(create_app(tmp_path / "data")).get("/api/eia/overview").json()
    assert body["publisher"]["id"] == "eia"
    assert "Energy Information Administration" in body["publisher"]["name"]
    # The terms are the ones PROVENANCE recorded, carried as data.
    assert "public domain" in body["publisher"]["terms"].lower()
    assert body["datasets"], "EIA is the largest source here"
    assert all(d["id"].startswith("eia_") for d in body["datasets"])
    assert body["counts"]["datasets"] == len(body["datasets"])
    assert body["counts"]["of_all_datasets"] > body["counts"]["datasets"], (
        "the page is about one publisher, not the whole catalogue"
    )
    # Both of EIA's channels are counted, because comparing them is the point.
    assert body["counts"]["by_channel"]["api"] >= 1
    assert body["counts"]["by_channel"]["spreadsheet"] >= 1


def test_an_empty_store_has_no_storage_breakdown(tmp_path):
    assert storage_of(tmp_path / "data") is None, "nothing published, so nothing claimed"


def test_the_regions_are_reported_against_the_published_total(tmp_path):
    dates = weeks(3)
    publish(tmp_path / "data", exact_split(dates), dates, [TOTAL, *REGIONS])
    s = storage_of(tmp_path / "data")
    assert s["week_ending"] == dates[-1].isoformat()
    assert s["total"] == 700.0
    assert [r["label"] for r in s["regions"]] == [label for _, label in STORAGE_REGIONS]
    assert s["sum_of_regions"] == 700.0
    assert s["difference"] == 0.0
    assert s["complete"] is True
    # Shares are of the published total, and add to about 100%.
    assert abs(sum(r["share_pct"] for r in s["regions"]) - 100) < 0.5
    assert s["identity"] == {"weeks": 3, "exact": 3, "worst_difference": 0.0, "unit": "Bcf"}


def test_rounding_is_measured_not_hidden(tmp_path):
    """The real case: every series is rounded independently, so the parts miss the whole.

    Measured over EIA's own history on 2026-10-19, the five regions matched the published
    Lower 48 total in 550 of 876 weeks and were never more than 2 Bcf out. A check with no
    tolerance would fire on a third of all weeks, which is why the gap is reported as a
    measurement rather than as a failure.
    """
    dates = weeks(4)
    values = exact_split(dates)
    # Three weeks where the rounded parts miss the rounded whole, one of them short: the
    # shape the real files have.
    values[REGIONS[0].id] = [101.0, 100.0, 101.0, 99.0]
    publish(tmp_path / "data", values, dates, [TOTAL, *REGIONS])
    s = storage_of(tmp_path / "data")
    assert s["identity"]["weeks"] == 4
    assert s["identity"]["exact"] == 1
    assert s["identity"]["worst_difference"] == 1.0
    # The newest week is the short one, and the page says so rather than rounding it away.
    assert s["difference"] == -1.0
    assert s["sum_of_regions"] == 699.0
    assert s["total"] == 700.0


def test_a_missing_region_does_not_fake_the_identity(tmp_path):
    """With a region unpublished, the sum is of what exists and the page admits it."""
    dates = weeks(2)
    publish(tmp_path / "data", exact_split(dates), dates, [TOTAL, *REGIONS[:-1]])
    s = storage_of(tmp_path / "data")
    assert s["complete"] is False
    assert len(s["regions"]) == len(STORAGE_REGIONS) - 1
    assert s["sum_of_regions"] == 640.0
    assert s["difference"] == -60.0
