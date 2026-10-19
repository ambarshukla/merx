"""The consoles' API: ops sees everything; the client sees only published data."""

from datetime import date, datetime, timedelta
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
    # The rows on display passed every check; what is held back is a different build,
    # so the two sets of reasons are reported separately.
    assert wti["quality_reasons"] == []
    assert [r["check"] for r in wti["latest_attempt_reasons"]] == ["no_withdrawals"]
    assert "1 previously known value" in wti["latest_attempt_reasons"][0]["description"]
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


def test_a_warning_says_which_check_caused_it(tmp_path):
    """A client seeing "warn" needs to know what the warning is about."""
    from test_publish import FUTURES, run_all

    stale = [(date(2024, 4, 1) + timedelta(days=i), 80.0 + i) for i in range(4)]
    run_all(tmp_path, FUTURES, stale, "RCLC1")
    client = TestClient(create_app(tmp_path / "data"))
    (futures,) = client.get("/api/client/datasets").json()
    assert futures["quality_status"] == "warn"
    (reason,) = futures["quality_reasons"]
    assert reason["check"] == "freshness"
    assert "days old" in reason["description"]


def test_a_passing_publication_gives_no_reasons(client):
    (wti,) = client.get("/api/client/datasets").json()
    assert (wti["quality_status"], wti["quality_reasons"]) == ("pass", [])


def test_provenance_describes_the_source_and_the_files_kept(client):
    """The client page answers "where did this number come from" without a human."""
    p = client.get("/api/client/datasets/eia_wti_spot/provenance").json()
    assert p["provider"]["name"] == "U.S. Energy Information Administration"
    assert p["provider"]["terms_url"].startswith("https://")
    assert p["provider"]["attribution"]
    assert p["request"]["url"] == WTI.source.url
    assert p["request"]["series_key"] == "RWTC"
    assert p["request"]["format"] == "Excel spreadsheet (one series per file)"
    assert p["request"]["partitioned_by"] is None
    # The raw zone is the evidence, so the page can show it rather than assert lineage.
    assert p["files"]["count"] == 1
    (newest,) = p["files"]["newest"]
    assert newest["name"].endswith(".xls")
    assert len(newest["sha256_short"]) == 16
    assert newest["origin"] == "direct"
    assert client.get("/api/client/datasets/eia_us_gas_storage/provenance").status_code == 404


def test_provenance_keeps_a_partitioned_url_as_a_template(tmp_path):
    """No single URL covers a partitioned dataset, so the placeholder is the answer."""
    from merx.api import data
    from merx.contracts import load_contract
    from merx.reference import load_reference

    cftc = load_contract(CONTRACTS_DIR / "cftc_wti_positions.yaml")
    p = data.provenance(data.Store(tmp_path), cftc, load_reference())
    assert "{year}" in p["request"]["url"]
    assert (p["request"]["partitioned_by"], p["request"]["partitions_from"]) == ("year", "2010")
    assert p["provider"]["name"] == "U.S. Commodity Futures Trading Commission"
    # Nothing has been downloaded into this empty store, and that reads as zero rather
    # than as a missing key.
    assert p["files"] == {"count": 0, "bytes": 0, "first_utc": None, "by_origin": {}, "newest": []}


def test_the_schedule_is_given_in_words(client):
    p = client.get("/api/client/datasets/eia_wti_spot/provenance").json()
    assert p["schedule"]["in_words"] == "every weekday at 18:30 America/New_York"
    assert p["schedule"]["cron"] == WTI.schedule.cron


def test_pages_and_assets_are_served(client):
    # Each page is identified by its title rather than by a tagline in the header: the
    # headers now carry only the name and the tabs.
    for path, title in (
        ("/client", "merx \u00b7 data"),
        ("/sources", "merx \u00b7 two sources"),
        ("/eia", "merx \u00b7 EIA"),
        ("/ops", "merx \u00b7 operations"),
    ):
        assert f"<title>{title}</title>" in client.get(path).text, path
    assert "lineChart" in client.get("/static/chart.js").text
    assert client.get("/", follow_redirects=False).status_code in (302, 307)


def test_an_empty_data_directory_is_read_without_being_created(tmp_path):
    client = TestClient(create_app(tmp_path / "nothing"))
    assert client.get("/api/client/datasets").json() == []
    assert client.get("/api/ops/exceptions").json() == []
    assert not (tmp_path / "nothing").exists()


def test_pages_answer_head_requests_too(client):
    # Link previews and uptime monitors check a page with HEAD before fetching it.
    for path in ("/client", "/sources", "/eia", "/ops"):
        assert client.head(path).status_code == 200


def test_a_daily_series_published_before_period_existed_still_reads(tmp_path):
    """A table published by an older version of the code has no period column, and
    never will: a blocked dataset keeps its last good publication indefinitely, so the
    gate will not replace it. Reading a daily series must therefore not mention period.

    This shipped broken. Eleven of twenty-nine series returned 500 in production while
    every check still said "pass", because the status came from tables that had been
    rebuilt and the chart came from one that had not."""
    import duckdb

    from merx.api import data
    from merx.config import CONTRACTS_DIR
    from merx.contracts import load_contract
    from merx.store import DB_FILE

    contract = load_contract(CONTRACTS_DIR / "eia_wti_spot.yaml")
    con = duckdb.connect(str(tmp_path / DB_FILE))
    con.execute("CREATE SCHEMA published")
    # Exactly the old shape: no period, no valid_time_utc.
    con.execute(
        'CREATE TABLE published."eia_wti_spot" '
        "(measure VARCHAR, valid_date DATE, value DOUBLE, quality_status VARCHAR)"
    )
    con.execute(
        'INSERT INTO published."eia_wti_spot" VALUES '
        "('price', '2026-10-13', 100.0, 'pass'), ('price', '2026-10-14', 101.0, 'pass')"
    )
    con.close()

    store = data.Store(tmp_path)
    points = data.published_series(store, contract, "price", None, None)
    assert points == [["2026-10-13", 100.0], ["2026-10-14", 101.0]]


def old_shape_database(tmp_path):
    """A database as an earlier version of the engine left it: no period anywhere, and
    no valid_time_utc. The website opens the database read-only, so it cannot migrate
    its way out of this — it has to read what is there."""
    import duckdb

    from merx.store import DB_FILE

    con = duckdb.connect(str(tmp_path / DB_FILE))
    con.execute("CREATE SCHEMA staged; CREATE SCHEMA pit; CREATE SCHEMA ops")
    con.execute(
        'CREATE TABLE staged."eia_wti_spot" (measure VARCHAR, valid_date DATE, value DOUBLE)'
    )
    con.execute("INSERT INTO staged.\"eia_wti_spot\" VALUES ('price', '2026-10-14', 101.0)")
    con.execute(
        'CREATE TABLE pit."eia_wti_spot" (measure VARCHAR, valid_date DATE, value DOUBLE, '
        "known_from_utc TIMESTAMP, known_to_utc TIMESTAMP, ended_by VARCHAR)"
    )
    con.execute(
        'INSERT INTO pit."eia_wti_spot" VALUES '
        "('price', '2026-10-13', 100.0, '2026-10-13 10:00:00', NULL, NULL), "
        "('price', '2026-10-14', 101.0, '2026-10-14 10:00:00', NULL, NULL)"
    )
    con.execute(
        "CREATE TABLE ops.accepted_withdrawals (dataset_id VARCHAR, measure VARCHAR, "
        "valid_date DATE, reason VARCHAR, accepted_at_utc TIMESTAMP)"
    )
    con.close()


def wti_contract():
    from merx.config import CONTRACTS_DIR
    from merx.contracts import load_contract

    return load_contract(CONTRACTS_DIR / "eia_wti_spot.yaml")


def test_missing_reads_a_history_written_before_period_existed(tmp_path):
    """This returned 500 on the live site for every dataset, taking the whole operations
    detail panel with it."""
    from merx.api import data

    old_shape_database(tmp_path)
    result = data.missing(data.Store(tmp_path), wti_contract())
    # 2026-10-13 is in the history and not in the staged build: one missing value.
    assert result["missing"] == 1
    assert result["accepted"] == 0
    assert result["examples"][0][:2] == ["price", "2026-10-13"]


def test_as_known_at_reads_a_history_without_period_or_time(tmp_path):
    from datetime import datetime

    from merx.api import data

    old_shape_database(tmp_path)
    points = data.series_as_known_at(
        data.Store(tmp_path), wti_contract(), "price", datetime(2026, 10, 15)
    )
    assert points == [["2026-10-13", 100.0], ["2026-10-14", 101.0]]


def test_a_sub_daily_history_is_dated_by_the_period_start(tmp_path):
    """Half-hourly data needs a time on the x axis: 48 points a day all dated with the
    same date would draw as a vertical line. The history stores the period's start for
    exactly this."""
    import duckdb

    from merx.api import data
    from merx.config import CONTRACTS_DIR
    from merx.contracts import load_contract
    from merx.store import DB_FILE

    con = duckdb.connect(str(tmp_path / DB_FILE))
    con.execute("CREATE SCHEMA pit")
    con.execute(
        'CREATE TABLE pit."elexon_gb_power_price" (measure VARCHAR, valid_date DATE, '
        "period INTEGER, valid_time_utc TIMESTAMP, value DOUBLE, "
        "known_from_utc TIMESTAMP, known_to_utc TIMESTAMP)"
    )
    con.execute(
        'INSERT INTO pit."elexon_gb_power_price" VALUES '
        "('price', '2026-10-15', 2, '2026-10-14 23:30:00', 88.0, '2026-10-16 06:00:00', NULL), "
        "('price', '2026-10-15', 1, '2026-10-14 23:00:00', 87.0, '2026-10-16 06:00:00', NULL)"
    )
    con.close()

    contract = load_contract(CONTRACTS_DIR / "elexon_gb_power_price.yaml")
    points = data.series_as_known_at(
        data.Store(tmp_path), contract, "price", datetime(2026, 10, 17)
    )
    # Ordered by period, and dated by the half hour rather than the day.
    assert points == [["2026-10-14T23:00", 87.0], ["2026-10-14T23:30", 88.0]]
