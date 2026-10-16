"""EIA's API as a second channel, and reconciliation of two channels value by value."""

import json
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import dagster as dg
import duckdb
import pytest
from conftest import eia_workbook

from merx.config import CONTRACTS_DIR
from merx.contracts import ContractError, load_contract, validate_reconciliations
from merx.engine import reconcile
from merx.orchestration.factory import build_assets
from merx.orchestration.resources import DataStore, HttpFetcher
from merx.reference import load_reference
from merx.sources import SourceFormatError, eia_api
from merx.sources.http import FetchResult
from merx.store import connect

SECRET = "TESTKEY0123456789"
XLS = load_contract(CONTRACTS_DIR / "eia_wti_spot.yaml")
API = load_contract(CONTRACTS_DIR / "eia_api_wti_spot.yaml")


def api_page(rows: list[tuple[date, float | None]], total: int, series: str = "RWTC") -> dict:
    return {
        "response": {
            "total": str(total),
            "dateFormat": "YYYY-MM-DD",
            "frequency": "daily",
            "data": [
                {
                    "period": d.isoformat(),
                    "series": series,
                    "value": None if v is None else f"{v}",
                    "units": "$/BBL",
                }
                for d, v in rows
            ],
        },
        "request": {"command": "/v2/petroleum/pri/spt/data/", "params": {"api_key": SECRET}},
        "apiVersion": "2.1.14",
    }


class ApiFetcher(HttpFetcher):
    """Serves the spreadsheet from a file and the API from pages of a fixed size."""

    xls_path: str
    api_rows: list[list]  # [iso date, value]
    page_size: int = 3
    polite_delay_s: float = 0.0

    def fetch(self, url: str) -> FetchResult:
        if url == XLS.source.url:
            return FetchResult(url, 200, {}, Path(self.xls_path).read_bytes())
        if url.startswith("https://web.archive.org/"):
            return FetchResult(url, 200, {}, b"[]")
        query = parse_qs(urlparse(url).query)
        assert query["api_key"] == [SECRET]
        offset = int(query["offset"][0])
        rows = [(date.fromisoformat(d), v) for d, v in self.api_rows]
        page = api_page(rows[offset : offset + self.page_size], total=len(rows))
        return FetchResult(url, 200, {}, json.dumps(page).encode())


# --- the API parser -----------------------------------------------------------------


def test_fetch_all_pages_and_removes_the_echoed_key():
    rows = [(date(2020, 4, 16) + timedelta(days=i), 20.0 + i) for i in range(7)]

    def fetch(url: str) -> bytes:
        offset = int(parse_qs(urlparse(url).query)["offset"][0])
        return json.dumps(api_page(rows[offset : offset + 3], total=7)).encode()

    content = eia_api.fetch_all(fetch, API.source.url, "RWTC", "daily", SECRET)
    assert SECRET.encode() not in content
    assert len(json.loads(content)["pages"]) == 3
    assert eia_api.parse(content, "RWTC") == rows


def test_parse_rejects_another_series_a_short_reply_and_mixed_units():
    d = date(2020, 4, 20)
    other = json.dumps({"pages": [api_page([(d, -36.98)], 1, series="RBRTE")]}).encode()
    with pytest.raises(SourceFormatError, match="expected 'RWTC'"):
        eia_api.parse(other, "RWTC")
    short = json.dumps({"pages": [api_page([(d, -36.98)], total=2)]}).encode()
    with pytest.raises(SourceFormatError, match="different total"):
        eia_api.parse(short, "RWTC")
    page = api_page([(d, 1.0), (d + timedelta(days=1), 2.0)], 2)
    page["response"]["data"][1]["units"] = "$/MMBTU"
    with pytest.raises(SourceFormatError, match="mixed units"):
        eia_api.parse(json.dumps({"pages": [page]}).encode(), "RWTC")


def test_negative_price_and_missing_value_survive():
    d = date(2020, 4, 20)
    content = json.dumps({"pages": [api_page([(d, -36.98), (d + timedelta(days=1), None)], 2)]})
    assert eia_api.parse(content.encode(), "RWTC") == [(d, -36.98), (d + timedelta(days=1), None)]


# --- contracts ------------------------------------------------------------------------


def test_reconcile_target_must_exist_and_match():
    with pytest.raises(ContractError, match="unknown dataset"):
        validate_reconciliations([XLS], load_reference())
    brent = load_contract(CONTRACTS_DIR / "eia_brent_spot.yaml")
    renamed = brent.model_copy(update={"id": "eia_api_wti_spot"})
    with pytest.raises(ContractError, match="instrument differs"):
        validate_reconciliations([XLS, renamed], load_reference())
    validate_reconciliations([XLS, API], load_reference())


# --- end to end ----------------------------------------------------------------------


def test_two_channels_reconcile_and_the_key_is_never_stored(tmp_path, monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", SECRET)
    start = date.today() - timedelta(days=10)
    d = [start + timedelta(days=i) for i in range(8)]
    # Spreadsheet: drops d[2], disagrees on d[4], and stops a day before the API.
    xls_points = [(d[0], 60.0), (d[1], 61.0), (d[3], 63.0), (d[4], 64.5), (d[5], 65.0)]
    xls_points += [(d[6], 66.0)]
    api_rows = [[x.isoformat(), 60.0 + i] for i, x in enumerate(d)]
    xls = tmp_path / "wti.xls"
    xls.write_bytes(eia_workbook("RWTC", xls_points))

    reference = load_reference()
    assets = [*build_assets(XLS, reference), *build_assets(API, reference)]
    result = dg.materialize(
        assets,
        resources={
            "store": DataStore(data_dir=str(tmp_path / "data")),
            "fetcher": ApiFetcher(xls_path=str(xls), api_rows=api_rows),
        },
        raise_on_error=False,
    )
    (recon,) = [
        e
        for e in result.get_asset_materialization_events()
        if e.asset_key == dg.AssetKey(["reconcile", "eia_wti_spot"])
    ]
    counts = {k: v.value for k, v in recon.materialization.metadata.items()}
    assert counts == {
        "match": 5,
        "value_differs": 1,
        "only_here": 0,
        "only_there": 1,
        "outside_range": 1,
    }
    con = connect(tmp_path / "data")
    try:
        breaks = con.execute(
            'SELECT valid_date, class FROM recon."eia_wti_spot" ORDER BY 1'
        ).fetchall()
        urls = [u for (u,) in con.execute("SELECT url FROM meta.raw_manifest").fetchall()]
        paths = [p for (p,) in con.execute("SELECT path FROM meta.raw_manifest").fetchall()]
    finally:
        con.close()
    assert breaks == [(d[2], "only_there"), (d[4], "value_differs"), (d[7], "outside_range")]
    assert not any(SECRET in u for u in urls)
    for p in paths:
        assert SECRET.encode() not in (tmp_path / "data" / p).read_bytes()


def test_two_sources_view_groups_every_difference(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from merx.api import create_app

    test_two_channels_reconcile_and_the_key_is_never_stored(tmp_path, monkeypatch)
    client = TestClient(create_app(tmp_path / "data"))
    (pair,) = [p for p in client.get("/api/client/sources").json() if p["id"] == "eia_wti_spot"]
    assert pair["counts"]["match"] == 5 and pair["other"] == "eia_api_wti_spot"
    view = client.get("/api/client/sources/eia_wti_spot").json()
    groups = {g["key"]: g["count"] for g in view["groups"]}
    # No history of earlier downloads here, so the missing date is "only ever in the API".
    assert groups == {"dropped": 0, "api_only": 1, "api_fill": 0, "here_only": 0, "differs": 1}
    assert len(view["here"]) == 6 and len(view["there"]) == 8
    assert client.get("/api/client/sources/eia_us_gas_storage").status_code == 404
    assert "two channels" in client.get("/sources").text


# --- comparing two institutions rather than two channels -------------------------


def monthly_pair(con):
    """A monthly series and a daily one for the same commodity."""
    con.execute("CREATE SCHEMA IF NOT EXISTS staged")
    con.execute(
        'CREATE TABLE staged."monthly" (measure VARCHAR, valid_date DATE, '
        "period INTEGER, value DOUBLE)"
    )
    con.execute(
        'CREATE TABLE staged."daily" (measure VARCHAR, valid_date DATE, '
        "period INTEGER, value DOUBLE)"
    )
    return con


def test_a_monthly_series_is_compared_with_the_mean_of_a_daily_one():
    con = monthly_pair(duckdb.connect())
    con.execute("INSERT INTO staged.\"monthly\" VALUES ('price', '2026-01-01', 0, 10.0)")
    # Twenty days averaging exactly 10.0.
    con.executemany(
        "INSERT INTO staged.\"daily\" VALUES ('price', ?, 0, ?)",
        [[f"2026-01-{d:02d}", 10.0 + (1 if d % 2 else -1)] for d in range(1, 21)],
    )
    sql = reconcile._other_source("daily", "monthly_mean", 15)
    rows = con.execute(sql).fetchall()
    assert rows == [("price", date(2026, 1, 1), 0, 10.0)]
    con.close()


def test_a_thinly_observed_month_is_not_compared_at_all():
    """A month with a handful of prices has no meaningful average. This is not
    hypothetical: 35 months lost enough days to EIA's withdrawn values that they can
    no longer be compared with the World Bank's monthly figure."""
    con = monthly_pair(duckdb.connect())
    con.executemany(
        "INSERT INTO staged.\"daily\" VALUES ('price', ?, 0, 10.0)",
        [[f"2026-01-{d:02d}"] for d in range(1, 12)],  # 11 days
    )
    assert con.execute(reconcile._other_source("daily", "monthly_mean", 15)).fetchall() == []
    # The same month passes once enough days are present.
    con.executemany(
        "INSERT INTO staged.\"daily\" VALUES ('price', ?, 0, 10.0)",
        [[f"2026-01-{d:02d}"] for d in range(12, 20)],
    )
    assert len(con.execute(reconcile._other_source("daily", "monthly_mean", 15)).fetchall()) == 1
    con.close()


def test_exact_alignment_is_unchanged():
    con = monthly_pair(duckdb.connect())
    con.execute("INSERT INTO staged.\"daily\" VALUES ('price', '2026-01-02', 0, 7.0)")
    rows = con.execute(reconcile._other_source("daily", "exact", 15)).fetchall()
    assert rows == [("price", date(2026, 1, 2), 0, 7.0)]
    con.close()
