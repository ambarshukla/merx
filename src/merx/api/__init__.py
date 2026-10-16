"""The two consoles, served by one small web app.

* ``/ops``: for the people running the data. Status of every dataset, its checks, open
  exceptions, values gone missing, reconciliation breaks, and the detector scores.
* ``/client``: for people using the data. Only the published layer, each series with its
  quality status, its "as known on" history and its revisions.

The JSON behind both lives under ``/api``. Everything is read-only: changing anything
(accepting a loss, assigning a root cause) is done with the ``merx`` command, which
records who decided what. Run with ``merx serve``.
"""

from datetime import date, datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from merx.api import data
from merx.config import data_dir as default_data_dir
from merx.contracts import Contract, load_contracts
from merx.reference import load_reference

STATIC = Path(__file__).parent / "static"


def create_app(data_dir: Path | None = None) -> FastAPI:
    store = data.Store(data_dir or default_data_dir())
    contracts = load_contracts(load_reference())
    by_id = {c.id: c for c in contracts}
    app = FastAPI(title="merx", docs_url="/api/docs", openapi_url="/api/openapi.json")

    def contract(dataset: str) -> Contract:
        if dataset not in by_id:
            raise HTTPException(404, f"unknown dataset {dataset}")
        return by_id[dataset]

    def published(dataset: str) -> Contract:
        c = contract(dataset)
        if not store.has("published", c.id):
            raise HTTPException(404, f"{dataset} has not been published")
        return c

    def measure_of(c: Contract, measure: str | None) -> str:
        m = measure or c.measures[0]
        if m not in c.measures:
            raise HTTPException(404, f"{c.id} has no measure {m}")
        return m

    # --- pages --------------------------------------------------------------------
    @app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
    def index():
        return RedirectResponse("/client")

    @app.api_route("/ops", methods=["GET", "HEAD"], include_in_schema=False)
    def ops_page():
        return FileResponse(STATIC / "ops.html")

    @app.api_route("/client", methods=["GET", "HEAD"], include_in_schema=False)
    def client_page():
        return FileResponse(STATIC / "client.html")

    @app.api_route("/sources", methods=["GET", "HEAD"], include_in_schema=False)
    def sources_page():
        return FileResponse(STATIC / "sources.html")

    app.mount("/static", StaticFiles(directory=STATIC), name="static")

    # --- ops ----------------------------------------------------------------------
    @app.get("/api/ops/datasets")
    def ops_datasets():
        return data.dataset_status(store, contracts)

    @app.get("/api/ops/exceptions")
    def ops_exceptions():
        return data.exceptions(store)

    @app.get("/api/ops/datasets/{dataset}")
    def ops_dataset(dataset: str):
        c = contract(dataset)
        return {
            "checks": data.checks(store, c),
            "missing": data.missing(store, c),
            "reconciliation": data.reconciliation(store, c),
        }

    @app.get("/api/ops/lab")
    def ops_lab():
        return data.lab_headline(store)

    # --- client: published data only ----------------------------------------------
    @app.get("/api/client/datasets")
    def client_datasets():
        return data.published_catalog(store, contracts)

    @app.get("/api/client/series/{dataset}")
    def client_series(
        dataset: str,
        measure: str | None = None,
        start: date | None = None,
        end: date | None = None,
    ):
        c = published(dataset)
        m = measure_of(c, measure)
        return {
            "dataset": c.id,
            "measure": m,
            "unit": c.unit,
            "points": data.published_series(store, c, m, start, end),
        }

    @app.get("/api/client/series/{dataset}/as-known-at")
    def client_as_known_at(dataset: str, at: datetime, measure: str | None = None):
        c = published(dataset)
        m = measure_of(c, measure)
        return {
            "dataset": c.id,
            "measure": m,
            "at": at.isoformat(),
            "basis": "Every version the history holds; archived copies count from their "
            "capture time, an upper bound on when they were public",
            "points": data.series_as_known_at(store, c, m, at),
        }

    @app.get("/api/client/sources")
    def client_sources():
        return data.reconciled_pairs(store, contracts)

    @app.get("/api/client/sources/{dataset}")
    def client_source(dataset: str):
        c = contract(dataset)
        if c.reconcile_with is None:
            raise HTTPException(404, f"{dataset} is not reconciled with a second channel")
        return data.two_sources(store, c, by_id[c.reconcile_with.dataset])

    @app.get("/api/client/series/{dataset}/revisions")
    def client_revisions(dataset: str, measure: str | None = None):
        c = published(dataset)
        return data.revisions(store, c, measure_of(c, measure))

    return app
