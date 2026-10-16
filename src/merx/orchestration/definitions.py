"""The Dagster entry point (``dagster dev`` loads ``defs`` from here; see pyproject.toml).

Every contract in ``contracts/`` is loaded and validated against ``reference/`` at import
time, so a broken contract shows up as a load error in the UI rather than at 18:30 on a
schedule.
"""

from pathlib import Path

import dagster as dg

from merx.config import data_dir as default_data_dir
from merx.config import load_dotenv
from merx.contracts import load_contracts
from merx.orchestration.factory import build_assets, build_schedules
from merx.orchestration.lab import build_lab_assets
from merx.orchestration.resources import DataStore, HttpFetcher
from merx.reference import load_reference


def build_definitions(
    data_dir: Path | None = None, fetcher: HttpFetcher | None = None
) -> dg.Definitions:
    load_dotenv()
    reference = load_reference()
    contracts = load_contracts(reference)
    jobs, schedules = build_schedules(contracts)
    return dg.Definitions(
        assets=[
            *(a for c in contracts for a in build_assets(c, reference)),
            *build_lab_assets(contracts),
        ],
        jobs=jobs,
        schedules=schedules,
        resources={
            "store": DataStore(data_dir=str(data_dir or default_data_dir())),
            "fetcher": fetcher or HttpFetcher(),
        },
        # DuckDB allows one writing process per file, so a run executes its steps one
        # after another in a single process.
        executor=dg.in_process_executor,
    )


defs = build_definitions()
