"""The quality lab as a Dagster asset: the detector bake-off over every staged series."""

import dagster as dg

from merx.contracts import Contract
from merx.orchestration.factory import staged_key
from merx.orchestration.resources import DataStore
from merx.quality.lab import lab_detectors, load_series, persist, run_bakeoff
from merx.store import connect

BAKEOFF_KEY = dg.AssetKey(["lab", "detector_bakeoff"])


def build_lab_assets(contracts: list[Contract]) -> list[dg.AssetsDefinition]:
    @dg.asset(
        key=BAKEOFF_KEY,
        deps=[staged_key(c) for c in contracts],
        group_name="quality_lab",
        description=(
            "Seeds synthetic defects into copies of every staged series, runs three "
            "detectors, and scores them against the ground truth. Writes lab.* tables."
        ),
        kinds={"duckdb"},
    )
    def detector_bakeoff(
        context: dg.AssetExecutionContext, store: DataStore
    ) -> dg.MaterializeResult:
        con = connect(store.path)
        try:
            series = load_series(con, contracts)
            result = run_bakeoff(series, lab_detectors())
            persist(con, result)
            headline = con.execute(
                """
                SELECT detector_id, sum(caught) / sum(defects),
                       sum(true_flags) / nullif(sum(true_flags + induced_false_flags), 0)
                FROM lab.scores WHERE primary_setting GROUP BY 1 ORDER BY 1
                """
            ).fetchall()
        finally:
            con.close()
        metadata: dict = {
            "series": len(series),
            "seeded_defect_dates": len(result.defects),
            "flags": len(result.flags),
        }
        for detector_id, recall, precision in headline:
            metadata[f"{detector_id} recall"] = round(recall, 3)
            metadata[f"{detector_id} precision"] = (
                None if precision is None else round(precision, 3)
            )
        context.log.info(f"bake-off over {len(series)} series")
        return dg.MaterializeResult(metadata=metadata)

    return [detector_bakeoff]
