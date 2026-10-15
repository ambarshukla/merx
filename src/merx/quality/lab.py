"""Run the detector bake-off over staged series and store every result in DuckDB.

Tables written (schema ``lab``, each replaced on every run):

* ``lab.defects``: the ground-truth manifest, one row per corrupted date;
* ``lab.flags``: every flag raised, on corrupted runs (seed >= 0) and on the untouched
  series (seed = -1);
* ``lab.scores``: one row per (detector, series, seed);
* ``lab.scores_by_type``: caught / total per defect type;
* ``lab.real_data_alerts``: flag counts on the untouched series, with the span in years.
"""

from dataclasses import dataclass

import duckdb
import pyarrow as pa

from merx.contracts import Contract
from merx.engine.stage import staged_table
from merx.quality import Point
from merx.quality.defects import inject, max_per_type
from merx.quality.detectors import Detector, RepeatRun, RobustZ, Seasonal, StaticBand, Suite
from merx.quality.scoring import caught_by_type, score

DEFAULT_SEEDS = (0, 1, 2, 3, 4)


@dataclass(frozen=True)
class LabSeries:
    series_id: str
    dataset_id: str
    measure: str
    frequency: str
    points: list[Point]


@dataclass(frozen=True)
class LabDetector:
    detector_id: str  # e.g. "robust_z k=5"
    detector: Detector
    primary: bool  # the default settings, as opposed to the threshold sweep


def lab_detectors() -> list[LabDetector]:
    """Every detector the lab scores.

    * the three detectors as first designed (v1), at their default thresholds;
    * v2 of the two robust detectors, with the two fixes the first run showed were
      needed (recovery after an adopted shift; unchanged values kept out of the
      yardstick), plus a threshold sweep around each v2;
    * ``repeat_run`` for stuck feeds, which no change-based detector can see;
    * two suites: robust z v2 or seasonal v2, each with repeat_run, because the lab
      shows the right detector depends on the series.
    """
    v2 = {"recover": True, "ignore_zero_changes": True}
    out = [
        LabDetector("static_band 20%", StaticBand(threshold=0.20), True),
        LabDetector("robust_z v1 k=5", RobustZ(k=5.0), True),
        LabDetector("seasonal v1 k=5", Seasonal(k=5.0), True),
        LabDetector("repeat_run", RepeatRun(), True),
        LabDetector("suite robust", Suite((RobustZ(k=5.0, **v2), RepeatRun())), True),
        LabDetector("suite seasonal", Suite((Seasonal(k=5.0, **v2), RepeatRun())), True),
    ]
    for t in (0.10, 0.40):
        out.append(LabDetector(f"static_band {t:.0%}", StaticBand(threshold=t), False))
    for k in (3.5, 5.0, 8.0):
        out.append(LabDetector(f"robust_z v2 k={k:g}", RobustZ(k=k, **v2), k == 5.0))
        out.append(LabDetector(f"seasonal v2 k={k:g}", Seasonal(k=k, **v2), k == 5.0))
    return out


def load_series(con: duckdb.DuckDBPyConnection, contracts: list[Contract]) -> list[LabSeries]:
    out = []
    for contract in contracts:
        rows = con.execute(
            f"SELECT measure, valid_date, value FROM {staged_table(contract)} "
            "WHERE value IS NOT NULL ORDER BY measure, valid_date"
        ).fetchall()
        by_measure: dict[str, list[Point]] = {}
        for measure, d, v in rows:
            by_measure.setdefault(measure, []).append((d, v))
        for measure, points in by_measure.items():
            out.append(
                LabSeries(
                    series_id=f"{contract.id}.{measure}",
                    dataset_id=contract.id,
                    measure=measure,
                    frequency=contract.frequency,
                    points=points,
                )
            )
    return out


@dataclass
class LabResult:
    defects: list[tuple]
    flags: list[tuple]
    scores: list[tuple]
    scores_by_type: list[tuple]
    real_data_alerts: list[tuple]


def run_bakeoff(
    series: list[LabSeries],
    detectors: list[LabDetector],
    seeds: tuple[int, ...] = DEFAULT_SEEDS,
) -> LabResult:
    result = LabResult([], [], [], [], [])
    for s in series:
        per_type = max_per_type(s.points)
        if per_type == 0:
            continue
        years = (s.points[-1][0] - s.points[0][0]).days / 365.25
        clean_flags = {}
        for ld in detectors:
            flags = ld.detector.detect(s.points, s.frequency)
            clean_flags[ld.detector_id] = flags
            result.flags.extend((ld.detector_id, s.series_id, -1, d) for d in sorted(flags))
            result.real_data_alerts.append(
                (ld.detector_id, ld.primary, s.series_id, len(flags), round(years, 2))
            )
        for seed in seeds:
            corrupted, defects = inject(s.points, s.series_id, seed, per_type=per_type)
            for defect in defects:
                for d, o, c in zip(defect.dates, defect.original, defect.corrupted, strict=True):
                    result.defects.append(
                        (s.series_id, seed, defect.defect_id, defect.defect_type, d, o, c)
                    )
            for ld in detectors:
                flags = ld.detector.detect(corrupted, s.frequency)
                result.flags.extend((ld.detector_id, s.series_id, seed, d) for d in sorted(flags))
                sc = score(flags, clean_flags[ld.detector_id], defects)
                result.scores.append(
                    (
                        ld.detector_id,
                        ld.primary,
                        s.series_id,
                        s.frequency,
                        seed,
                        sc.defects,
                        sc.caught,
                        sc.flags,
                        sc.true_flags,
                        sc.induced_false_flags,
                        sc.real_data_flags_seen,
                    )
                )
                for defect_type, (caught, total) in caught_by_type(flags, defects).items():
                    result.scores_by_type.append(
                        (ld.detector_id, ld.primary, s.series_id, seed, defect_type, caught, total)
                    )
    return result


_SCHEMAS = {
    "defects": [
        ("series_id", pa.string()),
        ("seed", pa.int32()),
        ("defect_id", pa.int32()),
        ("defect_type", pa.string()),
        ("valid_date", pa.date32()),
        ("original", pa.float64()),
        ("corrupted", pa.float64()),
    ],
    "flags": [
        ("detector_id", pa.string()),
        ("series_id", pa.string()),
        ("seed", pa.int32()),
        ("valid_date", pa.date32()),
    ],
    "scores": [
        ("detector_id", pa.string()),
        ("primary_setting", pa.bool_()),
        ("series_id", pa.string()),
        ("frequency", pa.string()),
        ("seed", pa.int32()),
        ("defects", pa.int32()),
        ("caught", pa.int32()),
        ("flags", pa.int32()),
        ("true_flags", pa.int32()),
        ("induced_false_flags", pa.int32()),
        ("real_data_flags_seen", pa.int32()),
    ],
    "scores_by_type": [
        ("detector_id", pa.string()),
        ("primary_setting", pa.bool_()),
        ("series_id", pa.string()),
        ("seed", pa.int32()),
        ("defect_type", pa.string()),
        ("caught", pa.int32()),
        ("total", pa.int32()),
    ],
    "real_data_alerts": [
        ("detector_id", pa.string()),
        ("primary_setting", pa.bool_()),
        ("series_id", pa.string()),
        ("flags", pa.int32()),
        ("years", pa.float64()),
    ],
}


def persist(con: duckdb.DuckDBPyConnection, result: LabResult) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS lab")
    for name, fields in _SCHEMAS.items():
        rows = getattr(result, name)
        table = pa.table(
            {col: pa.array([r[i] for r in rows], typ) for i, (col, typ) in enumerate(fields)}
        )
        con.register("incoming", table)
        con.execute(f"CREATE OR REPLACE TABLE lab.{name} AS SELECT * FROM incoming")
        con.unregister("incoming")
