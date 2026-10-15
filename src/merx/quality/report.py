"""Summarise the bake-off from the ``lab`` tables as Markdown.

Run ``python -m merx.quality.report`` after the lab assets have been materialised.
"""

import duckdb

from merx.config import data_dir
from merx.store import connect


def _pct(x: float | None) -> str:
    return "–" if x is None else f"{x:.0%}"


def _table(header: list[str], rows: list[list[str]]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(lines)


_TOTALS = """
SELECT s.detector_id,
       sum(caught) / sum(defects)                                  AS recall,
       min(caught * 1.0 / defects)                                 AS recall_min,
       max(caught * 1.0 / defects)                                 AS recall_max,
       sum(true_flags) / nullif(sum(true_flags + induced_false_flags), 0) AS precision,
       sum(induced_false_flags)                                    AS induced,
       a.alerts_per_year
FROM lab.scores s
JOIN (SELECT detector_id, sum(flags) / sum(years) AS alerts_per_year
      FROM lab.real_data_alerts GROUP BY 1) a USING (detector_id)
WHERE {where}
GROUP BY s.detector_id, a.alerts_per_year
ORDER BY s.detector_id
"""


def render(con: duckdb.DuckDBPyConnection) -> str:
    parts: list[str] = []

    seeds, series = con.execute(
        "SELECT count(DISTINCT seed), count(DISTINCT series_id) FROM lab.scores"
    ).fetchone()
    defects = con.execute(
        "SELECT count(*) FROM (SELECT DISTINCT series_id, seed, defect_id FROM lab.defects)"
    ).fetchone()[0]
    parts.append(
        f"{series} real series, {seeds} seeds, {defects} seeded defects in total. "
        "Recall: share of seeded defects with at least one flag (lowest and highest "
        "single run in brackets). Precision: share of flags that point at a seeded "
        "defect, leaving aside flags the detector also raises on the untouched series. "
        "Alerts per year: flags on the untouched real series, per series-year."
    )

    def totals(where: str) -> list[list[str]]:
        return [
            [
                det,
                f"{_pct(r)} ({_pct(lo)}–{_pct(hi)})",
                _pct(p),
                f"{induced:,}",
                f"{apy:.1f}",
            ]
            for det, r, lo, hi, p, induced, apy in con.execute(
                _TOTALS.format(where=where)
            ).fetchall()
        ]

    header = ["Detector", "Recall", "Precision", "False flags caused", "Alerts/yr on real data"]
    parts.append("### Headline (default settings)\n\n" + _table(header, totals("primary_setting")))

    types = con.execute(
        """
        SELECT defect_type, detector_id, sum(caught) / sum(total)
        FROM lab.scores_by_type WHERE primary_setting
        GROUP BY 1, 2 ORDER BY 1, 2
        """
    ).fetchall()
    detectors = sorted({d for _, d, _ in types})
    by_type: dict[str, dict[str, float]] = {}
    for t, d, r in types:
        by_type.setdefault(t, {})[d] = r
    parts.append(
        "### Recall by defect type (default settings)\n\n"
        + _table(
            ["Defect type", *detectors],
            [[t, *(_pct(by_type[t].get(d)) for d in detectors)] for t in sorted(by_type)],
        )
    )

    per_series = con.execute(
        """
        SELECT s.series_id, s.detector_id,
               sum(caught) / sum(defects),
               sum(true_flags) / nullif(sum(true_flags + induced_false_flags), 0),
               max(a.flags / a.years)
        FROM lab.scores s
        JOIN lab.real_data_alerts a USING (detector_id, series_id)
        WHERE s.primary_setting
        GROUP BY 1, 2 ORDER BY 1, 2
        """
    ).fetchall()
    parts.append(
        "### By series (default settings): recall / precision / alerts per year\n\n"
        + _table(
            ["Series", *detectors],
            [
                [sid, *(cells[d] for d in detectors)]
                for sid, cells in _group_cells(per_series).items()
            ],
        )
    )

    parts.append("### Threshold sweep\n\n" + _table(header, totals("true")))
    return "\n\n".join(parts) + "\n"


def _group_cells(rows) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for sid, det, r, p, apy in rows:
        out.setdefault(sid, {})[det] = f"{_pct(r)} / {_pct(p)} / {apy:.1f}"
    return out


if __name__ == "__main__":
    con = connect(data_dir())
    try:
        print(render(con))
    finally:
        con.close()
