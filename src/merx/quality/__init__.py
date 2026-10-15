"""The data-quality lab: seed known defects into real series, run detectors, score them.

Everything here is a plain function over ``(valid_date, value)`` points so it can be
tested on tiny hand-made series. ``merx.orchestration.lab`` runs it on the staged data.

Series in the lab are real; the defects are synthetic and recorded in a ground-truth
manifest, so every flag can be scored.
"""

from datetime import date

Point = tuple[date, float]
