"""Seeded defects: synthetic errors injected into a copy of a real series.

Each defect copies a failure mode that real data feeds have:

* ``decimal_shift``: one value multiplied or divided by 10 (a misplaced decimal point);
* ``sign_flip``: one value with its sign reversed;
* ``unit_scale_run``: a run of 5 to 15 values multiplied by 100 (e.g. cents sent as
  dollars for a while);
* ``stale_run``: a run of 3 to 7 values replaced by the last good value (a stuck feed);
* ``small_jump``: one value moved by 8 times the series' median absolute change (an
  error small enough to look plausible).

Every changed date is recorded with its original and corrupted value (the ground-truth
manifest), so detectors can be scored exactly. Injection is deterministic for a given
series id and seed. Defects are placed after a three-year warm-up, so every detector has
history to judge against, and are spaced apart so they do not overlap.
"""

import random
import statistics
from dataclasses import dataclass
from datetime import date, timedelta

from merx.quality import Point

DEFECT_TYPES = ("decimal_shift", "sign_flip", "unit_scale_run", "stale_run", "small_jump")
WARMUP = timedelta(days=3 * 365 + 1)
_SPACING = 30  # points between the end of one defect and the start of the next
_SMALL_JUMP_FACTOR = 8.0
_MAX_RUN = 15
_MIN_SLOT = _SPACING + _MAX_RUN + 1


def max_per_type(points: list[Point], cap: int = 8) -> int:
    """How many defects of each type fit in a series after the warm-up, up to ``cap``."""
    if not points:
        return 0
    eligible = sum(1 for d, _ in points if d >= points[0][0] + WARMUP)
    return min(cap, eligible // _MIN_SLOT // len(DEFECT_TYPES))


@dataclass(frozen=True)
class Defect:
    defect_id: int
    defect_type: str
    # Only dates whose value actually changed (a stale run can repeat an equal value).
    dates: tuple[date, ...]
    original: tuple[float, ...]
    corrupted: tuple[float, ...]


def median_abs_change(values: list[float]) -> float:
    return statistics.median(abs(b - a) for a, b in zip(values, values[1:], strict=False))


def _run_length(defect_type: str, rng: random.Random) -> int:
    if defect_type == "unit_scale_run":
        return rng.randint(5, _MAX_RUN)
    if defect_type == "stale_run":
        return rng.randint(3, 7)
    return 1


def inject(
    points: list[Point], series_id: str, seed: int, per_type: int = 8
) -> tuple[list[Point], list[Defect]]:
    """Return a corrupted copy of ``points`` and the defects injected into it.

    ``points`` must be sorted by date with no missing values.
    """
    if not points:
        return [], []
    rng = random.Random(f"{series_id}:{seed}")
    values = [v for _, v in points]
    dates = [d for d, _ in points]
    jump = _SMALL_JUMP_FACTOR * median_abs_change(values)

    start = next((i for i, d in enumerate(dates) if d >= dates[0] + WARMUP), len(dates))
    plan = [t for t in DEFECT_TYPES for _ in range(per_type)]
    rng.shuffle(plan)
    # Spread the defects over the eligible range: one slot each, a random position in it.
    slot = (len(points) - start) // max(len(plan), 1)
    if slot < _MIN_SLOT:
        raise ValueError(
            f"{series_id}: {len(points) - start} points after warm-up is too few for "
            f"{len(plan)} defects"
        )

    corrupted = list(values)
    defects: list[Defect] = []
    for n, defect_type in enumerate(plan):
        length = _run_length(defect_type, rng)
        i = start + n * slot + rng.randint(1, slot - _SPACING - length)
        idx = list(range(i, i + length))
        match defect_type:
            case "decimal_shift":
                factor = rng.choice((10.0, 0.1))
                new = [values[j] * factor for j in idx]
            case "sign_flip":
                new = [-values[j] for j in idx]
            case "unit_scale_run":
                new = [values[j] * 100.0 for j in idx]
            case "stale_run":
                new = [values[i - 1]] * length
            case "small_jump":
                new = [values[j] + rng.choice((jump, -jump)) for j in idx]
        changed = [(j, v) for j, v in zip(idx, new, strict=True) if v != values[j]]
        if not changed:
            continue
        for j, v in changed:
            corrupted[j] = v
        defects.append(
            Defect(
                defect_id=len(defects),
                defect_type=defect_type,
                dates=tuple(dates[j] for j, _ in changed),
                original=tuple(values[j] for j, _ in changed),
                corrupted=tuple(v for _, v in changed),
            )
        )
    return list(zip(dates, corrupted, strict=True)), defects
