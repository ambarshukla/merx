"""Three ways to flag a suspicious value, from crude to better.

All three judge each value when it arrives, using only earlier values (no peeking at
the future), as an operations team would have to.

* ``static_band``: flag a move of more than a fixed share (default 20%) from the
  previous value. The common hand-written rule.
* ``robust_z``: measure the change from the last *accepted* value in units of the
  typical change, using the median and MAD (median absolute deviation) of the last 60
  accepted changes. One extreme value cannot distort the yardstick.
* ``seasonal``: compare the change with changes at the same time of year in up to five
  earlier years, in units of their MAD (never less than the recent spread of changes).
  Built for series with a yearly rhythm, such as gas in storage.

The two robust detectors quarantine what they flag: a flagged value does not become the
new reference and does not enter the yardstick, so one bad value is flagged once, not
again when the series returns to normal. After ``reset_after`` flags in a row they
accept the new level, so a genuine regime change does not alarm for ever.
"""

import statistics
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta

from merx.quality import Point

_MAD_TO_SIGMA = 1.4826  # makes MAD comparable to a standard deviation for normal data


@dataclass(frozen=True)
class StaticBand:
    threshold: float = 0.20
    name: str = "static_band"

    def detect(self, points: list[Point], frequency: str) -> set[date]:
        flagged = set()
        for (_, prev), (d, x) in zip(points, points[1:], strict=False):
            if prev == 0:
                if x != 0:
                    flagged.add(d)
            elif abs(x / prev - 1) > self.threshold:
                flagged.add(d)
        return flagged


def _mad_scale(values: list[float]) -> float:
    center = statistics.median(values)
    return _MAD_TO_SIGMA * statistics.median(abs(v - center) for v in values)


def _z(delta: float, history: list[float], floor: float = 0.0) -> float:
    """How many typical deviations ``delta`` lies from the centre of ``history``.

    ``floor`` is a minimum for the yardstick: a handful of near-identical values would
    otherwise give a spread of zero and make every difference look infinite.
    """
    center = statistics.median(history)
    scale = max(_mad_scale(history), floor)
    if scale == 0:
        return 0.0 if delta == center else float("inf")
    return abs(delta - center) / scale


def _quarantine_scan(
    points: list[Point],
    is_anomalous: Callable[[date, float, deque[float]], bool],
    *,
    window: int,
    reset_after: int,
    recover: bool,
    ignore_zero_changes: bool,
) -> set[date]:
    """The loop shared by the robust detectors.

    Each value is judged by its change from the last accepted value. Flagged values are
    quarantined (they move neither the reference nor the yardstick). After
    ``reset_after`` flags in a row the new level is adopted.

    Two refinements, both off in the first version and found necessary by the lab:

    * ``recover``: after adopting a new level, a value that is normal relative to the
      level *before* the shift is accepted as a recovery, not flagged as a new jump.
      Without it, an error lasting longer than ``reset_after`` is adopted and the true
      recovery is flagged instead.
    * ``ignore_zero_changes``: an unchanged value does not enter the yardstick, so a
      stuck feed cannot shrink it and make ordinary moves look extreme afterwards.
    """
    flagged: set[date] = set()
    if not points:
        return flagged
    accepted: deque[float] = deque(maxlen=window)
    ref = points[0][1]
    baseline: float | None = None  # the level before the last adopted shift
    baseline_age = 0
    run = 0
    for d, x in points[1:]:
        delta = x - ref
        anomalous = is_anomalous(d, delta, accepted)
        recovered = (
            anomalous
            and recover
            and baseline is not None
            and not is_anomalous(d, x - baseline, accepted)
        )
        if recovered:
            anomalous, baseline = False, None
        if anomalous:
            flagged.add(d)
            run += 1
            if run >= reset_after:
                baseline, baseline_age = ref, 0
                ref, run = x, 0
        else:
            if not (ignore_zero_changes and delta == 0):
                accepted.append(delta)
            ref, run = x, 0
            if baseline is not None:
                baseline_age += 1
                if baseline_age > window:
                    baseline = None
    return flagged


@dataclass(frozen=True)
class RobustZ:
    k: float = 5.0
    window: int = 60
    min_history: int = 20
    reset_after: int = 10
    recover: bool = False
    ignore_zero_changes: bool = False
    name: str = "robust_z"

    def detect(self, points: list[Point], frequency: str) -> set[date]:
        def is_anomalous(d: date, delta: float, accepted: deque[float]) -> bool:
            return len(accepted) >= self.min_history and _z(delta, list(accepted)) > self.k

        return _quarantine_scan(
            points,
            is_anomalous,
            window=self.window,
            reset_after=self.reset_after,
            recover=self.recover,
            ignore_zero_changes=self.ignore_zero_changes,
        )


@dataclass(frozen=True)
class Seasonal:
    """Compare each change with changes at the same time of year in earlier years.

    Daily series: every change within a week either side of the same calendar date, in
    each of up to ``years`` earlier years. Weekly and monthly series: the single nearest
    observation to the same date in each earlier year, because a neighbouring week can
    be a different part of the season. The yardstick never drops below the spread of the
    last ``floor_window`` accepted changes.
    """

    k: float = 5.0
    years: int = 5
    min_years: int = 2
    floor_window: int = 52
    reset_after: int = 10
    recover: bool = False
    ignore_zero_changes: bool = False
    name: str = "seasonal"

    def _history(self, d: date, changes: dict[date, float], frequency: str) -> list[float]:
        history: list[float] = []
        for back in range(1, self.years + 1):
            try:
                anchor = d.replace(year=d.year - back)
            except ValueError:  # 29 February
                anchor = d.replace(year=d.year - back, day=28)
            if frequency == "daily":
                history.extend(
                    changes[c]
                    for offset in range(-7, 8)
                    if (c := anchor + timedelta(days=offset)) in changes
                )
            else:
                reach = 3 if frequency == "weekly" else 15
                for offset in sorted(range(-reach, reach + 1), key=abs):
                    if (c := anchor + timedelta(days=offset)) in changes:
                        history.append(changes[c])
                        break
        return history

    def detect(self, points: list[Point], frequency: str) -> set[date]:
        flagged: set[date] = set()
        if not points:
            return flagged
        # Changes between consecutive observations, keyed by the later date.
        changes = {d: x - prev for (_, prev), (d, x) in zip(points, points[1:], strict=False)}
        min_samples = self.min_years * (8 if frequency == "daily" else 1)
        histories: dict[date, list[float]] = {}

        def is_anomalous(d: date, delta: float, accepted: deque[float]) -> bool:
            if d not in histories:
                histories[d] = self._history(d, changes, frequency)
            history = histories[d]
            if len(history) < min_samples or len(accepted) < self.floor_window // 2:
                return False
            return _z(delta, history, _mad_scale(list(accepted))) > self.k

        return _quarantine_scan(
            points,
            is_anomalous,
            window=self.floor_window,
            reset_after=self.reset_after,
            recover=self.recover,
            ignore_zero_changes=self.ignore_zero_changes,
        )


@dataclass(frozen=True)
class RepeatRun:
    """Flag a value repeated unchanged too many times in a row: a stuck feed.

    Daily prices can legitimately repeat once, so a daily run needs ``daily`` identical
    values before it is flagged; weekly and monthly values almost never repeat, so two
    identical values in a row are enough.
    """

    daily: int = 3
    other: int = 2
    name: str = "repeat_run"

    def detect(self, points: list[Point], frequency: str) -> set[date]:
        need = self.daily if frequency == "daily" else self.other
        flagged: set[date] = set()
        run = 1
        for (_, prev), (d, x) in zip(points, points[1:], strict=False):
            run = run + 1 if x == prev else 1
            if run >= need:
                flagged.add(d)
        return flagged


@dataclass(frozen=True)
class Suite:
    """Several detectors run side by side; a value is flagged if any of them flags it."""

    members: tuple[RobustZ | Seasonal | StaticBand | RepeatRun, ...]
    name: str = "suite"

    def detect(self, points: list[Point], frequency: str) -> set[date]:
        return set().union(*(m.detect(points, frequency) for m in self.members))


Detector = StaticBand | RobustZ | Seasonal | RepeatRun | Suite
