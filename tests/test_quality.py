import random
from datetime import date, timedelta

import pytest

from merx.quality.defects import DEFECT_TYPES, WARMUP, Defect, inject, max_per_type
from merx.quality.detectors import RepeatRun, RobustZ, Seasonal, StaticBand, Suite
from merx.quality.scoring import caught_by_type, score


def noisy_daily(n: int = 3000, level: float = 60.0, seed: int = 1):
    """A random walk with small steps: a stand-in for a daily price."""
    rng = random.Random(seed)
    start = date(2000, 1, 3)
    points, x = [], level
    for i in range(n):
        x += rng.gauss(0, 0.5)
        points.append((start + timedelta(days=i), x))
    return points


def seasonal_weekly(years: int = 8, seed: int = 2):
    """Weekly storage-like series: +10 a week, except a normal +100 surge in late May."""
    rng = random.Random(seed)
    start = date(2010, 1, 1)
    points, x = [], 1000.0
    for i in range(years * 52):
        d = start + timedelta(weeks=i)
        x += (100.0 if (5, 20) <= (d.month, d.day) <= (5, 26) else 10.0) + rng.gauss(0, 2)
        points.append((d, x))
    return points


# --- defects -----------------------------------------------------------------------


def test_injection_is_deterministic_per_seed():
    points = noisy_daily()
    assert inject(points, "s", 0) == inject(points, "s", 0)
    assert inject(points, "s", 0) != inject(points, "s", 1)


def test_only_defect_dates_change_and_all_after_warmup():
    points = noisy_daily()
    corrupted, defects = inject(points, "s", 3)
    changed = {d for (d, a), (_, b) in zip(points, corrupted, strict=True) if a != b}
    assert changed == {d for defect in defects for d in defect.dates}
    assert min(changed) >= points[0][0] + WARMUP


def test_every_type_is_injected_the_requested_number_of_times():
    _, defects = inject(noisy_daily(), "s", 0, per_type=4)
    counts = {t: sum(1 for d in defects if d.defect_type == t) for t in DEFECT_TYPES}
    assert counts == dict.fromkeys(DEFECT_TYPES, 4)


def test_defect_values_follow_their_type():
    points = noisy_daily()
    value = dict(points)
    _, defects = inject(points, "s", 0, per_type=4)
    for defect in defects:
        for d, o, c in zip(defect.dates, defect.original, defect.corrupted, strict=True):
            assert o == value[d]
            match defect.defect_type:
                case "sign_flip":
                    assert c == -o
                case "unit_scale_run":
                    assert c == pytest.approx(o * 100)
                case "decimal_shift":
                    assert c in (pytest.approx(o * 10), pytest.approx(o / 10))
        if defect.defect_type == "stale_run":
            assert len(set(defect.corrupted)) == 1


def test_too_short_a_series_gets_no_defects():
    assert max_per_type(noisy_daily(n=1200)) == 0
    assert max_per_type(noisy_daily(n=3000)) > 0


# --- detectors ---------------------------------------------------------------------


def with_spike(points, i, factor=10.0):
    out = list(points)
    out[i] = (out[i][0], out[i][1] * factor)
    return out


def test_static_band_flags_the_spike_and_the_return():
    points = with_spike(noisy_daily(), 2000)
    flags = StaticBand().detect(points, "daily")
    assert {points[2000][0], points[2001][0]} <= flags


def test_robust_z_flags_a_spike_once_not_the_return():
    points = with_spike(noisy_daily(), 2000)
    flags = RobustZ().detect(points, "daily")
    assert points[2000][0] in flags
    assert points[2001][0] not in flags


def test_robust_z_flags_a_whole_unit_scale_run():
    points = noisy_daily()
    for i in range(2000, 2008):
        points[i] = (points[i][0], points[i][1] * 100)
    flags = RobustZ().detect(points, "daily")
    assert {points[i][0] for i in range(2000, 2008)} <= flags


def test_robust_z_cannot_see_a_stale_run():
    points = noisy_daily()
    for i in range(2000, 2006):
        points[i] = (points[i][0], points[1999][1])
    flags = RobustZ().detect(points, "daily")
    assert not flags & {points[i][0] for i in range(2000, 2006)}


def test_robust_z_accepts_a_new_level_after_reset_after_flags():
    points = noisy_daily()
    for i in range(2000, len(points)):
        points[i] = (points[i][0], points[i][1] + 500)  # a permanent shift
    flags = RobustZ(reset_after=10).detect(points, "daily")
    assert len(flags & {d for d, _ in points[2000:]}) == 10


def test_seasonal_accepts_a_normal_seasonal_surge_that_robust_z_flags():
    points = seasonal_weekly()
    late_may = {d for d, _ in points if d.year >= 2013 and (5, 20) <= (d.month, d.day) <= (5, 26)}
    assert late_may & RobustZ().detect(points, "weekly")
    assert not late_may & Seasonal().detect(points, "weekly")


def test_seasonal_flags_a_jump_outside_the_season():
    points = seasonal_weekly()
    i = next(i for i, (d, _) in enumerate(points) if d >= date(2016, 8, 1))
    points[i] = (points[i][0], points[i][1] + 80)
    assert points[i][0] in Seasonal().detect(points, "weekly")


# --- scoring -----------------------------------------------------------------------


def test_score_separates_true_induced_and_real_world_flags():
    d = [date(2020, 1, k) for k in range(1, 11)]
    defects = [
        Defect(0, "decimal_shift", (d[1],), (1.0,), (10.0,)),
        Defect(1, "stale_run", (d[4], d[5]), (1.0, 2.0), (0.5, 0.5)),
        Defect(2, "small_jump", (d[8],), (1.0,), (2.0,)),
    ]
    flags = {d[1], d[2], d[5], d[7]}  # d[2]: induced by the spike; d[7]: also on clean data
    s = score(flags, clean_flags={d[7]}, defects=defects)
    assert (s.defects, s.caught) == (3, 2)
    assert (s.true_flags, s.induced_false_flags, s.real_data_flags_seen) == (2, 1, 1)
    assert s.recall == pytest.approx(2 / 3)
    assert s.precision == pytest.approx(2 / 3)
    assert caught_by_type(flags, defects) == {
        "decimal_shift": (1, 1),
        "stale_run": (1, 1),
        "small_jump": (0, 1),
    }


# --- lab end to end ----------------------------------------------------------------


def test_bakeoff_persists_and_reports():
    import duckdb

    from merx.quality.lab import LabSeries, lab_detectors, persist, run_bakeoff
    from merx.quality.report import render

    series = [
        LabSeries("toy.price", "toy", "price", "daily", noisy_daily()),
        LabSeries("toy_weekly.level", "toy_weekly", "level", "weekly", seasonal_weekly(12)),
    ]
    result = run_bakeoff(series, lab_detectors(), seeds=(0, 1))
    con = duckdb.connect()
    persist(con, result)
    # Every defect is scored by every detector setting on every seed.
    (rows,) = con.execute("SELECT count(*) FROM lab.scores").fetchone()
    assert rows == len(series) * len(lab_detectors()) * 2
    # Clean-run flags are stored with seed -1 and add up to the real-data alert counts.
    clean = con.execute("SELECT count(*) FROM lab.flags WHERE seed = -1").fetchone()[0]
    alerts = con.execute("SELECT sum(flags) FROM lab.real_data_alerts").fetchone()[0]
    assert clean == alerts
    # The seasonal toy series has a normal late-May surge: robust z alarms on it.
    assert alerts > 0
    report = render(con)
    assert "### Headline" in report
    assert "robust_z v2 k=5" in report


# --- v2 fixes and the repeat rule ---------------------------------------------------


def with_long_unit_run(points, start=2000, length=14):
    out = list(points)
    for i in range(start, start + length):
        out[i] = (out[i][0], out[i][1] * 100)
    return out


def test_v1_adopts_a_long_error_and_then_flags_the_recovery():
    points = with_long_unit_run(noisy_daily())
    after = {d for d, _ in points[2014:2030]}
    assert RobustZ().detect(points, "daily") & after


def test_v2_accepts_the_recovery_after_a_long_error():
    points = with_long_unit_run(noisy_daily())
    flags = RobustZ(recover=True).detect(points, "daily")
    assert {d for d, _ in points[2000:2010]} <= flags  # the error is still caught
    assert not flags & {d for d, _ in points[2014:2030]}  # the recovery is not flagged


def test_v2_keeps_unchanged_values_out_of_the_yardstick():
    original = noisy_daily()
    stuck = original[1499][1]
    points = list(original[:1500])
    points += [(d, stuck) for d, _ in original[1500:1560]]  # 60 zero changes
    # The series then carries on from the stuck value with its usual small steps, so
    # there is no catch-up jump: any flag afterwards comes from the shrunken yardstick.
    points += [(d, x - original[1559][1] + stuck) for d, x in original[1560:]]
    after = {d for d, _ in points[1560:1600]}
    assert RobustZ().detect(points, "daily") & after  # v1: tiny yardstick, false alarms
    assert not RobustZ(ignore_zero_changes=True).detect(points, "daily") & after


def test_repeat_run_needs_three_daily_or_two_weekly_identical_values():
    days = [date(2020, 1, k) for k in range(1, 7)]
    points = list(zip(days, [1.0, 2.0, 2.0, 3.0, 3.0, 3.0], strict=True))
    assert RepeatRun().detect(points, "daily") == {days[5]}
    assert RepeatRun().detect(points, "weekly") == {days[2], days[4], days[5]}


def test_suite_flags_what_any_member_flags():
    points = noisy_daily()
    for i in range(2000, 2005):
        points[i] = (points[i][0], points[1999][1])
    points = with_spike(points, 2500)
    suite = Suite((RobustZ(recover=True, ignore_zero_changes=True), RepeatRun()))
    flags = suite.detect(points, "daily")
    assert points[2500][0] in flags
    assert points[2003][0] in flags
