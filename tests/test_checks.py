from datetime import date, timedelta

import duckdb
import pytest

from merx.contracts.model import FreshnessCheck, NotNullCheck, RangeCheck, UniqueCheck
from merx.engine.checks import run_check

TODAY = date(2026, 10, 15)


@pytest.fixture
def con():
    con = duckdb.connect()
    con.execute("CREATE TABLE t (measure VARCHAR, valid_date DATE, value DOUBLE)")
    con.execute(
        "INSERT INTO t VALUES "
        "('price', '2026-10-05', 80.0), ('price', '2026-10-06', -36.98), "
        "('price', '2026-10-07', NULL), ('price', '2026-10-08', 999.0), "
        "('price', '2026-10-08', 81.0)"
    )
    yield con
    con.close()


@pytest.fixture
def two_measures():
    con = duckdb.connect()
    con.execute("CREATE TABLE t (measure VARCHAR, valid_date DATE, value DOUBLE)")
    con.execute(
        "INSERT INTO t VALUES "
        "('long', '2026-10-06', 500.0), ('short', '2026-10-06', 400.0), "
        "('long', '2026-10-13', 510.0), ('short', '2026-10-13', -5.0)"
    )
    yield con
    con.close()


def test_not_null_fails_on_unexpected_gap(con):
    outcome = run_check(con, "t", NotNullCheck(type="not_null", column="value"), TODAY)
    assert not outcome.passed
    assert outcome.metadata["examples"] == [["price", "2026-10-07"]]


def test_not_null_accepts_a_named_gap_and_reports_filled_ones(con):
    check = NotNullCheck(
        type="not_null", column="value", allow=[date(2026, 10, 7), date(2026, 10, 5)]
    )
    outcome = run_check(con, "t", check, TODAY)
    assert outcome.passed
    assert outcome.metadata["accepted_nulls"] == 1
    assert outcome.metadata["accepted_gaps_now_filled"] == ["2026-10-05"]


def test_unique_finds_duplicate_keys(con):
    check = UniqueCheck(type="unique", columns=["measure", "valid_date"])
    outcome = run_check(con, "t", check, TODAY)
    assert not outcome.passed
    assert outcome.metadata["examples"] == [["price", "2026-10-08", "2"]]


def test_unique_on_measure_and_date_allows_the_same_date_across_measures(two_measures):
    check = UniqueCheck(type="unique", columns=["measure", "valid_date"])
    assert run_check(two_measures, "t", check, TODAY).passed


def test_range_allows_negative_prices_inside_the_bound(con):
    check = RangeCheck(type="range", column="value", min=-100, max=300)
    outcome = run_check(con, "t", check, TODAY)
    assert not outcome.passed
    assert outcome.metadata["examples"] == [["price", "2026-10-08", 999.0]]  # not the -36.98


def test_range_ignores_missing_values(con):
    check = RangeCheck(type="range", column="value", min=-1000, max=1000)
    assert run_check(con, "t", check, TODAY).passed


def test_range_can_be_scoped_to_one_measure(two_measures):
    everywhere = RangeCheck(type="range", column="value", min=0, max=1e6)
    only_long = RangeCheck(type="range", column="value", min=0, max=1e6, measure="long")
    assert not run_check(two_measures, "t", everywhere, TODAY).passed
    assert run_check(two_measures, "t", only_long, TODAY).passed


@pytest.mark.parametrize(("limit", "passed"), [(7, True), (6, False)])
def test_freshness_uses_newest_non_missing_value(con, limit, passed):
    # Newest value is 2026-10-08: 7 days before TODAY.
    outcome = run_check(con, "t", FreshnessCheck(type="freshness", max_age_days=limit), TODAY)
    assert outcome.passed is passed
    assert outcome.metadata["age_days"] == 7


def test_freshness_is_judged_on_the_stalest_measure(two_measures):
    # long is 1 day old, short is 2 days old: a 1-day limit fails on short.
    two_measures.execute("INSERT INTO t VALUES ('long', '2026-10-14', 520.0)")
    outcome = run_check(two_measures, "t", FreshnessCheck(type="freshness", max_age_days=1), TODAY)
    assert not outcome.passed
    assert outcome.metadata["stalest_measure"] == "short"
    assert outcome.metadata["age_days"] == 2


def test_freshness_fails_on_empty_table():
    con = duckdb.connect()
    con.execute("CREATE TABLE t (measure VARCHAR, valid_date DATE, value DOUBLE)")
    assert not run_check(con, "t", FreshnessCheck(type="freshness", max_age_days=5), TODAY).passed


def _walk(n: int, seed: int = 3):
    import random

    rng = random.Random(seed)
    start, x, out = date(2010, 1, 4), 60.0, []
    for i in range(n):
        x += rng.gauss(0, 0.5)
        out.append((start + timedelta(days=i), x))
    return out


def test_anomaly_flags_a_recent_spike_but_not_an_old_one():
    from merx.contracts.model import AnomalyCheck

    points = _walk(1500)
    con = duckdb.connect()
    con.execute("CREATE TABLE t (measure VARCHAR, valid_date DATE, value DOUBLE)")
    rows = [("price", d, v) for d, v in points]
    old_spike, new_spike = 600, 1495
    rows[old_spike] = ("price", rows[old_spike][1], rows[old_spike][2] * 10)
    con.executemany("INSERT INTO t VALUES (?, ?, ?)", rows)
    check = AnomalyCheck(type="anomaly", detector="robust_suite", lookback=20)
    clean = run_check(con, "t", check, TODAY, frequency="daily")
    assert clean.passed  # the old spike is outside the newest 20 values
    assert clean.metadata["flags_in_full_history"] >= 1
    con.execute("UPDATE t SET value = value * 10 WHERE valid_date = ?", [rows[new_spike][1]])
    flagged = run_check(con, "t", check, TODAY, frequency="daily")
    assert not flagged.passed
    assert flagged.metadata["examples"][0][1] == rows[new_spike][1].isoformat()
