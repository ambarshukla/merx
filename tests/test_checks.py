from datetime import date

import duckdb
import pytest

from merx.contracts.model import FreshnessCheck, NotNullCheck, RangeCheck, UniqueCheck
from merx.engine.checks import run_check

TODAY = date(2026, 10, 15)


@pytest.fixture
def con():
    con = duckdb.connect()
    con.execute("CREATE TABLE t (valid_date DATE, value DOUBLE)")
    con.execute(
        "INSERT INTO t VALUES "
        "('2026-10-05', 80.0), ('2026-10-06', -36.98), ('2026-10-07', NULL), "
        "('2026-10-08', 999.0), ('2026-10-08', 81.0)"
    )
    yield con
    con.close()


def test_not_null_fails_on_unexpected_gap(con):
    outcome = run_check(con, "t", NotNullCheck(type="not_null", column="value"), TODAY)
    assert not outcome.passed
    assert outcome.metadata["examples"] == ["2026-10-07"]


def test_not_null_accepts_a_named_gap_and_reports_filled_ones(con):
    check = NotNullCheck(
        type="not_null", column="value", allow=[date(2026, 10, 7), date(2026, 10, 5)]
    )
    outcome = run_check(con, "t", check, TODAY)
    assert outcome.passed
    assert outcome.metadata["accepted_nulls"] == 1
    assert outcome.metadata["accepted_gaps_now_filled"] == ["2026-10-05"]


def test_unique_finds_duplicate_dates(con):
    outcome = run_check(con, "t", UniqueCheck(type="unique", columns=["valid_date"]), TODAY)
    assert not outcome.passed
    assert outcome.metadata["examples"] == [["2026-10-08", "2"]]


def test_range_allows_negative_prices_inside_the_bound(con):
    check = RangeCheck(type="range", column="value", min=-100, max=300)
    outcome = run_check(con, "t", check, TODAY)
    assert not outcome.passed
    assert outcome.metadata["examples"] == [["2026-10-08", 999.0]]  # not the -36.98


def test_range_ignores_missing_values(con):
    check = RangeCheck(type="range", column="value", min=-1000, max=1000)
    assert run_check(con, "t", check, TODAY).passed


@pytest.mark.parametrize(("limit", "passed"), [(7, True), (6, False)])
def test_freshness_uses_newest_non_missing_value(con, limit, passed):
    # Newest value is 2026-10-08: 7 days before TODAY.
    outcome = run_check(con, "t", FreshnessCheck(type="freshness", max_age_days=limit), TODAY)
    assert outcome.passed is passed
    assert outcome.metadata["age_days"] == 7


def test_freshness_fails_on_empty_table():
    con = duckdb.connect()
    con.execute("CREATE TABLE t (valid_date DATE, value DOUBLE)")
    assert not run_check(con, "t", FreshnessCheck(type="freshness", max_age_days=5), TODAY).passed
