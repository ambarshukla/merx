"""The model changes the new sectors need: periods within a day, and a unit per measure.

Sub-daily data is keyed by (valid_date, period) rather than by a timestamp, because that
is how the market itself settles — and because a timestamp cannot express a day with 50
half-hours. Daily data uses period 0, "the whole day", so one set of keys serves both.
"""

from datetime import date

import duckdb
import pytest
from pydantic import ValidationError

from merx.contracts.model import (
    Contract,
    NoGapsCheck,
    PeriodsPerDayCheck,
    check_name,
)
from merx.engine.checks import run_check

TODAY = date(2026, 10, 16)


def half_hourly_table(days: dict[str, int]) -> duckdb.DuckDBPyConnection:
    """A table with the given number of periods on each given day."""
    con = duckdb.connect()
    con.execute("CREATE TABLE t (measure VARCHAR, valid_date DATE, period INTEGER, value DOUBLE)")
    for day, periods in days.items():
        con.executemany(
            "INSERT INTO t VALUES ('system_price', ?, ?, 50.0)",
            [[day, p] for p in range(1, periods + 1)],
        )
    return con


def contract(**overrides) -> Contract:
    base = dict(
        id="gb_power",
        title="GB half-hourly system price",
        domain="power",
        owner="power-data-steward",
        tier=2,
        frequency="half_hourly",
        instrument="GB_SYSTEM_PRICE",
        unit="GBP/MWh",
        measures=["system_price"],
        source={
            "provider": "eia",
            "format": "eia_api_v2",
            "url": "https://example.invalid/x",
            "series_key": "X",
            "api_key_env": "EIA_API_KEY",
        },
        schedule={"cron": "0 6 * * *"},
        checks=[{"type": "unique", "columns": ["measure", "valid_date", "period"]}],
    )
    return Contract.model_validate(base | overrides)


# --- periods per day -------------------------------------------------------------


def test_a_full_day_passes():
    con = half_hourly_table({"2026-10-14": 48, "2026-10-15": 48, "2026-10-16": 20})
    check = PeriodsPerDayCheck(type="periods_per_day", expected=48)
    outcome = run_check(con, "t", check, TODAY)
    # The newest day is still filling up and is not judged.
    assert outcome.passed
    assert outcome.metadata["bad_days"] == 0


def test_a_short_day_fails():
    con = half_hourly_table({"2026-10-14": 47, "2026-10-15": 48, "2026-10-16": 1})
    outcome = run_check(con, "t", PeriodsPerDayCheck(type="periods_per_day", expected=48), TODAY)
    assert not outcome.passed
    assert outcome.metadata["examples"] == [["system_price", "2026-10-14", 47]]


def test_clock_change_days_are_allowed_when_declared():
    """The clocks going back gives Britain a 50-period day; forward, a 46-period one."""
    con = half_hourly_table({"2025-10-26": 50, "2026-03-29": 46, "2026-10-15": 48, "2026-10-16": 4})
    strict = PeriodsPerDayCheck(type="periods_per_day", expected=48)
    assert not run_check(con, "t", strict, TODAY).passed

    allowing = PeriodsPerDayCheck(type="periods_per_day", expected=48, also_allow=[46, 50])
    assert run_check(con, "t", allowing, TODAY).passed


# --- gaps within a day -----------------------------------------------------------


def test_no_gaps_passes_on_contiguous_periods():
    con = half_hourly_table({"2026-10-15": 48, "2026-10-16": 10})
    assert run_check(con, "t", NoGapsCheck(type="no_gaps"), TODAY).passed


def test_no_gaps_catches_a_hole_the_count_would_miss():
    """48 periods present, but period 7 missing and 49 present: the count is right."""
    con = half_hourly_table({"2026-10-15": 48, "2026-10-16": 2})
    con.execute("DELETE FROM t WHERE valid_date = '2026-10-15' AND period = 7")
    con.execute("INSERT INTO t VALUES ('system_price', '2026-10-15', 49, 50.0)")

    assert run_check(
        con, "t", PeriodsPerDayCheck(type="periods_per_day", expected=48), TODAY
    ).passed
    gaps = run_check(con, "t", NoGapsCheck(type="no_gaps"), TODAY)
    assert not gaps.passed
    assert gaps.metadata["days_with_holes"] == 1


# --- what the contract schema now insists on -------------------------------------


def test_sub_daily_contracts_must_key_on_period():
    with pytest.raises(ValidationError, match="needs period in its unique check"):
        contract(checks=[{"type": "unique", "columns": ["measure", "valid_date"]}])


def test_periods_per_day_is_refused_on_daily_data():
    with pytest.raises(ValidationError, match="sub-daily data only"):
        contract(
            frequency="daily",
            checks=[
                {"type": "unique", "columns": ["measure", "valid_date"]},
                {"type": "periods_per_day", "expected": 48},
            ],
        )


def test_the_new_checks_have_stable_names():
    assert check_name(PeriodsPerDayCheck(type="periods_per_day", expected=48)) == "periods_per_day"
    assert check_name(NoGapsCheck(type="no_gaps")) == "no_gaps"


# --- a unit per measure ----------------------------------------------------------


def test_one_unit_still_serves_every_measure():
    c = contract(measures=["system_price"], unit="GBP/MWh")
    assert c.unit_for("system_price") == "GBP/MWh"


def test_a_file_may_mix_units():
    c = contract(
        frequency="monthly",
        measures=["copper", "crude_oil_wti", "gold"],
        unit={"copper": "USD/mt", "crude_oil_wti": "USD/bbl", "gold": "USD/troy oz"},
        source={
            "provider": "eia",
            "format": "cftc_disaggregated",
            "url": "https://example.invalid/x",
            "series_key": "X",
            "columns": {"copper": "C", "crude_oil_wti": "O", "gold": "G"},
        },
        checks=[{"type": "unique", "columns": ["measure", "valid_date"]}],
    )
    assert c.unit_for("gold") == "USD/troy oz"
    assert c.unit_for("copper") == "USD/mt"


def test_a_unit_per_measure_must_cover_every_measure():
    with pytest.raises(ValidationError, match="every measure"):
        contract(
            frequency="monthly",
            measures=["copper", "gold"],
            unit={"copper": "USD/mt"},
            source={
                "provider": "eia",
                "format": "cftc_disaggregated",
                "url": "https://example.invalid/x",
                "series_key": "X",
                "columns": {"copper": "C", "gold": "G"},
            },
            checks=[{"type": "unique", "columns": ["measure", "valid_date"]}],
        )
