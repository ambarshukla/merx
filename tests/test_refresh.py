"""Which jobs a daily timer runs."""

from datetime import UTC, datetime

from merx.contracts import load_contracts
from merx.orchestration.refresh import cron_days, due_jobs
from merx.reference import load_reference

CONTRACTS = load_contracts(load_reference())


def names(when: datetime) -> set[str]:
    return {j.name for j in due_jobs(CONTRACTS, when)}


def test_cron_day_fields():
    assert cron_days("1-5") == {1, 2, 3, 4, 5}
    assert cron_days("3") == {3}
    assert cron_days("*") == set(range(7))
    assert cron_days("0,7") == {0}


def test_a_weekday_night_runs_the_daily_prices_only():
    # 00:30 UTC on a Tuesday is Monday evening in New York.
    due = names(datetime(2026, 10, 20, 0, 30, tzinfo=UTC))
    assert due == {"refresh_30_18_x_x_1to5_new_york"}


def test_wednesday_adds_crude_stocks_and_thursday_gas_storage():
    wednesday = names(datetime(2026, 10, 22, 0, 30, tzinfo=UTC))  # Wednesday 20:30 NY
    thursday = names(datetime(2026, 10, 23, 0, 30, tzinfo=UTC))
    assert "refresh_0_12_x_x_3_new_york" in wednesday
    assert "refresh_0_12_x_x_4_new_york" in thursday


def test_friday_refreshes_this_years_cftc_partition():
    jobs = due_jobs(CONTRACTS, datetime(2026, 10, 24, 0, 30, tzinfo=UTC))  # Friday NY
    cftc = {j.name: j.partition_key for j in jobs if "cftc" in j.name}
    assert cftc == {
        "refresh_cftc_henry_hub_positions": "2026",
        "refresh_cftc_wti_positions": "2026",
    }


def test_the_weekend_runs_nothing():
    assert names(datetime(2026, 10, 25, 0, 30, tzinfo=UTC)) == set()  # Saturday NY
