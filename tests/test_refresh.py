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


def test_a_weekday_night_runs_the_daily_prices_and_the_gb_half_hourly_sets():
    # 00:30 UTC on a Tuesday is Monday evening in New York, and Tuesday morning in
    # London: the GB power and carbon datasets have their own per-month jobs.
    due = names(datetime(2026, 10, 20, 0, 30, tzinfo=UTC))
    assert due == {
        "refresh_30_18_x_x_1to5_new_york",
        "refresh_elexon_gb_power_price",
        "refresh_neso_gb_carbon_intensity",
    }


def test_wednesday_adds_crude_stocks_and_thursday_gas_storage():
    wednesday = names(datetime(2026, 10, 22, 0, 30, tzinfo=UTC))  # Wednesday 20:30 NY
    thursday = names(datetime(2026, 10, 23, 0, 30, tzinfo=UTC))
    assert "refresh_0_12_x_x_3_new_york" in wednesday
    assert "refresh_0_12_x_x_4_new_york" in thursday


def test_friday_refreshes_this_years_cftc_partition():
    jobs = due_jobs(CONTRACTS, datetime(2026, 10, 24, 0, 30, tzinfo=UTC))  # Friday NY
    cftc = {j.name: j.partition_key for j in jobs if "cftc" in j.name}
    # Derived from the contracts rather than listed, so adding a market — which is a
    # configuration change — does not also require editing this test.
    expected = {f"refresh_{c.id}": "2026" for c in CONTRACTS if c.source.provider == "cftc"}
    assert expected, "no CFTC contracts found"
    assert cftc == expected


def test_the_weekend_runs_only_the_markets_that_trade_then():
    """US spot prices and the weekly reports stop at the weekend. Electricity does not:
    GB settles every half hour of every day, so those two datasets still refresh."""
    saturday = names(datetime(2026, 10, 25, 0, 30, tzinfo=UTC))  # Saturday NY
    assert saturday == {"refresh_elexon_gb_power_price", "refresh_neso_gb_carbon_intensity"}


def test_a_monthly_source_refreshes_the_month_in_progress():
    jobs = due_jobs(CONTRACTS, datetime(2026, 10, 20, 0, 30, tzinfo=UTC))
    monthly = {j.name: j.partition_key for j in jobs if "gb_" in j.name}
    assert monthly == {
        "refresh_elexon_gb_power_price": "2026-10",
        "refresh_neso_gb_carbon_intensity": "2026-10",
    }
