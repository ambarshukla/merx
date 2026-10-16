"""The two Great Britain half-hourly sources, and the settlement clock they share."""

import json
from datetime import UTC, date, datetime

import pytest

from merx.sources import carbon, elexon
from merx.sources.gb import periods_in_day, settlement_period
from merx.sources.rangefetch import fetch_in_chunks

# --- the settlement clock --------------------------------------------------------


def test_an_ordinary_day_has_48_periods():
    assert periods_in_day(date(2026, 10, 15)) == 48


def test_the_clocks_going_forward_give_a_46_period_day():
    assert periods_in_day(date(2026, 3, 29)) == 46


def test_the_clocks_going_back_give_a_50_period_day():
    assert periods_in_day(date(2025, 10, 26)) == 50


def test_the_hour_that_happens_twice_is_two_different_periods():
    """01:30 occurs twice on the autumn clock-change day. A key built from local
    wall-clock time would collide the two; settlement periods do not."""
    first = settlement_period(datetime(2025, 10, 26, 0, 30, tzinfo=UTC))  # 01:30 BST
    second = settlement_period(datetime(2025, 10, 26, 1, 30, tzinfo=UTC))  # 01:30 GMT
    assert first == (date(2025, 10, 26), 4)
    assert second == (date(2025, 10, 26), 6)


def test_the_british_summer_day_starts_the_evening_before_in_utc():
    assert settlement_period(datetime(2026, 10, 14, 23, 0, tzinfo=UTC)) == (date(2026, 10, 15), 1)


# --- Elexon market index data ----------------------------------------------------


def mid_row(
    day="2026-10-15",
    period=3,
    start="2026-10-15T00:00:00Z",
    provider="APXMIDP",
    price=157.59,
    volume=2297.8,
):
    return {
        "dataset": "MID",
        "startTime": start,
        "dataProvider": provider,
        "settlementDate": day,
        "settlementPeriod": period,
        "price": price,
        "volume": volume,
    }


def test_only_the_named_exchange_is_taken():
    """Elexon publishes several exchanges' indices. Averaging them would be a
    methodology decision disguised as a parsing one."""
    content = json.dumps({"data": [mid_row(), mid_row(provider="N2EXMIDP", price=0.0)]}).encode()
    parsed = elexon.parse_mid(content, "APXMIDP", {"price": "price"})
    assert [(m, d, p, v) for m, d, p, _, v in parsed.observations] == [
        ("price", date(2026, 10, 15), 3, 157.59)
    ]


def test_a_provider_that_is_not_there_fails_loudly():
    content = json.dumps({"data": [mid_row(provider="N2EXMIDP")]}).encode()
    with pytest.raises(elexon.ElexonFormatError, match="N2EXMIDP"):
        elexon.parse_mid(content, "APXMIDP", {"price": "price"})


def test_a_start_time_that_contradicts_the_stated_period_fails():
    """If Elexon ever changed its convention, every value would shift by half an hour.
    Better to fail than to publish a silently shifted series."""
    content = json.dumps({"data": [mid_row(period=7)]}).encode()
    with pytest.raises(elexon.ElexonFormatError, match="disagree"):
        elexon.parse_mid(content, "APXMIDP", {"price": "price"})


def test_the_month_filter_drops_the_padding():
    """A month is fetched with a day of padding so no settlement day is half-fetched;
    the padding must not land in two partitions at once."""
    content = json.dumps(
        {
            "data": [
                mid_row(day="2026-09-30", start="2026-09-30T00:00:00Z"),
                mid_row(day="2026-10-01", start="2026-10-01T00:00:00Z"),
            ]
        }
    ).encode()
    parsed = elexon.parse_mid(content, "APXMIDP", {"price": "price"}, month="2026-10")
    assert [d for _, d, _, _, _ in parsed.observations] == [date(2026, 10, 1)]


# --- carbon intensity ------------------------------------------------------------


def intensity(start, forecast, actual):
    return {"from": start, "to": start, "intensity": {"forecast": forecast, "actual": actual}}


def test_forecast_and_outturn_become_two_measures():
    content = json.dumps({"data": [intensity("2026-10-15T00:00Z", 72, 64)]}).encode()
    parsed = carbon.parse(content, {"forecast": "forecast", "outturn": "actual"})
    assert {(m, v) for m, _, _, _, v in parsed.observations} == {
        ("forecast", 72.0),
        ("outturn", 64.0),
    }


def test_a_missing_outturn_is_a_gap_not_a_failure():
    """The outturn for the newest half hours is published later than the forecast."""
    content = json.dumps({"data": [intensity("2026-10-16T12:00Z", 190, None)]}).encode()
    parsed = carbon.parse(content, {"forecast": "forecast", "outturn": "actual"})
    assert ("outturn", None) in {(m, v) for m, _, _, _, v in parsed.observations}


def test_periods_are_derived_from_utc_the_same_way_elexon_settles():
    content = json.dumps({"data": [intensity("2026-10-14T23:00Z", 100, 100)]}).encode()
    parsed = carbon.parse(content, {"forecast": "forecast"})
    _, day, period, _, _ = parsed.observations[0]
    assert (day, period) == (date(2026, 10, 15), 1)


# --- fetching a range an API will not serve in one go ----------------------------


def test_a_wide_window_is_walked_in_chunks_and_merged():
    asked: list[str] = []

    def get(url: str) -> bytes:
        asked.append(url)
        return json.dumps({"data": [{"id": url}]}).encode()

    merged = fetch_in_chunks(
        get,
        "https://example.invalid/?from={from}&to={to}",
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 10, 1, tzinfo=UTC),
        max_days=7,
        key=lambda row: (row["id"],),
    )
    assert len(asked) == 5  # four whole weeks and a remainder
    assert len(json.loads(merged)["data"]) == 5


def test_rows_repeated_at_a_chunk_boundary_are_kept_once():
    def get(url: str) -> bytes:
        return json.dumps({"data": [{"id": "same"}]}).encode()

    merged = fetch_in_chunks(
        get,
        "https://example.invalid/?from={from}&to={to}",
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 29, tzinfo=UTC),
        max_days=7,
        key=lambda row: (row["id"],),
    )
    assert json.loads(merged)["data"] == [{"id": "same"}]
