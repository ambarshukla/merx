from datetime import date

import pytest
from conftest import WTI_POINTS, eia_workbook

from merx.sources import SourceFormatError
from merx.sources.eia_xls import parse


def test_parses_points_release_dates_and_title(wti_xls):
    parsed = parse(wti_xls, "RWTC")
    assert parsed.series_key == "RWTC"
    assert parsed.title == "Data 1: Synthetic Series (Dollars per Barrel)"
    assert parsed.release_date == date(2026, 10, 15)
    assert parsed.next_release_date == date(2026, 10, 21)
    assert parsed.points == WTI_POINTS


def test_negative_price_survives(wti_xls):
    values = dict(parse(wti_xls, "RWTC").points)
    assert values[date(2020, 4, 20)] == -36.98


def test_empty_value_cell_becomes_none_not_dropped(wti_xls):
    parsed = parse(wti_xls, "RWTC")
    assert (date(2020, 4, 22), None) in parsed.points
    assert len(parsed.points) == len(WTI_POINTS)


def test_wrong_series_key_fails():
    with pytest.raises(SourceFormatError, match="contract expects 'RWTC'"):
        parse(eia_workbook("RBRTE", WTI_POINTS), "RWTC")


def test_missing_data_sheet_fails():
    with pytest.raises(SourceFormatError, match="'Data 1' not found"):
        parse(eia_workbook("RWTC", WTI_POINTS, data_sheet="Data"), "RWTC")


def test_changed_header_fails():
    renamed = ("Back to Contents", "Series ID", "Date")
    with pytest.raises(SourceFormatError, match="unexpected header rows"):
        parse(eia_workbook("RWTC", WTI_POINTS, header_labels=renamed), "RWTC")


def test_extra_column_fails():
    with pytest.raises(SourceFormatError, match="expected 2 columns"):
        parse(eia_workbook("RWTC", WTI_POINTS, extra_column=True), "RWTC")


def test_missing_release_date_fails():
    with pytest.raises(SourceFormatError, match="Release Date"):
        parse(eia_workbook("RWTC", WTI_POINTS, release_date=None), "RWTC")


def test_unreadable_release_date_fails():
    with pytest.raises(SourceFormatError, match="unreadable"):
        parse(eia_workbook("RWTC", WTI_POINTS, release_date="2026-10-15"), "RWTC")


def test_not_an_excel_file_fails():
    with pytest.raises(SourceFormatError, match="not a readable"):
        parse(b"<html>Service unavailable</html>", "RWTC")


def test_worded_next_release_date_is_none_not_a_failure():
    """Copies captured before about 2016 word the next release instead of dating it. The
    phrase is unusable but the file is not: observed in the archive's 2012 and 2015 copies
    of EIA's Texas crude production spreadsheet, 2026-10-18."""
    worded = eia_workbook("RWTC", WTI_POINTS, next_release_date="Last Week of December 2012")
    parsed = parse(worded, "RWTC")
    assert parsed.release_date == date(2026, 10, 15)
    assert parsed.next_release_date is None
    assert parsed.points == WTI_POINTS
