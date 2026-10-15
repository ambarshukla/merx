"""Shared test helpers.

Test inputs are synthetic look-alikes written at test time (D-002: no fetched data is
committed). The EIA builder copies the layout observed in the real files on 2026-10-15,
including the edge cases that matter: a negative price and a missing value.
"""

import io
from datetime import date

import pytest
import xlwt

DATE_STYLE = xlwt.easyxf(num_format_str="M/D/YYYY")


def eia_workbook(
    series_key: str,
    points: list[tuple[date, float | None]],
    *,
    title: str = "Synthetic Series (Dollars per Barrel)",
    release_date: str | None = "10/15/2026",
    next_release_date: str | None = "10/21/2026",
    data_sheet: str = "Data 1",
    header_labels: tuple[str, str, str] = ("Back to Contents", "Sourcekey", "Date"),
    extra_column: bool = False,
) -> bytes:
    """An .xls file shaped like EIA's hist_xls downloads."""
    book = xlwt.Workbook()
    contents = book.add_sheet("Contents")
    contents.write(1, 1, "Workbook Contents")
    contents.write(2, 1, title)
    if release_date is not None:
        contents.write(8, 1, "Release Date:")
        contents.write(8, 2, release_date)
    if next_release_date is not None:
        contents.write(9, 1, "Next Release Date:")
        contents.write(9, 2, next_release_date)

    data = book.add_sheet(data_sheet)
    data.write(0, 0, header_labels[0])
    data.write(0, 1, f"Data 1: {title}")
    data.write(1, 0, header_labels[1])
    data.write(1, 1, series_key)
    data.write(2, 0, header_labels[2])
    data.write(2, 1, title)
    for i, (d, v) in enumerate(points, start=3):
        data.write(i, 0, d, DATE_STYLE)
        if v is not None:
            data.write(i, 1, v)
        if extra_column:
            data.write(i, 2, 0)
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


# Around the day WTI went negative, plus one missing value.
WTI_POINTS: list[tuple[date, float | None]] = [
    (date(2020, 4, 16), 19.82),
    (date(2020, 4, 17), 18.31),
    (date(2020, 4, 20), -36.98),
    (date(2020, 4, 21), 8.91),
    (date(2020, 4, 22), None),
    (date(2020, 4, 23), 15.06),
]


@pytest.fixture
def wti_xls() -> bytes:
    return eia_workbook("RWTC", WTI_POINTS)
