"""Parser for EIA's keyless "hist_xls" downloads (``https://www.eia.gov/dnav/.../hist_xls/``).

Layout observed 2026-10-15 across daily and weekly series (old binary Excel, .xls):

* sheet ``Contents``: labels in column B and values in column C, including
  ``Release Date:`` and ``Next Release Date:`` as ``MM/DD/YYYY`` text. Copies captured
  before about 2016 state the next release as a phrase instead of a date ("Last Week of
  December 2012"), so that one is optional; the release date itself is not, because it is
  what the file claims about itself and the honest knowledge time of an archived copy;
* sheet ``Data 1``: row 1 ``Back to Contents`` and the series title, row 2
  ``Sourcekey`` and the series key, row 3 ``Date`` and the column title, then one row
  per observation: an Excel date serial and a number, or an empty cell where the source
  has no value.

Anything else raises SourceFormatError: a layout change must stop the run, not be guessed
around.
"""

from dataclasses import dataclass
from datetime import date, datetime

import xlrd

from merx.sources import SourceFormatError

_DATA_SHEET = "Data 1"
_CONTENTS_SHEET = "Contents"


@dataclass(frozen=True)
class ParsedSeries:
    series_key: str
    title: str
    release_date: date
    # None where the file names its next release in words rather than as a date.
    next_release_date: date | None
    # (valid_date, value); value is None where the source has an empty cell.
    points: list[tuple[date, float | None]]


def _release_dates(sheet: xlrd.sheet.Sheet) -> tuple[date, date | None]:
    labels = ("Release Date:", "Next Release Date:")
    found: dict[str, date | None] = {}
    for r in range(sheet.nrows):
        row = sheet.row_values(r)
        if len(row) < 3:
            continue
        label = str(row[1]).strip()
        if label in labels:
            text = str(row[2]).strip()
            try:
                found[label] = datetime.strptime(text, "%m/%d/%Y").date()
            except ValueError as e:
                # Older copies word the next release ("Last Week of December 2012"). That
                # is a schedule, not a date, and nothing downstream can use it; the
                # release date must still be a date.
                if label == "Release Date:":
                    raise SourceFormatError(f"unreadable {label} {text!r}") from e
                found[label] = None
    missing = set(labels) - found.keys()
    if missing:
        raise SourceFormatError(f"Contents sheet is missing {sorted(missing)}")
    return found["Release Date:"], found["Next Release Date:"]


def parse(content: bytes, expected_series_key: str) -> ParsedSeries:
    try:
        book = xlrd.open_workbook(file_contents=content)
    except xlrd.XLRDError as e:
        raise SourceFormatError(f"not a readable .xls workbook: {e}") from e

    names = book.sheet_names()
    for required in (_CONTENTS_SHEET, _DATA_SHEET):
        if required not in names:
            raise SourceFormatError(f"sheet {required!r} not found; sheets are {names}")
    release_date, next_release_date = _release_dates(book.sheet_by_name(_CONTENTS_SHEET))

    sheet = book.sheet_by_name(_DATA_SHEET)
    if sheet.ncols != 2:
        raise SourceFormatError(f"expected 2 columns in {_DATA_SHEET!r}, found {sheet.ncols}")
    if sheet.nrows < 4:
        raise SourceFormatError(f"{_DATA_SHEET!r} has no data rows")
    header = [sheet.row_values(r) for r in range(3)]
    if header[0][0] != "Back to Contents" or header[1][0] != "Sourcekey" or header[2][0] != "Date":
        raise SourceFormatError(f"unexpected header rows: {header}")
    series_key = str(header[1][1]).strip()
    if series_key != expected_series_key:
        raise SourceFormatError(
            f"file holds series {series_key!r}, contract expects {expected_series_key!r}"
        )

    points: list[tuple[date, float | None]] = []
    for r in range(3, sheet.nrows):
        date_cell, value_cell = sheet.cell(r, 0), sheet.cell(r, 1)
        if date_cell.ctype not in (xlrd.XL_CELL_DATE, xlrd.XL_CELL_NUMBER):
            raise SourceFormatError(f"row {r + 1}: date cell is not a date: {date_cell!r}")
        valid_date = xlrd.xldate_as_datetime(date_cell.value, book.datemode).date()
        if value_cell.ctype == xlrd.XL_CELL_NUMBER:
            points.append((valid_date, float(value_cell.value)))
        elif value_cell.ctype in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK):
            points.append((valid_date, None))
        else:
            raise SourceFormatError(f"row {r + 1}: value cell is not a number: {value_cell!r}")

    return ParsedSeries(
        series_key=series_key,
        title=str(header[0][1]).strip(),
        release_date=release_date,
        next_release_date=next_release_date,
        points=points,
    )
