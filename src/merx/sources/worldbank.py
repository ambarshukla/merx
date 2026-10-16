"""The World Bank's Pink Sheet: monthly benchmark prices for ~70 commodities.

One spreadsheet covers metals, agriculture and energy, which is why this is the source
that makes the catalogue cross-sector. It is also the first file here that mixes units:
$/bbl, $/mt, $/kg and $/troy oz sit side by side in one table, with the unit on its own
header row.

Because the unit is in the file, it is *checked* against the contract rather than
assumed. A benchmark quietly repriced from $/mt to $/kg would otherwise multiply a
series by a thousand and pass every range check that was wide enough to allow it.

Layout, verified against the real file on 2026-10-15 and again on 2026-10-16:
row 4 the release date, row 5 the commodity names, row 6 the units, row 7 onward the
data, with periods labelled ``1960M01``. Missing values are a placeholder glyph, not a
blank, so anything that is not a number is a gap.
"""

import re
from dataclasses import dataclass
from datetime import date

import openpyxl

SHEET = "Monthly Prices"
NAME_ROW, UNIT_ROW, FIRST_DATA_ROW = 5, 6, 7
_PERIOD = re.compile(r"^(\d{4})M(\d{1,2})$")
_RELEASE = re.compile(r"([A-Z][a-z]+)\s+(\d{1,2}),\s*(\d{4})")
_MONTHS = {
    m: i
    for i, m in enumerate(
        [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ],
        start=1,
    )
}


class PinkSheetError(ValueError):
    """The file is not shaped the way the contract expects."""


@dataclass(frozen=True)
class ParsedPinkSheet:
    # (measure, month start date, value)
    observations: list[tuple[str, date, float | None]]
    # The unit the file states for each measure, for checking against the contract.
    units: dict[str, str]
    release_date: date | None


def _clean_unit(text: object) -> str:
    """``($/mt)`` as the file writes it becomes ``USD/mt`` as reference data writes it."""
    unit = str(text or "").strip().strip("()").strip()
    return unit.replace("$", "USD", 1) if unit.startswith("$") else unit


def parse(content: bytes, columns: dict[str, str]) -> ParsedPinkSheet:
    """Read the named columns. ``columns`` maps each measure to its header in the file."""
    import io

    book = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    if SHEET not in book.sheetnames:
        raise PinkSheetError(f"no {SHEET!r} sheet; the file holds {book.sheetnames}")
    sheet = book[SHEET]
    rows = list(sheet.iter_rows(values_only=True))

    header = rows[NAME_ROW - 1]
    units_row = rows[UNIT_ROW - 1]
    # Header names carry footnote markers and stray spaces that drift between releases.
    index = {str(name).strip(): i for i, name in enumerate(header) if name}
    missing = [c for c in columns.values() if c not in index]
    if missing:
        raise PinkSheetError(
            f"columns not in the Pink Sheet: {missing} "
            f"(it has {len(index)} columns, e.g. {sorted(index)[:5]})"
        )

    release_date = None
    for cell in rows[3] if len(rows) > 3 else ():
        found = _RELEASE.search(str(cell or ""))
        if found and found.group(1) in _MONTHS:
            release_date = date(int(found.group(3)), _MONTHS[found.group(1)], int(found.group(2)))
            break

    units = {m: _clean_unit(units_row[index[c]]) for m, c in columns.items()}
    observations: list[tuple[str, date, float | None]] = []
    for row in rows[FIRST_DATA_ROW - 1 :]:
        label = str(row[0] or "").strip()
        period = _PERIOD.match(label)
        if not period:
            continue  # trailing notes and blank rows
        month_start = date(int(period.group(1)), int(period.group(2)), 1)
        for measure, column in columns.items():
            cell = row[index[column]]
            value = float(cell) if isinstance(cell, int | float) else None
            observations.append((measure, month_start, value))

    if not observations:
        raise PinkSheetError("no monthly rows found; the period labels may have changed")
    return ParsedPinkSheet(observations=observations, units=units, release_date=release_date)
