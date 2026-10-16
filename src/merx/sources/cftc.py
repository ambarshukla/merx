"""Parser for the CFTC's yearly "Disaggregated Futures Only" history files
(``https://www.cftc.gov/files/dea/history/fut_disagg_txt_<year>.zip``).

Layout observed 2026-10-15 across 2010 to 2026:

* a zip holding one comma-separated text file (named ``f_year.txt`` whatever the year)
  with a header row and 191 columns, one row per market per weekly report;
* numbers padded with spaces;
* the market is identified by ``CFTC_Contract_Market_Code``. Its display name changes
  over time (code 067651 was "CRUDE OIL, LIGHT SWEET" up to the report of 2022-02-01
  and "WTI-PHYSICAL" from 2022-02-08), so the name is never used to match;
* the report date column is ``Report_Date_as_YYYY-MM-DD`` from 2013, but
  ``Report_Date_as_MM_DD_YYYY`` in 2010 to 2012, where it nevertheless holds
  ``YYYY-MM-DD`` values. Either name is accepted, the value must be ISO, and it must
  agree with ``As_of_Date_In_Form_YYMMDD``;
* the CFTC's own header spells two swap-dealer columns with a double underscore
  (``Swap__Positions_Short_All``). Contracts name columns exactly as the file does.

Anything else raises SourceFormatError.
"""

import csv
import io
import zipfile
from dataclasses import dataclass
from datetime import date, datetime

from merx.sources import SourceFormatError

_DATE_COLUMNS = ("Report_Date_as_YYYY-MM-DD", "Report_Date_as_MM_DD_YYYY")
_CODE = "CFTC_Contract_Market_Code"
_NAME = "Market_and_Exchange_Names"
_YYMMDD = "As_of_Date_In_Form_YYMMDD"


@dataclass(frozen=True)
class ParsedPositions:
    # (measure, report_date, value); value is None where the cell is empty.
    observations: list[tuple[str, date, float | None]]
    market_names: list[str]  # every display name the market had in this file


def _number(raw: str, where: str) -> float | None:
    text = raw.strip()
    if not text:
        return None
    try:
        return float(int(text))
    except ValueError:
        raise SourceFormatError(f"{where}: not a whole number: {raw!r}") from None


def parse(content: bytes, market_code: str, columns: dict[str, str]) -> ParsedPositions:
    try:
        archive = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile as e:
        raise SourceFormatError(f"not a zip file: {e}") from e
    members = archive.namelist()
    if len(members) != 1 or not members[0].lower().endswith(".txt"):
        raise SourceFormatError(f"expected one .txt file in the zip, found {members}")

    try:
        # Read the whole member up front: a damaged archive often fails only at the end,
        # on its checksum, and must not be half-parsed first.
        text = archive.read(members[0]).decode("latin-1")
    except zipfile.BadZipFile as e:
        raise SourceFormatError(f"damaged zip: {e}") from e
    reader = csv.DictReader(io.StringIO(text))
    header = reader.fieldnames or []
    date_column = next((c for c in _DATE_COLUMNS if c in header), None)
    if date_column is None:
        raise SourceFormatError(f"no report date column; expected one of {_DATE_COLUMNS}")
    missing = [c for c in (_CODE, _NAME, _YYMMDD, *columns.values()) if c not in header]
    if missing:
        raise SourceFormatError(f"columns not in file: {missing}")

    observations: list[tuple[str, date, float | None]] = []
    names: set[str] = set()
    for line, row in enumerate(reader, start=2):
        if row[_CODE].strip() != market_code:
            continue
        where = f"line {line}"
        try:
            report_date = date.fromisoformat(row[date_column].strip())
        except ValueError:
            raise SourceFormatError(
                f"{where}: report date not YYYY-MM-DD: {row[date_column]!r}"
            ) from None
        try:
            as_of = datetime.strptime(row[_YYMMDD].strip(), "%y%m%d").date()
        except ValueError:
            raise SourceFormatError(f"{where}: unreadable {_YYMMDD}: {row[_YYMMDD]!r}") from None
        if as_of != report_date:
            raise SourceFormatError(f"{where}: {date_column} {report_date} != {_YYMMDD} {as_of}")
        names.add(row[_NAME].strip())
        for measure, column in columns.items():
            observations.append((measure, report_date, _number(row[column], where)))
    return ParsedPositions(observations=observations, market_names=sorted(names))
