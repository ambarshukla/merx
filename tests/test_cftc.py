"""The CFTC parser, partitioned raw files, the balance check, and a partitioned pipeline run.

Inputs are synthetic zips shaped like the CFTC's yearly files (observed 2026-10-15):
padded numbers, the old and new names of the date column, the double-underscore column
names, and a market whose display name differs between years.
"""

import csv
import io
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import dagster as dg
import duckdb
import pytest
from test_pipeline import FileFetcher

from merx.config import CONTRACTS_DIR
from merx.contracts import load_contract
from merx.contracts.model import BalanceCheck
from merx.engine.checks import run_check
from merx.orchestration.factory import build_assets, build_checks, report_year
from merx.orchestration.resources import DataStore
from merx.reference import load_reference
from merx.sources import SourceFormatError
from merx.sources.cftc import parse
from merx.store import connect
from merx.store.raw import RawZone

CONTRACT = load_contract(CONTRACTS_DIR / "cftc_wti_positions.yaml")
COLUMNS = dict(CONTRACT.source.columns)
LONGS = ["producer_long", "swap_long", "managed_money_long", "other_reportable_long"]
LONGS += ["nonreportable_long"]
SHORTS = ["producer_short", "swap_short", "managed_money_short", "other_reportable_short"]
SHORTS += ["nonreportable_short"]
SPREADS = ["swap_spread", "managed_money_spread", "other_reportable_spread"]


def positions(scale: int = 1) -> dict[str, int]:
    """One week of positions that satisfy both open-interest identities."""
    p = dict.fromkeys(LONGS, 100 * scale) | dict.fromkeys(SHORTS, 100 * scale)
    p |= dict.fromkeys(SPREADS, 10 * scale)
    p["open_interest"] = 5 * 100 * scale + 3 * 10 * scale
    return p


def cftc_zip(
    weeks: list[tuple[str, date, str, dict[str, int]]],
    *,
    date_column: str = "Report_Date_as_YYYY-MM-DD",
    drop: tuple[str, ...] = (),
    members: tuple[str, ...] = ("f_year.txt",),
    yymmdd_offset_days: int = 0,
) -> bytes:
    """weeks: (market code, report date, market name, {measure: value})."""
    header = [
        "Market_and_Exchange_Names",
        "As_of_Date_In_Form_YYMMDD",
        date_column,
        "CFTC_Contract_Market_Code",
        *COLUMNS.values(),
    ]
    header = [h for h in header if h not in drop]
    text = io.StringIO()
    writer = csv.writer(text)
    writer.writerow(header)
    for code, d, name, values in weeks:
        as_of = date.fromordinal(d.toordinal() + yymmdd_offset_days)
        row = {
            "Market_and_Exchange_Names": name + " ",
            "As_of_Date_In_Form_YYMMDD": as_of.strftime("%y%m%d") + " ",
            date_column: d.isoformat(),
            "CFTC_Contract_Market_Code": code + " ",
            **{COLUMNS[m]: f"{v:>9}" for m, v in values.items()},
        }
        writer.writerow([row.get(h, "") for h in header])
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        for member in members:
            z.writestr(member, text.getvalue())
    return buffer.getvalue()


WTI_OLD_NAME = "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE"
WTI_NEW_NAME = "WTI-PHYSICAL - NEW YORK MERCANTILE EXCHANGE"


# --- parser ------------------------------------------------------------------------


def test_parses_one_market_by_code_and_ignores_others():
    content = cftc_zip(
        [
            ("067651", date(2026, 10, 6), WTI_NEW_NAME, positions()),
            (
                "023651",
                date(2026, 10, 6),
                "NAT GAS NYME - NEW YORK MERCANTILE EXCHANGE",
                positions(7),
            ),
        ]
    )
    parsed = parse(content, "067651", COLUMNS)
    values = {m: v for m, _, v in parsed.observations}
    assert values["open_interest"] == 530.0
    assert values["swap_short"] == 100.0  # read from the double-underscore column
    assert len(parsed.observations) == len(COLUMNS)
    assert parsed.market_names == [WTI_NEW_NAME]


def test_accepts_the_old_date_column_name_and_reports_name_changes():
    content = cftc_zip(
        [
            ("067651", date(2010, 12, 28), WTI_OLD_NAME, positions()),
            ("067651", date(2010, 12, 21), WTI_NEW_NAME, positions()),
        ],
        date_column="Report_Date_as_MM_DD_YYYY",
    )
    parsed = parse(content, "067651", COLUMNS)
    assert {d for _, d, _ in parsed.observations} == {date(2010, 12, 28), date(2010, 12, 21)}
    assert parsed.market_names == [WTI_OLD_NAME, WTI_NEW_NAME]


def test_report_date_must_agree_with_the_yymmdd_column():
    content = cftc_zip(
        [("067651", date(2026, 10, 6), WTI_NEW_NAME, positions())], yymmdd_offset_days=1
    )
    with pytest.raises(SourceFormatError, match="!="):
        parse(content, "067651", COLUMNS)


def test_missing_column_fails():
    content = cftc_zip(
        [("067651", date(2026, 10, 6), WTI_NEW_NAME, positions())],
        drop=("M_Money_Positions_Long_All",),
    )
    with pytest.raises(SourceFormatError, match="M_Money_Positions_Long_All"):
        parse(content, "067651", COLUMNS)


def test_unknown_date_column_fails():
    content = cftc_zip(
        [("067651", date(2026, 10, 6), WTI_NEW_NAME, positions())], date_column="Report_Date"
    )
    with pytest.raises(SourceFormatError, match="no report date column"):
        parse(content, "067651", COLUMNS)


def test_zip_must_hold_exactly_one_text_file():
    content = cftc_zip([], members=("a.txt", "b.txt"))
    with pytest.raises(SourceFormatError, match="one .txt file"):
        parse(content, "067651", COLUMNS)


def test_not_a_zip_fails():
    with pytest.raises(SourceFormatError, match="not a zip"):
        parse(b"<html>maintenance</html>", "067651", COLUMNS)


def test_non_integer_position_fails():
    p = positions() | {"managed_money_long": "10.5"}
    content = cftc_zip([("067651", date(2026, 10, 6), WTI_NEW_NAME, p)])
    with pytest.raises(SourceFormatError, match="not a whole number"):
        parse(content, "067651", COLUMNS)


def test_damaged_zip_is_a_format_error():
    content = cftc_zip([("067651", date(2026, 10, 6), WTI_NEW_NAME, positions())])
    # Change one byte of the compressed data: the archive opens but fails its checksum.
    damaged = content.replace(b"      100", b"      101", 1)
    assert damaged != content
    with pytest.raises(SourceFormatError, match="damaged zip"):
        parse(damaged, "067651", COLUMNS)


# --- balance check -----------------------------------------------------------------


def long_balance() -> BalanceCheck:
    return BalanceCheck(type="balance", name="longs", total="open_interest", parts=LONGS + SPREADS)


def positions_table(weeks: dict[date, dict[str, int]]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE TABLE t (measure VARCHAR, valid_date DATE, value DOUBLE)")
    for d, p in weeks.items():
        con.executemany("INSERT INTO t VALUES (?, ?, ?)", [(m, d, v) for m, v in p.items()])
    return con


def test_balance_passes_when_the_identity_holds():
    con = positions_table({date(2026, 10, 6): positions(), date(2026, 10, 13): positions(2)})
    assert run_check(con, "t", long_balance(), date(2026, 10, 15)).passed


def test_balance_fails_and_shows_the_broken_date():
    broken = positions() | {"managed_money_long": 99}
    con = positions_table({date(2026, 10, 6): positions(), date(2026, 10, 13): broken})
    outcome = run_check(con, "t", long_balance(), date(2026, 10, 15))
    assert not outcome.passed
    assert outcome.metadata["examples"] == [["2026-10-13", 530.0, 529.0, 8]]


def test_balance_fails_when_a_part_is_missing():
    incomplete = {k: v for k, v in positions().items() if k != "swap_spread"}
    con = positions_table({date(2026, 10, 6): incomplete})
    assert not run_check(con, "t", long_balance(), date(2026, 10, 15)).passed


# --- partitions and schedules -------------------------------------------------------


def test_raw_zone_keeps_the_newest_file_per_partition(tmp_path):
    zone = RawZone(tmp_path)
    for when, partition, content in [
        (datetime(2026, 10, 2, tzinfo=UTC), "2026", b"old 2026"),
        (datetime(2026, 10, 9, tzinfo=UTC), "2026", b"new 2026"),
        (datetime(2026, 10, 9, tzinfo=UTC), "2025", b"2025"),
    ]:
        zone.land(
            dataset_id="cftc_wti_positions",
            provider="cftc",
            url=f"https://example.test/{partition}.zip",
            content=content,
            http_status=200,
            headers={},
            partition=partition,
            fetched_at_utc=when,
        )
    latest = zone.latest_per_partition("cftc_wti_positions")
    assert [(f.partition, zone.read(f)) for f in latest] == [
        ("2025", b"2025"),
        ("2026", b"new 2026"),
    ]
    assert "/2026/" in latest[1].path


@pytest.mark.parametrize(
    ("when", "year"),
    [
        (datetime(2027, 1, 1, 18), "2026"),  # Friday: latest report Tuesday 2026-12-29
        (datetime(2027, 1, 8, 18), "2027"),  # Friday: latest report Tuesday 2027-01-05
        (datetime(2026, 10, 13, 9), "2026"),  # a Tuesday counts as its own report date
    ],
)
def test_report_year_follows_the_latest_tuesday(when, year):
    assert report_year(when) == year


# --- partitioned pipeline -----------------------------------------------------------


def run_partitions(tmp_path: Path, files: dict[str, bytes]) -> list[dg.ExecuteInProcessResult]:
    urls = {}
    for year, content in files.items():
        path = tmp_path / f"{year}.zip"
        path.write_bytes(content)
        urls[CONTRACT.source.url.replace("{year}", year)] = str(path)
    assets = [*build_assets(CONTRACT, load_reference()), *build_checks(CONTRACT)]
    resources = {
        "store": DataStore(data_dir=str(tmp_path / "data")),
        "fetcher": FileFetcher(files=urls),
    }
    raw_only = [assets[0]]
    results = [
        dg.materialize(raw_only, partition_key=year, resources=resources, raise_on_error=False)
        for year in files
    ]
    staged_and_checks = assets[1:]
    results.append(
        dg.materialize(
            staged_and_checks,
            resources=resources,
            selection=dg.AssetSelection.assets(assets[1]),
            raise_on_error=False,
        )
    )
    return results


def test_two_yearly_files_stage_into_one_table(tmp_path):
    results = run_partitions(
        tmp_path,
        {
            "2010": cftc_zip(
                [("067651", date(2010, 12, 28), WTI_OLD_NAME, positions())],
                date_column="Report_Date_as_MM_DD_YYYY",
            ),
            "2026": cftc_zip([("067651", date(2026, 10, 6), WTI_NEW_NAME, positions(2))]),
        },
    )
    assert all(r.success for r in results)
    checks = {e.check_name: e.passed for e in results[-1].get_asset_check_evaluations()}
    assert checks["balance_longs"] and checks["balance_shorts"]
    con = connect(tmp_path / "data")
    try:
        rows = con.execute(
            'SELECT valid_date, value FROM staged."cftc_wti_positions" '
            "WHERE measure = 'open_interest' ORDER BY 1"
        ).fetchall()
        partitions = con.execute(
            "SELECT DISTINCT partition FROM meta.raw_manifest ORDER BY 1"
        ).fetchall()
    finally:
        con.close()
    assert rows == [(date(2010, 12, 28), 530.0), (date(2026, 10, 6), 1060.0)]
    assert partitions == [("2010",), ("2026",)]
