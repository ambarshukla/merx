# Build log

Dated, newest last. What was built, what was verified, and what surprised us.

## 2026-10-14: repository scaffolded

Project skeleton only: README, licence, ignore rules, and the three docs files.
No code yet. Decision D-001 records the founding design choice.

## 2026-10-14: provenance statement and environment template

Added `docs/PROVENANCE.md` (public sources only, nothing fetched is committed,
synthetic parts labelled, no confidential information), a provenance section in the
README, decision D-002, and `.env.example` with placeholders. Documentation only.

## 2026-10-15: Phase 0 foundations

**Built.** The `merx` Python package; five EIA dataset contracts; reference data
(instruments, units, crosswalk); the raw landing zone with its manifest; a strict
parser for EIA's keyless spreadsheet downloads; staging into DuckDB; four check types
(not null, unique, range, freshness); the Dagster factory, definitions and schedules;
41 tests; CI (ruff format, ruff lint, pytest). Decisions D-003 to D-005.

**Verified on this machine.** On Windows with Python 3.14, duckdb 1.5.6, pyarrow 26,
dagster 1.13.26, xlrd 2.0.2 and xlwt 1.3.0 install, import and run.
`dagster definitions validate` passes.

**First live run (all five datasets, 2026-10-15).** Predictions were written first.
All matched:

| Dataset | Rows | Range of dates | Checks |
|---|---|---|---|
| WTI spot | 9,523 | 1986-01-02 to 2026-10-14 | all pass; minimum -36.98 on 2020-04-20 |
| Brent spot | 9,108 | 1987-05-20 to 2026-10-14 | all pass |
| Henry Hub spot | 5,709 | 1997-01-07 to 2026-10-14 | all pass; one accepted gap |
| WTI futures, contract 1 | 10,183 | 1983-04-04 to 2024-04-05 | freshness fails (warning): 923 days old |
| US crude stocks ex SPR | 2,298 | 1982-08-20 to 2026-10-09 | all pass |

Five downloads, 1.8 MB in total.

**What the real files taught us.**
- **WTI went negative in the data, not just in the news.** The spot file holds -36.98
  on 2020-04-20, its only negative value. The futures file's minimum is -37.63. Spot
  and futures are different series with different numbers for the same day.
- **A file can be refreshed while its data is not.** The futures file carries today's
  release date, but its newest value is 2024-04-05. Only a check on the data (the
  freshness check) notices; a check on the file's date would not. The contract keeps
  the check, at warning severity, as a standing example.
- **The source has real gaps.** Henry Hub has no value for 2018-01-05. The contract
  accepts that one date by name, so any new gap still fails.
- **The release calendar is in the file.** Each download's `Contents` sheet states its
  release date and next release date. Both are now logged per download in
  `meta.release_log`, ready for timeliness checks.
- **Names drift; codes do not.** The positioning report for the main US crude futures
  market (CFTC code 067651) now names it "WTI-PHYSICAL". The crosswalk maps the code.
- **Real spikes exist.** Henry Hub printed 30.72 on 2026-01-23, the series high. It is
  inside the range check. It will be a test case for the spike detectors: a real event
  that a naive detector would flag as a defect.

## 2026-10-15: first onboarding by configuration alone (US gas storage)

Added `eia_us_gas_storage` (Lower 48 working gas in storage, weekly) as **one contract
file (25 lines) and three reference rows (instrument, unit, crosswalk), with no code
changes**. It was the first dataset added after the engine existed, so it is the first
real test of D-001. The source's layout had been probed beforehand and matched the
existing EIA parser exactly.

Live run: 876 weekly values from 2010-01-01 to 2026-10-09, between 824 and 4,047 Bcf,
all four checks pass. Prediction (written first): same layout, about 876 rows, all
pass. Matched.

How we will report time to onboard: as *what had to change* (files, lines, code or
none). Wall-clock times would mostly measure who did the typing. A schedule test that
listed exact schedule names had to change too; it now derives them from the
contracts, so the next dataset will not touch it.

## 2026-10-15: long form for every staged table

Staged tables now have a `measure` column (D-006); each contract names its measures
(`price`, `stocks`, `working_gas`). Reading a file is now a per-format reader in
`merx.engine.formats`. Checks report the measure in their evidence, a range can be
scoped to one measure, and freshness looks at the stalest measure. 47 tests.

Live rerun of all six datasets: identical row counts, date ranges and extremes to the
previous run, and the same single failing check (futures freshness, a warning).
Prediction: identical. Matched.

## 2026-10-15: the data-quality lab and detector bake-off

**Built.** `merx.quality`: seeded defects with a ground-truth manifest (five types),
five detectors (static band, robust z, seasonal, repeat run, and suites), scoring
(recall, precision, alerts per year on real data), a lab runner writing `lab.*` tables,
a Markdown report, and the Dagster asset `lab/detector_bakeoff` (65 seconds for six
series, five seeds, 15 detector settings). 66 tests. Decision D-007; full write-up in
[`DQ_LAB.md`](DQ_LAB.md).

**Predictions against results.** Static band precision exactly 50%, as predicted; no
change-based detector catches stale runs, as predicted; robust z precision 55% against a
prediction of over 90%, which exposed two flaws in the design (an error longer than the
reset is adopted and its recovery flagged; stale values shrink the yardstick). Version 2
fixed both: precision 83% to 87% on every price series. On gas in storage it stays
low (26%), and the reason is structural, not a bug: change-based detectors are fragile on
seasonal series. The seasonal suite is the best overall: 87% recall, 87% precision,
2.4 alerts a year per series.

**Real events, correctly flagged by every detector on untouched data:** WTI spot
-36.98 on 2020-04-20, and Henry Hub 30.72 on 2026-01-23.

## 2026-10-15: CFTC positioning, partitioned by year

**Built.** A parser for the CFTC's yearly disaggregated futures files, year partitions
for raw assets (D-008), a `balance` check, and two contracts: WTI and Henry Hub
futures positions by trader group, 14 measures each. A manifest column records each
download's partition. 87 tests.

**Probed first, then built on what was found** (2026-10-15):
- The current year's zip is updated weekly (last modified Friday 2026-10-09, holding the
  report of Tuesday 2026-10-06), so yearly files alone cover history and the present.
- **The report date column changed name between 2012 and 2013**, from
  `Report_Date_as_MM_DD_YYYY` to `Report_Date_as_YYYY-MM-DD`, and the old name was
  wrong about its own contents: its values were already `YYYY-MM-DD`. The parser
  accepts either name, insists on ISO values, and cross-checks each date against the
  separate `YYMMDD` column.
- The CFTC's own header spells two swap-dealer columns with a double underscore
  (`Swap__Positions_Short_All`). The contract names columns exactly as the file does.
- Market display names drift; codes do not. WTI (067651) was "CRUDE OIL, LIGHT
  SWEET" up to the report of 2022-02-01 and "WTI-PHYSICAL" from 2022-02-08; Henry Hub
  (023651) moved from "NATURAL GAS" to "NAT GAS NYME".

**Live backfill.** 34 yearly files (2010 to 2026, both markets), 60.6 MB. Each market:
875 weekly reports from 2010-01-05 to 2026-10-06, 12,250 rows, no gaps. **Both
open-interest identities hold in every one of the 875 weeks for both markets.**
Predictions (written first): about 875 weeks, at least two display names per market,
identities hold, freshness passes. All matched.

**Found while testing.** A damaged zip (bad checksum) escaped as Python's own
`BadZipFile` rather than a format error. The parser now reads the whole file up front
and reports damage as a `SourceFormatError`.
