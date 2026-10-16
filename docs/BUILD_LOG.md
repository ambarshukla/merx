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

## 2026-10-15: the publish gate and exceptions as data

**Built.** Staged builds with ids (`meta.stage_builds`), checks run inside the staging
step, a check-result log (`ops.check_results`), exceptions with owners, occurrences,
open and close times and root-cause classes (`ops.exceptions`), the gate
(`merx/engine/publish.py`), a `published/<id>` asset per contract, and a publication log
(`meta.publications`). Decision D-009. 96 tests.

**Found while testing.** With checks as separate Dagster steps, a tier 3 publish ran
before its checks finished and was refused as "unchecked", and Dagster's blocking
skipped a tier 1 publish without leaving a record. Running the checks in the staging
step fixed both (D-009).

**Live run (all eight datasets).** Prediction: eight publications, seven `pass` and one
`warn` (futures freshness), one open exception. Result: exactly that. The exception
needed a root-cause class the list did not have, so `source_discontinued` was added and
assigned, with a note that the series ends 2024-04-05.

## 2026-10-15: point-in-time history, backfilled from the Wayback Machine

**Built.** `pit."<id>"` for every dataset: each version of each value with
`known_from_utc`, `known_to_utc` and how it ended (`revised`, `withdrawn`), rebuilt by
replaying every raw file; "as known at" and revision queries; an `archive/<id>` asset
that imports dated copies from the Internet Archive's Wayback Machine (opt-in per
contract; enabled for WTI spot, US crude stocks and US gas storage); `origin` and
`archive_url` in the manifest. Staging never reads archived copies. Decision D-010.
104 tests.

**Probed first.** The archive holds 15 distinct copies of the gas storage file
(2015-2026), 36 of crude stocks (2011-2026) and 51 of WTI spot (2010-2026). Every one
of them parses with today's strict parser: the layout has not changed since 2011.

**Live import.** 95 of 102 copies landed; the archive refused 7 requests (rerunning the
import retries them). One copy, captured in 2022, was stored gzip-compressed; the
reader now undoes a gzip layer before parsing, and the raw file stays as received.

**What the history shows** (predictions in brackets, written first):

| Dataset | Raw files | Known since | Revised values | Withdrawn |
|---|---|---|---|---|
| US gas in storage | 15 | 2015-12-27 | 7 (about 7) | 0 |
| US crude stocks | 34 | 2011-10-19 | 1,753 (about 1,750) | 0 |
| WTI spot | 52 | 2010-07-31 | 17 (under 20) | **744 (not predicted)** |
| Five datasets with no archive | 2 to 17 | 2026-10-15 | 0 (0) | 0 |

- **Gas storage:** four weeks of mid-2017 revised down by about 10 Bcf (first seen in a
  2019 copy), one week of 2021 up by 4, two weeks of 2024 up by 1.
- **Crude stocks:** between the copies of 2016-05-08 and 2016-11-12, every weekly value
  from 1982-08-20 to 2016-04-29 was revised down, by 15.7 to 34.1 million barrels
  (median 18.7). A whole-history change of this shape is a redefinition of the series,
  not a set of corrections; the reason has not been verified here.
- **WTI spot:** 17 daily prices of December 2011 revised by a few cents (first seen in a
  copy of 2012-11-06).
- **WTI spot, 744 values withdrawn.** Between the copies of 2026-09-05 (503,808 bytes)
  and 2026-10-11 (471,040 bytes), 744 daily prices from 1986-01-31 to 2019-11-11
  disappeared from the spreadsheet; 689 of them had been in it since the earliest copy
  (2010). They fall on every weekday, and only 20 repeat the previous day's price.
  **EIA's API still returns them** (checked: 1986-01-31 = 18.95, 1986-02-10 = 16.78,
  2019-11-11 = 56.61, identical to the withdrawn values), and its WTI series has 10,272
  rows against the spreadsheet's 9,523. The two channels of the same source disagree.

**The gap this exposed in our own controls.** The staged WTI table, built from the
spreadsheet, is missing those 744 days, and every check passed: not-null, uniqueness,
range and freshness all judge only the rows that are present. Silent disappearance needs
a different kind of check. Next: a check that fails when a build withdraws previously
known values (the point-in-time store already detects them), and a reconciliation of
the spreadsheet against the API.

## 2026-10-16: no silent withdrawals, and an operations command

**Built.** The `no_withdrawals` check (D-011), now in every contract; acceptances in
`ops.accepted_withdrawals`; and `merx`, a command for the operations tasks a person does
by hand: list exceptions, assign a root cause, list and accept missing values, list
revisions, and show a dataset as known at any moment. 108 tests.

**Live run.** Prediction: WTI fails with about 744 missing values and is blocked;
everything else passes. Result: exactly that. Exception 2 (`eia_wti_spot`,
`no_withdrawals`) was classified `source_data_error`; the values were deliberately not
accepted, pending a reconciliation against EIA's API.

## 2026-10-16: EIA's API as a second channel, reconciled with the spreadsheet

**Built.** A parser and paged fetcher for EIA's API v2, the `eia_api_wti_spot`
contract, the `reconcile_with` field and `reconcile/<id>` asset (D-012), and a small
`.env` loader so scripts and tests see the key as `dagster dev` does. 113 tests,
including one that checks the key is in neither the manifest nor any raw file.

**First run** (predictions in brackets). The API returned 10,272 daily values,
1986-01-02 to 2026-10-14 (10,272). Against the spreadsheet's 9,523:

| Class | Count | Prediction |
|---|---|---|
| match | 9,515 | |
| value_differs | **0** | 0 |
| only_there (API only) | 757 | about 749 |
| only_here (spreadsheet only) | **8** | 0 |
| outside_range | 0 | a few |

- **Where both channels have a date, they agree exactly.** The disagreement is entirely
  about which dates exist.
- **744 of the 757 API-only dates are the values the spreadsheet dropped** in
  September or October 2026. The other **13 are US public holidays** (Thanksgiving,
  Christmas, New Year's Day, Labor Day, Martin Luther King Day, Presidents' Day,
  Memorial Day, Columbus Day, Veterans Day, Independence Day observed) on which the
  spreadsheet has never had a value. On 10 of them the API's value repeats the previous
  trading day exactly, a carried-forward fill rather than a traded price; on 3 it does not.
- **8 dates are in the spreadsheet and not in the API**, all of them Tuesdays, between
  2014-09-30 and 2024-04-16. No explanation is offered here.
- No stored URL or raw file contains the API key.

WTI spot stays blocked at the gate (D-011) until someone decides between accepting the
744 losses and repairing them from the API.

## 2026-10-16: the operations console and the client view

**Built.** `merx.api`: a FastAPI app (D-013) with `/ops` and `/client`, a read-only JSON
API under `/api` (documented at `/api/docs`), a dependency-free SVG line chart, and
`merx serve`. 119 tests, including one that the client cannot read unpublished data and
one that an empty data directory is read without being created.

**Ops console** (`/ops`): nine datasets; seven published and passing, one published with
a warning (futures, stale), one blocked (WTI spot, 744 missing values); two open
exceptions with owners and root causes; 142 raw files kept (93.1 MB). Selecting a
dataset shows its checks on the current build, the values the history knows and the
build lacks, and its reconciliation breaks.

**Client view** (`/client`): only published series, each with its quality status. WTI
spot shows its last passing publication with a note that newer data is held back. "As
known on" overlays the series as it stood on any date: US crude stocks as known on
2016-06-01 sit about 20 million barrels above today's values all the way back to 2011,
the 2016 redefinition made visible. A revisions table lists every changed or removed
value. Links can carry the series and date, e.g.
`/client?dataset=eia_us_crude_stocks&asof=2016-06-01`.

**Found while building.** "As known on 2015-01-01" for crude stocks only reached
2012-11-30: the archive holds no copies from 2013 or 2014, so nothing newer was known to
the history then. Correct, but a reminder that archive-based history is only as dense as
the archive's copies.
