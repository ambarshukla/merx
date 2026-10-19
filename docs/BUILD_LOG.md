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

## 2026-10-16: Brent and Henry Hub through a second channel; the losses are systematic

**Built, by configuration only.** Two contracts (`eia_api_brent_spot`,
`eia_api_henry_hub_spot`) and three lines each on the spreadsheet contracts
(`reconcile_with`, `archive_backfill`). No code changed. 119 tests.

**Probed first.** API rows against spreadsheet rows (2026-10-16): crude stocks 2,298 =
2,298 and gas storage 876 = 876 (weekly series agree), but Brent 9,993 against 9,108 and
Henry Hub 7,484 against 5,709 over the same span (both start 1997-01-07 / 1987-05-20).
Prediction before the probe was that WTI's loss was a one-off. It was not.

**Live run.**

| | WTI spot | Brent spot | Henry Hub spot |
|---|---|---|---|
| Archive copies | 50 of 51 | 47 of 50 | 39 of 39 |
| Values withdrawn in the big loss | 744 (1986-01-31 to 2019-11-11) | 890 (1987-12-21 to 2014-10-14) | 1,768 (1997-01-22 to 2011-07-12) |
| When (between) | 2026-09-05 and 2026-10-11 copies | 2026-08-30 and 2026-09-17 copies | 2026-05-05 copy and our 2026-10-15 download |
| Still in the API | all 744 | 892 of 894 withdrawn | all |
| Reconciliation: match / value differs | 9,515 / 0 | 9,096 / **3** | 5,707 / 0 |
| API only (besides the withdrawn) | 13 US holidays | 8 holidays, incl. UK bank holidays | (counted with the withdrawn) |
| Spreadsheet only | 8 | 9 | 2 |

- **All three daily price spreadsheets lost large parts of their older history within the
  same few months of 2026**, while the API kept them. For Henry Hub, the spreadsheet now
  holds 58 to 99 values a year for 1998-2006 against about 250 in the API. The cause is
  not known and is not guessed here.
- **The API's own gaps repeat across series**: WTI and Brent lack the same dates
  (2014-09-30 to 2014-10-02, 2022-10-25, 2022-12-13, 2023-03-07, 2024-02-06, 2024-04-16)
  that their spreadsheets have.
- **Three real value disagreements** (Brent 2016-12-28, 2016-12-29, 2018-04-16): the
  first, after more than 24,000 shared dates across the three series.
- **A precision change**: 57 of Henry Hub's 59 revisions happened together in a copy of
  2013-04-21, when three-decimal prices were rounded to two (2.975 became 2.98).
- Henry Hub's 2018-01-05 gap is in both channels; the API contract accepts it by name, and
  the exception it had opened closed on the next run (time to recover: under a minute).

**Gate state:** WTI, Brent and Henry Hub spot (all tier 1) are blocked by
`no_withdrawals`; exceptions 2, 3 and 5 carry the root cause `source_data_error`; the
withdrawn values are not accepted, pending a decision on repairing them from the API.

**Not every withdrawal is an error.** Henry Hub's eight earlier, scattered withdrawals
are non-trading days: six Sundays removed in a copy of 2025-02-03 (2020-03-01 to
2020-03-29, 2024-06-16) and two US holidays (Memorial Day 2015, Veterans Day 2019). They
look like the source cleaning up, unlike the bulk losses of 2026. The check flags both
kinds the same way, which is why accepting a withdrawal is a person's decision with a
reason, not an automatic rule.

## 2026-10-16: the lab's detectors in production

**Built.** The `anomaly` check (D-014) and named detector settings; every EIA contract now
names its detector (seasonal suite, or robust suite for Henry Hub). 120 tests.

**First run** (prediction: no recent flags on most series, one or two on Henry Hub).
Henry Hub: none, both channels. Crude stocks, gas storage, futures: none. Oil: one recent
flag each, identical in both channels: **Brent 2026-10-02 (135.51, up 18% from 114.82; the
level stayed near 125-130 afterwards)** and **WTI 2026-09-28 (99.37, a rebound from 85.23
on 2026-09-25)**. Both channels carry the same values, so these look like market moves,
not data errors. For WTI the detector flagged the rebound rather than the dip before it:
the dip was not extreme against a volatile window, the rebound from an accepted low was. A
flag points a person *near* something worth a look; it does not say which value is wrong.
Prediction partly held (most series clean), partly not (oil flagged, gas not).

## 2026-10-16: the "Two sources" page

**Built.** `/sources`: for each series carried by two channels (WTI, Brent, Henry Hub),
both channels on one chart, with the spreadsheet's line broken where it lost dates and a
row of ticks per class of difference under it; every difference grouped in plain words
(dropped from the spreadsheet, only ever in the API, holiday fills, only in the
spreadsheet, values that differ), each clickable to list its dates; and when the history
saw the spreadsheet lose values, bounded by the two copies either side. Each
reconciliation's counts are now kept in `meta.reconciliations`, so matches are shown
too. 121 tests.

**What it shows.** Across the three series: 24,318 dates agree exactly, 3 values differ,
3,434 dates exist only in the API and 19 only in the spreadsheets. For Henry Hub, the
1,768 dropped prices form a solid band from 1997 to 2011, removed between the archive
copy of 2026-05-05 and the first download on 2026-10-15.

## 2026-10-16: hosted at merx.ambarshukla.dev

**Built.** A container image, `deploy/docker-compose.yml` (web, Dagster UI, refresh),
`merx refresh` (the jobs due today, run through Dagster), a systemd timer, and
`deploy/README.md`. Decision D-015. CFTC reuse terms confirmed (public domain,
acknowledgement requested); both sources are credited on the client pages. 127 tests.

**Deployed and checked.** The image builds on the host (Python 3.14, 781 MB); the
seeded data (144 MB, including every archived copy) reads correctly in Linux
containers. The first `merx refresh`, started by hand, downloaded the day's files and ran
the daily and CFTC jobs; the only failed steps were the three expected blocked
publications. The proxy obtained Let's Encrypt certificates as soon as DNS resolved;
`https://merx.ambarshukla.dev/client`, `/sources` and `/ops` answer 200, and the internal
Dagster UI answers 401 without the password and 200 with it.

**Found while deploying.** Dagster's webserver exits if `DAGSTER_HOME` does not exist
(it did not on the fresh data directory); the image now creates it on start. Pages
answered `HEAD` requests with 405, which link previews and uptime monitors use; they now
answer 200. The chart's unit label overlapped the top axis value, and zero was printed
as "-0"; both fixed.

## 2026-10-16: merging to main deploys

**Built.** A `deploy` job in the CI workflow, gated on the test job and on `main`, with a
concurrency group so two merges queue rather than build over each other; a smoke test
against the public URL; `deploy/README.md` rewritten around the new flow; three tests
that assert the job's guards as data. The host's `/opt/merx/src` became a clone of the
public repository, and a root-owned `/opt/merx/bin/deploy.sh` is pinned as the forced
command for a key that can do nothing else. Decision D-016. 130 tests.

**Checked.** The conversion of the host's source directory to a clone reported no
difference between the deployed copy and `origin/main`, which confirms that the previous
copy-based deploy had landed exactly the commit `DEPLOYED_COMMIT` claimed. A deploy was
then run through the restricted key end to end: the key was refused when asked for
anything but `deploy`, the image rebuilt, both containers were recreated, and the commit
was recorded.

**Found while deploying.** Asking for the site immediately after that deploy returned 502:
the containers were `running` while the server was still binding its port. "Running" is
not "serving", and a container that started and then died would have passed the same
check, so the deploy script now waits for the consoles to answer from inside the
container, and the workflow waits for the public URL on top of that.

## 2026-10-16: a model that fits more than one value a day

**Built.** Observations now carry a `period` (0 for daily and coarser, 1..n within a day
below that) and a `valid_time_utc` for display; `unit` may be given per measure; the
`periods_per_day` and `no_gaps` checks; `sector` and `data_type` columns in
`reference/instruments.csv`. Decisions D-017 and D-018. 142 tests.

**Checked against the real data, not only fixtures.** Both representative datasets were
restaged and their histories rebuilt from the raw zone, and the counts are unchanged:
gas storage 876 staged rows and 883 history versions before and after; WTI spot 9,523 and
10,284. Every existing row landed in period 0 with its unit intact, which is what the
change is supposed to do — nothing.

**Found.** Great Britain's clock-change days really do carry 46 and 50 settlement
periods: Elexon returned 46 for 2026-03-29, 50 for 2025-10-26 and 48 for an ordinary day,
each matching the prediction written before the probe. A timestamp key would have had to
represent two distinct periods with the same local time on the autumn day.

## 2026-10-16: power and carbon, Great Britain, every half hour

**Built.** Two datasets on the new sub-daily model: `elexon_gb_power_price` (the GB
day-ahead market index price and traded volume, from Elexon) and
`neso_gb_carbon_intensity` (the grid's carbon intensity, **forecast and outturn**, from
the National Grid API). Both arrive per half-hourly settlement period, both are fetched
a month at a time, and both have their licences recorded in PROVENANCE before anything
was published. 157 tests.

**A shared clock.** Elexon states the settlement date and period; the carbon API
publishes UTC half-hour windows and says nothing about periods, so merx derives them
with the same rule. That is what makes the two comparable half hour by half hour — and
the derivation is checked against Elexon's own numbers rather than trusted: it returns
46 periods for 2026-03-29, 50 for 2025-10-26 and 48 otherwise, matching Elexon exactly,
and 23:00Z in summer is period 1 of the *next* settlement day, as Elexon also reports.
Where Elexon states both, merx compares them and fails if they ever disagree, because a
provider changing convention would otherwise shift every value by half an hour.

**Fetching a range the APIs will not serve.** Elexon caps a query at 7 days and the
carbon API at about a month, while a partition here is a padded calendar month. Both are
walked in chunks and merged into one raw file, so the raw zone keeps whole artifacts
rather than fragments whose meaning depends on how they were split.

**What the data says** (2026-03 to 2026-10, 5,177 half hours of each measure):
- GB power ran from **−£24.93** to **£366.66** per MWh. **118 half hours priced below
  zero** — 2.3% of them — the cheapest around midday on Sunday 2026-03-15. Negative
  prices are not an error here; they are what happens when there is more generation than
  the system wants.
- Carbon intensity averaged 116 gCO2/kWh outturn against 114 forecast. The forecast
  error has a mean of **+2.2** and a mean absolute error of **12.0** gCO2/kWh, with
  **82.5%** of half hours within 20, and a worst case of 165. No modelling was needed to
  measure that: the publisher gives both numbers, so accuracy is simply data.
- The carbon API publishes forecasts about two days ahead, so the dataset legitimately
  contains **96 rows dated in the future**, whose outturn is still empty. Freshness is
  governed by the stalest measure, which is the outturn, so a stalled feed still shows.

## 2026-10-16: metals, agriculture and a second opinion on oil

**Built.** Eight datasets from the World Bank's Pink Sheet — copper, aluminium, gold,
wheat, maize, soybeans, and monthly WTI and Brent — each a contract of about thirty
lines reading one column of the same monthly spreadsheet. The reader is one file; the
eight datasets are configuration. With these, merx covers all six sectors: oil, gas,
power, carbon, metals and agriculture. 164 tests.

**Units are checked, not assumed.** The Pink Sheet states each column's unit on its own
header row, and the reader compares it with the contract. A benchmark quietly repriced
from $/mt to $/kg would otherwise multiply a series by a thousand and sail through any
range check wide enough to allow it.

**Two institutions, one benchmark.** The World Bank publishes a monthly WTI price; EIA
publishes WTI daily. Over the **489 months** both cover, the World Bank's figure differs
from the mean of EIA's daily prices by **17 cents on average**, and by as much as
**$3.03** (May 2026). Only 34 of those months agree to within a cent. Part of that is
rounding — the World Bank publishes to a tenth of a dollar — and the rest is method.
Neither is wrong. It is the difference between two institutions computing "the monthly
average price of WTI", and it is the kind of difference a tolerance exists for, unlike
the two-channel reconciliations already here, where the answer should be exact.

**Found in the file.** The published workbook's *first* sheet is called "Mismatch
Details" — the World Bank's own internal comparison of the release against the previous
one, listing cells that went from a number to blank (2020 barley and sorghum). The
publisher ships its own reconciliation report inside the data file.

**Volume.** Each contract keeps its own copy of the 586 KB download, about 19 MB a month
across the eight. That is deliberate — one contract, one pipeline, no shared state — and
it is the clearest argument yet for deduplicating identical raw downloads by checksum,
which is still the recorded follow-up.

## 2026-10-16: two things the live site taught us

**A page can be broken while every check passes.** The client page failed on load with
`renderConcepts is not defined`. `static/explain.js` held a byte-for-byte copy of
`chart.js` — a bad scripted edit had written the wrong contents — so the page loaded
chart.js twice and the explainer's functions existed nowhere. Both files were valid
JavaScript, both served 200, and every API behind the page worked, so checking the
endpoints proved only that the server was healthy while the page was dead.

Four tests now cover it: every function a page calls must be defined by a script that
page loads; no two static files may be copies of each other; `explain.js` must define
what the client page needs; and every `data-explain` term must have an explanation.
Reintroducing the bug fails all four. Finding calls in JavaScript means first removing
string literals — including nested template literals, where `${...}` holds real code
and the text around it does not — so that scanner is tested too.

**A missing key is not always a missing field.** Backfilling carbon intensity to January
2025 failed outright: 26 half hours early in that month carry no `forecast` key at all,
only the outturn, while every later month is complete. The parser treated any absent key
as the API changing shape. It now distinguishes the two — a key missing from *one* half
hour is a gap and becomes a null, while a key missing from *every* half hour is still a
format error, because publishing a column of nulls would make an empty series look like
a present one.

## 2026-10-16: eight more positioning datasets, and no new code

**Built.** CFTC Commitments of Traders for corn, soybeans, wheat, gold, silver, copper,
RBOB gasoline and ULSD — eight contracts, using the parser, the checks and the
accounting identity the WTI and Henry Hub datasets already had. **No Python was
written.** Each market code was verified against the real 2026 file before the contracts
existed; all eight predictions were right.

**Checked.** Corn, gold and copper were run end to end against the live source: all
seven checks pass on each, including the balance identity — open interest equals every
long position plus spreads, and equally every short position plus spreads. Peak open
interest in 2026: corn 1,958,771 contracts, gold 528,004, copper 313,832.

**What this is for.** merx now holds 29 datasets, and metals and agriculture have both a
price and a positioning series rather than a price alone. The point of the exercise is
the cost: a sector gained a second kind of data for the price of a configuration file,
which is the claim the whole contract design exists to make.

**Said plainly, because it matters:** there is no plain ICE Brent market in this file.
What carries the name is `06765T` BRENT LAST DAY on NYMEX — a cash-settled look-alike —
and four spread contracts. Brent positioning is therefore absent rather than
approximated, and PROVENANCE says so.

## 2026-10-16: the two-institution comparison, as a control rather than an anecdote

**Built.** `align: monthly_mean` and `min_observations` on `reconcile_with`, so a
monthly benchmark can be compared with the mean of a daily series. The World Bank's
monthly WTI is now reconciled against EIA's daily WTI on every run, with a $1 tolerance.
Decision D-021. 176 tests.

**What it says.** 442 of the comparable months agree within a dollar; 12 differ by more,
the largest being $3.03 in May 2026. The World Bank rounds to a tenth of a dollar, so
part of the gap is rounding and the rest is method. That is the difference between this
and a channel reconciliation, where any gap at all is a fault.

**What it found.** **35 months cannot be compared at all.** They fall between 1994 and
2015 and have fewer than 15 EIA days left in them — because of the 744 values EIA's
spreadsheet dropped. A known problem turns out to have a second cost: it quietly
degrades a control that depends on that data. Without the `min_observations` guard those
months would have been compared against an 11-day "monthly average" and reported as
ordinary breaks, which would have been worse than reporting them as not comparable.

## 2026-10-17: eleven charts that said "pass" and drew nothing

**The symptom.** On the live site a dataset showed a green `pass` badge, a last date and
a publication time, and an empty chart. Eleven of twenty-nine series were in that state.

**The cause.** Adding `period` to the staged and published tables (D-017) changed the
shape of tables already on disk, and the series query ordered by `valid_date, period`.
Every table rebuilt since the change has the column; the ones that had not been rebuilt
do not, so the query failed with a binder error and the page received a 500 while every
other panel — fed by tables that *had* been rebuilt — carried on reporting health.

**Why re-staging is not the whole answer.** For three of the eleven it could never have
been. WTI, Brent and Henry Hub spot are blocked at the publish gate, which means their
`published` table is deliberately *not* replaced: clients keep the last publication that
passed. A blocked dataset therefore keeps a table in the old shape indefinitely, and
code that assumes otherwise stays broken for exactly the datasets already in trouble.

**The fix.** Daily and coarser data has one value per date, so the query no longer
mentions `period` or `valid_time_utc` at all; only sub-daily data, which genuinely needs
them, asks for them. The same applies to the point-in-time query. A test builds a
published table in the old shape and reads it, and fails with the production error when
the fix is removed.

**Worth naming.** "Status is green" and "the page works" were being measured on
different tables. A check that reads one artifact says nothing about another, and the
endpoints had been tested while the page had not.

## 2026-10-17: the rest of the same mistake

Fixing the empty charts uncovered two more faces of one error — adding `period` and
`valid_time_utc` to the engine's tables without a path for data already on disk.

**The operations detail panel was returning 500 for every dataset**, not just the stale
ones, because `ops.accepted_withdrawals` on the host predates the column too and the
query joined on it. The website opens the database **read-only**, so unlike the engine it
cannot migrate its way out: it has to read whatever shape it finds. It now asks each
table which columns it has and matches on the ones both sides share. For daily data
`period` is always 0, so leaving it out of a key changes no answer.

**"As known on" was broken for every sub-daily series, by design rather than by
staleness** — the point-in-time store had never carried `valid_time_utc` at all. Half an
hour's data dated only by its day would put 48 points on the same x value and draw as a
vertical line. The history now stores the period's start alongside the period, verified
by rebuilding the GB power history and reading it back: 6,665 versions, dated
`2026-03-01T00:00`, `00:30`, `01:00` as they should be.

**The pattern worth naming.** Three failures, one cause: a column was added and the
queries that use it shipped before the tables had it. The parts that rebuild themselves
each run recovered silently, which is exactly why this was hard to see — the broken
parts were the ones that *don't* rebuild, and they are the blocked datasets and the
read-only reader.

**And once more, inside the engine.** Re-staging the eleven legacy datasets then failed
on all eleven, with the same binder error in a third place: the `no_withdrawals` check
compares the freshly staged table against the history, and the history is rebuilt
*after* staging — so on the first run following the change, one side has `period` and
the other does not. That comparison now matches on the columns both sides share, which
also removes a hidden ordering dependency between two assets.

## Saying what a warning is about (2026-10-17)

The client page showed a `warn` badge and nothing else. A badge that cannot be
questioned is not much better than no badge: it tells a user the data is suspect without
telling them whether the suspicion matters to them. A stale futures curve and an unusual
price are both `warn`, and the second one may well be the number they came for.

The catalogue now carries the checks a publication failed, with what each of them said,
and the page spells them out under the badge — in a person's words as well as the
contract's name for the check. The same list explains a held-back series: which critical
check stopped the newer data.

**Where the reasons come from matters.** The obvious source — re-read the checks on the
build behind the published rows — is wrong. `no_withdrawals` compares a build against the
history, and the history keeps growing, so a build that passed cleanly when it was
published can fail that same check weeks later. Reporting that as the reason the rows
were published with a warning would be a lie about the past. The reasons instead come
from `meta.publications`, which records what each publication attempt decided at the time
it decided it; `ops.check_results` is used only to fill in what each check said. A
blocked dataset therefore shows nothing against its published rows, which passed, and
the blocking checks against the attempt being held back.

## `merx backfill` (2026-10-17)

Going back for a partitioned source's older files was a two-command recipe held in a
session note rather than in the repository:

```sh
dagster asset materialize -m merx.orchestration.definitions --select "raw/<id>" --partition <key>
dagster asset materialize -m merx.orchestration.definitions --select "staged/<id>,published/<id>,pit/<id>"
```

repeated once per partition. It is now `merx backfill <dataset> --from 2010`, which does
the same thing and knows which partitions the contract has, which downstream assets
belong to it (the reconciliation too, where there is one), and that the rebuild should
happen **once at the end** rather than after every file — the staged table is built from
the whole raw zone, so rebuilding per partition reaches the same answer by a far longer
road.

Two details are deliberate. A partition that fails does not stop the ones after it: a
source that has lost one old file should not cost the caller the rest of the range, and
the rebuild runs on whatever was fetched. And a rebuild that fails on a tier-1 dataset
says so in one line, because for WTI, Brent and Henry Hub that failure is the publish
gate doing its job, not a broken backfill.

`--dry-run` prints the partitions and stops; an unknown partition is refused with the
range the contract actually covers.

## "About this data" says where the data came from (2026-10-17)

Two changes to the client page, both about the same thing: putting the useful
information where someone is looking, and taking away what was getting in the way.

**The "?" markers are gone.** Every explained term in the running text carried a small
question mark that opened a popover. The glossary behind them was worth having; the
markers were not. A paragraph speckled with them interrupts the reading they were meant
to support, and the eye goes to the markers rather than the sentence. The same
explanations are still on the page, as one list at the bottom, where someone who wants
them can read them straight through. The popover and its stylesheet rules are deleted
rather than hidden.

**"About this data" is now about *this* data.** It used to be three paragraphs of
boilerplate, identical for all 29 datasets, listing every source the project uses. It
now answers, for the series on screen: who publishes it, how it is reached, the exact
request URL (with its `{year}` or `{from}`/`{to}` placeholders intact, because for a
partitioned source no single URL is the truth), the file format, the series key within
that file and which columns are read, when it is fetched in words as well as cron, how
many raw files are kept and how large they are, the newest few by name with their size,
fetch time and checksum prefix, and the licence with a link and the credit line it asks
for.

Downloaded files and copies imported from the Wayback Machine are counted separately. A
WTI download from today and an archive capture from 2010 are both evidence, but of
different things, and adding them together would make the raw zone look older than it
is.

None of it is written by hand. It is read from the contract and from
`meta.raw_manifest`, so it cannot drift away from what the pipeline actually does — the
failure mode of every hand-written provenance page.

**Providers became reference data.** `reference/providers.csv` holds each publisher's
name, access method, terms, terms URL and attribution, loaded and cross-checked like
instruments and units: a crosswalk entry naming a provider that `providers.csv` does not
describe now fails at load. That is the same rule as the licence policy, enforced by the
loader rather than by memory — nothing is published from a source whose terms are not
recorded, so a source with no record cannot reach the page at all.

The boilerplate that was genuinely general — what the published layer is, what "as known
on" means, that every download is kept — moved into its own panel, "How this site treats
the data", where it belongs.

## The catalogue took three and a half seconds to say anything (2026-10-17)

The client page was blank for a few seconds after every refresh. It looked like a
cosmetic problem — nothing to show while the data loads — and the fix looked like a
spinner. Measuring first said otherwise.

```
/api/client/datasets                 3.65s   27 KB
/api/client/series/eia_wti_spot      0.34s  200 KB
/api/client/datasets/.../provenance  0.30s    4 KB
/client  (the page itself)           0.15s   20 KB
```

27 KB taking ten times longer than 200 KB is not a data problem. Timing the database
directly:

```
open the file, run SELECT 1, close   30.9 ms
SELECT 1 on an already-open connection 0.12 ms
```

`Store.query` opened the database, ran one statement and closed it again. The catalogue
asks three questions per dataset plus a few more — **77 opens for one request**, about
2.3 seconds on the laptop and 3.6 on the server, of which essentially none was spent
reading. The page was not slow; it was opening the same file seventy-seven times.

`Store.batch()` holds one connection for a block, and every `/api` endpoint is wrapped in
one, so a request opens the file once. Blocks nest, so a function can batch its own
queries without knowing whether its caller already did. The connection lives in a
thread-local: a request is served start to finish on one thread, which is exactly the
scope a held connection should have — never shared between requests, always closed with
the block that opened it, including when the block raises.

| | before | after |
|---|---|---|
| `/api/client/datasets` | 2.29 s, 77 opens | 0.11 s, 1 open |
| `/api/ops/datasets` | 0.81 s, 29 opens | 0.05 s, 1 open |

The database is still opened read-only and still never held between requests, so the
single-writer rule and the busy-retry are untouched.

**Worth remembering:** the first explanation was "the page needs a loading state", and it
was wrong — or rather, it was a true statement about a symptom. A loading state was still
worth building afterwards, but it would have permanently hidden a twenty-fold
regression behind a tasteful animation.

## Showing the shape of the page while it loads (2026-10-17)

With the catalogue down from 3.6 s to 0.1 s there is much less blank page to cover, but
there is still a first paint with nothing in it, and on a slow connection there is more.
The page now ships its own shape: grey blocks where the dataset cards, the title, the
chart, the revisions table and the provenance table will be, written into the HTML rather
than drawn by a script. A skeleton drawn by JavaScript arrives only after the page's own
code has run, which is a good part of the wait it is meant to cover.

`<main>` carries `data-loading` and `aria-busy="true"` until the first dataset is fully
drawn — chart and provenance included, not just the catalogue, since removing the
placeholders between those two steps swaps one empty page for another. The pulse is
scoped to `data-loading` and switched off under `prefers-reduced-motion`.

The failure path changed with it. A page that could not reach its data used to replace
itself with a `<pre>` holding the exception. It now says what happened in its own voice
— that the site is up and the data behind it did not answer, that a pipeline run holding
the database is the usual cause — shows the underlying message quietly, and offers a
retry button. Verified by serving the page with every `/api` call failing.

Both states were checked by loading the real page in jsdom against a local server:
seventeen placeholder shapes at parse, cards at 400 ms, chart and provenance by 1.2 s,
`data-loading` gone; and with 700 ms of artificial latency per call, the placeholders
correctly stay up longer.

## Leading with what the checks caught (2026-10-17)

The site held several real findings and told no one. A visitor landed on the catalogue,
saw 29 datasets and a chart, and had to click into WTI and read a note before learning
that anything had ever been caught. The page now opens with a band of them:

```
 3,401  published prices the source later removed      caught by the no withdrawals check
    12  months where two institutions disagree         caught by the reconciliation check
     3  values differ between one publisher's own      caught by the reconciliation check
       two channels
   925  days since this source last published          caught by the freshness check
```

Each is a button. Clicking it selects the dataset **and sets the replay date to a day
when the missing values were still being published**, so the chart redraws with the lost
history visible and the note underneath says how many values were known then. The claim
and its evidence are one click apart.

**No finding is written down anywhere.** They are derived from the same records the
operations console shows — open exceptions with a root cause, withdrawn values in the
point-in-time history, break counts in the reconciliation tables. A finding appears
because a control caught something and disappears when the thing is closed; its numbers
cannot disagree with the numbers behind it, because they are the same read. A test pins
both halves: a clean pipeline reports nothing, and closing the exception removes the
finding.

Choosing the replay date needed care. A series has many withdrawal moments — replaying
archived copies finds old ones, one value at a time, going back years — so "the first
one" pointed at 2014 and proved nothing. The date comes from the **largest** removal, and
it is the day that removal was noticed rather than the day before it: a series is
replayed as at midnight, and the build that dropped the values landed hours into the day,
so midnight still sees the complete copy. Stepping back a whole day would break a dataset
whose history begins on the same day it first lost something.

Verified against the real history, all three series:

| | replayed at | then | published now |
|---|---|---|---|
| Henry Hub | 2026-10-15 | 7,359 | 5,709 |
| Brent | 2026-09-17 | 9,963 | 9,108 |
| WTI | 2026-10-11 | 10,236 | 9,523 |

## The catalogue moved out of the way (2026-10-17)

29 dataset cards in a four-column grid take about 950 px. On a laptop screen that is the
entire viewport: the chart — the thing anyone came for — began below the fold, and the
page read as a wall of boxes.

A wide screen has horizontal room going spare, so above 1100 px the browser becomes a
column of its own: a sticky panel holding the sector chips and the dataset list, which
scrolls inside itself, with the findings band across the top and the chart beside it. The
cards drop their tags there, since the detail panel repeats all of them anyway and in a
280 px column they wrap into a mess; the quality badge stays, because that is the one
worth seeing in a list. Below 1100 px nothing changes and it stacks as before.

Two details that are easy to get wrong: a sticky flex column needs `min-height: 0` on the
scrolling child or it grows instead of scrolling, and prose in a 1,200 px column needs a
`max-width` or it runs to unreadable line lengths.

**Also fixed: the Two sources page was describing the wrong comparison.** Its labels were
written for two channels of one publisher — "Only in the spreadsheet", "Dropped from the
spreadsheet… still in the API" — and the World Bank against EIA is not that. Where the
tolerance is zero and the alignment exact, the channel wording stays; otherwise the
labels name the two publishers and the tolerance: *"Only World Bank Commodity Markets
publishes this period"*, *"Both publish it, and they differ by more than 1.0 USD/bbl"*.

## The headline was undercounting itself (2026-10-17)

The findings band said **3,401 prices removed**. The exceptions behind it said 744, 894
and 1,776 — which is 3,414.

The cause was one variable doing two jobs. `_before_the_largest_removal` answers "which
date should the button replay at", and returns the size of that removal along with the
date; the finding then used that size as the dataset's count. For a series whose values
all vanished at once the two are identical, which is why it read correctly at first
glance — but each of these three had also lost a handful of values in smaller, earlier
removals, found by replaying archived copies. Thirteen values across the three.

They are two different questions and now have two different queries: the count is every
withdrawn value the history holds, and the date still comes from the largest single
removal, because that is the one worth showing. A test pins the difference by taking one
value away and then seven more, and asserting the count is eight while the link points at
the removal of seven.

Small, and worth writing down: the number was on the page that makes the claims, it was
wrong in the direction that flatters nobody, and the only reason it was caught is that two
documents quoting the same fact disagreed.

## Production, and the first series whose past is rewritten (2026-10-18)

The question that started this was about Waha, the West Texas gas hub that printed
negative prices on most trading days through the spring of 2024: could it go on the site?
It cannot. The price is a proprietary index and EIA's API carries no Waha series at all —
checked, rather than assumed, and both the check and the refusal are written into
`docs/PROVENANCE.md`.

What came out of asking was a better question. Every one of the 29 datasets here is a
price, a position or a stock level, and none of those get revised. That is why the
project's showpiece finding had to be values *withdrawn* — a source deleting history is
the only way the past had ever changed. Production figures are different: they are
estimates that firm up for years.

So before writing anything, a prediction was recorded and then tested against 14 archived
copies of each of two EIA spreadsheets, Texas crude oil production and Texas marketed gas
production:

* **overlapping months do differ** — abundantly;
* **routine releases revise exactly the last two months**, every time;
* **and roughly once a year a benchmarking release rewrites a hundred or more months at
  once**, back as far as 2007, when the publisher swaps its own estimates for what states
  finally reported. The pattern is bimodal, which makes it detectable: two months, or a
  hundred, and almost nothing in between;
* **no month has ever been withdrawn** — 0 losses across 26 vintage-to-vintage
  comparisons, the exact opposite of the price series;
* revisions are small for crude (a median well under 1%, worst about 4%) and **not small
  for gas**, where single months have moved by up to 12.5%. That one was predicted wrong,
  which is the useful kind of wrong.

Both datasets are a contract file each and no new code, which is the claim this project
keeps making. They are named after Texas and not the Permian, because EIA publishes by
state and the basin straddles two of them; the reasoning is D-030.

**What the data now shows.** January 2015 Texas gas production was published as 653,831
MMcf. It now reads 666,792. The "as known on" explorer answers 653,831 for a replay date
in 2017 and 666,792 for one in 2021, which is the first time that page has had a series
whose past moves as a matter of routine rather than through loss.

**Two things the probe found that the build had to deal with.**

The first is a correctness problem in how knowledge time was assigned. The archive's
oldest copy of the gas spreadsheet is stamped 2011-07-06 and contains a file whose own
Contents sheet says it was released 2017-01-31. A capture cannot precede its file's
publication, so the stamp is wrong, and trusting it placed five years of later revisions
before the values they replaced: "as known on 2011-07-06" answered with data from 2017.
Knowledge time is now the capture time *unless the file disproves it* (D-029), applied in
one function that both staging and the point-in-time replay call, so the two cannot drift
apart. The correction only ever moves a knowledge time later, which could in principle
push one file past the next; nothing in the data does, and the replay now stops with a
message naming both files rather than inventing revisions.

The second is smaller: copies captured before about 2016 state the next release as a
phrase — "Last Week of December 2012" — rather than a date. The strict parser rejected
those two vintages outright. A worded next-release date is now read as absent; the
release date itself is still required, because it is what the first fix depends on.

**Verified, not assumed.** Both datasets materialise clean through raw, archive, staged,
published and point-in-time, with every check passing. The point-in-time tables hold 252
revised months for crude and 199 for gas, and zero withdrawals. The client page renders
both — 451 monthly values, `kbbl` and `MMcf` on the axis, the 15-year default window
giving 181 points — checked by loading the real page in jsdom and asserting on the DOM.

A gotcha for next time: `document.body.textContent` in jsdom includes the text of inline
`<script>` elements, so searching it for the error panel's wording matched the string
literal in the page's own source and reported a failure that was not there. Strip
`script`, `style` and `template` before asserting on a page's text.

**Found while running the suite, unrelated to any of this.** A findings test compared a
recorded date against `date.today()` evaluated later in the same test, so it failed any
run that crossed midnight — which this one did, at 00:04. The test now carries the date
forward from the moment the loss was noticed.

**Nothing yet notices a revision.** No check looks at one: `no_withdrawals` sees deletions
only. That is the obvious next control, and the shape of the data says what it should
look for — a release that rewrites two months is routine, and one that rewrites a hundred
is an event worth an exception.

## A page for one publisher, and five regions that do not add up (2026-10-19)

"What is Lower 48?" turned into this. The Lower 48 is the contiguous states, and the
reason a gas report says it is pipelines: Alaska and Hawaii are not connected to the North
American network, so their gas cannot reach the market the price is set in. Which raised
the obvious follow-up — the total is published as five regions, so where are they?

**Five contracts, no new code.** East, Midwest, South Central, Mountain and Pacific, each
a weekly file at the same URL pattern as the total, read by the same parser. Predictions
were written down first. Two were wrong, and one of those was worth the whole exercise.

**The parts do not add up to the whole.** The prediction was that five regions sum to the
Lower 48 total every week, because that is what a breakdown means. Measured across all 876
weeks: they match exactly **550 times, 63%**, and the rest are out by one or two Bcf. Every
one of the eight series is published as a whole number of Bcf, including the total, so five
independently rounded parts cannot be expected to match a separately rounded whole. The
error is bounded at **2 Bcf on a total near 3,500 — 0.12%**, exactly what rounding five
numbers predicts, and salt plus non-salt against South Central behaves identically within
1 Bcf.

This is a more useful result than the identity holding would have been. A balance check
written on the assumption it holds would fail on 37% of weeks, and would be switched off
within a fortnight. The page therefore *measures* the gap and states the tolerance a real
check would need, rather than asserting an identity the data does not have. merx has no
cross-dataset check yet — the existing `balance` check compares measures inside one
dataset, and these are six datasets — so this is reported, not enforced, and the gap is
named as a gap (D-031).

**A check added and then removed.** The region contracts were first written with the lab's
seasonal detector, reasoning that a region fills and empties on the same rhythm as the
total. The first run flagged a recent value in Mountain and Pacific — the two smallest
regions — and in none of the three large ones: two warning exceptions conjured out of
nothing. The lab scored that detector on the Lower 48 total, not on a series a tenth the
size. Keeping it would have meant asserting a threshold was good instead of knowing it,
which is the one thing the quality lab exists to stop. The check came out, the contracts
say why, and "score the detectors on regional storage" is now written down as the work
that would earn it back.

**The page.** `/eia` sits beside Data, Two sources and Operations. EIA is 16 of the 36
datasets here, 13 through the bulk spreadsheets and 3 through the API, across oil and gas
— and that concentration was invisible when the catalogue only sorted by commodity. The
page carries the publisher's terms and credit line, every dataset with its release
schedule and status, the findings that came out of EIA data, and the storage breakdown
with its chart. A publisher page answers the question a data team actually gets asked:
how much of this depends on one relationship, and what happens when that source changes
its mind.

**The header.** Four tabs pushed to the far edge of a 1,560 px page left nothing to anchor
the eye, so the tabs now sit beside the name, and the name is a link back to the data.
The taglines are gone from all four pages: each page already says what it is in its
browser title, in its highlighted tab, and in a full sentence in its first panel, and the
header line was the copy most likely to go stale — "published commodity data, with its
history" had already outlived the project it described (D-032).

**Verified.** 16 EIA datasets listed, the storage table showing all five regions summing
to 3,567 against a published 3,567 for the week ending 2026-10-09, a five-line chart
labelled in Bcf, three findings, and the identity sentence quoting 550 of 876 and 2 Bcf —
all read out of the rendered DOM in jsdom, on all four pages, with the nav asserted on
each. 236 tests. A deep link now carries a replay date, so a finding on the EIA page opens
the series in the explorer at a date that still shows the problem; a malformed date in the
URL is ignored rather than handed to the date input.

**Worth repeating, because it cost time twice:** a stale `merx serve` from an earlier
session was still bound to its port and answering with old code, so a new page 404ed that
existed on disk. `netstat -ano | grep :<port>` first, every time.
