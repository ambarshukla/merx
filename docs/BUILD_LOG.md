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
