# Decisions

One entry per real choice: what was decided, what else was considered, and what
it costs. Newest last. Superseded entries stay, marked as such.

## D-001: One declarative contract per dataset, one shared engine (2026-10-14)

**Context.** Commodity datasets differ in source, format, units, calendar and
release rhythm, but the work done to each is the same shape: land, stage,
validate, enrich, publish, monitor. Writing a pipeline per dataset duplicates
logic and lets the same rule drift between copies.

**Decision.** Each dataset is described by a declarative contract (source,
format, schema, identifiers, units, calendar, tier, release schedule, owner,
validation rules). One engine executes any contract. A new dataset is a new
contract plus, only where the format is new, a small parser.

**Alternatives considered.** A pipeline script per dataset (fastest to start,
duplicates logic, no shared controls); a heavyweight ingestion framework
(more than the problem needs, hides the pattern being shown).

**Consequences.** Time to onboard a dataset becomes a measurable number. Controls
and monitoring are written once. The cost is up-front design of the contract
shape, and a contract language that must stay small enough to read.

## D-002: Public sources only; fetched data is never committed (2026-10-14)

**Context.** The project works on real commodity data, and some sources sit behind
licences that restrict redistribution. A reference repository must be safe to
publish and easy to trust.

**Decision.** Use only public sources. Fetch data on demand into a gitignored
directory; commit code, configuration, documentation and small synthetic fixtures
only. Skip any licensed benchmark that blocks automated access rather than working
around the block. Label every synthetic component as synthetic. Record each source's
terms in [`PROVENANCE.md`](PROVENANCE.md) before anything derived from it is shared.

**Alternatives considered.** Committing sample extracts so the tests run offline
(convenient, but it redistributes data whose terms are not yet confirmed); using a
paid or licensed feed (not reproducible by a reader, and brings real licence
obligations).

**Consequences.** Anyone can run the project from a clean checkout with only the
public internet. Tests that need real data must either fetch it or use synthetic
fixtures, so fixtures are written deliberately to carry the edge cases (negative
prices, clock-change days, missing releases).

## D-003: Contracts are YAML files validated by a strict schema (2026-10-15)

**Context.** D-001 makes the contract the unit of onboarding. It needs a format that
a reviewer can read in a pull request and that rejects mistakes before anything runs.

**Decision.** One YAML file per dataset in `contracts/`, named after its `id`, loaded
into a pydantic model that forbids unknown keys and validates every field (tier,
frequency, check types and the columns they apply to). Each contract is then checked
against the reference data: its instrument must exist, its unit must match the
instrument's, and its source identifier must map to that instrument in the crosswalk.
A test loads every committed contract, so a broken one fails CI.

**Alternatives considered.** Contracts as Python objects (editor type checking, but
onboarding becomes writing code, which is what D-001 wants to avoid); JSON (no
comments, and the comments are where the reasons live, e.g. why a known gap is
accepted).

**Consequences.** Adding a dataset in an existing format is one file. The schema is
deliberately small and grows only when a contract needs a new field. YAML's implicit
typing is a known trap (dates become dates, `no` becomes false); the strict schema
catches it where it matters.

## D-004: Dagster is used directly, driven by a contract factory (2026-10-15)

**Context.** The pipeline needs scheduling, retries, lineage, a record of every run
and check, and a UI. Dagster's concepts map closely onto this project: assets
(datasets), asset checks (controls), blocking checks (a publish gate), resources
(swappable services), schedules (release calendars).

**Decision.** Use Dagster's own concepts rather than wrapping it. A factory turns
each contract into a `raw/<id>` asset, a `staged/<id>` asset, and one asset check per
contract check. Parsing, staging and checking stay in plain functions under
`merx.engine` and `merx.sources` that the assets call, so they are testable without
Dagster. Error-severity checks on tier 1 datasets are blocking. Runs use the
in-process executor, and contracts that share a schedule share one job, because
DuckDB allows one writing process per database file.

**Alternatives considered.** A thin wrapper so the engine could swap orchestrators
(less lock-in, but hides the tool this project sets out to use well); a plain runner
plus scheduled CI (simpler, but no lineage, run history or check UI).

**Consequences.** The Dagster UI is the first operations view. The single-writer
constraint means two runs started at the same moment can collide on the database;
grouping by schedule avoids it for scheduled runs, and a run-concurrency limit is the
follow-up when manual and scheduled runs start to overlap.

## D-005: The raw zone keeps every download, unchanged (2026-10-15)

**Context.** Sources overwrite their files in place. Once a new version is published,
the previous one is gone unless someone kept it.

**Decision.** Every download is written once, untouched, to
`data/raw/<provider>/<dataset>/<fetched_at>__<sha256 prefix>.<ext>` and recorded in
`meta.raw_manifest` (URL, fetch time, hash, size, HTTP status, `Last-Modified`,
`ETag`). Byte-identical repeats are kept too. Staging reads the newest file, verifies
its hash, and rebuilds `staged.<id>` from it in one statement.

**Alternatives considered.** Keeping only a manifest row when the bytes are unchanged
(saves space; deferred until the raw zone passes 2 GB, which at current sizes is years
away); keeping only the latest file (no replay, no history).

**Consequences.** Any staged table can be rebuilt from disk, a parser fix can be
replayed without re-downloading, and successive versions of a file are the raw
material for point-in-time history. Losing `data/` loses that history, because the
sources only serve their current version.

## D-006: Every staged table is in long form (2026-10-15)

**Context.** The first datasets each held one number per date. The positioning reports
coming next hold several per date (open interest, and long and short positions for
each trader group), and the quality lab needs to treat every number as a series.

**Decision.** Every staged table has one row per `(measure, valid_date)`. A contract
lists its `measures`; single-series formats must name exactly one. Unique checks must
include `valid_date` (and include `measure` in practice), a range check can be scoped
to one measure, and freshness is judged on the stalest measure. A small reader registry
(`merx.engine.formats`) turns each file format into these rows, so a new format adds
one reader and touches nothing else.

**Alternatives considered.** One contract per measure (no table change, but many
near-identical contracts and one published report split across files); wide tables
with one column per measure (natural for reading, but every check and detector would
need to know each dataset's columns).

**Consequences.** Checks and detectors are written once for every dataset. Reading a
whole report back in its published shape needs a pivot, which a published view can do.

## D-007: Detectors are scored on seeded defects, and kept when they lose (2026-10-15)

**Context.** Any anomaly detector can be made to look good by choosing its examples.
The question that matters to an operations team is how many real kinds of error it
finds and how many false alarms it costs, on real series.

**Decision.** A lab (`merx.quality`) seeds five types of synthetic defect into copies of
every staged series, with a ground-truth manifest, and scores each detector on recall
(defects with at least one flag), precision (flags on seeded defects, with flags the
detector also raises on the untouched series set aside as real-world alerts), and
alerts per year on the untouched series. Detectors judge each value using only earlier
values. Predictions are written before every run. Earlier detector versions stay in the
results when a revision beats them, so the improvement is visible.

**Alternatives considered.** Scoring on hand-picked known incidents (too few, and
selected by hindsight); counting every flag on real data as false (punishes detectors
for noticing real market events); tuning thresholds to the results (makes the numbers
meaningless as a forecast).

**Consequences.** The first run found two flaws in the first robust detector, fixed in
v2. It also showed that the right detector depends on the series (seasonal series
need a seasonal detector), so detector choice should become a contract field. Scores are
an upper bound: real errors are messier than seeded ones. See
[`DQ_LAB.md`](DQ_LAB.md).

## D-008: Yearly source files are Dagster partitions; accounting identities are checks (2026-10-15)

**Context.** The CFTC publishes its positioning history as one zip per year, and keeps
updating the current year's file every week. Its reports also carry an accounting
identity: every open futures contract has a long side and a short side.

**Decision.** A contract can declare `partitions: {by: year, start: ...}` with a
`{year}` placeholder in its URL. Its raw asset is then partitioned by year in Dagster:
each year is fetched, landed (under its own folder, with the partition in the
manifest) and re-fetched on its own, and the staged table is rebuilt from the newest
file of every year. Its schedule refreshes only the year of the latest Tuesday report,
so the first run in January still refreshes December's file. A new `balance` check
asserts that one measure equals the sum of others on every date; the positioning
contracts use it for open interest against all longs plus spreads, and against all
shorts plus spreads.

**Alternatives considered.** Downloading every year on every run (17 files a week for
one changing file); the headerless current-week file plus a one-off history load (two
formats for one dataset, and the history load is invisible to the orchestrator).

**Consequences.** Backfill is "materialise all partitions", and a single bad year can
be re-fetched alone. The current year's partition list is computed when definitions
load, so a new year appears after a reload. A balance check catches a whole class of
corruption (a dropped or shifted column) that range checks cannot.

## D-009: Checks run with staging; the publish gate is code; exceptions are data (2026-10-15)

**Context.** Data should reach users only when its checks allow it, and every failure
should become a tracked piece of work with an owner, not a red light someone may or may
not notice. The first design ran each check as its own Dagster step. A tier 3 publish
step then ran before its checks had finished, and Dagster's own blocking skipped a tier
1 publish step entirely, so the blocked attempt left no record.

**Decision.** The contract's checks run inside the staging step, straight after the
table is rebuilt (Dagster `check_specs`), so they have always finished before anything
downstream starts; Dagster still records and shows each result with its severity. Each
staged table carries a build id (a fingerprint of the raw files it was built from), and
every result is logged in `ops.check_results` against that build. A `published/<id>`
asset applies the gate in code: every check must have a result for this exact build;
tier 1 is blocked by any failed error-severity check; tiers 2 and 3 publish marked
`warn`. Every attempt is logged in `meta.publications`. A failing check opens an
exception in `ops.exceptions` (owner from the contract), later failures update it, and a
pass closes it. A person assigns a root-cause class.

**Alternatives considered.** Separate check steps with Dagster blocking only (no record
of blocked attempts, and non-blocking checks race the publish step); publishing
everything with a status column and letting readers filter (pushes the gate onto every
consumer).

**Consequences.** One rule, readable in `merx/engine/publish.py`, decides what users
see, and it cannot publish a build nobody checked. Re-running one check alone means
re-running staging, which is cheap here. Exceptions give the operating numbers: open
count, time to recover, and repeat share. Root-cause classes are validated in code so
the list can grow; the first live exception needed a class that was missing
(`source_discontinued`).

## D-010: Point-in-time history is replayed from raw files, backfilled from a web archive (2026-10-15)

**Context.** Sources revise published values, and sometimes redefine a whole series,
overwriting their files in place. Anyone asking "what did we know on date X" (to
reproduce a past report, or to backtest without using numbers that were not yet
published) needs every version of every value and when it became known. This project
only began downloading on 2026-10-15, so its own history starts then.

**Decision.** History is derived, never edited: `pit."<id>"` is rebuilt by replaying
every raw file of a dataset in knowledge-time order and recording, for each (measure,
valid date), each distinct value with `known_from_utc`, `known_to_utc` and how it ended
(`revised` or `withdrawn`). Missing values count as values, so a filled gap is a
revision. For single-file sources that opt in (`archive_backfill`), an `archive/<id>`
asset imports dated copies from the Internet Archive's Wayback Machine into the raw zone,
marked `origin = archive`, with the capture time as knowledge time. Staging never reads
archived copies; only history does.

**Alternatives considered.** Recording versions incrementally as each download is
staged (cheaper per run, but history becomes state that a bug can corrupt, with no way
to rebuild it); starting history at the project's first download (honest but empty:
no revisions would be visible for months or years).

**Consequences.** History can always be rebuilt from the raw zone, which is why keeping
every download matters (D-005). Archive knowledge times are upper bounds: a capture says
the archive saw the file then, not that it was first published then, and the archive
only has the copies it happened to take. The replay re-reads every raw file each run,
which is fine at current sizes and is the first thing to make incremental if they grow.
