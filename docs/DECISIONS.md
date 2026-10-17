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

## D-011: Values may not silently disappear; accepting a loss is a recorded decision (2026-10-16)

**Context.** EIA's WTI spreadsheet lost 744 daily prices in September or October 2026,
and every check passed: not-null, uniqueness, range and freshness all judge only the
rows that are present. The point-in-time history (D-010) saw the loss.

**Decision.** A `no_withdrawals` check, in every contract, compares the build with the
history: every (measure, valid date) ever known must still be present, unless a person
has accepted its removal with a reason. Acceptances are rows in
`ops.accepted_withdrawals`, made with `merx accept-withdrawals`, not lines in the
contract, because one source change can remove hundreds of values at once and each
acceptance needs a time and a reason. A value that comes back simply stops being
missing. A first build, with no history yet, passes.

**Alternatives considered.** A row-count check against the previous build (catches a
drop in count but not a swap of one date for another, and says nothing about which
values went); listing accepted dates in the contract (744 dates in a YAML file, with no
record of who decided or when).

**Consequences.** WTI spot is blocked at the gate until someone decides what to do about
the 744 values; the previous publication stays in place. The check is only as good as
the history: it cannot see values removed before the earliest raw file.

## D-012: A second channel is a dataset of its own, reconciled value by value (2026-10-16)

**Context.** EIA serves the WTI spot price through a spreadsheet and through an API.
The point-in-time history showed the spreadsheet had lost 744 values the API still
serves (D-010, D-011). Deciding what to publish needs a full, classified comparison,
not a sample.

**Decision.** The API is onboarded as its own dataset (`eia_api_wti_spot`, format
`eia_api_v2`), with its own contract, checks, gate and history. A `reconcile_with` field
on the spreadsheet's contract adds a `reconcile/<id>` asset that compares the two value
by value and stores every difference in `recon."<id>"`, classed `value_differs`,
`only_here`, `only_there` or `outside_range` (timing at the ends). The API key is read
from the environment; the API echoes it back in every reply, and it is removed before
the reply is landed, so no key is stored anywhere.

**Alternatives considered.** Replacing the spreadsheet with the API (the API has gaps
and fill values of its own, see below, so neither channel is simply right); comparing
row counts only (a count says nothing about which dates, or whether values agree).

**Consequences.** Every disagreement between the channels is a row someone can explain.
The reconciliation is a warning-level check, not part of the publish gate: it informs a
decision rather than making it. Which channel to trust for which dates is now a choice
with evidence behind it.

## D-013: The consoles are FastAPI with plain HTML and JavaScript (2026-10-16)

**Context.** The platform needs two front ends: an operations console for the people
running the data, and a client view for the people using it. The original plan was
React with TypeScript, built with Vite.

**Decision.** One FastAPI app serves a read-only JSON API and two pages written in plain
HTML and JavaScript, with a small hand-written SVG chart. No build step, no Node
toolchain, no runtime downloads from a CDN. The client endpoints read only the published
layer (and the point-in-time history for "as known on"); the ops endpoints read
everything. Nothing in the web app writes: decisions are made with the `merx` command,
which records them. Database connections are read-only, opened per request, and
retried briefly while a pipeline run is writing.

**Alternatives considered.** React and Vite (a richer component model, but Vite depends
on a native build binary installed inside the project, the kind of non-standard-path
executable this development machine's application control blocks, and a build step for
two pages is more machinery than they need); server-side templates (fine, but the JSON
API is useful on its own and easy to test).

**Consequences.** The consoles run anywhere Python runs, from `merx serve`. If they grow
past a few pages, a component framework becomes worth its build step; the JSON API would
not change.

## D-014: The lab's recommended detectors run in production as warning-level checks (2026-10-16)

**Context.** The bake-off (D-007, `docs/DQ_LAB.md`) scored detectors on seeded defects
and found the best choice depends on the series: the seasonal suite for most, the robust
suite catches more on volatile gas prices. A score in a report protects nothing until the
detector runs on every build.

**Decision.** An `anomaly` check type runs a named detector (`seasonal_suite`,
`robust_suite` or `static_band`, defined once in `merx.quality.detectors.NAMED` with the
lab's settings) over each measure and fails if it flags any of the newest `lookback`
values (20 by default). Each EIA contract names its detector, following the lab. The check
is warning-level by default: at about 87% precision, roughly one flag in eight is a false
alarm, too many to block publication on.

**Alternatives considered.** Blocking on anomalies (the lab's own numbers say this would
hold back good data); flagging the whole history on every run (the lab already does that;
an operations check is about what just arrived).

**Consequences.** New extreme values reach a person as a warning, an exception with an
owner. The positioning datasets have no anomaly check yet: the lab has not scored
detectors on them.

## D-015: Hosted on one small host: containers, a shared proxy, a daily timer (2026-10-16)

**Context.** The consoles should be reachable at a public address, and the pipeline should
keep running, and keep adding to the history, without a laptop being on. The host is a
small virtual machine (2 CPUs, 4 GB) that already runs a reverse proxy for other sites.

**Decision.** One container image runs everything. `merx-web` serves the consoles and
`merx-dagster` serves Dagster's UI, both long-running with restart policies, attached to
the proxy's Docker network so the proxy reaches them by name. The proxy terminates TLS
(automatic certificates) and puts Dagster's UI behind basic authentication, because it
has no login of its own. Instead of Dagster's always-on scheduler, a systemd timer runs
`merx refresh` once a day: it runs the jobs whose schedule falls on that day, in each
schedule's time zone, through Dagster, so the runs appear in its UI; the quality lab is
rerun on Sundays. All processes share one data directory on the host, seeded from the
development machine so the history (including archived copies) comes along.

**Alternatives considered.** Dagster's daemon (native schedules, but another always-on
process on a small host); a static export to a hosting service (no live refresh, and the
gate and exceptions would stop changing); one container per dataset (more moving parts
for no benefit at this size).

**Consequences.** The site shows the platform operating: new downloads each weekday,
checks, the gate's decisions. The host's data directory is also an off-laptop copy of the
raw zone. A run started by hand in Dagster's UI while the timer's run is writing could
collide on DuckDB's single writer; both are short and rare, and a collision fails loudly.

## D-016: Merging to main deploys, through a key that can only deploy (2026-10-16)

**Context.** Updating the host was a manual copy of the working tree followed by a build
over SSH. That is fine once and a liability afterwards: it can ship code that never
passed the tests, it can ship a dirty working tree, and it drifts — the live site and
`main` have no enforced relationship. The host also runs unrelated things, so whatever
performs the deploy should not be able to touch them.

**Decision.** The host's source directory is a clone of the public repository, so an
update is `git fetch` and `git reset --hard origin/main` rather than a copy. A `deploy`
job in the existing CI workflow runs after the test job, only on `main`, and connects to
the host with a dedicated key. That key is pinned in `authorized_keys` to a forced
command: sshd runs `/opt/merx/bin/deploy.sh` whatever the client asks for, and the script
accepts exactly one word, `deploy`. The script is root-owned and is **not** in this
repository, so no push can change what the key may do; `deploy/README.md` documents it
and the script logs its own checksum, so drift is visible. The job then checks the public
URL through the proxy, retrying for a minute, and fails if the site does not answer.

**Alternatives considered.** Reusing the existing deploy key on the host (one fewer
secret, but a single key for two unrelated stacks); a full shell key (simpler, and able
to do anything on a host that is not only ours); keeping the script in the repository
(reviewable in pull requests, but then the forced command bounds nothing); a pull-based
deploy, where the host polls for new commits (no inbound access needed, at the cost of a
polling delay and another always-on process); verifying the deploy from inside the
proxy's network rather than over the internet (fewer moving parts, but it would not
notice the proxy or the certificate being broken).

**Consequences.** The live site is `main`, as a matter of mechanism rather than of
diligence, and `DEPLOYED_COMMIT` says which commit. A failed deploy or a site that does
not come back fails the workflow on `main` and is visible. The runner accepts the host's
key on first use (`ssh-keyscan`); the exposure is a deploy key that can only deploy, and
the smoke test would show a deploy that never arrived. Data is out of scope: a new
dataset's backfill is still run on the host.

## D-017: Sub-daily data is keyed by (date, period), not by a timestamp (2026-10-16)

**Context.** Every dataset so far has at most one value per date. Power does not: Great
Britain settles electricity in half-hourly periods, and the obvious model — a UTC
timestamp per observation — cannot represent the days the clocks change. Those days have
23 or 25 local hours, so 46 or 50 settlement periods, and two of the local times on the
autumn day name two different periods.

**Decision.** Observations carry a `period` alongside `valid_date`: 1..n within the day
for sub-daily data, and **0 for daily and coarser data, meaning "the whole day"**. This
is how the market itself settles, which is the reason it works — the clock-change days
are 46 and 50 periods and nothing is ambiguous. The UTC start of each period is stored
beside it as `valid_time_utc`, for display and ordering, and is never part of a key.
Keeping `period` non-null for every dataset, rather than null for daily data, means each
key, join and grouping in the engine is written once: in SQL a null never equals a null,
so a nullable key column would have silently broken every reconciliation.

Two checks come with it. `periods_per_day` counts the periods on each day and accepts the
clock-change lengths when the contract declares them; `no_gaps` looks for holes, so a day
with period 7 missing and period 49 present fails even though the count is right. Both
skip the newest day, which is usually still filling up; freshness is what notices a day
that never arrives.

**Alternatives considered.** A UTC timestamp as the key (fails on clock-change days, and
diverges from how the market and the data provider both speak); local time plus a
daylight-saving flag (expressible, but every query then needs the flag and it is easy to
forget); a separate table shape for sub-daily datasets (no change to existing code, at
the cost of two engines to maintain, which is the opposite of this project's claim).

**Consequences.** Existing datasets were rebuilt and came out byte-identical in count:
gas storage 876 rows and 883 history versions before and after, WTI 9,523 and 10,284.
The detectors still take one value per date, so anomaly checks are not yet available on
sub-daily series; the lab scores them before that happens. Acceptances recorded in
`ops.accepted_withdrawals` before this change are migrated in place rather than being
re-asked of a person.

## D-018: A unit per measure, and a sector taxonomy in reference data (2026-10-16)

**Context.** A contract declares one `unit` for the whole dataset. That holds while a
file carries one series, and breaks on the first file that does not: the World Bank's
price sheet puts $/bbl, $/mt and $/troy oz side by side in one table. Separately, the
catalogue needs to be browsable by sector once it stops being only oil and gas.

**Decision.** `unit` accepts either a string, as now, or a mapping of measure to unit,
validated to name every measure and only those; `contract.unit_for(measure)` is what the
engine calls, and staged rows already carried a unit each. The taxonomy goes in
`reference/instruments.csv` as `sector` and `data_type` columns rather than onto the
contract, because they describe the instrument, not the dataset — and because the file
already carried `kind`, which `data_type` is derived from. `domain` on the contract is
now constrained to the sector list it was informally using.

**Alternatives considered.** Moving units inside `measures` as a mapping of objects (more
expressive, rewrites all eleven contracts and every reader that treats `measures` as a
list); a `data_type` field on each contract (duplicates what the instrument already says,
and lets the two disagree); a separate sectors table (a column on the instrument is
enough until a sector needs attributes of its own).

**Consequences.** A dataset may mix units, which the client has to show per measure
rather than once per dataset. Unit *conversion* is deliberately not built: it waits for
the first source that needs the same quantity compared across units.

## D-019: One clock for Great Britain, derived where a source does not state it (2026-10-16)

**Context.** The two GB sources describe the same half hours differently. Elexon states a
settlement date and a period number. The carbon intensity API publishes UTC half-hour
windows and never mentions periods. Comparing them — the whole point of having both — is
only possible if they end up on one clock.

**Decision.** `merx.sources.gb` owns the rule: a settlement period is counted in elapsed
local time since local midnight, so a day holds 46, 48 or 50 of them. Elexon's stated
values are used as given but **checked** against the rule, and a disagreement fails the
parse. The carbon API's periods are derived with the same rule. Elapsed time is used
rather than wall-clock arithmetic because on the autumn day 01:30 happens twice, and an
hour-of-day calculation would collide the two into one period.

**Alternatives considered.** Trusting each provider's own convention (no shared clock, so
no comparison); storing UTC timestamps and deriving periods at query time (pushes the
hard part into every query, and the ambiguous hour stays ambiguous); using Elexon as the
authority and joining the carbon data by timestamp (works until a clock-change day).

**Consequences.** The derivation was verified against Elexon rather than assumed: 46
periods for 2026-03-29, 50 for 2025-10-26, 48 otherwise, and 23:00Z in summer is period
1 of the next settlement day — all matching Elexon's own output. Both GB datasets can be
joined half hour by half hour. A provider that changes convention breaks loudly instead
of shifting every value by thirty minutes.

## D-020: One contract per commodity, and units checked against the file (2026-10-16)

**Context.** The World Bank's Pink Sheet is a single monthly spreadsheet holding about
seventy commodities across three sectors, each in its own column and its own unit. A
dataset in merx has one instrument, so a single contract covering the whole sheet would
have to abandon the instrument and crosswalk model that everything else relies on.

**Decision.** Each commodity is its own contract reading its own column, named by
`series_key`, with its own instrument and crosswalk entry. The format is treated as
single-series. Separately, because the file states each column's unit on a header row,
the reader compares that against the contract's declared unit and fails on a mismatch.

**Alternatives considered.** One contract with a measure per commodity (fewer downloads,
but one instrument cannot describe copper and wheat, and the crosswalk would have
nothing to key on); a shared "source" asset feeding many contracts (saves the duplicate
downloads at the cost of shared state between pipelines, which is the thing this design
avoids); trusting the stated units (a benchmark repriced from $/mt to $/kg rescales a
series by a thousand and still looks plausible).

**Consequences.** Eight datasets were added as configuration, with no code beyond the one
reader, and the catalogue reached all six sectors. Each contract keeps its own copy of
the 586 KB file, about 19 MB a month in total; that duplication is the price of one
contract per pipeline and is the clearest argument yet for the recorded follow-up of
deduplicating identical raw downloads by checksum.

## D-021: Reconciling two institutions, not just two channels (2026-10-16)

**Context.** Every reconciliation so far compared two channels of one publisher — EIA's
spreadsheet against EIA's API — where the right answer is that they agree exactly and
any difference is a fault. The World Bank and EIA both publish a price for WTI, and
that is a different question: two institutions, two methods, one quantity. They should
*not* agree exactly, and a comparison that demanded they did would be wrong about what
it was measuring. They are also on different clocks: monthly against daily.

**Decision.** `reconcile_with` gains `align` and `min_observations`. With
`align: monthly_mean` the other dataset's values are averaged within each calendar
month before comparison, and months with fewer than `min_observations` underlying values
are left out entirely rather than compared against a handful of days. The tolerance
carries the meaning: zero for two channels, a stated amount for two institutions, where
it records how far apart is unremarkable.

The contract validation was loosened in exactly one direction. Two channels must still
share an instrument, unit and measures. Two institutions need not share an instrument —
a monthly average genuinely is not a daily spot price, and the reference data says so —
but must share the **commodity**, so comparing copper with wheat is still an error.

**Alternatives considered.** A derived "monthly WTI" dataset built from the daily one
(more faithful to the pipeline model, at the cost of a dataset that exists only to be
compared); comparing against month-end rather than the mean (simpler, but it is not what
either institution publishes); allowing any two datasets to be reconciled (removes the
guard that catches a typo pointing at the wrong dataset).

**Consequences.** Over the 489 months both cover, 442 agree within a dollar and 12
differ by more — the largest $3.03 in May 2026. The World Bank publishes to a tenth of
a dollar, so some of the gap is rounding and the rest is method; neither is a fault, and
the tolerance is where that judgement is written down.

The comparison also surfaced a second consequence of a problem already known. **35
months cannot be compared at all**, because EIA's spreadsheet lost so many days from
them that fewer than 15 remain — the same 744 withdrawn values that blocked WTI at the
publish gate. Without `min_observations` those months would have been compared against
an 11-day average and counted as ordinary breaks, which would have been a worse answer
than admitting the comparison is not available.

## D-022: A quality flag states its reason, and the reason comes from the decision (2026-10-17)

**Context.** The client console showed each published dataset's quality status as a
badge — `pass` or `warn` — with no way to find out what a warning referred to. The
information existed, in `ops.check_results` and `meta.publications`, but only the
operations console showed it.

**Decision.** The published catalogue returns, per dataset, the checks that the
publication failed (`quality_reasons`) and, separately, the checks that blocked the most
recent attempt if one is being held back (`latest_attempt_reasons`). The client page
renders both in plain English beside the badge.

The reasons are read from `meta.publications` — the record the publish gate wrote when it
made the decision — and not by re-running or re-reading the checks now. `ops.check_results`
supplies only each check's description.

**Why.** A check's verdict is not a property of a build alone; some checks compare a
build with state that moves. `no_withdrawals` compares the staged build against the
point-in-time history, which grows with every run, so the same raw files can pass today
and fail next month. Attributing that later failure to an earlier publication would
misreport what the gate actually did. The publication log is the only record of the
decision as taken, so it is the only honest source for "why is this marked warn".

**Consequence.** The two lists can disagree, and that is the intended reading: a blocked
dataset shows a clean `pass` for the rows on display and a list of blocking checks for
the update that is not being shown. The cost is that a description can go missing if a
build's check results are later pruned; the check's name still appears.

**Alternatives rejected.** Re-reading the current checks (wrong for the reason above);
putting the reasons only on the operations console (leaves the user who sees the badge
without the answer); storing a frozen description inside `meta.publications` (duplicates
`ops.check_results` for a rare case).
