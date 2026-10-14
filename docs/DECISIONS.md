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
