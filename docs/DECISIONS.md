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
