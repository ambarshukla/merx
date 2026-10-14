# merx

*merx* (Latin: "wares", "goods"): a commodities data operations platform, built
as a small, honest version of how commodity datasets are manufactured and kept
trustworthy.

> **Status: scaffolding.** Nothing is built yet. This README will be replaced as
> the first phase lands.

## Provenance

**Everything here is built from public sources or is synthetic. There is no
confidential, proprietary or employer information anywhere in this repository.**
It is an independent personal project, not affiliated with or written on behalf of
any employer, data vendor or exchange. Fetched data is never committed, and every
synthetic component is labelled as such. Details and the list of sources:
[`docs/PROVENANCE.md`](docs/PROVENANCE.md).

## What it will be

One shared engine that turns declarative **dataset contracts** into controlled
pipelines (land raw, stage, validate, enrich, publish, monitor), plus a console
for running them. The goal is that onboarding a new dataset is configuration,
not a new pipeline.

- **Real public data**: oil and gas prices and curves, weekly fundamentals,
  positioning, metals and agricultural series, GB power prices, carbon
  intensity. Fetched on demand; vendor data is never committed.
- **Controls as code**: completeness, timeliness, range, jump, stale and
  cross-series checks; a publish gate by tier; exceptions kept as data with
  root-cause classes.
- **A data-quality lab**: seeded defects with a ground-truth manifest, and
  detectors scored on precision and recall.
- **Point-in-time history**: every value carries when it applied and when it
  was learned, so any past view can be rebuilt as it was known.
- **Legacy re-engineering**: a deliberately fragile synthetic estate, ported
  with a parallel run in which every difference is explained.

Synthetic parts (the legacy estate, injected defects, the full forward curve)
are labelled as synthetic wherever they appear.

## Working agreements

Trunk-based development: short-lived branches, pull requests, protected `main`,
tests and CI from the first commit, decisions recorded in
[`docs/DECISIONS.md`](docs/DECISIONS.md), terms in
[`docs/GLOSSARY.md`](docs/GLOSSARY.md), history in
[`docs/BUILD_LOG.md`](docs/BUILD_LOG.md).

## Licence

MIT. See [`LICENSE`](LICENSE).
