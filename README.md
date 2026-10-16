# merx

*merx* (Latin: "wares", "goods"): a commodities data operations platform, built
as a small, honest version of how commodity datasets are manufactured and kept
trustworthy.

> **Status: early.** Six real EIA datasets run end to end through the engine
> (contract → raw download → staged table → checks, orchestrated by Dagster), and the
> data-quality lab scores anomaly detectors on them. Still to come: positioning data,
> the publish gate, point-in-time views, and the consoles.

## What runs today

| Dataset | Source | Frequency | Tier |
|---|---|---|---|
| WTI crude oil spot (Cushing) | EIA | daily | 1 |
| Brent crude oil spot | EIA | daily | 1 |
| Henry Hub natural gas spot | EIA | daily | 1 |
| US crude oil stocks excluding the SPR | EIA | weekly | 1 |
| US Lower 48 natural gas in storage | EIA | weekly | 1 |
| WTI crude oil futures, contract 1 | EIA | daily (series ended 2024-04-05) | 3 |

Each dataset is one YAML file in [`contracts/`](contracts/). A factory turns every
contract into two Dagster assets and its checks:

```
contracts/eia_wti_spot.yaml
        │
        ▼
raw/eia_wti_spot      download, keep the file untouched in data/raw/, record it in the manifest
        │
        ▼
staged/eia_wti_spot   parse strictly, rebuild staged."eia_wti_spot" in DuckDB
        │
        ├─ not_null_value              ├─ range_value
        └─ unique_measure_valid_date   └─ freshness
```

Every staged table has the same long shape: one row per `(measure, valid_date)`, with
the value, its unit, our instrument identifier, and the raw file and fetch time it came
from. A single-series dataset has one measure (`price`, `stocks`, `working_gas`).

## The data-quality lab

Which detectors actually catch bad data, and at what cost in false alarms? The lab
seeds five kinds of synthetic error into copies of the real series, runs the
detectors, and scores every flag against a ground-truth list. Headline from the
2026-10-15 run (six series, five seeds, 1,075 seeded defects):

| Detector | Recall | Precision | Alerts per year on real data |
|---|---|---|---|
| Static band (20% move) | 64% | 50% | 0.6 |
| Robust z-score, first version | 71% | 55% | 4.8 |
| Robust z-score, fixed after the first run | 70% | 72% | 3.9 |
| Seasonal + repeated-value rule | 87% | 87% | 2.4 |

Method, predictions against results, findings and limits:
[`docs/DQ_LAB.md`](docs/DQ_LAB.md).

## Run it

Needs Python 3.14.

```sh
python -m venv .venv
.venv/Scripts/pip install -e ".[dev]"      # .venv/bin/pip on macOS and Linux
.venv/Scripts/pytest                        # no network needed
.venv/Scripts/dagster dev                   # UI at http://localhost:3000
```

In the UI, open **Assets** and choose **Materialize all** to download and stage every
dataset (about 2 MB). Downloads go to `./data` (set `MERX_DATA_DIR` to change it);
set `DAGSTER_HOME` to an absolute path to keep run history between sessions.

## Layout

| Path | What it holds |
|---|---|
| `contracts/` | One YAML contract per dataset |
| `reference/` | Instruments, units, and the crosswalk from provider codes to our identifiers |
| `src/merx/contracts/` | The contract schema and loader |
| `src/merx/sources/` | Downloading, and one strict parser per file format |
| `src/merx/store/` | The raw zone and its manifest; the DuckDB connection |
| `src/merx/engine/` | Staging and checks, as plain functions |
| `src/merx/quality/` | The data-quality lab: seeded defects, detectors, scoring |
| `src/merx/orchestration/` | Dagster: the asset factory, resources, schedules |
| `tests/` | Tests on synthetic files generated at test time |

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
