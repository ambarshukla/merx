# The data-quality lab: detector bake-off

How well do simple anomaly detectors find real kinds of data error in real commodity
series, and what do they cost in false alarms? The lab answers with numbers: it seeds
known, synthetic defects into copies of real series, runs each detector, and scores
every flag against the list of what was broken.

**The series are real (EIA, fetched 2026-10-15). The defects are synthetic.** Nothing in
the published series was altered; the lab works on copies.

## Method

**Series.** Six staged series: WTI spot, Brent spot, Henry Hub spot (daily prices),
WTI futures contract 1 (daily, ends 2024-04-05), US crude stocks and US gas in storage
(weekly).

**Seeded defects** (`merx/quality/defects.py`), five types copying real failure modes:

| Type | What it does | Real-world cause |
|---|---|---|
| `decimal_shift` | one value ×10 or ÷10 | a misplaced decimal point |
| `sign_flip` | one value with its sign reversed | a sign convention mix-up |
| `unit_scale_run` | 5 to 15 values ×100 | cents sent as dollars for a while |
| `stale_run` | 3 to 7 values replaced by the last good one | a stuck feed |
| `small_jump` | one value moved by 8× the median absolute change | a plausible-looking error |

Up to 8 of each type per series per run (weekly gas storage, with less history, gets
3), placed after a three-year warm-up and spaced apart. Five seeds per series. Every
corrupted date and its original value is recorded in `lab.defects`, the ground truth.

**Detectors** (`merx/quality/detectors.py`). All judge each value when it arrives,
using only earlier values:

- `static_band`: flag a move of more than 20% from the previous value.
- `robust_z`: the change from the last accepted value, in units of the MAD (median
  absolute deviation) of the last 60 accepted changes; flag above 5.
- `seasonal`: the change compared with changes at the same time of year in up to five
  earlier years, in units of their MAD (never below the recent spread); flag above 5.
- `repeat_run`: identical consecutive values (3 in a row daily, 2 weekly).
- Suites: a robust or seasonal detector plus `repeat_run`.

The robust detectors **quarantine** what they flag (a flagged value moves neither the
reference nor the yardstick) and adopt a new level after 10 flags in a row.

**Scores** (`merx/quality/scoring.py`):

- **Recall**: share of seeded defects with at least one flag. One alert is enough to
  send someone to an incident.
- **Precision**: share of flags that point at a seeded defect, *leaving aside flags the
  detector also raises on the untouched series*. Those are real-world alerts (the WTI
  print of -36.98, a gas price spike) that only a person can judge; counting them as
  false would punish a detector for noticing real events, and counting them as true
  would flatter it.
- **Alerts per year on real data**: flags on the untouched series, per series-year. The
  cost in reviewer time.

## Predictions, written before each run

First run (static band, robust z v1, seasonal v1):

1. Static band precision about 50% (every one-day spike also flags the day it returns).
   **Held: exactly 50%.**
2. Nobody catches stale runs. **Held: 0% to 3%.**
3. Robust z precision above 90%. **Missed: 55%.**
4. Every detector flags WTI on 2020-04-20. **Held**, and Henry Hub on 2026-01-23 too.

The miss in (3) was the lab doing its job: it found two flaws in the first robust
design. (a) An error lasting longer than 10 values was adopted as the new level, and
the true recovery was then flagged up to 10 times. (b) A stuck feed put zero changes
into the yardstick, shrinking it, so ordinary moves afterwards looked extreme. Version 2
accepts a return to the pre-shift level as a recovery and keeps unchanged values out of
the yardstick. Both versions are kept in the results.

Second run, predictions for v2: robust z v2 precision above 80% (**missed overall at
72%, held on every price series at 83% to 87%**; see gas storage below); seasonal v2
above 80% (**held: 83%**); `repeat_run` catches over 90% of stale runs at under one
alert a year (**held: 100%, 0.3 a year**); a suite reaches about 85% recall (**held:
87% to 89%**).

## Findings

1. **No single detector is enough; a two-part suite is.** Change-based detectors cannot
   see a stuck feed (recall 0% to 3%), and the repeat rule sees nothing else. Together:
   87% to 89% recall.
2. **The right detector depends on the series, so the choice belongs in each dataset's
   contract.** On gas in storage, robust z reaches 80% recall but only 26% precision:
   it sits on the edge at the start of every withdrawal season, a defect nudges it over,
   and quarantine compounds the error because the reference stays frozen while the
   series keeps falling. The seasonal detector on the same series: 79% recall, 61%
   precision; the seasonal suite: 99% and 69%.
3. **Static band is cheap in alerts but blind to anything subtle.** Fewest real-data
   alerts (0.6 a year) but 21% recall on small jumps, and precision fixed at 50% by
   design.
4. **Thresholds trade recall for reviewer time, steeply.** Robust z v2 at k=3.5 catches
   76% of defects but raises 13 alerts a year per series; at k=8, 65% and 0.8.
5. **Small plausible errors are the hard class.** Best recall on small jumps is about
   half. Catching more needs information the series alone does not have: a second
   source for the same quantity (cross-series checks), or a person.

## Recommendation

Per dataset, in its contract: **the seasonal suite** (seasonal v2 + repeat run) for
series with a yearly rhythm (gas storage, crude stocks) and for daily prices where
reviewer time is scarce (82% to 88% recall, 2 to 5 alerts a year); **the robust suite**
where catching more matters more than alert volume (Henry Hub: 91% recall at 8 alerts a
year). Static bands only as a backstop.

## Honest limits

- **We injected the defects, so we know their shapes.** Real errors are messier; these
  scores are an upper bound on what the detectors would do in production.
- **Precision depends on how real-world alerts are treated** (set aside here, and shown
  separately as alerts per year).
- **Quarantine makes runs path-dependent**: after a defect, a detector's later flags can
  differ from its flags on clean data. Those differences count as false flags caused by
  the defect, which is correct, but it means one defect can cost several false alarms.
- Thresholds were chosen once, up front, and not tuned to these results; the sweep
  shows what tuning would buy.
- The numbers change slightly as the series grow; this snapshot is from data fetched
  2026-10-15. Rerun with `dagster dev` → materialise `lab/detector_bakeoff`, then
  `python -m merx.quality.report`.

## Results (snapshot 2026-10-15)

Generated by `python -m merx.quality.report`.

6 real series, 5 seeds, 1075 seeded defects in total. Recall: share of seeded defects with at least one flag (lowest and highest single run in brackets). Precision: share of flags that point at a seeded defect, leaving aside flags the detector also raises on the untouched series. Alerts per year: flags on the untouched real series, per series-year.

### Headline (default settings)

| Detector | Recall | Precision | False flags caused | Alerts/yr on real data |
|---|---|---|---|---|
| repeat_run | 20% (20%–22%) | 100% | 4 | 0.3 |
| robust_z v1 k=5 | 71% (62%–87%) | 55% | 2,208 | 4.8 |
| robust_z v2 k=5 | 70% (62%–87%) | 72% | 1,050 | 3.9 |
| seasonal v1 k=5 | 67% (57%–80%) | 65% | 1,411 | 2.3 |
| seasonal v2 k=5 | 67% (57%–80%) | 83% | 536 | 2.1 |
| static_band 20% | 64% (60%–80%) | 50% | 697 | 0.6 |
| suite robust | 89% (82%–100%) | 77% | 1,054 | 4.2 |
| suite seasonal | 87% (78%–100%) | 87% | 540 | 2.4 |

### Recall by defect type (default settings)

| Defect type | repeat_run | robust_z v1 k=5 | robust_z v2 k=5 | seasonal v1 k=5 | seasonal v2 k=5 | static_band 20% | suite robust | suite seasonal |
|---|---|---|---|---|---|---|---|---|
| decimal_shift | 0% | 100% | 100% | 98% | 98% | 100% | 100% | 98% |
| sign_flip | 0% | 100% | 100% | 98% | 98% | 100% | 100% | 98% |
| small_jump | 0% | 53% | 47% | 40% | 38% | 21% | 47% | 38% |
| stale_run | 100% | 3% | 2% | 1% | 1% | 0% | 100% | 100% |
| unit_scale_run | 1% | 100% | 100% | 100% | 100% | 100% | 100% | 100% |

### By series (default settings): recall / precision / alerts per year

| Series | repeat_run | robust_z v1 k=5 | robust_z v2 k=5 | seasonal v1 k=5 | seasonal v2 k=5 | static_band 20% | suite robust | suite seasonal |
|---|---|---|---|---|---|---|---|---|
| eia_brent_spot.price | 21% / 100% / 0.3 | 72% / 61% / 6.3 | 71% / 87% / 5.1 | 68% / 62% / 3.7 | 68% / 85% / 3.4 | 66% / 50% / 0.2 | 90% / 90% / 5.4 | 88% / 88% / 3.6 |
| eia_henry_hub_spot.price | 20% / 98% / 2.0 | 75% / 58% / 10.1 | 71% / 83% / 6.4 | 62% / 65% / 4.2 | 62% / 83% / 3.0 | 67% / 51% / 3.0 | 91% / 86% / 8.3 | 82% / 86% / 5.0 |
| eia_us_crude_stocks.stocks | 20% / 100% / 0.0 | 69% / 61% / 0.0 | 68% / 78% / 0.0 | 65% / 64% / 0.0 | 64% / 83% / 0.0 | 60% / 50% / 0.0 | 88% / 83% / 0.0 | 84% / 87% / 0.0 |
| eia_us_gas_storage.working_gas | 20% / 100% / 0.0 | 81% / 22% / 0.0 | 80% / 26% / 0.0 | 79% / 56% / 0.0 | 79% / 61% / 0.0 | 69% / 46% / 0.0 | 97% / 33% / 0.0 | 99% / 69% / 0.0 |
| eia_wti_futures_c1.price | 20% / 100% / 0.0 | 68% / 68% / 5.7 | 68% / 85% / 5.5 | 68% / 68% / 2.3 | 67% / 85% / 2.2 | 62% / 50% / 0.3 | 88% / 88% / 5.5 | 87% / 88% / 2.2 |
| eia_wti_spot.price | 20% / 99% / 0.0 | 70% / 63% / 5.6 | 68% / 84% / 5.1 | 68% / 70% / 3.2 | 68% / 91% / 3.1 | 65% / 50% / 0.4 | 88% / 87% / 5.1 | 87% / 93% / 3.2 |

### Threshold sweep

| Detector | Recall | Precision | False flags caused | Alerts/yr on real data |
|---|---|---|---|---|
| repeat_run | 20% (20%–22%) | 100% | 4 | 0.3 |
| robust_z v1 k=5 | 71% (62%–87%) | 55% | 2,208 | 4.8 |
| robust_z v2 k=3.5 | 76% (68%–93%) | 67% | 1,343 | 13.0 |
| robust_z v2 k=5 | 70% (62%–87%) | 72% | 1,050 | 3.9 |
| robust_z v2 k=8 | 65% (60%–80%) | 88% | 370 | 0.8 |
| seasonal v1 k=5 | 67% (57%–80%) | 65% | 1,411 | 2.3 |
| seasonal v2 k=3.5 | 72% (60%–87%) | 74% | 910 | 7.5 |
| seasonal v2 k=5 | 67% (57%–80%) | 83% | 536 | 2.1 |
| seasonal v2 k=8 | 63% (52%–80%) | 93% | 194 | 0.5 |
| static_band 10% | 70% (60%–80%) | 51% | 762 | 2.9 |
| static_band 20% | 64% (60%–80%) | 50% | 697 | 0.6 |
| static_band 40% | 60% (60%–60%) | 50% | 656 | 0.1 |
| suite robust | 89% (82%–100%) | 77% | 1,054 | 4.2 |
| suite seasonal | 87% (78%–100%) | 87% | 540 | 2.4 |

