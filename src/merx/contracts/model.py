"""The shape of a dataset contract.

A contract is a YAML file that says everything the engine needs to run one dataset:
where it comes from, what it measures, how often it arrives, how critical it is, and
which checks it must pass. Unknown keys are rejected, so a typo fails loudly instead of
being silently ignored.

Every dataset is staged in "long" form: one row per (measure, valid_date). A
single-series file has one measure (e.g. ``price``); a report with several numbers per
date (e.g. open interest and positions) has one measure per number.
"""

from datetime import date
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# The data columns of every staged table that checks may refer to.
SeriesColumn = Literal["measure", "valid_date", "period", "value"]
Severity = Literal["error", "warn"]
MeasureName = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]*$")]

# The sectors the catalogue is organised by. The client's menu is built from these.
Sector = Literal["oil", "gas", "power", "carbon", "metals", "ags"]

# How often an observation arrives. Below a day, observations are keyed by a numbered
# period within the day rather than by a timestamp: see ``period`` in merx.engine.stage.
Frequency = Literal["half_hourly", "hourly", "daily", "weekly", "monthly", "annual"]
SUB_DAILY: frozenset[str] = frozenset({"half_hourly", "hourly"})

# File formats that hold exactly one series, so their contract names exactly one measure.
# A Pink Sheet contract reads one column of a many-column file, named by series_key,
# so like the single-series formats it needs no columns mapping.
SINGLE_SERIES_FORMATS = {"eia_hist_xls", "eia_api_v2", "worldbank_pink_sheet"}


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class YearlyPartitions(_Strict):
    """The source publishes one file per year; ``{year}`` in the URL is replaced by each."""

    by: Literal["year"]
    start: int = Field(ge=1990)


class MonthlyPartitions(_Strict):
    """The source answers a date range, so it is fetched a month at a time.

    ``{from}`` and ``{to}`` in the URL become instants covering the month, padded by
    ``pad_days`` at each end: a settlement day can begin the evening before in UTC, so
    an unpadded request returns a half-fetched day. The reader then keeps only the days
    belonging to the partition, so two partitions never claim the same day.
    """

    by: Literal["month"]
    start: str = Field(pattern=r"^\d{4}-\d{2}$")
    pad_days: int = Field(default=1, ge=0, le=5)


class Source(_Strict):
    provider: Literal["eia", "cftc", "elexon", "neso", "worldbank"]
    format: Literal[
        "eia_hist_xls",
        "eia_api_v2",
        "cftc_disaggregated",
        "elexon_mid",
        "carbon_intensity",
        "worldbank_pink_sheet",
    ]
    url: str = Field(pattern=r"^https://")
    # The provider's own identifier for the series, checked against the file on every run.
    series_key: str
    partitions: YearlyPartitions | MonthlyPartitions | None = Field(
        default=None, discriminator="by"
    )
    # For files with many columns: which source column holds each measure.
    columns: dict[MeasureName, str] | None = None
    # Import dated copies of this file from the Wayback Machine to backfill history from
    # before the project began (point-in-time store). Single-file sources only.
    archive_backfill: bool = False
    # For APIs that need a key: the environment variable holding it (never the key itself).
    api_key_env: str | None = None

    @model_validator(mode="after")
    def _partitions_match_url(self) -> Source:
        yearly = isinstance(self.partitions, YearlyPartitions)
        monthly = isinstance(self.partitions, MonthlyPartitions)
        if yearly != ("{year}" in self.url):
            raise ValueError("a {year} placeholder in the url and yearly partitions go together")
        if monthly != ("{from}" in self.url and "{to}" in self.url):
            raise ValueError("monthly partitions need {from} and {to} placeholders in the url")
        if self.archive_backfill and self.partitions is not None:
            raise ValueError("archive_backfill is for single-file sources, not partitioned ones")
        if (self.format == "eia_api_v2") != (self.api_key_env is not None):
            raise ValueError("api_key_env is required for eia_api_v2 and only for it")
        return self


class PeriodsPerDayCheck(_Strict):
    """Every day must carry the expected number of periods.

    Written for markets settled in numbered periods: Great Britain has 48 half-hourly
    settlement periods a day, and 46 or 50 on the two days the clocks change. The
    newest day is skipped, because it is usually still filling up.
    """

    type: Literal["periods_per_day"]
    expected: int = Field(gt=0)
    # Day lengths that are also correct, such as the clock-change days.
    also_allow: list[int] = []
    severity: Severity = "error"


class NoGapsCheck(_Strict):
    """Within each day the periods present must run 1..n with none missing.

    ``periods_per_day`` counts; this one looks for holes, so a day with period 7 absent
    and period 49 present fails here even though the count is right.
    """

    type: Literal["no_gaps"]
    severity: Severity = "error"


class Schedule(_Strict):
    cron: str
    timezone: str = "UTC"


class ReconcileWith(_Strict):
    """Another dataset carrying the same quantity, to be compared value by value.

    Two situations, and the tolerance is what separates them. Two **channels** of one
    publisher — a spreadsheet and an API — should agree exactly, so the tolerance is
    zero and any difference is a fault. Two **institutions** computing the same
    quantity will not agree exactly, because their methods differ; there a tolerance
    says how far apart is unremarkable, and the job is to notice when they drift
    further.
    """

    dataset: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    # Largest absolute difference still counted as a match.
    tolerance: float = Field(default=0.0, ge=0)
    # How to line the two up when they are not on the same clock. "exact" needs the
    # same date; "monthly_mean" averages the other dataset's values within each month
    # and compares that with this dataset's value for the month.
    align: Literal["exact", "monthly_mean"] = "exact"
    # Months with fewer than this many underlying observations are not compared: a
    # month with three prices in it has no meaningful average.
    min_observations: int = Field(default=15, ge=1)


class NotNullCheck(_Strict):
    type: Literal["not_null"]
    column: SeriesColumn
    severity: Severity = "error"
    # Dates on which a missing value is known and accepted. Any other gap still fails.
    allow: list[date] = []


class UniqueCheck(_Strict):
    type: Literal["unique"]
    columns: list[SeriesColumn] = Field(min_length=1)
    severity: Severity = "error"


class RangeCheck(_Strict):
    type: Literal["range"]
    column: Literal["value"]
    min: float
    max: float
    # Limit the check to one measure; by default it applies to all of them.
    measure: MeasureName | None = None
    severity: Severity = "error"

    @model_validator(mode="after")
    def _ordered(self) -> RangeCheck:
        if self.min >= self.max:
            raise ValueError(f"range min ({self.min}) must be below max ({self.max})")
        return self


class FreshnessCheck(_Strict):
    type: Literal["freshness"]
    # How many calendar days old the newest value of every measure may be.
    max_age_days: int = Field(gt=0)
    severity: Severity = "error"


class AnomalyCheck(_Strict):
    """Run one of the lab's recommended detectors (docs/DQ_LAB.md) over each measure and
    fail if it flags any of the newest ``lookback`` values. Warning-level by default: the
    lab measured precision near 87%, so about one flag in eight is a false alarm."""

    type: Literal["anomaly"]
    detector: Literal["seasonal_suite", "robust_suite", "static_band"]
    lookback: int = Field(default=20, gt=0)
    severity: Severity = "warn"


class NoWithdrawalsCheck(_Strict):
    """Every value ever known must still be in the build, unless a person has accepted
    its removal (``ops.accepted_withdrawals``). Checks on the rows present cannot see
    rows that vanished; this one compares against the point-in-time history."""

    type: Literal["no_withdrawals"]
    severity: Severity = "error"


class BalanceCheck(_Strict):
    """On every date, one measure must equal the sum of others (an accounting identity,
    e.g. open interest equals all long positions plus spreads)."""

    type: Literal["balance"]
    name: MeasureName
    total: MeasureName
    parts: list[MeasureName] = Field(min_length=2)
    tolerance: float = Field(default=0.0, ge=0)
    severity: Severity = "error"


Check = Annotated[
    NotNullCheck
    | UniqueCheck
    | RangeCheck
    | FreshnessCheck
    | BalanceCheck
    | NoWithdrawalsCheck
    | AnomalyCheck
    | PeriodsPerDayCheck
    | NoGapsCheck,
    Field(discriminator="type"),
]


class Contract(_Strict):
    id: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    title: str
    domain: Sector
    owner: str
    # 1 = critical (failed error checks block the data), 3 = best effort.
    tier: Literal[1, 2, 3]
    frequency: Frequency
    instrument: str
    # One unit for the whole dataset, or one per measure when a file mixes them (the
    # World Bank's price sheet carries $/bbl, $/mt and $/troy oz side by side).
    unit: str | dict[MeasureName, str]
    measures: list[MeasureName] = Field(min_length=1)
    source: Source
    schedule: Schedule
    checks: list[Check] = Field(min_length=1)
    # Whether the data-quality lab seeds defects into this dataset's series.
    in_quality_lab: bool = True
    # Another dataset carrying the same series through a different channel; the two are
    # reconciled value by value (reconcile/<id>).
    reconcile_with: ReconcileWith | None = None

    def unit_for(self, measure: str) -> str:
        """The unit of one measure. A dataset with a single unit gives it for all."""
        if isinstance(self.unit, str):
            return self.unit
        return self.unit[measure]

    @property
    def sub_daily(self) -> bool:
        """Observations are numbered within the day rather than one per date."""
        return self.frequency in SUB_DAILY

    @model_validator(mode="after")
    def _consistent(self) -> Contract:
        if len(set(self.measures)) != len(self.measures):
            raise ValueError(f"duplicate measures: {self.measures}")
        if isinstance(self.unit, dict) and set(self.unit) != set(self.measures):
            raise ValueError("a unit per measure must name every measure, and only those")
        if self.source.format in SINGLE_SERIES_FORMATS:
            if len(self.measures) != 1:
                raise ValueError(f"format {self.source.format} holds one series: name one measure")
            if self.source.columns is not None:
                raise ValueError(f"format {self.source.format} takes no columns mapping")
        elif self.source.columns is None or set(self.source.columns) != set(self.measures):
            raise ValueError("source.columns must map every measure, and only those")
        for check in self.checks:
            if isinstance(check, RangeCheck) and check.measure not in (None, *self.measures):
                raise ValueError(f"range check names unknown measure {check.measure!r}")
            if isinstance(check, UniqueCheck) and "valid_date" not in check.columns:
                raise ValueError("a unique check must include valid_date")
            # Below a day, the date alone is not a key: two values share a date.
            if self.sub_daily and isinstance(check, UniqueCheck) and "period" not in check.columns:
                raise ValueError(f"{self.frequency} data needs period in its unique check")
            if isinstance(check, PeriodsPerDayCheck) and not self.sub_daily:
                raise ValueError("periods_per_day applies to sub-daily data only")
            if isinstance(check, BalanceCheck):
                unknown = {check.total, *check.parts} - set(self.measures)
                if unknown:
                    raise ValueError(f"balance check names unknown measures {sorted(unknown)}")
        names = [check_name(c) for c in self.checks]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise ValueError(f"duplicate checks: {duplicates}")
        return self


def check_name(check: Check) -> str:
    """A stable name for a check, used as its Dagster check name."""
    match check:
        case NotNullCheck(column=column):
            return f"not_null_{column}"
        case UniqueCheck(columns=columns):
            return "unique_" + "_".join(columns)
        case RangeCheck(column=column, measure=None):
            return f"range_{column}"
        case RangeCheck(column=column, measure=measure):
            return f"range_{column}_{measure}"
        case FreshnessCheck():
            return "freshness"
        case BalanceCheck(name=name):
            return f"balance_{name}"
        case AnomalyCheck(detector=detector):
            return f"anomaly_{detector}"
        case NoWithdrawalsCheck():
            return "no_withdrawals"
        case PeriodsPerDayCheck():
            return "periods_per_day"
        case NoGapsCheck():
            return "no_gaps"
    raise TypeError(f"unknown check: {check!r}")
