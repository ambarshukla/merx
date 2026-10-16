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
SeriesColumn = Literal["measure", "valid_date", "value"]
Severity = Literal["error", "warn"]
MeasureName = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]*$")]

# File formats that hold exactly one series, so their contract names exactly one measure.
SINGLE_SERIES_FORMATS = {"eia_hist_xls"}


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class YearlyPartitions(_Strict):
    """The source publishes one file per year; ``{year}`` in the URL is replaced by each."""

    by: Literal["year"]
    start: int = Field(ge=1990)


class Source(_Strict):
    provider: Literal["eia", "cftc"]
    format: Literal["eia_hist_xls", "cftc_disaggregated"]
    url: str = Field(pattern=r"^https://")
    # The provider's own identifier for the series, checked against the file on every run.
    series_key: str
    partitions: YearlyPartitions | None = None
    # For files with many columns: which source column holds each measure.
    columns: dict[MeasureName, str] | None = None

    @model_validator(mode="after")
    def _partitions_match_url(self) -> Source:
        if (self.partitions is not None) != ("{year}" in self.url):
            raise ValueError("a {year} placeholder in the url and partitions go together")
        return self


class Schedule(_Strict):
    cron: str
    timezone: str = "UTC"


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
    NotNullCheck | UniqueCheck | RangeCheck | FreshnessCheck | BalanceCheck,
    Field(discriminator="type"),
]


class Contract(_Strict):
    id: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    title: str
    domain: str
    owner: str
    # 1 = critical (failed error checks block the data), 3 = best effort.
    tier: Literal[1, 2, 3]
    frequency: Literal["daily", "weekly", "monthly"]
    instrument: str
    unit: str
    measures: list[MeasureName] = Field(min_length=1)
    source: Source
    schedule: Schedule
    checks: list[Check] = Field(min_length=1)
    # Whether the data-quality lab seeds defects into this dataset's series.
    in_quality_lab: bool = True

    @model_validator(mode="after")
    def _consistent(self) -> Contract:
        if len(set(self.measures)) != len(self.measures):
            raise ValueError(f"duplicate measures: {self.measures}")
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
    raise TypeError(f"unknown check: {check!r}")
