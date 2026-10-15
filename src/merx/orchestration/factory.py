"""The asset factory: one contract in, Dagster definitions out.

For each contract:

* ``raw/<id>``: downloads the source file and lands it, untouched, in the raw zone;
* ``staged/<id>``: parses the newest raw file into ``staged.<id>`` in DuckDB;
* one asset check on ``staged/<id>`` per check in the contract.

Adding a dataset is adding a YAML file; nothing here changes unless the dataset brings a
new file format.
"""

import re
from datetime import UTC, datetime, timedelta
from typing import Any

import dagster as dg

from merx.contracts import Contract, check_name
from merx.contracts.model import Check
from merx.engine import formats
from merx.engine.checks import run_check
from merx.engine.stage import stage, staged_table
from merx.orchestration.resources import DataStore, HttpFetcher
from merx.reference import Reference
from merx.store import connect
from merx.store.raw import RawZone

_SEVERITY = {"error": dg.AssetCheckSeverity.ERROR, "warn": dg.AssetCheckSeverity.WARN}


def raw_key(contract: Contract) -> dg.AssetKey:
    return dg.AssetKey(["raw", contract.id])


def staged_key(contract: Contract) -> dg.AssetKey:
    return dg.AssetKey(["staged", contract.id])


def _tags(contract: Contract) -> dict[str, str]:
    return {"tier": str(contract.tier), "provider": contract.source.provider}


def partitions_def(contract: Contract) -> dg.StaticPartitionsDefinition | None:
    """One partition per year, from the contract's start year to the current year."""
    p = contract.source.partitions
    if p is None:
        return None
    return dg.StaticPartitionsDefinition(
        [str(y) for y in range(p.start, datetime.now(UTC).year + 1)]
    )


def build_assets(contract: Contract, reference: Reference) -> list[dg.AssetsDefinition]:
    instrument = reference.resolve(contract.source.provider, contract.source.series_key)
    if contract.source.format not in formats.READERS:
        raise ValueError(f"{contract.id}: no reader for format {contract.source.format}")
    partitioned = contract.source.partitions is not None

    @dg.asset(
        key=raw_key(contract),
        group_name=contract.domain,
        description=f"{contract.title}: the source file exactly as downloaded."
        + (" One partition per yearly file." if partitioned else ""),
        owners=[f"team:{contract.owner}"],
        tags=_tags(contract),
        kinds={"file"},
        metadata={"url": contract.source.url},
        partitions_def=partitions_def(contract),
        retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
    )
    def raw_asset(
        context: dg.AssetExecutionContext, fetcher: HttpFetcher, store: DataStore
    ) -> dg.MaterializeResult:
        partition = context.partition_key if partitioned else None
        url = contract.source.url.replace("{year}", partition or "")
        result = fetcher.fetch(url)
        landed = RawZone(store.path).land(
            dataset_id=contract.id,
            provider=contract.source.provider,
            url=url,
            content=result.content,
            http_status=result.status,
            headers=result.headers,
            partition=partition,
        )
        return dg.MaterializeResult(
            metadata={
                "path": landed.path,
                "sha256": landed.sha256,
                "bytes": landed.bytes,
                "fetched_at_utc": landed.fetched_at_utc.isoformat(),
                "server_last_modified": landed.last_modified,
            }
        )

    @dg.asset(
        key=staged_key(contract),
        deps=[raw_key(contract)],
        group_name=contract.domain,
        description=f"{contract.title}: typed rows parsed from the newest raw file"
        + (" of every partition." if partitioned else "."),
        owners=[f"team:{contract.owner}"],
        tags=_tags(contract),
        kinds={"duckdb"},
        metadata={"table": staged_table(contract), "unit": contract.unit},
    )
    def staged_asset(store: DataStore) -> dg.MaterializeResult:
        zone = RawZone(store.path)
        files = (
            zone.latest_per_partition(contract.id) if partitioned else [zone.latest(contract.id)]
        )
        parts = [(landed, formats.read(zone.read(landed), contract)) for landed in files]
        result = stage(contract, instrument.instrument_id, parts, store.path)
        metadata = {
            "dagster/row_count": result.rows,
            "null_values": result.null_values,
            "measures": result.measures,
            "first_valid_date": result.first_date.isoformat(),
            "last_valid_date": result.last_date.isoformat(),
            "raw_files": len(files),
        }
        if partitioned:
            metadata["partitions"] = [landed.partition for landed in files]
        else:
            metadata["raw_path"] = files[0].path
        labels = sorted({label for _, read in parts for label in read.source_labels})
        if labels:
            metadata["source_labels"] = labels
        last_read = parts[-1][1]
        if last_read.release_date is not None:
            metadata["source_release_date"] = last_read.release_date.isoformat()
        if last_read.next_release_date is not None:
            metadata["source_next_release_date"] = last_read.next_release_date.isoformat()
        return dg.MaterializeResult(metadata=metadata)

    return [raw_asset, staged_asset]


def build_checks(contract: Contract) -> list[dg.AssetChecksDefinition]:
    return [_build_check(contract, check) for check in contract.checks]


def _build_check(contract: Contract, check: Check) -> dg.AssetChecksDefinition:
    # In a tier 1 dataset a failed error-severity check blocks anything downstream in the
    # same run: the seed of the publish gate.
    blocking = contract.tier == 1 and check.severity == "error"

    @dg.asset_check(
        asset=staged_key(contract),
        name=check_name(check),
        blocking=blocking,
        description=check.model_dump_json(),
    )
    def _check(store: DataStore) -> dg.AssetCheckResult:
        con = connect(store.path)
        try:
            outcome = run_check(con, staged_table(contract), check, datetime.now(UTC).date())
        finally:
            con.close()
        return dg.AssetCheckResult(
            passed=outcome.passed,
            severity=_SEVERITY[check.severity],
            description=outcome.description,
            metadata=outcome.metadata,
        )

    return _check


def schedule_job_name(cron: str, timezone: str) -> str:
    """A readable, stable job name for a schedule, e.g. ``refresh_30_18_x_x_1to5_new_york``."""
    slug = cron.replace("*", "x").replace("-", "to").replace(",", "and").replace("/", "every")
    zone = timezone.rsplit("/", 1)[-1]
    return re.sub(r"[^a-z0-9]+", "_", f"refresh_{slug}_{zone}".lower()).strip("_")


def report_year(when: datetime) -> str:
    """The year of the most recent Tuesday on or before ``when``: the yearly file that the
    latest weekly report (dated Tuesday) belongs to. A run on Friday 2 January refreshes the
    previous year's file, which holds the report of Tuesday 30 December."""
    tuesday = when.date() - timedelta(days=(when.weekday() - 1) % 7)
    return str(tuesday.year)


def _partitioned_schedule(contract: Contract) -> tuple[dg.UnresolvedAssetJobDefinition, Any]:
    job = dg.define_asset_job(
        name=f"refresh_{contract.id}",
        selection=dg.AssetSelection.assets(raw_key(contract), staged_key(contract)),
        description=f"Refresh: {contract.id} (the current year's file, then the staged table)",
    )

    @dg.schedule(
        name=f"refresh_{contract.id}_schedule",
        job=job,
        cron_schedule=contract.schedule.cron,
        execution_timezone=contract.schedule.timezone,
    )
    def _schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
        when = context.scheduled_execution_time or datetime.now(UTC)
        return dg.RunRequest(partition_key=report_year(when))

    return job, _schedule


def build_schedules(
    contracts: list[Contract],
) -> tuple[list[dg.UnresolvedAssetJobDefinition], list[dg.ScheduleDefinition]]:
    """One job per distinct schedule, covering every contract on that schedule.

    Grouping keeps datasets that refresh together in one run, which runs its steps in one
    process: DuckDB allows a single writing process per database file. Partitioned
    contracts get a job and schedule of their own, which picks the partition to refresh.
    """
    groups: dict[tuple[str, str], list[Contract]] = {}
    jobs, schedules = [], []
    for contract in contracts:
        if contract.source.partitions is not None:
            job, schedule = _partitioned_schedule(contract)
            jobs.append(job)
            schedules.append(schedule)
            continue
        groups.setdefault((contract.schedule.cron, contract.schedule.timezone), []).append(contract)

    for (cron, timezone), members in sorted(groups.items()):
        keys = [k for c in members for k in (raw_key(c), staged_key(c))]
        job = dg.define_asset_job(
            name=schedule_job_name(cron, timezone),
            selection=dg.AssetSelection.assets(*keys),
            description="Refresh: " + ", ".join(c.id for c in members),
        )
        jobs.append(job)
        schedules.append(
            dg.ScheduleDefinition(job=job, cron_schedule=cron, execution_timezone=timezone)
        )
    return jobs, schedules
