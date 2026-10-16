"""The asset factory: one contract in, Dagster definitions out.

For each contract:

* ``raw/<id>``: downloads the source file and lands it, untouched, in the raw zone;
* ``staged/<id>``: parses the newest raw file into ``staged.<id>`` in DuckDB;
* one asset check on ``staged/<id>`` per check in the contract.

Adding a dataset is adding a YAML file; nothing here changes unless the dataset brings a
new file format.
"""

import os
import re
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import dagster as dg

from merx.contracts import Contract, check_name
from merx.engine import formats, ops, pit, reconcile
from merx.engine.checks import run_check
from merx.engine.publish import publish, published_table
from merx.engine.stage import stage, staged_table
from merx.orchestration.resources import DataStore, HttpFetcher
from merx.reference import Reference
from merx.sources import eia_api, wayback
from merx.store import connect
from merx.store.raw import RawZone

_SEVERITY = {"error": dg.AssetCheckSeverity.ERROR, "warn": dg.AssetCheckSeverity.WARN}


def raw_key(contract: Contract) -> dg.AssetKey:
    return dg.AssetKey(["raw", contract.id])


def staged_key(contract: Contract) -> dg.AssetKey:
    return dg.AssetKey(["staged", contract.id])


def published_key(contract: Contract) -> dg.AssetKey:
    return dg.AssetKey(["published", contract.id])


def archive_key(contract: Contract) -> dg.AssetKey:
    return dg.AssetKey(["archive", contract.id])


def pit_key(contract: Contract) -> dg.AssetKey:
    return dg.AssetKey(["pit", contract.id])


def reconcile_key(contract: Contract) -> dg.AssetKey:
    return dg.AssetKey(["reconcile", contract.id])


def refresh_keys(contract: Contract) -> list[dg.AssetKey]:
    """What a scheduled refresh rebuilds. The archive import is run by hand."""
    keys = [raw_key(contract), staged_key(contract), published_key(contract), pit_key(contract)]
    if contract.reconcile_with is not None:
        keys.append(reconcile_key(contract))
    return keys


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


def _fetch_api(contract: Contract, fetcher: HttpFetcher) -> bytes:
    env = contract.source.api_key_env
    key = os.environ.get(env or "")
    if not key:
        raise dg.Failure(description=f"{contract.id}: environment variable {env} is not set")
    return eia_api.fetch_all(
        lambda u: fetcher.fetch(u).content,
        contract.source.url,
        contract.source.series_key,
        contract.frequency,
        key,
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
        if contract.source.format == "eia_api_v2":
            content, status, headers = _fetch_api(contract, fetcher), 200, {}
        else:
            result = fetcher.fetch(url)
            content, status, headers = result.content, result.status, result.headers
        landed = RawZone(store.path).land(
            dataset_id=contract.id,
            provider=contract.source.provider,
            url=url,  # never includes an API key
            content=content,
            http_status=status,
            headers=headers,
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
        # The contract's checks run in this same step, straight after staging (D-009), so
        # they have always finished before the publish gate looks at them.
        check_specs=[
            dg.AssetCheckSpec(
                name=check_name(check),
                asset=staged_key(contract),
                description=check.model_dump_json(),
            )
            for check in contract.checks
        ],
    )
    def staged_asset(store: DataStore) -> dg.MaterializeResult:
        zone = RawZone(store.path)
        files = (
            zone.latest_per_partition(contract.id) if partitioned else [zone.latest(contract.id)]
        )
        parts = [(landed, formats.read(zone.read(landed), contract)) for landed in files]
        result = stage(contract, instrument.instrument_id, parts, store.path)
        check_results = run_contract_checks(contract, store.path, result.build_id)
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
        metadata["build_id"] = result.build_id
        return dg.MaterializeResult(metadata=metadata, check_results=check_results)

    @dg.asset(
        key=published_key(contract),
        deps=[staged_key(contract)],
        group_name=contract.domain,
        description=(
            f"{contract.title}: the staged build, published only if the gate allows it "
            f"(tier {contract.tier})."
        ),
        owners=[f"team:{contract.owner}"],
        tags=_tags(contract),
        kinds={"duckdb"},
        metadata={"table": published_table(contract)},
    )
    def published_asset(store: DataStore) -> dg.MaterializeResult:
        con = connect(store.path)
        try:
            decision = publish(con, contract)
        finally:
            con.close()
        if not decision.publish:
            raise dg.Failure(
                description=f"{contract.id} not published: {decision.status}",
                metadata={"failed_checks": decision.failed, "missing_checks": decision.missing},
            )
        return dg.MaterializeResult(
            metadata={"quality_status": decision.status, "failed_checks": decision.failed}
        )

    return [raw_asset, staged_asset, published_asset, *build_history_assets(contract)]


def build_history_assets(contract: Contract) -> list[dg.AssetsDefinition]:
    """``pit/<id>`` for every contract; ``archive/<id>`` where the contract asks for it."""
    assets = []
    if contract.source.archive_backfill:

        @dg.asset(
            key=archive_key(contract),
            group_name=contract.domain,
            description=(
                f"{contract.title}: dated copies imported from the Wayback Machine, to "
                "backfill history from before the project began. Knowledge time is the "
                "archive's capture time. Run by hand; resumable."
            ),
            owners=[f"team:{contract.owner}"],
            tags=_tags(contract),
            kinds={"file"},
        )
        def archive_asset(
            context: dg.AssetExecutionContext, fetcher: HttpFetcher, store: DataStore
        ) -> dg.MaterializeResult:
            zone = RawZone(store.path)
            captures = wayback.parse_index(
                fetcher.fetch(wayback.index_url(contract.source.url)).content
            )
            already = zone.archived_captures(contract.id)
            landed, failed = 0, []
            for capture in captures:
                url = wayback.capture_url(contract.source.url, capture)
                if url in already:
                    continue
                time.sleep(fetcher.polite_delay_s)
                try:
                    result = fetcher.fetch(url)
                except Exception as e:  # the archive refuses some requests under load
                    failed.append(f"{capture.timestamp}: {type(e).__name__}")
                    continue
                zone.land(
                    dataset_id=contract.id,
                    provider=contract.source.provider,
                    url=contract.source.url,
                    content=result.content,
                    http_status=result.status,
                    headers=result.headers,
                    fetched_at_utc=capture.captured_at_utc,
                    origin="archive",
                    archive_url=url,
                )
                landed += 1
            if failed:
                context.log.warning(f"{len(failed)} captures not fetched; rerun to retry: {failed}")
            return dg.MaterializeResult(
                metadata={
                    "captures_listed": len(captures),
                    "already_landed": len(already),
                    "landed_now": landed,
                    "failed": failed,
                }
            )

        assets.append(archive_asset)

    @dg.asset(
        key=pit_key(contract),
        deps=[staged_key(contract)]
        + ([archive_key(contract)] if contract.source.archive_backfill else []),
        group_name=contract.domain,
        description=(
            f"{contract.title}: every version of every value and when it was known, "
            "replayed from every raw file (direct downloads and archived copies)."
        ),
        owners=[f"team:{contract.owner}"],
        tags=_tags(contract),
        kinds={"duckdb"},
        metadata={"table": pit.pit_table(contract)},
    )
    def pit_asset(store: DataStore) -> dg.MaterializeResult:
        con = connect(store.path)
        try:
            summary = pit.build(con, contract, RawZone(store.path))
        finally:
            con.close()
        summary["first_known_utc"] = summary["first_known_utc"].isoformat()
        return dg.MaterializeResult(metadata=summary)

    assets.append(pit_asset)

    if contract.reconcile_with is not None:
        other = contract.reconcile_with.dataset
        tolerance = contract.reconcile_with.tolerance

        @dg.asset(
            key=reconcile_key(contract),
            deps=[staged_key(contract), dg.AssetKey(["staged", other])],
            group_name=contract.domain,
            description=(
                f"{contract.title}: every value compared with {other}, the same series "
                "through another channel. Differences are kept in recon.<id> with a class."
            ),
            owners=[f"team:{contract.owner}"],
            tags=_tags(contract),
            kinds={"duckdb"},
            metadata={"table": reconcile.recon_table(contract), "other": other},
            check_specs=[dg.AssetCheckSpec(name="reconciled", asset=reconcile_key(contract))],
        )
        def reconcile_asset(store: DataStore) -> dg.MaterializeResult:
            con = connect(store.path)
            try:
                counts = reconcile.reconcile(con, contract, other, tolerance)
            finally:
                con.close()
            breaks = sum(counts[c] for c in reconcile.BREAK_CLASSES)
            return dg.MaterializeResult(
                metadata=counts,
                check_results=[
                    dg.AssetCheckResult(
                        check_name="reconciled",
                        passed=breaks == 0,
                        severity=dg.AssetCheckSeverity.WARN,
                        description=f"{breaks} breaks against {other}",
                        metadata=counts,
                    )
                ],
            )

        assets.append(reconcile_asset)
    return assets


def run_contract_checks(
    contract: Contract, data_dir: Path, build_id: str
) -> list[dg.AssetCheckResult]:
    """Run every check in the contract on the staged build, logging each result and
    opening or closing exceptions, and return them for Dagster to record and display."""
    out = []
    con = connect(data_dir)
    try:
        for check in contract.checks:
            outcome = run_check(
                con,
                staged_table(contract),
                check,
                datetime.now(UTC).date(),
                contract.id,
                contract.frequency,
            )
            change = ops.record(
                con,
                dataset_id=contract.id,
                owner=contract.owner,
                build_id=build_id,
                check_name=check_name(check),
                severity=check.severity,
                outcome=outcome,
            )
            metadata = dict(outcome.metadata)
            if change.exception_id is not None:
                metadata["exception"] = f"{change.exception_id} ({change.action})"
            out.append(
                dg.AssetCheckResult(
                    asset_key=staged_key(contract),
                    check_name=check_name(check),
                    passed=outcome.passed,
                    severity=_SEVERITY[check.severity],
                    description=outcome.description,
                    metadata=metadata,
                )
            )
    finally:
        con.close()
    return out


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
        selection=dg.AssetSelection.assets(*refresh_keys(contract)),
        description=f"Refresh: {contract.id} (the current year's file, then staged, "
        "published and history)",
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
        keys = [k for c in members for k in refresh_keys(c)]
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
