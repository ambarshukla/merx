"""Rebuild a partitioned dataset's history, one raw file at a time.

A partitioned source keeps one file per year or per month, and the daily refresh only
ever asks for the current one. Everything older has to be fetched deliberately: when a
dataset is first onboarded, when a source republishes an old file, or when a parser is
fixed and the files have to be read again.

The work is in two halves, and the order matters. Each partition is fetched on its own,
because a partition names one file; the staged table, the publish gate and the history
are then built **once**, from every file the raw zone now holds. Doing it the other way
— a full rebuild after each file — would re-read the whole raw zone once per partition
and reach the same answer by a much longer road.
"""

from dataclasses import dataclass

import dagster as dg

from merx.contracts import Contract
from merx.orchestration.factory import (
    partitions_def,
    pit_key,
    published_key,
    raw_key,
    reconcile_key,
    staged_key,
)


class NotPartitioned(ValueError):
    """The dataset is one file, so there is no older partition to go back for."""


@dataclass(frozen=True)
class Step:
    name: str
    ok: bool
    message: str = ""


def partitions(contract: Contract, start: str | None, end: str | None) -> list[str]:
    """The partition keys to rebuild, oldest first.

    Keys are ``YYYY`` or ``YYYY-MM``, both of which sort as text in date order, so a
    range is a plain string comparison.
    """
    definition = partitions_def(contract)
    if definition is None:
        raise NotPartitioned(
            f"{contract.id} is fetched as a single file; `merx refresh` rebuilds it"
        )
    keys = sorted(definition.get_partition_keys())
    unknown = [k for k in (start, end) if k is not None and k not in keys]
    if unknown:
        raise ValueError(
            f"{contract.id} has no partition {unknown[0]!r}; it runs from {keys[0]} to {keys[-1]}"
        )
    return [k for k in keys if (start is None or k >= start) and (end is None or k <= end)]


def downstream_keys(contract: Contract) -> list[dg.AssetKey]:
    keys = [staged_key(contract), published_key(contract), pit_key(contract)]
    if contract.reconcile_with is not None:
        keys.append(reconcile_key(contract))
    return keys


def backfill(
    defs: dg.Definitions,
    contract: Contract,
    keys: list[str],
    instance: dg.DagsterInstance | None = None,
    on_step=None,
) -> list[Step]:
    """Fetch each partition, then rebuild the tables once from all of them.

    Fetching does not stop at the first failure: a source that has lost one old file
    should not cost the caller every other file in the range. The rebuild runs on
    whatever was fetched, which is what the raw zone holds anyway.
    """
    instance = instance or dg.DagsterInstance.get()
    job = defs.resolve_implicit_global_asset_job_def()
    steps: list[Step] = []

    def record(step: Step) -> None:
        steps.append(step)
        if on_step is not None:
            on_step(step)

    for key in keys:
        result = job.execute_in_process(
            instance=instance,
            asset_selection=[raw_key(contract)],
            partition_key=key,
            raise_on_error=False,
        )
        record(Step(key, result.success))

    rebuild = job.execute_in_process(
        instance=instance,
        asset_selection=downstream_keys(contract),
        raise_on_error=False,
    )
    # A tier-1 dataset whose gate is holding data back fails this step every time, by
    # design. Saying so here saves the caller reading it as a broken backfill.
    note = ""
    if not rebuild.success and contract.tier == 1:
        note = "the publish gate may be holding this dataset back; check `merx exceptions`"
    record(Step("rebuild", rebuild.success, note))
    return steps
