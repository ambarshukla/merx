"""The publish gate: staged data becomes published data only if its checks allow it.

The rule, by tier:

* every check in the contract must have a result for the exact staged build being
  published; a build that was never checked is never published;
* tier 1: any failed error-severity check blocks publication;
* tiers 2 and 3: failures do not block, but the published rows are marked ``warn``.

Every attempt, published or blocked, is logged in ``meta.publications`` with the checks
that failed. ``published."<id>"`` is replaced in one statement, so readers see either the
previous publication or the new one, never a mix.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

import duckdb

from merx.contracts import Contract, check_name
from merx.engine import ops
from merx.engine.stage import current_build, staged_table


@dataclass(frozen=True)
class GateDecision:
    publish: bool
    status: str  # "pass", "warn", "blocked" or "unchecked"
    failed: list[str]
    missing: list[str]


def decide(contract: Contract, results: dict[str, tuple[bool, str]]) -> GateDecision:
    """Pure decision from a contract and the latest result of each of its checks."""
    expected = [check_name(c) for c in contract.checks]
    missing = [name for name in expected if name not in results]
    if missing:
        return GateDecision(False, "unchecked", [], missing)
    failed = [name for name in expected if not results[name][0]]
    blocking = [name for name in failed if results[name][1] == "error"]
    if contract.tier == 1 and blocking:
        return GateDecision(False, "blocked", failed, [])
    return GateDecision(True, "warn" if failed else "pass", failed, [])


def published_table(contract: Contract) -> str:
    return f'published."{contract.id}"'


def _log(con, contract: Contract, build_id: str | None, decision: GateDecision, now) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS meta.publications (
            dataset_id       VARCHAR   NOT NULL,
            build_id         VARCHAR,
            attempted_at_utc TIMESTAMP NOT NULL,
            status           VARCHAR   NOT NULL,
            failed_checks    VARCHAR[] NOT NULL,
            missing_checks   VARCHAR[] NOT NULL
        )
        """
    )
    con.execute(
        "INSERT INTO meta.publications VALUES (?, ?, ?, ?, ?, ?)",
        [contract.id, build_id, now, decision.status, decision.failed, decision.missing],
    )


def publish(con: duckdb.DuckDBPyConnection, contract: Contract) -> GateDecision:
    """Apply the gate to the current staged build and publish it if allowed."""
    now = datetime.now(UTC).replace(tzinfo=None)
    build_id = current_build(con, contract.id)
    if build_id is None:
        decision = GateDecision(False, "unchecked", [], ["<no staged build>"])
        _log(con, contract, None, decision, now)
        return decision
    decision = decide(contract, ops.latest_results(con, contract.id, build_id))
    if decision.publish:
        con.execute("CREATE SCHEMA IF NOT EXISTS published")
        con.execute(
            f"CREATE OR REPLACE TABLE {published_table(contract)} AS "
            f"SELECT *, ? AS quality_status, ?::TIMESTAMP AS published_at_utc "
            f"FROM {staged_table(contract)} WHERE build_id = ?",
            [decision.status, now, build_id],
        )
    _log(con, contract, build_id, decision, now)
    return decision
