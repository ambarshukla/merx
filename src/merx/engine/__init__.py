"""The engine: generic steps that run any contract (stage, check). Orchestration lives
in ``merx.orchestration``; this package has no Dagster imports so each step is a plain
function that tests can call directly."""
