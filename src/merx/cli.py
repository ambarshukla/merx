"""``merx``: the operations commands a person runs by hand.

    merx exceptions                         list exceptions (open first)
    merx assign-root-cause ID CAUSE --note  classify an exception
    merx missing DATASET                    values known before but missing now
    merx accept-withdrawals DATASET --reason "..."
    merx revisions DATASET                  every revised value
    merx as-known-at DATASET 2018-01-01     the dataset as it stood at that moment
    merx serve                              the consoles at http://127.0.0.1:8000
    merx refresh                            run the jobs due today (for a daily timer)

Everything else (fetching, staging, checks, publishing) runs in Dagster.
"""

import argparse
import sys
from datetime import UTC, datetime

from merx.config import data_dir
from merx.contracts import load_contracts
from merx.engine import ops, pit, withdrawals
from merx.engine.stage import staged_table
from merx.reference import load_reference
from merx.store import connect


def _contract(dataset_id: str):
    for contract in load_contracts(load_reference()):
        if contract.id == dataset_id:
            return contract
    raise SystemExit(f"unknown dataset {dataset_id!r}")


def _print(rows, header: list[str], limit: int | None = None) -> None:
    shown = rows if limit is None else rows[:limit]
    print("\t".join(header))
    for row in shown:
        print("\t".join("" if v is None else str(v) for v in row))
    if limit is not None and len(rows) > limit:
        print(f"... {len(rows) - limit} more")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="merx", description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("exceptions")
    p = sub.add_parser("assign-root-cause")
    p.add_argument("exception_id", type=int)
    p.add_argument("root_cause", choices=ops.ROOT_CAUSES)
    p.add_argument("--note", default="")
    p = sub.add_parser("missing")
    p.add_argument("dataset")
    p = sub.add_parser("accept-withdrawals")
    p.add_argument("dataset")
    p.add_argument("--reason", required=True)
    p = sub.add_parser("revisions")
    p.add_argument("dataset")
    p = sub.add_parser("as-known-at")
    p.add_argument("dataset")
    p.add_argument("when", help="UTC date or date-time, e.g. 2018-01-01 or 2018-01-01T12:00")
    p.add_argument("--limit", type=int, default=20)
    p = sub.add_parser("serve")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p = sub.add_parser("refresh")
    p.add_argument("--dry-run", action="store_true", help="list the jobs due today and stop")
    args = parser.parse_args(argv)

    if args.command == "refresh":
        # Before opening the database: the jobs write to it.
        from merx.orchestration.definitions import defs
        from merx.orchestration.refresh import due_jobs, refresh

        contracts = load_contracts(load_reference())
        if args.dry_run:
            for job in due_jobs(contracts, datetime.now(UTC)):
                print(job.name, job.partition_key or "")
            return 0
        for name, ok in refresh(defs, contracts):
            print(f"{'ok    ' if ok else 'failed'} {name}")
        return 0

    if args.command == "serve":
        # Before opening the database: the consoles only ever read it, briefly.
        import uvicorn

        from merx.api import create_app

        print(f"merx consoles: http://{args.host}:{args.port}/client and /ops")
        uvicorn.run(create_app(), host=args.host, port=args.port, log_level="warning")
        return 0

    con = connect(data_dir())
    try:
        match args.command:
            case "exceptions":
                ops.ensure_tables(con)
                rows = con.execute(
                    "SELECT exception_id, dataset_id, check_name, owner, occurrences, "
                    "opened_at_utc, closed_at_utc, root_cause, last_description "
                    "FROM ops.exceptions ORDER BY closed_at_utc IS NOT NULL, opened_at_utc"
                ).fetchall()
                _print(
                    rows,
                    [
                        "id",
                        "dataset",
                        "check",
                        "owner",
                        "seen",
                        "opened",
                        "closed",
                        "root_cause",
                        "last",
                    ],
                )
                print(ops.summary(con))
            case "assign-root-cause":
                ops.assign_root_cause(con, args.exception_id, args.root_cause, args.note)
                print(f"exception {args.exception_id}: {args.root_cause}")
            case "missing":
                contract = _contract(args.dataset)
                rows = withdrawals.missing(con, contract.id, staged_table(contract))
                if rows is None:
                    print("no history yet")
                else:
                    _print(
                        rows,
                        [
                            "measure",
                            "valid_date",
                            "period",
                            "last_value",
                            "last_seen_utc",
                            "accepted",
                        ],
                        limit=20,
                    )
                    print(f"{len(rows)} missing, {sum(r[5] for r in rows)} accepted")
            case "accept-withdrawals":
                contract = _contract(args.dataset)
                n = withdrawals.accept(con, contract.id, staged_table(contract), args.reason)
                print(f"accepted {n} missing values of {contract.id}")
            case "revisions":
                rows = pit.revisions(con, _contract(args.dataset))
                _print(
                    rows,
                    ["measure", "valid_date", "first", "latest", "versions", "first_changed_utc"],
                    limit=50,
                )
                print(f"{len(rows)} revised values")
            case "as-known-at":
                when = datetime.fromisoformat(args.when)
                rows = pit.as_known_at(con, _contract(args.dataset), when)
                print(f"{len(rows)} values known at {when.isoformat()} (UTC); latest first:")
                _print(rows[::-1], ["measure", "valid_date", "value"], limit=args.limit)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
