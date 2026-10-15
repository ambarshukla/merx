"""Storage: the raw landing zone (files exactly as received) and the DuckDB database."""

from pathlib import Path

import duckdb

DB_FILE = "merx.duckdb"


def connect(data_dir: Path) -> duckdb.DuckDBPyConnection:
    """Open the database in the data directory, creating its schemas if needed.

    Callers open a connection per operation and close it promptly: DuckDB allows one
    writing process per file, so nothing should hold it open between steps.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(data_dir / DB_FILE))
    con.execute("CREATE SCHEMA IF NOT EXISTS meta")
    con.execute("CREATE SCHEMA IF NOT EXISTS staged")
    return con
