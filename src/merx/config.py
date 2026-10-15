"""Where things live on disk.

Contracts and reference data are configuration and live in the repository. Fetched data
lives in the data directory, which is never committed (see docs/DECISIONS.md, D-002).
"""

import os
from pathlib import Path

# src/merx/config.py -> repository root. The package is installed in editable mode, so
# the contracts and reference files next to the code are the ones in the checkout.
REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACTS_DIR = REPO_ROOT / "contracts"
REFERENCE_DIR = REPO_ROOT / "reference"


def data_dir() -> Path:
    """The data directory: ``MERX_DATA_DIR`` if set, else ``./data`` in the repository."""
    configured = os.environ.get("MERX_DATA_DIR")
    return Path(configured) if configured else REPO_ROOT / "data"
