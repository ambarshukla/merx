"""Dagster resources: shared services that assets ask for by name and tests can replace."""

from pathlib import Path

import dagster as dg

from merx.sources import http


class DataStore(dg.ConfigurableResource):
    """The data directory holding the raw landing zone and the DuckDB database."""

    data_dir: str

    @property
    def path(self) -> Path:
        return Path(self.data_dir)


class HttpFetcher(dg.ConfigurableResource):
    """Downloads source files over HTTPS."""

    timeout_s: float = 60.0
    # Pause between requests when importing many files from one host (web archives).
    polite_delay_s: float = 1.0

    def fetch(self, url: str) -> http.FetchResult:
        return http.fetch(url, timeout_s=self.timeout_s)
