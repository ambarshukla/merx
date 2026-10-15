"""The raw landing zone.

Every download is kept exactly as received, including byte-identical repeats, under
``<data_dir>/raw/<provider>/<dataset_id>/[<partition>/]<fetched_at>__<sha256 prefix>.<ext>``
and recorded in ``meta.raw_manifest``. Nothing downstream edits these files: staging,
replays and point-in-time history are all rebuilt from them. A partition is one slice
of a source that publishes several files, such as one year of a yearly archive.

The file is written before its manifest row, so the manifest never points at a file
that does not exist. A crash between the two leaves an unrecorded file, which is
harmless and visible.
"""

import hashlib
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

import duckdb

from merx.store import connect

_COLUMNS = (
    "dataset_id, provider, url, fetched_at_utc, sha256, bytes, path, http_status, "
    "last_modified, etag, partition, origin, archive_url"
)

# Where a raw file came from. "direct": downloaded from the source by us, so its
# fetched_at_utc is when we learned its contents. "archive": a dated copy from a public
# web archive, so its fetched_at_utc is the archive's capture time: when the archive saw
# the file, an upper bound on when it was first public.
ORIGINS = ("direct", "archive")


def _ensure_manifest(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS meta.raw_manifest (
            dataset_id     VARCHAR   NOT NULL,
            provider       VARCHAR   NOT NULL,
            url            VARCHAR   NOT NULL,
            fetched_at_utc TIMESTAMP NOT NULL,
            sha256         VARCHAR   NOT NULL,
            bytes          BIGINT    NOT NULL,
            path           VARCHAR   NOT NULL PRIMARY KEY,  -- relative to the data directory
            http_status    INTEGER   NOT NULL,
            last_modified  VARCHAR,                          -- the server's header, verbatim
            etag           VARCHAR
        )
        """
    )
    # Added after the first downloads were landed; older rows take the defaults.
    con.execute("ALTER TABLE meta.raw_manifest ADD COLUMN IF NOT EXISTS partition VARCHAR")
    con.execute(
        "ALTER TABLE meta.raw_manifest ADD COLUMN IF NOT EXISTS origin VARCHAR DEFAULT 'direct'"
    )
    con.execute("ALTER TABLE meta.raw_manifest ADD COLUMN IF NOT EXISTS archive_url VARCHAR")


@dataclass(frozen=True)
class LandedFile:
    dataset_id: str
    provider: str
    url: str
    fetched_at_utc: datetime
    sha256: str
    bytes: int
    path: str
    http_status: int
    last_modified: str | None
    etag: str | None
    partition: str | None = None
    origin: str = "direct"
    archive_url: str | None = None


class RawZone:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir

    def land(
        self,
        *,
        dataset_id: str,
        provider: str,
        url: str,
        content: bytes,
        http_status: int,
        headers: dict[str, str],
        partition: str | None = None,
        fetched_at_utc: datetime | None = None,
        origin: str = "direct",
        archive_url: str | None = None,
    ) -> LandedFile:
        if origin not in ORIGINS:
            raise ValueError(f"unknown origin {origin!r}")
        fetched = (fetched_at_utc or datetime.now(UTC)).astimezone(UTC).replace(tzinfo=None)
        digest = hashlib.sha256(content).hexdigest()
        suffix = PurePosixPath(urlparse(url).path).suffix or ".bin"
        subfolders = [p for p in (partition, "archive" if origin == "archive" else None) if p]
        folder = PurePosixPath("raw", provider, dataset_id, *subfolders)
        rel = folder / f"{fetched:%Y%m%dT%H%M%S%fZ}__{digest[:12]}{suffix}"
        target = self.data_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise FileExistsError(f"refusing to overwrite raw file {target}")
        partial = target.with_name(target.name + ".part")
        partial.write_bytes(content)
        os.replace(partial, target)

        lowered = {k.lower(): v for k, v in headers.items()}
        landed = LandedFile(
            dataset_id=dataset_id,
            provider=provider,
            url=url,
            fetched_at_utc=fetched,
            sha256=digest,
            bytes=len(content),
            path=str(rel),
            http_status=http_status,
            last_modified=lowered.get("last-modified"),
            etag=lowered.get("etag"),
            partition=partition,
            origin=origin,
            archive_url=archive_url,
        )
        con = connect(self.data_dir)
        try:
            _ensure_manifest(con)
            con.execute(
                f"INSERT INTO meta.raw_manifest ({_COLUMNS}) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    landed.dataset_id,
                    landed.provider,
                    landed.url,
                    landed.fetched_at_utc,
                    landed.sha256,
                    landed.bytes,
                    landed.path,
                    landed.http_status,
                    landed.last_modified,
                    landed.etag,
                    landed.partition,
                    landed.origin,
                    landed.archive_url,
                ],
            )
        finally:
            con.close()
        return landed

    def latest(self, dataset_id: str) -> LandedFile:
        """The most recent direct download for a dataset. Raises if there is none.

        Archived copies are history, not a substitute for fetching: staging always reads
        a file we downloaded ourselves."""
        con = connect(self.data_dir)
        try:
            _ensure_manifest(con)
            row = con.execute(
                f"SELECT {_COLUMNS} FROM meta.raw_manifest "
                "WHERE dataset_id = ? AND origin = 'direct' "
                "ORDER BY fetched_at_utc DESC LIMIT 1",
                [dataset_id],
            ).fetchone()
        finally:
            con.close()
        if row is None:
            raise LookupError(f"no raw file landed yet for {dataset_id}")
        return LandedFile(*row)

    def latest_per_partition(self, dataset_id: str) -> list[LandedFile]:
        """The most recent direct download of each partition, ordered by partition."""
        con = connect(self.data_dir)
        try:
            _ensure_manifest(con)
            rows = con.execute(
                f"SELECT {_COLUMNS} FROM meta.raw_manifest "
                "WHERE dataset_id = ? AND partition IS NOT NULL AND origin = 'direct' "
                "QUALIFY row_number() OVER "
                "(PARTITION BY partition ORDER BY fetched_at_utc DESC) = 1 "
                "ORDER BY partition",
                [dataset_id],
            ).fetchall()
        finally:
            con.close()
        if not rows:
            raise LookupError(f"no raw partitions landed yet for {dataset_id}")
        return [LandedFile(*r) for r in rows]

    def history(self, dataset_id: str) -> list[LandedFile]:
        """Every raw file of a dataset, direct and archived, oldest knowledge first."""
        con = connect(self.data_dir)
        try:
            _ensure_manifest(con)
            rows = con.execute(
                f"SELECT {_COLUMNS} FROM meta.raw_manifest WHERE dataset_id = ? "
                "ORDER BY fetched_at_utc, path",
                [dataset_id],
            ).fetchall()
        finally:
            con.close()
        return [LandedFile(*r) for r in rows]

    def archived_captures(self, dataset_id: str) -> set[str]:
        """The archive URLs already landed for a dataset, so imports can skip them."""
        return {f.archive_url for f in self.history(dataset_id) if f.archive_url}

    def read(self, landed: LandedFile) -> bytes:
        """The bytes of a landed file, verified against the hash recorded when it landed."""
        content = (self.data_dir / landed.path).read_bytes()
        if hashlib.sha256(content).hexdigest() != landed.sha256:
            raise ValueError(f"raw file {landed.path} no longer matches its recorded sha256")
        return content
