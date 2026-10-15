from datetime import UTC, datetime

import pytest

from merx.store.raw import RawZone

URL = "https://example.test/files/RWTCd.xls"


def land(zone: RawZone, content: bytes, when: datetime):
    return zone.land(
        dataset_id="eia_wti_spot",
        provider="eia",
        url=URL,
        content=content,
        http_status=200,
        headers={"Last-Modified": "Thu, 15 Oct 2026 13:52:15 GMT", "ETag": '"abc"'},
        fetched_at_utc=when,
    )


def test_every_download_is_kept_even_when_identical(tmp_path):
    zone = RawZone(tmp_path)
    first = land(zone, b"same bytes", datetime(2026, 10, 15, 21, 0, tzinfo=UTC))
    second = land(zone, b"same bytes", datetime(2026, 10, 16, 21, 0, tzinfo=UTC))
    assert first.path != second.path
    assert first.sha256 == second.sha256
    files = sorted((tmp_path / "raw" / "eia" / "eia_wti_spot").iterdir())
    assert [f.read_bytes() for f in files] == [b"same bytes", b"same bytes"]


def test_latest_is_the_newest_download(tmp_path):
    zone = RawZone(tmp_path)
    land(zone, b"old", datetime(2026, 10, 15, 21, 0, tzinfo=UTC))
    newest = land(zone, b"new", datetime(2026, 10, 16, 21, 0, tzinfo=UTC))
    latest = zone.latest("eia_wti_spot")
    assert latest == newest
    assert zone.read(latest) == b"new"


def test_manifest_records_headers_and_relative_path(tmp_path):
    landed = land(RawZone(tmp_path), b"x", datetime(2026, 10, 15, 21, 0, 5, tzinfo=UTC))
    assert landed.path.startswith("raw/eia/eia_wti_spot/20261015T210005")
    assert landed.path.endswith(".xls")
    assert landed.last_modified == "Thu, 15 Oct 2026 13:52:15 GMT"
    assert landed.etag == '"abc"'
    assert landed.fetched_at_utc.tzinfo is None  # stored as naive UTC


def test_tampered_file_is_detected(tmp_path):
    zone = RawZone(tmp_path)
    landed = land(zone, b"original", datetime(2026, 10, 15, 21, 0, tzinfo=UTC))
    (tmp_path / landed.path).write_bytes(b"edited")
    with pytest.raises(ValueError, match="no longer matches"):
        zone.read(landed)


def test_latest_without_any_download_fails(tmp_path):
    with pytest.raises(LookupError, match="no raw file"):
        RawZone(tmp_path).latest("eia_wti_spot")
