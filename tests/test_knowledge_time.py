"""The knowledge time of a staged row (D-029).

A downloaded file is known when it was fetched. An archived copy is known when the
archive captured it — unless the file's own release date is later than that capture,
which cannot be true and means the capture stamp is wrong.
"""

from datetime import date, datetime

import pytest

from merx.engine.formats import ReadResult
from merx.engine.stage import knowledge_time_for
from merx.store.raw import LandedFile


def landed(origin: str, fetched: datetime) -> LandedFile:
    return LandedFile(
        dataset_id="eia_tx_gas_marketed_production",
        provider="eia",
        url="https://example.invalid/n9050tx2m.xls",
        fetched_at_utc=fetched,
        sha256="0" * 64,
        bytes=1,
        path=f"raw/eia/d/{origin}.xls",
        http_status=200,
        last_modified=None,
        etag=None,
        origin=origin,
    )


def read(release: date | None) -> ReadResult:
    return ReadResult(observations=[], release_date=release)


CAPTURE = datetime(2011, 7, 6, 18, 9, 5)


def test_downloaded_file_is_known_when_it_was_fetched():
    fetched = datetime(2026, 10, 18, 9, 30)
    assert knowledge_time_for(landed("direct", fetched), read(date(2026, 9, 30))) == fetched


def test_archived_copy_is_known_when_it_was_captured():
    assert knowledge_time_for(landed("archive", CAPTURE), read(date(2011, 6, 30))) == CAPTURE


def test_release_date_later_than_capture_wins():
    """The real case: the archive's oldest copy of the Texas gas spreadsheet is stamped
    2011-07-06 and holds a file released 2017-01-31."""
    assert knowledge_time_for(landed("archive", CAPTURE), read(date(2017, 1, 31))) == datetime(
        2017, 1, 31
    )


def test_a_file_with_no_release_date_keeps_its_capture_time():
    assert knowledge_time_for(landed("archive", CAPTURE), read(None)) == CAPTURE


@pytest.mark.parametrize("origin", ["direct", "archive"])
def test_the_rule_never_moves_knowledge_earlier(origin):
    """A release date before the fetch must not rewind knowledge time: we did know the
    contents then, whatever the file says about itself."""
    fetched = datetime(2026, 10, 18, 9, 30)
    assert knowledge_time_for(landed(origin, fetched), read(date(1999, 1, 1))) == fetched
