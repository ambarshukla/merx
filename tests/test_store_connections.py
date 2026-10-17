"""How the consoles' read-only store uses connections.

Opening the database file costs around 30 ms; a query on a connection already open costs
a fraction of a millisecond. The catalogue asks roughly eighty small questions, so before
`Store.batch` existed the page spent over two seconds opening and closing one file. These
tests pin the behaviour that fixed it, and the safety properties around it.
"""

import threading

import duckdb
import pytest
from fastapi.testclient import TestClient

from merx.api import create_app, data
from merx.store import DB_FILE, connect


@pytest.fixture
def store(tmp_path) -> data.Store:
    con = connect(tmp_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS published")
    con.execute("CREATE TABLE published.t AS SELECT 1 AS a")
    con.close()
    return data.Store(tmp_path)


def count_opens(monkeypatch) -> list[int]:
    opened = [0]
    real = duckdb.connect

    def counting(*args, **kwargs):
        opened[0] += 1
        return real(*args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", counting)
    return opened


def test_each_query_opens_the_file_when_not_batched(store, monkeypatch):
    opened = count_opens(monkeypatch)
    for _ in range(4):
        store.query("SELECT 1")
    assert opened[0] == 4


def test_a_batch_opens_the_file_once(store, monkeypatch):
    opened = count_opens(monkeypatch)
    with store.batch():
        for _ in range(20):
            store.query("SELECT 1")
        assert store.has("published", "t")
    assert opened[0] == 1


def test_batches_nest_so_a_function_need_not_know_its_caller(store, monkeypatch):
    opened = count_opens(monkeypatch)
    with store.batch():
        store.query("SELECT 1")
        with store.batch():
            store.query("SELECT 1")
        # The inner block must not have closed the connection the outer one still needs.
        assert store.query("SELECT 1") == [(1,)]
    assert opened[0] == 1


def test_the_connection_is_released_when_the_block_ends(store, monkeypatch):
    with store.batch():
        store.query("SELECT 1")
    opened = count_opens(monkeypatch)
    store.query("SELECT 1")
    assert opened[0] == 1, "a query after the block must open its own connection"


def test_the_connection_is_released_even_when_the_block_raises(store, monkeypatch):
    with pytest.raises(ValueError), store.batch():
        store.query("SELECT 1")
        raise ValueError("boom")
    opened = count_opens(monkeypatch)
    store.query("SELECT 1")
    assert opened[0] == 1


def test_a_batch_over_a_database_that_does_not_exist_yet(tmp_path):
    """The site starts before the first pipeline run, and answers empty rather than 500."""
    store = data.Store(tmp_path / "nothing")
    with store.batch():
        assert store.query("SELECT 1") == []
        assert store.tables() == set()


def test_one_thread_never_sees_another_thread_connection(store):
    """Requests are served on separate threads; a held connection belongs to one of them."""
    seen: dict[str, object] = {}
    started = threading.Event()

    def outsider():
        started.wait(timeout=5)
        seen["other"] = getattr(data._held, "connection", None)

    thread = threading.Thread(target=outsider)
    thread.start()
    with store.batch():
        seen["mine"] = data._held.connection
        started.set()
        thread.join(timeout=5)
    assert seen["mine"] is not None
    assert seen["other"] is None


def test_a_database_held_by_a_writer_is_reported_as_busy(tmp_path, monkeypatch):
    """A run writing the file must not surface as a stack trace on the page."""
    (tmp_path / DB_FILE).write_bytes(b"")
    monkeypatch.setattr(data, "_PAUSE_S", 0)

    def always_locked(*args, **kwargs):
        raise duckdb.IOException("locked")

    monkeypatch.setattr(duckdb, "connect", always_locked)
    store = data.Store(tmp_path)
    with pytest.raises(data.DatabaseBusy):
        store.query("SELECT 1")
    with pytest.raises(data.DatabaseBusy), store.batch():
        pass


def test_a_request_uses_exactly_one_connection(tmp_path, monkeypatch):
    con = connect(tmp_path / "data")
    con.execute("CREATE SCHEMA IF NOT EXISTS published")
    con.close()
    client = TestClient(create_app(tmp_path / "data"))
    opened = count_opens(monkeypatch)
    assert client.get("/api/client/datasets").status_code == 200
    assert opened[0] == 1
