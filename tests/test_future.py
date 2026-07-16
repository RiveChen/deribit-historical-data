"""Tests for future.py task preparation, especially last_seq failure semantics.

Regression guard: when get_last_trade_seq cannot determine a future's last_seq
(returns None after exhausting retries), that future must be left INCOMPLETE and
allocated NO chunks — so it is retried on the next run rather than silently
marked complete (which would permanently drop its entire trade history).
"""

import pytest
import pytest_asyncio

from deribit_fetcher.future import fetch_future_chunk, prepare_future_tasks, sync_future_db
from deribit_fetcher.progress import DatabaseClient, FutureProgressRepo

pytestmark = pytest.mark.asyncio


class FakeClient:
    """Minimal stand-in for DeribitClient exposing only get_last_trade_seq."""

    def __init__(self, seq_map: dict[str, int | None]):
        """Initialize the mock client with a sequence map."""
        self.seq_map = seq_map
        self.calls: list[str] = []

    async def get_instruments(self, currency: str, kind: str) -> list:
        """Stub implementation returning an empty list."""
        return []

    async def get_last_trade_seq(self, instrument: str) -> int | None:
        """Return the pre-configured last trade seq for the given instrument."""
        self.calls.append(instrument)
        return self.seq_map[instrument]


@pytest_asyncio.fixture
async def repo(tmp_path):
    """Create a FutureProgressRepo fixture backed by a temp database."""
    async with DatabaseClient(tmp_path / "future.db") as conn:
        yield FutureProgressRepo(conn)


async def _seed(repo, instrument, is_expired=1):
    await repo.db.execute(
        "INSERT INTO future_meta (instrument, is_expired, is_completed) VALUES (?, ?, 0)",
        (instrument, is_expired),
    )
    await repo.db.commit()


async def _is_completed(repo, instrument) -> int:
    cur = await repo.db.execute(
        "SELECT is_completed FROM future_meta WHERE instrument=?", (instrument,)
    )
    return (await cur.fetchone())["is_completed"]


async def _chunk_count(repo, instrument) -> int:
    cur = await repo.db.execute(
        "SELECT COUNT(*) AS c FROM future_chunk WHERE instrument=?", (instrument,)
    )
    return (await cur.fetchone())["c"]


# ---------------------------------------------------------------------------
# Tests for task preparation (original test suite)
# ---------------------------------------------------------------------------


async def test_undetermined_seq_is_not_marked_complete(repo):
    """last_seq is None -> stay incomplete, no chunks, retried next run."""
    await _seed(repo, "BTC-FAIL")
    client = FakeClient({"BTC-FAIL": None})

    await prepare_future_tasks(repo, client, refresh_list=False)

    assert await _is_completed(repo, "BTC-FAIL") == 0, "failed lookup must NOT complete"
    assert await _chunk_count(repo, "BTC-FAIL") == 0, "failed lookup must allocate no chunks"


async def test_zero_seq_is_marked_complete(repo):
    """last_seq == 0 -> genuinely no trades -> marked complete, no chunks."""
    await _seed(repo, "BTC-EMPTY")
    client = FakeClient({"BTC-EMPTY": 0})

    await prepare_future_tasks(repo, client, refresh_list=False)

    assert await _is_completed(repo, "BTC-EMPTY") == 1
    assert await _chunk_count(repo, "BTC-EMPTY") == 0


async def test_positive_seq_allocates_chunks(repo):
    """last_seq > 0 -> chunks pre-allocated, instrument stays incomplete."""
    await _seed(repo, "BTC-HAS")
    client = FakeClient({"BTC-HAS": 25_000})  # CHUNK_SIZE 10000 -> seqs 1, 10001, 20001

    pending = await prepare_future_tasks(repo, client, refresh_list=False)

    assert await _is_completed(repo, "BTC-HAS") == 0
    assert await _chunk_count(repo, "BTC-HAS") == 3
    assert {c["chunk_no"] for c in pending} == {1, 10_001, 20_001}


async def test_mixed_batch_isolates_failures(repo):
    """A failing lookup must not affect its healthy neighbours in the same batch."""
    for instr in ("BTC-FAIL", "BTC-EMPTY", "BTC-HAS"):
        await _seed(repo, instr)
    client = FakeClient({"BTC-FAIL": None, "BTC-EMPTY": 0, "BTC-HAS": 15_000})

    await prepare_future_tasks(repo, client, refresh_list=False)

    assert await _is_completed(repo, "BTC-FAIL") == 0
    assert await _chunk_count(repo, "BTC-FAIL") == 0
    assert await _is_completed(repo, "BTC-EMPTY") == 1
    assert await _is_completed(repo, "BTC-HAS") == 0
    assert await _chunk_count(repo, "BTC-HAS") == 2  # seqs 1, 10001


# ---------------------------------------------------------------------------
# Tests for extracted callbacks (new — these were previously untestable closures)
# ---------------------------------------------------------------------------


class FakeClientForFetch:
    """A mock DeribitClient that returns pre-configured trades."""

    def __init__(self, trades, has_more):
        """Initialize mock with pre-configured return values."""
        self._trades = trades
        self._has_more = has_more

    async def get_trades_chunk(self, instrument, start_seq, end_seq):
        """Return pre-configured trades and has_more flag."""
        return (self._trades, self._has_more)


async def test_fetch_future_chunk_returns_expected_shape():
    """fetch_future_chunk should return a dict with the expected keys."""
    client = FakeClientForFetch(
        trades=[{"trade_seq": 1, "price": 50000}],
        has_more=True,
    )
    result = await fetch_future_chunk(
        {"instrument": "BTC-PERPETUAL", "chunk_no": 1},
        client=client,
    )
    assert result["instrument"] == "BTC-PERPETUAL"
    assert result["chunk_no"] == 1
    assert result["has_more"] is True
    assert result["data"] == [{"trade_seq": 1, "price": 50000}]


async def test_fetch_future_chunk_handles_no_trades():
    """fetch_future_chunk should set data=None when no trades returned."""
    client = FakeClientForFetch(trades=[], has_more=False)
    result = await fetch_future_chunk(
        {"instrument": "BTC-PERPETUAL", "chunk_no": 10},
        client=client,
    )
    assert result["data"] is None
    assert result["has_more"] is False


class FakeSink:
    """A JSONLinesSink mock that captures flushed data."""

    def __init__(self):
        """Initialize mock with an empty capture list."""
        self.captured = []

    async def flush(self, buffers):
        """Record flushed buffers for later assertions."""
        self.captured.append(buffers)
        return


class FakeRepo:
    """A FutureProgressRepo mock that captures update_chunks calls."""

    def __init__(self):
        """Initialize mock with an empty updates list."""
        self.updates = []

    async def update_chunks(self, chunks):
        """Capture update_chunks calls for later assertions."""
        self.updates.extend(chunks)


async def test_sync_future_db_flushes_and_updates():
    """sync_future_db should call sink.flush then repo.update_chunks."""
    sink = FakeSink()
    repo = FakeRepo()

    buffers = {
        "BTC-A": [
            {"instrument": "BTC-A", "chunk_no": 1, "has_more": True, "data": [{"seq": 1}]},
        ],
        "BTC-B": [
            {"instrument": "BTC-B", "chunk_no": 5, "has_more": False, "data": [{"seq": 10}]},
        ],
    }

    await sync_future_db(buffers, sink=sink, repo=repo)

    # Sink should have received the buffers
    assert len(sink.captured) == 1
    assert sink.captured[0] is buffers

    # Repo should have received per-chunk updates
    assert len(repo.updates) == 2
    assert repo.updates[0] == (1, True, "BTC-A", 1)
    assert repo.updates[1] == (1, False, "BTC-B", 5)


async def test_sync_future_db_handles_empty_data():
    """sync_future_db should handle chunks with no data (count=0)."""
    sink = FakeSink()
    repo = FakeRepo()

    buffers = {
        "BTC-A": [
            {"instrument": "BTC-A", "chunk_no": 3, "has_more": False, "data": None},
        ],
    }

    await sync_future_db(buffers, sink=sink, repo=repo)

    assert len(sink.captured) == 1
    assert repo.updates[0] == (0, False, "BTC-A", 3)
