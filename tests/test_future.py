"""Tests for future.py task preparation, especially last_seq failure semantics.

Regression guard: when get_last_trade_seq cannot determine a future's last_seq
(returns None after exhausting retries), that future must be left INCOMPLETE and
allocated NO chunks — so it is retried on the next run rather than silently
marked complete (which would permanently drop its entire trade history).
"""

import pytest
import pytest_asyncio

from deribit_fetcher.progress import DatabaseClient, FutureProgressRepo
from deribit_fetcher.future import _prepare_tasks

pytestmark = pytest.mark.asyncio


class FakeClient:
    """Minimal stand-in for DeribitClient exposing only get_last_trade_seq."""

    def __init__(self, seq_map: dict[str, int | None]):
        self.seq_map = seq_map
        self.calls: list[str] = []

    async def get_last_trade_seq(self, instrument: str) -> int | None:
        self.calls.append(instrument)
        return self.seq_map[instrument]


@pytest_asyncio.fixture
async def repo(tmp_path):
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


async def test_undetermined_seq_is_not_marked_complete(repo):
    """last_seq is None -> stay incomplete, no chunks, retried next run."""
    await _seed(repo, "BTC-FAIL")
    client = FakeClient({"BTC-FAIL": None})

    await _prepare_tasks(repo, client, refresh_list=False)

    assert await _is_completed(repo, "BTC-FAIL") == 0, "failed lookup must NOT complete"
    assert await _chunk_count(repo, "BTC-FAIL") == 0, "failed lookup must allocate no chunks"


async def test_zero_seq_is_marked_complete(repo):
    """last_seq == 0 -> genuinely no trades -> marked complete, no chunks."""
    await _seed(repo, "BTC-EMPTY")
    client = FakeClient({"BTC-EMPTY": 0})

    await _prepare_tasks(repo, client, refresh_list=False)

    assert await _is_completed(repo, "BTC-EMPTY") == 1
    assert await _chunk_count(repo, "BTC-EMPTY") == 0


async def test_positive_seq_allocates_chunks(repo):
    """last_seq > 0 -> chunks pre-allocated, instrument stays incomplete."""
    await _seed(repo, "BTC-HAS")
    client = FakeClient({"BTC-HAS": 25_000})  # CHUNK_SIZE 10000 -> seqs 1, 10001, 20001

    pending = await _prepare_tasks(repo, client, refresh_list=False)

    assert await _is_completed(repo, "BTC-HAS") == 0
    assert await _chunk_count(repo, "BTC-HAS") == 3
    assert {c["chunk_no"] for c in pending} == {1, 10_001, 20_001}


async def test_mixed_batch_isolates_failures(repo):
    """A failing lookup must not affect its healthy neighbours in the same batch."""
    for instr in ("BTC-FAIL", "BTC-EMPTY", "BTC-HAS"):
        await _seed(repo, instr)
    client = FakeClient({"BTC-FAIL": None, "BTC-EMPTY": 0, "BTC-HAS": 15_000})

    await _prepare_tasks(repo, client, refresh_list=False)

    assert await _is_completed(repo, "BTC-FAIL") == 0
    assert await _chunk_count(repo, "BTC-FAIL") == 0
    assert await _is_completed(repo, "BTC-EMPTY") == 1
    assert await _is_completed(repo, "BTC-HAS") == 0
    assert await _chunk_count(repo, "BTC-HAS") == 2  # seqs 1, 10001
