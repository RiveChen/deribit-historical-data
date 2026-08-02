"""Tests for option.py: fetch_option_chunk, on_option_success, sync_option_db.

All tests use mocks — no real API calls, no network.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from deribit_fetcher.config import settings
from deribit_fetcher.option import fetch_option_chunk, on_option_success, sync_option_db

# =============================================================================
# Fake client
# =============================================================================


class FakeClient:
    """A mock DeribitClient that returns pre-configured (trades, has_more)."""

    def __init__(self, trades: list, has_more: bool):
        """Initialize mock with pre-configured response."""
        self._trades = trades
        self._has_more = has_more

    async def get_trades_chunk(self, instrument: str, start_seq: int, end_seq: int):
        """Return pre-configured (trades, has_more) tuple."""
        return (self._trades, self._has_more)


# =============================================================================
# Test fetch_option_chunk
# =============================================================================


class TestFetchOptionChunk:
    """Test the fetch callback that determines whether streaming should continue."""

    @pytest.fixture(autouse=True)
    def _patch_chunk_size(self, monkeypatch):
        monkeypatch.setattr(settings, "CHUNK_SIZE", 10)

    async def test_chunk_full_should_continue(self):
        """When trades count >= CHUNK_SIZE, should_continue=True."""
        # Provide CHUNK_SIZE trades to trigger "full chunk" condition
        trades = [{"trade_seq": 42}] * settings.CHUNK_SIZE
        client = FakeClient(
            trades=trades,
            has_more=False,
        )
        result = await fetch_option_chunk(
            {"instrument": "BTC-OPT", "start_seq": 1, "is_expired": True},
            client=client,
        )
        assert result["should_continue"] is True, "Full chunk should continue"
        assert result["next_seq"] == 43, "next_seq should be last_trade_seq + 1"
        assert result["finished"] is False, "Should not mark finished when continuing"

    async def test_has_more_triggers_continue(self):
        """Even when trades are few, has_more=True should trigger continue."""
        client = FakeClient(
            trades=[{"trade_seq": 5}],
            has_more=True,
        )
        result = await fetch_option_chunk(
            {"instrument": "BTC-OPT", "start_seq": 1, "is_expired": True},
            client=client,
        )
        assert result["should_continue"] is True, "has_more=True should continue"
        assert result["finished"] is False

    async def test_expired_no_more_finished(self):
        """Expired instrument with no more data -> finished=True."""
        client = FakeClient(
            trades=[{"trade_seq": 15}],
            has_more=False,
        )
        result = await fetch_option_chunk(
            {"instrument": "BTC-OPT", "start_seq": 10, "is_expired": True},
            client=client,
        )
        assert result["should_continue"] is False, "No more data should not continue"
        assert result["finished"] is True, "Expired with no more data should be finished"

    async def test_active_no_more_not_finished(self):
        """Active (non-expired) instrument with no more data -> NOT finished.

        Active instruments may receive new trades in the future, so they
        should NOT be marked complete even if the current chunk is empty.
        """
        client = FakeClient(
            trades=[{"trade_seq": 8}],
            has_more=False,
        )
        result = await fetch_option_chunk(
            {"instrument": "BTC-OPT", "start_seq": 5, "is_expired": False},
            client=client,
        )
        assert result["should_continue"] is False
        assert result["finished"] is False, "Active instrument must NOT be marked finished"

    async def test_no_trades_uses_start_seq_as_fallback(self):
        """When no trades are returned, last_seq_in_chunk defaults to start_seq."""
        client = FakeClient(trades=[], has_more=False)
        result = await fetch_option_chunk(
            {"instrument": "BTC-OPT", "start_seq": 100, "is_expired": True},
            client=client,
        )
        assert result["should_continue"] is False
        assert result["next_seq"] == 101, "next_seq should fall back to start_seq + 1"
        assert result["finished"] is True, "Expired with no trades should be finished"

    async def test_next_seq_from_max_trade_seq_in_unsorted_response(self):
        """next_seq must use the maximum trade_seq, not the first response row."""
        client = FakeClient(
            trades=[{"trade_seq": 90}, {"trade_seq": 100}, {"trade_seq": 95}],
            has_more=True,
        )
        result = await fetch_option_chunk(
            {"instrument": "BTC-OPT", "start_seq": 1, "is_expired": False},
            client=client,
        )
        assert result["next_seq"] == 101, "next_seq = largest trade_seq + 1"


# =============================================================================
# Test on_option_success
# =============================================================================


class TestOnOptionSuccess:
    """Test the callback that enqueues the next chunk."""

    @pytest.fixture
    def engine(self):
        """A mock engine that captures enqueue_task calls."""
        engine = MagicMock()
        engine.enqueue_task = AsyncMock()
        return engine

    @pytest.fixture
    def stop_event(self):
        """Return a fresh, unset asyncio.Event."""
        return asyncio.Event()

    async def test_should_continue_enqueues_next(self, engine, stop_event):
        """When should_continue=True, a follow-up task should be enqueued."""
        tasking = {"instrument": "BTC-OPT", "is_expired": True}
        result_item = {"should_continue": True, "next_seq": 200}

        await on_option_success(tasking, result_item, engine=engine, stop_event=stop_event)

        engine.enqueue_task.assert_awaited_once_with(
            {"instrument": "BTC-OPT", "start_seq": 200, "is_expired": True}
        )

    async def test_should_not_continue_skips_enqueue(self, engine, stop_event):
        """When should_continue=False, no task should be enqueued."""
        tasking = {"instrument": "BTC-OPT", "is_expired": True}
        result_item = {"should_continue": False, "next_seq": 200}

        await on_option_success(tasking, result_item, engine=engine, stop_event=stop_event)

        engine.enqueue_task.assert_not_called()

    async def test_stop_event_blocks_enqueue(self, engine):
        """When stop_event is set, no task should be enqueued even if should_continue."""
        stop_event = asyncio.Event()
        stop_event.set()

        tasking = {"instrument": "BTC-OPT", "is_expired": True}
        result_item = {"should_continue": True, "next_seq": 300}

        await on_option_success(tasking, result_item, engine=engine, stop_event=stop_event)

        engine.enqueue_task.assert_not_called()


# =============================================================================
# Test sync_option_db
# =============================================================================


class FakeSink:
    """A JSONLinesSink mock that captures flushed data."""

    def __init__(self):
        """Initialize mock with empty capture list."""
        self.captured = []
        self.flush_call_count = 0

    async def flush(self, buffers):
        """Record flushed buffers for later assertion."""
        self.captured.append(buffers)
        self.flush_call_count += 1


class FakeRepo:
    """An OptionProgressRepo mock that captures updates."""

    def __init__(self):
        """Initialize mock with empty capture lists."""
        self.last_no_updates = []
        self.completed = []

    async def update_option_last_no(self, updates):
        """Capture update_option_last_no calls."""
        self.last_no_updates.extend(updates)

    async def mark_options_complete(self, instruments):
        """Capture mark_options_complete calls."""
        self.completed.extend(instruments)


class TestSyncOptionDb:
    """Test the sync callback that flushes data to disk and updates DB."""

    async def test_max_seq_is_taken(self):
        """The checkpoint uses the max seq across every row of every item."""
        sink = FakeSink()
        repo = FakeRepo()

        buffers = {
            "BTC-OPT": [
                {
                    "data": [{"trade_seq": 50}, {"trade_seq": 125}, {"trade_seq": 80}],
                    "finished": False,
                },
                {"data": [{"trade_seq": 100}], "finished": False},
                {"data": [{"trade_seq": 75}], "finished": False},
            ],
        }

        await sync_option_db(buffers, sink=sink, repo=repo)

        assert repo.last_no_updates == [(125, "BTC-OPT")], "Should use max trade_seq"

    async def test_finished_instrument_marked_complete(self):
        """When an instrument has finished=True, it should be marked complete."""
        sink = FakeSink()
        repo = FakeRepo()

        buffers = {
            "BTC-EXPIRED": [
                {"data": [{"trade_seq": 200}], "finished": True},
            ],
        }

        await sync_option_db(buffers, sink=sink, repo=repo)

        assert repo.completed == ["BTC-EXPIRED"]

    async def test_not_finished_not_marked(self):
        """When no item has finished=True, nothing should be marked complete."""
        sink = FakeSink()
        repo = FakeRepo()

        buffers = {
            "BTC-ACTIVE": [
                {"data": [{"trade_seq": 300}], "finished": False},
            ],
        }

        await sync_option_db(buffers, sink=sink, repo=repo)

        assert repo.completed == [], "Active instrument should not be marked complete"

    async def test_data_none_skips_seq_update(self):
        """When an item has data=None, it should not contribute a seq update."""
        sink = FakeSink()
        repo = FakeRepo()

        buffers = {
            "BTC-OPT": [
                {"data": None, "finished": False},
            ],
        }

        await sync_option_db(buffers, sink=sink, repo=repo)

        assert repo.last_no_updates == [], "No data means no seq update"

    async def test_sink_flush_before_db_update(self, monkeypatch):
        """Data durability: sink.flush must be called before any DB update.

        We verify this by tracking the call order.
        """
        call_order = []

        class OrderedSink:
            async def flush(self, buffers):
                call_order.append("flush")

        class OrderedRepo:
            async def update_option_last_no(self, updates):
                call_order.append("db_update")

            async def mark_options_complete(self, instruments):
                call_order.append("mark_complete")

        buffers = {
            "BTC-OPT": [
                {"data": [{"trade_seq": 10}], "finished": True},
            ],
        }

        await sync_option_db(buffers, sink=OrderedSink(), repo=OrderedRepo())

        assert call_order == ["flush", "db_update", "mark_complete"], (
            "flush must come before DB updates"
        )

    async def test_multiple_instruments(self):
        """Multiple instruments should each get their own update."""
        sink = FakeSink()
        repo = FakeRepo()

        buffers = {
            "BTC-A": [
                {"data": [{"trade_seq": 10}], "finished": True},
            ],
            "BTC-B": [
                {"data": [{"trade_seq": 20}], "finished": False},
            ],
        }

        await sync_option_db(buffers, sink=sink, repo=repo)

        assert set(repo.last_no_updates) == {(10, "BTC-A"), (20, "BTC-B")}
        assert repo.completed == ["BTC-A"]
