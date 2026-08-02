"""Tests for progress.py: database finalize/resume logic."""

from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from deribit_fetcher.progress import (
    DatabaseClient,
    FutureProgressRepo,
    OptionProgressRepo,
)


@pytest_asyncio.fixture
async def db(tmp_path):
    """Create a database connection fixture backed by a temp file."""
    db_path = tmp_path / "test.db"
    async with DatabaseClient(db_path) as conn:
        yield conn


@pytest_asyncio.fixture
async def future_repo(db):
    """Create a FutureProgressRepo fixture."""
    return FutureProgressRepo(db)


@pytest_asyncio.fixture
async def option_repo(db):
    """Create an OptionProgressRepo fixture."""
    return OptionProgressRepo(db)


pytestmark = pytest.mark.asyncio


class TestFutureProgressRepo:
    """Tests for FutureProgressRepo finalize/resume logic."""

    async def test_finalize_chunks_marks_confirmed_full_range_done(self, future_repo):
        """A full response is final only when the API also reports has_more=false."""
        # Insert a "complete" chunk with count >= CHUNK_SIZE
        await future_repo.db.execute(
            "INSERT INTO future_chunk (instrument, chunk_no, count, has_more, is_done) VALUES (?, ?, ?, ?, 0)",  # noqa: E501
            ("BTC-PERPETUAL", 1, 10000, 0),
        )
        await future_repo.db.commit()

        await future_repo.finalize_chunks()

        cur = await future_repo.db.execute(
            "SELECT is_done FROM future_chunk WHERE instrument=? AND chunk_no=?",
            ("BTC-PERPETUAL", 1),
        )
        row = await cur.fetchone()
        assert row["is_done"] == 1, "Completed chunk should be marked done"

    async def test_finalize_chunks_keeps_full_response_with_more_pending(self, future_repo):
        """count=CHUNK_SIZE cannot override an explicit has_more=true response."""
        await future_repo.db.execute(
            "INSERT INTO future_chunk (instrument, chunk_no, count, has_more, is_done) VALUES (?, ?, ?, ?, 0)",  # noqa: E501
            ("BTC-PERPETUAL", 1, 10_000, 1),
        )
        await future_repo.db.commit()

        await future_repo.finalize_chunks()

        pending = await future_repo.get_pending_chunks()
        assert pending == [{"instrument": "BTC-PERPETUAL", "chunk_no": 1}]

    async def test_finalize_chunks_skips_partial(self, future_repo):
        """A chunk with count < CHUNK_SIZE and has_more=1 should NOT be marked done."""
        await future_repo.db.execute(
            "INSERT INTO future_chunk (instrument, chunk_no, count, has_more, is_done) VALUES (?, ?, ?, ?, 0)",  # noqa: E501
            ("BTC-PERPETUAL", 1, 5000, 1),
        )
        await future_repo.db.commit()

        await future_repo.finalize_chunks()

        cur = await future_repo.db.execute(
            "SELECT is_done FROM future_chunk WHERE instrument=? AND chunk_no=?",
            ("BTC-PERPETUAL", 1),
        )
        row = await cur.fetchone()
        assert row["is_done"] == 0, "Partial chunk should NOT be marked done"

    async def test_finalize_chunks_marks_expired_partial_tail_done(self, future_repo):
        """An expired future's partial tail cannot grow and should be finalized."""
        await future_repo.db.execute(
            "INSERT INTO future_meta (instrument, is_expired, is_completed) VALUES (?, 1, 0)",
            ("BTC-EXPIRED",),
        )
        await future_repo.db.execute(
            "INSERT INTO future_chunk (instrument, chunk_no, count, has_more, is_done) VALUES (?, ?, ?, ?, 0)",  # noqa: E501
            ("BTC-EXPIRED", 5, 350, 0),
        )
        await future_repo.db.commit()

        await future_repo.finalize_chunks()

        cur = await future_repo.db.execute(
            "SELECT is_done FROM future_chunk WHERE instrument=? AND chunk_no=?",
            ("BTC-EXPIRED", 5),
        )
        row = await cur.fetchone()
        assert row["is_done"] == 1, "Expired partial tail should be marked done"

    async def test_finalize_chunks_keeps_active_partial_tail_pending(self, future_repo):
        """An active future can grow inside its current tail chunk on the next run."""
        await future_repo.db.execute(
            "INSERT INTO future_meta (instrument, is_expired, is_completed) VALUES (?, 0, 0)",
            ("BTC-PERPETUAL",),
        )
        await future_repo.db.execute(
            "INSERT INTO future_chunk (instrument, chunk_no, count, has_more, is_done) VALUES (?, ?, ?, ?, 0)",  # noqa: E501
            ("BTC-PERPETUAL", 10_001, 5_000, 0),
        )
        await future_repo.db.commit()

        await future_repo.finalize_chunks()

        pending = await future_repo.get_pending_chunks()
        assert pending == [{"instrument": "BTC-PERPETUAL", "chunk_no": 10_001}]

    async def test_active_tail_is_reopened_until_fixed_range_is_full(self, future_repo):
        """Growth from seq 15000 to 25000 must schedule the 15001..20000 interval."""
        instrument = "BTC-PERPETUAL"
        await future_repo.db.execute(
            "INSERT INTO future_meta (instrument, is_expired, is_completed) VALUES (?, 0, 0)",
            (instrument,),
        )
        await future_repo.upsert_chunks([(instrument, 1), (instrument, 10_001)])
        await future_repo.update_chunks(
            [
                (10_000, False, instrument, 1),
                (5_000, False, instrument, 10_001),
            ]
        )
        await future_repo.finalize_chunks()

        await future_repo.upsert_chunks(
            [(instrument, 1), (instrument, 10_001), (instrument, 20_001)]
        )

        pending = await future_repo.get_pending_chunks()
        assert {chunk["chunk_no"] for chunk in pending} == {10_001, 20_001}

    async def test_get_pending_chunks_excludes_done(self, future_repo):
        """get_pending_chunks should only return is_done=0 chunks."""
        await future_repo.db.execute(
            "INSERT INTO future_chunk (instrument, chunk_no, count, has_more, is_done) VALUES (?, ?, ?, ?, ?)",  # noqa: E501
            ("BTC-1", 1, 10000, 1, 1),  # done
        )
        await future_repo.db.execute(
            "INSERT INTO future_chunk (instrument, chunk_no, count, has_more, is_done) VALUES (?, ?, ?, ?, ?)",  # noqa: E501
            ("BTC-1", 2, 10000, 1, 0),  # pending
        )
        await future_repo.db.commit()

        pending = await future_repo.get_pending_chunks()
        assert len(pending) == 1
        assert pending[0]["chunk_no"] == 2

    async def test_upsert_chunks_ignore_duplicate(self, future_repo):
        """INSERT OR IGNORE should not create duplicate chunk entries."""
        await future_repo.upsert_chunks([("BTC-PERPETUAL", 1)])
        await future_repo.upsert_chunks([("BTC-PERPETUAL", 1)])  # duplicate

        cur = await future_repo.db.execute(
            "SELECT COUNT(*) as cnt FROM future_chunk WHERE instrument=? AND chunk_no=?",
            ("BTC-PERPETUAL", 1),
        )
        row = await cur.fetchone()
        assert row["cnt"] == 1, "Duplicate chunk insert should be ignored"

    async def test_finalize_future_meta_completes_expired(self, future_repo):
        """Expired future with all chunks done should be marked is_completed."""
        await future_repo.db.execute(
            "INSERT INTO future_meta (instrument, is_expired, is_completed) VALUES (?, 1, 0)",
            ("BTC-EXPIRED-1",),
        )
        await future_repo.db.execute(
            "INSERT INTO future_chunk (instrument, chunk_no, count, has_more, is_done) VALUES (?, ?, ?, ?, 1)",  # noqa: E501
            ("BTC-EXPIRED-1", 1, 10000, 0),
        )
        await future_repo.db.commit()

        await future_repo.finalize_future_meta()

        cur = await future_repo.db.execute(
            "SELECT is_completed FROM future_meta WHERE instrument=?",
            ("BTC-EXPIRED-1",),
        )
        row = await cur.fetchone()
        assert row["is_completed"] == 1, "Expired future with all chunks done should be completed"

    async def test_finalize_future_meta_skips_active(self, future_repo):
        """Active (non-expired) future should NOT be marked completed even if chunks done."""
        await future_repo.db.execute(
            "INSERT INTO future_meta (instrument, is_expired, is_completed) VALUES (?, 0, 0)",
            ("BTC-ACTIVE-1",),
        )
        await future_repo.db.execute(
            "INSERT INTO future_chunk (instrument, chunk_no, count, has_more, is_done) VALUES (?, ?, ?, ?, 1)",  # noqa: E501
            ("BTC-ACTIVE-1", 1, 10000, 1),
        )
        await future_repo.db.commit()

        await future_repo.finalize_future_meta()

        cur = await future_repo.db.execute(
            "SELECT is_completed FROM future_meta WHERE instrument=?",
            ("BTC-ACTIVE-1",),
        )
        row = await cur.fetchone()
        assert row["is_completed"] == 0, "Active future should NOT be marked completed"

    async def test_finalize_future_meta_skips_with_pending_chunks(self, future_repo):
        """Expired future with pending chunks should NOT be marked completed."""
        await future_repo.db.execute(
            "INSERT INTO future_meta (instrument, is_expired, is_completed) VALUES (?, 1, 0)",
            ("BTC-EXPIRED-PENDING",),
        )
        await future_repo.db.execute(
            "INSERT INTO future_chunk (instrument, chunk_no, count, has_more, is_done) VALUES (?, ?, ?, ?, 0)",  # noqa: E501
            ("BTC-EXPIRED-PENDING", 1, 5000, 1),
        )
        await future_repo.db.commit()

        await future_repo.finalize_future_meta()

        cur = await future_repo.db.execute(
            "SELECT is_completed FROM future_meta WHERE instrument=?",
            ("BTC-EXPIRED-PENDING",),
        )
        row = await cur.fetchone()
        assert row["is_completed"] == 0, (
            "Expired future with pending chunks should not be completed"
        )


class TestOptionProgressRepo:
    """Tests for OptionProgressRepo resume logic."""

    async def test_update_option_last_no_monotonic(self, option_repo):
        """update_option_last_no should never decrease last_no (MAX guard)."""
        await option_repo.db.execute(
            "INSERT INTO option_meta (instrument, last_no, is_expired, is_completed) VALUES (?, ?, ?, 0)",  # noqa: E501
            ("BTC-OPTION-1", 100, 1),
        )
        await option_repo.db.commit()

        # Update with a LOWER value
        await option_repo.update_option_last_no([(50, "BTC-OPTION-1")])

        cur = await option_repo.db.execute(
            "SELECT last_no FROM option_meta WHERE instrument=?",
            ("BTC-OPTION-1",),
        )
        row = await cur.fetchone()
        assert row["last_no"] == 100, "last_no should not decrease (MAX guard)"

    async def test_update_option_last_no_increases(self, option_repo):
        """update_option_last_no should increase last_no with higher values."""
        await option_repo.db.execute(
            "INSERT INTO option_meta (instrument, last_no, is_expired, is_completed) VALUES (?, ?, ?, 0)",  # noqa: E501
            ("BTC-OPTION-1", 100, 1),
        )
        await option_repo.db.commit()

        await option_repo.update_option_last_no([(200, "BTC-OPTION-1")])

        cur = await option_repo.db.execute(
            "SELECT last_no FROM option_meta WHERE instrument=?",
            ("BTC-OPTION-1",),
        )
        row = await cur.fetchone()
        assert row["last_no"] == 200, "last_no should increase"

    async def test_update_option_last_no_propagates_database_failure(self):
        """A failed checkpoint write must fail the pipeline rather than log and continue."""
        failing_db = AsyncMock()
        failing_db.executemany.side_effect = OSError("simulated checkpoint failure")
        repo = OptionProgressRepo(failing_db)

        with pytest.raises(OSError, match="simulated checkpoint failure"):
            await repo.update_option_last_no([(200, "BTC-OPTION-1")])

        failing_db.commit.assert_not_awaited()

    async def test_get_incomplete_excludes_completed(self, option_repo):
        """get_incomplete_option_list should only return is_completed=0 instruments."""
        await option_repo.db.execute(
            "INSERT INTO option_meta (instrument, last_no, is_expired, is_completed) VALUES (?, ?, ?, ?)",  # noqa: E501
            ("BTC-OPTION-1", 5000, 0, 0),  # incomplete
        )
        await option_repo.db.execute(
            "INSERT INTO option_meta (instrument, last_no, is_expired, is_completed) VALUES (?, ?, ?, ?)",  # noqa: E501
            ("BTC-OPTION-2", 10000, 0, 1),  # completed
        )
        await option_repo.db.commit()

        incomplete = await option_repo.get_incomplete_option_list()
        assert len(incomplete) == 1
        assert incomplete[0]["instrument"] == "BTC-OPTION-1"

    async def test_resume_from_last_no(self, option_repo):
        """Resume should start from last_no + 1."""
        await option_repo.db.execute(
            "INSERT INTO option_meta (instrument, last_no, is_expired, is_completed) VALUES (?, ?, ?, 0)",  # noqa: E501
            ("BTC-OPTION-1", 5000, 1),
        )
        await option_repo.db.commit()

        incomplete = await option_repo.get_incomplete_option_list()
        assert incomplete[0]["last_no"] == 5000
        start_seq = incomplete[0]["last_no"] + 1
        assert start_seq == 5001, "Resume should start from last_no + 1"

    async def test_mark_options_complete(self, option_repo):
        """mark_options_complete should set is_completed=1."""
        await option_repo.db.execute(
            "INSERT INTO option_meta (instrument, last_no, is_expired, is_completed) VALUES (?, ?, ?, 0)",  # noqa: E501
            ("BTC-OPTION-1", 5000, 1),
        )
        await option_repo.db.commit()

        await option_repo.mark_options_complete(["BTC-OPTION-1"])

        cur = await option_repo.db.execute(
            "SELECT is_completed FROM option_meta WHERE instrument=?",
            ("BTC-OPTION-1",),
        )
        row = await cur.fetchone()
        assert row["is_completed"] == 1
