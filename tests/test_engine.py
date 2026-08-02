"""Tests for engine.py: graceful shutdown and producer/consumer behavior."""

import asyncio

import pytest

from deribit_fetcher.engine import FetcherEngine, FetchTasksFailedError

pytestmark = pytest.mark.asyncio


@pytest.fixture
def engine():
    """Create a FetcherEngine fixture with small batch sizes for testing."""
    return FetcherEngine(
        worker_count=2,
        write_batch_size=5,
        task_queue_size=10,
        storage_queue_size=10,
    )


class TestGracefulShutdown:
    """Test suite for engine graceful shutdown behavior."""

    async def test_stop_event_during_task_distribution(self, engine):
        """If stop_event is set before engine.run starts, it should return early."""
        stop_event = asyncio.Event()
        stop_event.set()

        tasks = [{"id": i} for i in range(10)]

        async def fetch_func(tasking):
            return tasking

        async def sync_db(buffers):
            pass

        await engine.run(
            initial_tasks=tasks,
            fetch_func=fetch_func,
            sync_db_func=sync_db,
            stop_event=stop_event,
            pbar_desc="Test",
        )
        # Should complete without error. Tasks won't be processed since stop is set
        # after task distribution (but before workers process them, stop event makes producers exit)
        assert True

    async def test_shutdown_with_pending_storage(self, engine):
        """Flush remaining buffers when stop_event is set mid-flight.

        Verifies by counting how many times sync_db is called.
        """
        stop_event = asyncio.Event()
        sync_call_count = 0

        async def fetch_func(tasking):
            return {"instrument": "test", "data": [{"seq": tasking["seq"]}]}

        async def sync_db(buffers):
            nonlocal sync_call_count
            sync_call_count += 1

        # Create tasks
        tasks = [{"seq": i} for i in range(10)]

        # Run engine but trigger stop shortly after
        async def run_and_stop():
            engine_task = asyncio.create_task(
                engine.run(
                    initial_tasks=tasks,
                    fetch_func=fetch_func,
                    sync_db_func=sync_db,
                    stop_event=stop_event,
                    pbar_desc="Test",
                )
            )
            # Small delay to let some tasks process
            await asyncio.sleep(0.3)
            stop_event.set()
            await engine_task

        await run_and_stop()
        # sync_db should have been called at least once (flush during shutdown)
        assert sync_call_count >= 1, "Consumer should flush buffers on shutdown"

    async def test_poison_pill_triggers_flush(self, engine):
        """When None is sent to storage_queue, consumer should flush and exit."""
        stop_event = asyncio.Event()
        flush_called = False

        async def fetch_func(tasking):
            return {"instrument": "test", "data": [{"seq": tasking["seq"]}]}

        async def sync_db(buffers):
            nonlocal flush_called
            flush_called = True

        tasks = [{"seq": i} for i in range(3)]

        await engine.run(
            initial_tasks=tasks,
            fetch_func=fetch_func,
            sync_db_func=sync_db,
            stop_event=stop_event,
            pbar_desc="Test",
        )

        assert flush_called, "Consumer should flush at least once"

    async def test_empty_tasks_returns_immediately(self, engine):
        """engine.run with no initial tasks should return immediately."""
        stop_event = asyncio.Event()

        async def fetch_func(tasking):
            return tasking

        async def sync_db(buffers):
            pass

        await engine.run(
            initial_tasks=[],
            fetch_func=fetch_func,
            sync_db_func=sync_db,
            stop_event=stop_event,
        )
        assert True, "Should complete without error"

    async def test_error_in_fetch_retries_task(self, engine):
        """If fetch_func raises an exception, the task should be re-queued."""
        stop_event = asyncio.Event()
        attempt_count = 0

        async def fetch_func(tasking):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count == 1:
                raise ValueError("Simulated error")
            return {"instrument": "test", "data": [{"seq": 1}]}

        async def sync_db(buffers):
            pass

        tasks = [{"seq": 1}]

        await engine.run(
            initial_tasks=tasks,
            fetch_func=fetch_func,
            sync_db_func=sync_db,
            stop_event=stop_event,
            pbar_desc="Test",
        )

        assert attempt_count >= 2, "Task should be retried after error"

    async def test_dead_letter_on_persistent_failure(self, engine):
        """A task that always fails should end up in dead_letters, not loop forever."""
        stop_event = asyncio.Event()

        async def always_fail(tasking):
            raise ValueError("Permanent failure")

        async def noop_sync(buffers):
            pass

        tasks = [{"id": 1}]

        with pytest.raises(FetchTasksFailedError) as error:
            await engine.run(
                initial_tasks=tasks,
                fetch_func=always_fail,
                sync_db_func=noop_sync,
                stop_event=stop_event,
            )

        assert len(engine.dead_letters) == 1, "Failed task should be in dead_letters"
        assert engine.dead_letters[0]["id"] == 1
        assert error.value.dead_letters == engine.dead_letters

    async def test_dead_letter_does_not_block_other_tasks(self, engine):
        """A permanently failing task should not prevent a healthy task from succeeding."""
        stop_event = asyncio.Event()
        success_flag = False

        async def mixed_fetch(tasking):
            nonlocal success_flag
            if tasking.get("id") == "fail":
                raise ValueError("Permanent failure")
            success_flag = True
            return {"instrument": "test", "data": []}

        async def noop_sync(buffers):
            pass

        tasks = [{"id": "fail"}, {"id": "ok"}]

        with pytest.raises(FetchTasksFailedError, match="1 fetch task"):
            await engine.run(
                initial_tasks=tasks,
                fetch_func=mixed_fetch,
                sync_db_func=noop_sync,
                stop_event=stop_event,
            )

        assert success_flag is True, "Healthy task should complete"
        assert len(engine.dead_letters) == 1, "Exactly one dead-letter"
        assert engine.dead_letters[0]["id"] == "fail"

    async def test_multiple_instruments_buffered_correctly(self, engine):
        """Consumer should buffer per instrument correctly."""
        stop_event = asyncio.Event()
        received_buffers = []

        async def fetch_func(tasking):
            return {
                "instrument": tasking["instrument"],
                "data": [{"seq": tasking["seq"]}],
            }

        async def sync_db(buffers):
            received_buffers.append(dict(buffers))

        tasks = [
            {"instrument": "BTC-A", "seq": 1},
            {"instrument": "BTC-B", "seq": 2},
            {"instrument": "BTC-A", "seq": 3},
        ]

        await engine.run(
            initial_tasks=tasks,
            fetch_func=fetch_func,
            sync_db_func=sync_db,
            stop_event=stop_event,
            pbar_desc="Test",
        )

        # Since write_batch_size=5 and we have 3 items, they should all flush at shutdown
        assert len(received_buffers) >= 1, "Should have received at least one flush"
        # Check that instruments are grouped correctly in the final flush
        final_buffers = received_buffers[-1]
        assert "BTC-A" in final_buffers
        assert "BTC-B" in final_buffers

    async def test_dynamic_tasks_do_not_deadlock_when_task_queue_is_full(self):
        """Follow-up tasks must not block every producer on the bounded task queue."""
        engine = FetcherEngine(
            worker_count=2,
            write_batch_size=1,
            task_queue_size=1,
            storage_queue_size=10,
        )
        assert engine.task_queue.maxsize == 3
        stop_event = asyncio.Event()
        release_fetches = asyncio.Event()
        both_workers_started = asyncio.Event()
        started_count = 0
        fetched = []

        async def fetch_func(tasking):
            nonlocal started_count
            if tasking["depth"] == 0 and tasking["id"] < 2:
                started_count += 1
                if started_count == 2:
                    both_workers_started.set()
                await release_fetches.wait()
            fetched.append((tasking["id"], tasking["depth"]))
            return {"instrument": "test", "data": [], "finished": False}

        async def on_success(tasking, result_item):
            if tasking["depth"] == 0:
                await engine.enqueue_task({"id": tasking["id"], "depth": 1})

        async def noop_sync(buffers):
            pass

        runner = asyncio.create_task(
            engine.run(
                initial_tasks=[{"id": i, "depth": 0} for i in range(3)],
                fetch_func=fetch_func,
                sync_db_func=noop_sync,
                stop_event=stop_event,
                on_success=on_success,
            )
        )

        await asyncio.wait_for(both_workers_started.wait(), timeout=1.0)
        while engine.task_queue.qsize() == 0:
            await asyncio.sleep(0)
        release_fetches.set()

        await asyncio.wait_for(runner, timeout=1.0)
        assert sorted(fetched) == [
            (0, 0),
            (0, 1),
            (1, 0),
            (1, 1),
            (2, 0),
            (2, 1),
        ]

    async def test_consumer_failure_is_propagated_without_queue_hang(self):
        """A failed storage consumer must abort producers instead of stalling queues."""
        engine = FetcherEngine(
            worker_count=1,
            write_batch_size=1,
            task_queue_size=1,
            storage_queue_size=1,
        )
        stop_event = asyncio.Event()

        async def fetch_func(tasking):
            return {"instrument": "test", "data": [tasking]}

        async def failing_sync(buffers):
            raise OSError("simulated disk failure")

        with pytest.raises(OSError, match="simulated disk failure"):
            await asyncio.wait_for(
                engine.run(
                    initial_tasks=[{"id": i} for i in range(20)],
                    fetch_func=fetch_func,
                    sync_db_func=failing_sync,
                    stop_event=stop_event,
                ),
                timeout=1.0,
            )
