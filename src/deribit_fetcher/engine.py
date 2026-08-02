"""Generic async producer-consumer engine for data fetching workloads."""

import asyncio
from asyncio import Queue
from collections import defaultdict
from collections.abc import Awaitable, Callable

from tqdm.asyncio import tqdm

from deribit_fetcher.config import logger

# Maximum number of times a single task will be retried before being moved to dead-letters.
MAX_TASK_RETRIES = 3


class FetchTasksFailedError(RuntimeError):
    """Raised after all safely persistable results drain when tasks exhaust retries."""

    def __init__(self, dead_letters: list[dict]):
        """Store failed task snapshots and build a concise operational message."""
        self.dead_letters = [dict(task) for task in dead_letters]
        identifiers = [
            str(task.get("instrument", task.get("id", "?"))) for task in self.dead_letters[:5]
        ]
        suffix = " ..." if len(self.dead_letters) > 5 else ""
        super().__init__(
            f"{len(self.dead_letters)} fetch task(s) exhausted retries: "
            f"{', '.join(identifiers)}{suffix}"
        )


class FetcherEngine:
    """Generic async producer-consumer engine for data fetching workloads.

    Manages a task queue (producers fetch data) and a storage queue (consumer
    persists results). Supports graceful shutdown via an external stop_event.
    """

    def __init__(
        self,
        worker_count: int,
        write_batch_size: int,
        task_queue_size: int = 200,
        storage_queue_size: int = 80,
    ):
        """Initialize the engine with concurrency and queue settings."""
        self.worker_count = worker_count
        self.write_batch_size = write_batch_size
        # Initial work may occupy at most ``task_queue_size`` slots. One extra
        # slot per active producer is reserved for its single follow-up/retry,
        # which breaks circular waits while keeping the queue strictly bounded.
        self.task_queue = Queue(maxsize=task_queue_size + worker_count)
        self._initial_task_slots = asyncio.Semaphore(task_queue_size)
        self.storage_queue = Queue(maxsize=storage_queue_size)
        self._pending_task_count = 0
        self._all_tasks_done = asyncio.Event()
        self._all_tasks_done.set()
        # Tracks number of completed instruments (used by option streaming)
        self.completed_count: int = 0
        # Tasks that exceeded the maximum retry count — collected rather than dropped silently.
        self.dead_letters: list[dict] = []

    def _add_pending_task(self) -> None:
        """Register one new logical task before making it visible to workers."""
        self._pending_task_count += 1
        self._all_tasks_done.clear()

    def _finish_pending_task(self) -> None:
        """Mark one logical task complete, including terminal failures."""
        if self._pending_task_count <= 0:
            raise RuntimeError("Pending task count underflow")
        self._pending_task_count -= 1
        if self._pending_task_count == 0:
            self._all_tasks_done.set()

    async def enqueue_task(self, task: dict) -> None:
        """Public method to enqueue a new task during streaming.

        Used by ``on_success`` callbacks (e.g. option's streaming fetch)
        to dynamically add one follow-up chunk. Reserved producer slots make
        this non-blocking without turning the task queue into an unbounded queue.
        """
        self._add_pending_task()
        try:
            self.task_queue.put_nowait((task, False))
        except asyncio.QueueFull as error:
            self._finish_pending_task()
            raise RuntimeError("A callback scheduled more than one follow-up task") from error

    def _retry_task(self, task: dict) -> None:
        """Schedule an existing logical task for retry without double-counting it."""
        try:
            self.task_queue.put_nowait((task, False))
        except asyncio.QueueFull as error:
            raise RuntimeError("Reserved retry slot invariant violated") from error

    async def _producer_worker(
        self,
        fetch_func: Callable[[dict], Awaitable[dict]],
        on_success: Callable[[dict, dict], Awaitable[None]] | None,
        pbar: tqdm,
        stop_event: asyncio.Event,
    ):
        """Pull tasks from the queue, fetch data, and enqueue results for storage."""
        while not stop_event.is_set():
            try:
                queued_task = await asyncio.wait_for(self.task_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            # None is the poison pill — signals shutdown
            if queued_task is None:
                self.task_queue.task_done()
                break

            tasking, uses_initial_slot = queued_task
            if uses_initial_slot:
                self._initial_task_slots.release()

            logical_task_finished = False
            try:
                result_item = await fetch_func(tasking)

                # Execute success callback (e.g. enqueue next chunk for streaming fetches)
                if on_success:
                    await on_success(tasking, result_item)

                await self.storage_queue.put(result_item)

                # Default: advance progress bar by one chunk
                pbar.update(1)
                logical_task_finished = True

            except Exception as e:
                attempts = tasking.get("_attempts", 0) + 1
                tasking["_attempts"] = attempts
                if attempts >= MAX_TASK_RETRIES:
                    logger.error(
                        f"Task failed {attempts} times, moving to dead-letter: "
                        f"{tasking.get('instrument', tasking.get('id', '?'))}: {e}"
                    )
                    self.dead_letters.append(dict(tasking))
                    # Task is NOT re-queued — permanently discarded
                    logical_task_finished = True
                else:
                    logger.warning(
                        f"Error executing task (attempt {attempts}/{MAX_TASK_RETRIES}): "
                        f"{tasking}: {e}"
                    )
                    if not stop_event.is_set():
                        try:
                            self._retry_task(tasking)
                        except RuntimeError as retry_error:
                            logger.error(f"Could not schedule task retry: {retry_error}")
                            self.dead_letters.append(dict(tasking))
                            logical_task_finished = True
                    else:
                        logical_task_finished = True

            finally:
                if logical_task_finished:
                    self._finish_pending_task()
                self.task_queue.task_done()

    async def _consumer_worker(
        self,
        sync_db_func: Callable[[dict[str, list[dict]]], Awaitable[None]],
        pbar: tqdm,
        custom_pbar_updater: Callable[[dict, tqdm], None] | None = None,
    ):
        """Receive result items from producers, batch them in memory, and flush to storage.

        When the batch fills up, data is written to storage/DB.
        """
        buffers = defaultdict(list)
        total_buffered = 0

        logger.info("Storage consumer started.")

        while True:
            try:
                item = await asyncio.wait_for(self.storage_queue.get(), timeout=1.0)

                # None is the poison pill — flush remaining buffered data and exit
                if item is None:
                    if total_buffered > 0:
                        await sync_db_func(buffers)
                        buffers.clear()
                        total_buffered = 0
                    self.storage_queue.task_done()
                    break

                if item.get("finished"):
                    self.completed_count += 1

                if custom_pbar_updater:
                    custom_pbar_updater(item, pbar)

                buffers[item["instrument"]].append(item)
                total_buffered += 1

                if total_buffered >= self.write_batch_size:
                    await sync_db_func(buffers)
                    buffers.clear()
                    total_buffered = 0

                self.storage_queue.task_done()

            except asyncio.TimeoutError:
                # Flush partial buffer on idle timeout (keeps data moving)
                if total_buffered > 0:
                    await sync_db_func(buffers)
                    buffers.clear()
                    total_buffered = 0

    async def run(
        self,
        initial_tasks: list[dict],
        fetch_func: Callable[[dict], Awaitable[dict]],
        sync_db_func: Callable[[dict[str, list[dict]]], Awaitable[None]],
        stop_event: asyncio.Event,
        on_success: Callable[[dict, dict], Awaitable[None]] | None = None,
        custom_pbar_updater: Callable[[dict, tqdm], None] | None = None,
        pbar_desc: str = "Fetching",
        pbar_unit: str = "chunk",
    ):
        """Run the engine: distribute tasks, start workers, and block until completion.

        Distributes initial tasks, starts producer and consumer workers, and
        blocks until all tasks complete or stop_event is set.
        """
        if not initial_tasks:
            logger.info("No tasks to run.")
            return

        pbar = tqdm(
            total=len(initial_tasks),
            desc=pbar_desc,
            unit=pbar_unit,
            dynamic_ncols=True,
        )

        consumer_task = asyncio.create_task(
            self._consumer_worker(sync_db_func, pbar, custom_pbar_updater)
        )

        workers = [
            asyncio.create_task(self._producer_worker(fetch_func, on_success, pbar, stop_event))
            for _ in range(self.worker_count)
        ]

        completion_task = None
        stop_signal_task = asyncio.create_task(stop_event.wait())

        def raise_if_background_stopped(task: asyncio.Task, name: str) -> None:
            """Propagate a background failure instead of letting queue writes hang."""
            if not task.done():
                return
            if task.cancelled():
                raise RuntimeError(f"{name} stopped unexpectedly")
            error = task.exception()
            if error is not None:
                raise error
            raise RuntimeError(f"{name} stopped unexpectedly")

        try:
            # Feed initial tasks into the task queue
            for task in initial_tasks:
                if stop_event.is_set():
                    logger.info("Stop event set during task distribution.")
                    break

                self._add_pending_task()
                enqueued = False
                slot_task = asyncio.create_task(self._initial_task_slots.acquire())
                acquired_slot = False
                try:
                    done, _ = await asyncio.wait(
                        [slot_task, stop_signal_task, consumer_task],
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if slot_task in done:
                        await slot_task
                        acquired_slot = True

                    if not stop_event.is_set():
                        raise_if_background_stopped(consumer_task, "Storage consumer")
                        if acquired_slot:
                            self.task_queue.put_nowait((task, True))
                            enqueued = True
                finally:
                    if not slot_task.done():
                        slot_task.cancel()
                    await asyncio.gather(slot_task, return_exceptions=True)
                    if acquired_slot and not enqueued:
                        self._initial_task_slots.release()
                    if not enqueued:
                        self._finish_pending_task()

                if stop_event.is_set():
                    logger.info("Stop event set during task distribution.")
                    break

            # Wait for logical completion, shutdown, or a supervised worker failure.
            completion_task = asyncio.create_task(self._all_tasks_done.wait())

            done, _ = await asyncio.wait(
                [completion_task, stop_signal_task, consumer_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if not stop_event.is_set():
                if consumer_task in done:
                    raise_if_background_stopped(consumer_task, "Storage consumer")

        finally:
            for waiter in (completion_task, stop_signal_task):
                if waiter is not None:
                    waiter.cancel()
            await asyncio.gather(
                *(waiter for waiter in (completion_task, stop_signal_task) if waiter is not None),
                return_exceptions=True,
            )

            # Cancel all producer workers
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

            try:
                # Send poison pill to consumer and wait for it to drain.
                try:
                    if not consumer_task.done():
                        await asyncio.wait_for(self.storage_queue.put(None), timeout=2.0)
                    await asyncio.wait_for(consumer_task, timeout=10.0)
                except asyncio.TimeoutError:
                    logger.error("Consumer forced shutdown.")
                    consumer_task.cancel()
                    await asyncio.gather(consumer_task, return_exceptions=True)
            finally:
                pbar.close()
            logger.info("Engine run completed.")

        if self.dead_letters:
            raise FetchTasksFailedError(self.dead_letters)
