import asyncio
from collections import defaultdict
from asyncio import Queue
from collections.abc import Callable, Awaitable
from tqdm.asyncio import tqdm
import typing
from deribit_fetcher.config import logger


class FetcherEngine:
    def __init__(
        self,
        worker_count: int,
        write_batch_size: int,
        task_queue_size: int = 200,
        storage_queue_size: int = 80,
    ):
        self.worker_count = worker_count
        self.write_batch_size = write_batch_size
        self.task_queue = Queue(maxsize=task_queue_size)
        self.storage_queue = Queue(maxsize=storage_queue_size)

    async def _producer_worker(
        self,
        fetch_func: Callable[[dict], Awaitable[dict]],
        on_success: Callable[[dict, dict], Awaitable[None]] | None,
        pbar: tqdm,
        stop_event: asyncio.Event,
    ):
        while not stop_event.is_set():
            try:
                tasking = await asyncio.wait_for(self.task_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if tasking is None:
                self.task_queue.task_done()
                break

            try:
                # fetch_func returns the data item to be stored
                result_item = await fetch_func(tasking)

                # Execute success callback if defined. Useful for enqueueing next sequences or updating pbar logic
                if on_success:
                    await on_success(tasking, result_item)

                await self.storage_queue.put(result_item)
                
                # Default behavior: update pbar by 1 for each completed chunk task
                pbar.update(1)

            except Exception as e:
                logger.warning(f"Error executing task {tasking}: {e}. Retrying...")
                if not stop_event.is_set():
                    await self.task_queue.put(tasking)

            finally:
                self.task_queue.task_done()

    async def _consumer_worker(
        self,
        sync_db_func: Callable[[dict[str, list[dict]]], Awaitable[None]],
        pbar: tqdm,
        custom_pbar_updater: Callable[[dict, tqdm], None] | None = None,
    ):
        buffers = defaultdict(list)
        total_buffered = 0

        logger.info("Storage consumer started.")

        while True:
            try:
                item = await asyncio.wait_for(self.storage_queue.get(), timeout=1.0)

                if item is None:
                    if total_buffered > 0:
                        await sync_db_func(buffers)
                        buffers.clear()
                        total_buffered = 0
                    self.storage_queue.task_done()
                    break

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
            asyncio.create_task(
                self._producer_worker(fetch_func, on_success, pbar, stop_event)
            )
            for _ in range(self.worker_count)
        ]

        try:
            for task in initial_tasks:
                if stop_event.is_set():
                    logger.info("Stop event set during task distribution.")
                    break
                while not stop_event.is_set():
                    try:
                        await asyncio.wait_for(self.task_queue.put(task), timeout=0.5)
                        break
                    except asyncio.TimeoutError:
                        continue

            queue_finished = asyncio.create_task(self.task_queue.join())
            stop_signal_task = asyncio.create_task(stop_event.wait())

            await asyncio.wait(
                [queue_finished, stop_signal_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if stop_event.is_set() and not queue_finished.done():
                queue_finished.cancel()

        finally:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

            try:
                await asyncio.wait_for(self.storage_queue.put(None), timeout=2.0)
                await asyncio.wait_for(consumer_task, timeout=10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                logger.error("Consumer forced shutdown.")

            pbar.close()
            logger.info("Engine run completed.")
