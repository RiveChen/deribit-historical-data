"""Future data fetcher — fetches historical trade data for futures instruments."""

import asyncio
from typing import Protocol

from tqdm.asyncio import tqdm

from deribit_fetcher import run_main
from deribit_fetcher.client import DeribitClient
from deribit_fetcher.config import logger, settings
from deribit_fetcher.engine import FetcherEngine
from deribit_fetcher.log import setup_logging
from deribit_fetcher.progress import DatabaseClient, FutureProgressRepo
from deribit_fetcher.storage import JSONLinesSink


class _ClientProtocol(Protocol):
    """Protocol defining the minimal client interface needed by _prepare_tasks."""

    async def get_instruments(self, currency: str, kind: str) -> list: ...

    async def get_last_trade_seq(self, instrument: str) -> int | None: ...


MAX_WORKER_TASKS = 15
WRITE_BATCH_SIZE = 1
STORAGE_QUEUE_SIZE = 50
TASK_QUEUE_SIZE = 200


async def _fetch_all_sequences(incompleted_futures, deribit_client):
    """Fetch the latest trade_seq for each incomplete future in parallel."""
    tasks = [deribit_client.get_last_trade_seq(f["instrument"]) for f in incompleted_futures]
    results = await tqdm.gather(*tasks, desc="Fetching last seq")

    for future, seq in zip(incompleted_futures, results, strict=False):
        future["last_seq"] = seq

    return incompleted_futures


async def _prepare_tasks(
    repo: FutureProgressRepo,
    deribit_client: _ClientProtocol,
    refresh_list: bool = True,
    refresh_chunks: bool = True,
):
    """Prepare the task list for fetching.

    1. Upsert the instrument list from the API (if refresh_list)
    2. Get incomplete futures, fetch their last_seq, pre-allocate chunks (if refresh_chunks)
    3. Return the list of pending chunks to be fetched
    """
    if refresh_list:
        futures = await deribit_client.get_instruments(currency=settings.CURRENCY, kind="future")
        await repo.upsert_future_list(futures)

    if refresh_chunks:
        incompleted_futures = await repo.get_incomplete_future_list()
        if not incompleted_futures:
            logger.info("All futures are completed.")
            return
        logger.info(f"Found {len(incompleted_futures)} incompleted futures.")

        incompleted_futures = await _fetch_all_sequences(incompleted_futures, deribit_client)

        # last_seq semantics (see DeribitClient.get_last_trade_seq):
        #   None -> could not determine this run; leave incomplete and retry next
        #           run. Do NOT mark complete (that would silently drop its data).
        #   0    -> genuinely no trades; mark complete now.
        #   > 0  -> has trades; pre-allocate chunks.
        undetermined = [f for f in incompleted_futures if f.get("last_seq") is None]
        if undetermined:
            logger.warning(
                f"{len(undetermined)} futures could not resolve last_seq this run "
                f"(will retry on next run): "
                f"{', '.join(f['instrument'] for f in undetermined[:5])}"
                f"{' ...' if len(undetermined) > 5 else ''}"
            )

        no_trade_futures = [f for f in incompleted_futures if f.get("last_seq") == 0]
        for f in no_trade_futures:
            await repo.mark_future_complete(f["instrument"])
        todo_futures = [f for f in incompleted_futures if (f.get("last_seq") or 0) > 0]

        # Pre-allocate chunks: partition [1, last_seq] into fixed-size ranges
        for f in todo_futures:
            chunks = []
            for i in range(1, f.get("last_seq") + 1, settings.CHUNK_SIZE):
                chunks.append((f["instrument"], i))
            await repo.upsert_chunks(chunks)

    pending_chunks = await repo.get_pending_chunks()
    if not pending_chunks:
        logger.info("All chunks are completed.")
        return []
    logger.info(f"Found {len(pending_chunks)} pending chunks.")
    return pending_chunks


async def run(stop_event: asyncio.Event):
    """Fetch all future trades using a producer-consumer engine for concurrent chunk fetching."""
    setup_logging()

    async with DatabaseClient(settings.FUTURE_DB_PATH) as db_conn:
        repo = FutureProgressRepo(db_conn)
        sink = JSONLinesSink(settings.DATA_FUTURE_DIR)

        async with DeribitClient() as client:
            chunks = await _prepare_tasks(repo, client)
            if stop_event.is_set() or not chunks:
                return

            engine = FetcherEngine(
                worker_count=MAX_WORKER_TASKS,
                write_batch_size=WRITE_BATCH_SIZE,
                task_queue_size=TASK_QUEUE_SIZE,
                storage_queue_size=STORAGE_QUEUE_SIZE,
            )

            async def fetch_chunk(tasking: dict) -> dict:
                """Fetch a single chunk of trades and return it with metadata."""
                instrument = tasking["instrument"]
                start_seq = tasking["chunk_no"]
                end_seq = start_seq + settings.CHUNK_SIZE - 1

                trades, has_more = await client.get_trades_chunk(instrument, start_seq, end_seq)
                return {
                    "instrument": instrument,
                    "chunk_no": start_seq,
                    "has_more": has_more,
                    "data": trades if trades else None,
                }

            async def sync_db(buffers: dict[str, list[dict]]):
                """Flush data to disk and update chunk progress."""
                await sink.flush(buffers)
                db_updates = []
                for items in buffers.values():
                    for item in items:
                        db_updates.append(
                            (
                                len(item["data"]) if item.get("data") else 0,
                                item["has_more"],
                                item["instrument"],
                                item["chunk_no"],
                            )
                        )
                await repo.update_chunks(db_updates)
                logger.debug(f"Flushed {len(db_updates)} chunks.")

            await engine.run(
                initial_tasks=chunks,
                fetch_func=fetch_chunk,
                sync_db_func=sync_db,
                stop_event=stop_event,
                pbar_desc="Downloading Trades",
            )

            # Finalize: mark completed chunks and instruments so they're skipped on restart
            await repo.finalize_chunks()
            await repo.finalize_future_meta()


async def main():
    """Entry point: sets up signal handlers for graceful shutdown and runs the fetcher."""
    await run_main(run)


if __name__ == "__main__":
    asyncio.run(main())
