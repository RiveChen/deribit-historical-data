"""Option data fetcher — fetches historical trade data for options instruments."""

import asyncio

from tqdm.asyncio import tqdm

from deribit_fetcher import run_main
from deribit_fetcher.client import DeribitClient
from deribit_fetcher.config import logger, settings
from deribit_fetcher.engine import FetcherEngine
from deribit_fetcher.log import setup_logging
from deribit_fetcher.progress import DatabaseClient, OptionProgressRepo
from deribit_fetcher.storage import JSONLinesSink

MAX_WORKER_TASKS = 10
WRITE_BATCH_SIZE = 1
STORAGE_QUEUE_SIZE = 80
TASK_QUEUE_SIZE = 200


async def _prepare_initial_tasks(
    repo: OptionProgressRepo, deribit_client: DeribitClient, refresh_list: bool = True
) -> list[dict]:
    """Prepare initial fetch tasks for options.

    Unlike futures (pre-allocated chunks), options use a streaming approach:
    start from last_no + 1 and dynamically enqueue subsequent chunks via
    the on_success callback.
    """
    if refresh_list:
        logger.info("Fetching option instrument list...")
        options = await deribit_client.get_instruments(currency=settings.CURRENCY, kind="option")
        await repo.upsert_option_list(options)

    incomplete_options = await repo.get_incomplete_option_list()

    if not incomplete_options:
        return []

    logger.info(f"Found {len(incomplete_options)} incomplete options.")

    tasks = []
    for opt in incomplete_options:
        start_seq = opt["last_no"] + 1

        tasks.append(
            {
                "instrument": opt["instrument"],
                "start_seq": start_seq,
                "is_expired": opt["is_expired"],
            }
        )

    return tasks


async def run(stop_event: asyncio.Event):
    """Fetch all option trades using streaming chunks via the on_success callback."""
    setup_logging()

    async with DatabaseClient(settings.OPTION_DB_PATH) as db_conn:
        repo = OptionProgressRepo(db_conn)
        sink = JSONLinesSink(settings.DATA_OPTION_DIR)

        async with DeribitClient() as client:
            initial_tasks = await _prepare_initial_tasks(repo, client)

            if not initial_tasks:
                logger.info("No options to fetch.")
                return

            engine = FetcherEngine(
                worker_count=MAX_WORKER_TASKS,
                write_batch_size=WRITE_BATCH_SIZE,
                task_queue_size=TASK_QUEUE_SIZE,
                storage_queue_size=STORAGE_QUEUE_SIZE,
            )

            async def fetch_chunk(tasking: dict) -> dict:
                """Fetch a chunk of option trades. Determines whether to continue streaming."""
                instrument = tasking["instrument"]
                start_seq = tasking["start_seq"]
                end_seq = start_seq + settings.CHUNK_SIZE - 1

                trades, has_more = await client.get_trades_chunk(instrument, start_seq, end_seq)

                storage_item = {
                    "instrument": instrument,
                    "data": trades if trades else None,
                    "is_expired": tasking["is_expired"],
                    "finished": False,
                    "start_seq": start_seq,
                }

                should_continue = False
                last_seq_in_chunk = start_seq

                if trades:
                    last_seq_in_chunk = trades[0]["trade_seq"]
                    # Continue if there's more data in this range or chunk was full
                    if has_more or len(trades) >= settings.CHUNK_SIZE:
                        should_continue = True

                # Only mark finished if the instrument is expired (no more trades will appear)
                if not should_continue and tasking["is_expired"]:
                    storage_item["finished"] = True

                storage_item["should_continue"] = should_continue
                storage_item["next_seq"] = last_seq_in_chunk + 1
                return storage_item

            async def on_success(tasking: dict, result_item: dict):
                """Enqueue the next chunk if there's more data to fetch for this option."""
                if result_item["should_continue"]:
                    next_task = {
                        "instrument": tasking["instrument"],
                        "start_seq": result_item["next_seq"],
                        "is_expired": tasking["is_expired"],
                    }
                    if not stop_event.is_set():
                        await engine.enqueue_task(next_task)

            def custom_pbar_updater(item: dict, pbar: tqdm):
                """Update progress bar: show completed count and expand total on new chunks."""
                if item.get("finished"):
                    pbar.set_postfix({"Done": engine.completed_count})

                if item.get("should_continue"):
                    with pbar.get_lock():
                        if pbar.total is not None:
                            pbar.total += 1
                            pbar.refresh()

            async def sync_db(buffers: dict[str, list[dict]]):
                """Write data to disk and update DB progress.

                Write order: disk first, then DB. This is intentional:
                if a crash happens between flush and DB update, the restart
                will re-fetch from a slightly older last_no, producing
                duplicate trades in JSONL — which is tolerable (dedup at
                Parquet stage). The MAX(last_no, ?) guard prevents rollback.
                """
                # Compute DB updates first (pure computation, before any I/O)
                db_updates = []
                completed_instruments = []

                for instrument, items in buffers.items():
                    max_seq_in_batch = 0
                    is_finished = False

                    for item in items:
                        if item["data"]:
                            last_seq = item["data"][0]["trade_seq"]
                            if last_seq > max_seq_in_batch:
                                max_seq_in_batch = last_seq
                        if item.get("finished"):
                            is_finished = True

                    if max_seq_in_batch > 0:
                        db_updates.append((max_seq_in_batch, instrument))

                    if is_finished:
                        completed_instruments.append(instrument)

                # Write data to disk first (data durability takes priority)
                await sink.flush(buffers)

                # Then update DB progress
                if db_updates:
                    await repo.update_option_last_no(db_updates)

                if completed_instruments:
                    await repo.mark_options_complete(completed_instruments)

            await engine.run(
                initial_tasks=initial_tasks,
                fetch_func=fetch_chunk,
                sync_db_func=sync_db,
                stop_event=stop_event,
                on_success=on_success,
                custom_pbar_updater=custom_pbar_updater,
                pbar_desc="Streaming Options",
                pbar_unit="chunk",
            )


async def main():
    """Entry point: sets up signal handlers for graceful shutdown and runs the fetcher."""
    await run_main(run)


if __name__ == "__main__":
    asyncio.run(main())
