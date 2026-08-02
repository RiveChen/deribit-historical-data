"""Future data fetcher — fetches historical trade data for futures instruments."""

import argparse
import asyncio
from typing import Protocol

from tqdm.asyncio import tqdm

from deribit_fetcher import run_main
from deribit_fetcher.config import logger, set_base_dir, settings
from deribit_fetcher.fetcher import run_fetcher
from deribit_fetcher.progress import FutureProgressRepo


class _ClientProtocol(Protocol):
    """Protocol defining the minimal client interface needed by _prepare_tasks."""

    async def get_instruments(self, currency: str, kind: str) -> list: ...
    async def get_last_trade_seq(self, instrument: str) -> int | None: ...


class _FetchClientProtocol(Protocol):
    """Protocol for the client methods used by fetch_future_chunk."""

    async def get_trades_chunk(
        self, instrument: str, start_seq: int, end_seq: int
    ) -> tuple[list, bool]: ...  # noqa: E501


class _SyncSinkProtocol(Protocol):
    """Protocol for the sink interface used by sync_future_db."""

    async def flush(self, buffers: dict[str, list[dict]]) -> None: ...


class _SyncRepoProtocol(Protocol):
    """Protocol for the repo interface used by sync_future_db."""

    async def update_chunks(self, chunks: list[tuple[int, bool, str, int]]) -> None: ...


# Engine constants — shared with option via config if needed later
MAX_WORKER_TASKS = 15
WRITE_BATCH_SIZE = 1
STORAGE_QUEUE_SIZE = 50
TASK_QUEUE_SIZE = 200
MAX_RANGE_RECOVERY_REQUESTS = 32


class IncompleteTradeRangeError(RuntimeError):
    """A future sequence range could not be recovered completely."""


# ---------------------------------------------------------------------------
# Module-level callbacks (extracted from closures for testability)
# ---------------------------------------------------------------------------


async def _fetch_complete_range(
    client: _FetchClientProtocol,
    instrument: str,
    start_seq: int,
    end_seq: int,
    request_count: list[int],
) -> list[dict]:
    """Fetch an exact sequence range, splitting responses that omit in-range rows."""
    request_count[0] += 1
    if request_count[0] > MAX_RANGE_RECOVERY_REQUESTS:
        raise IncompleteTradeRangeError(
            f"Recovery request budget exceeded for {instrument} [{start_seq}, {end_seq}]"
        )

    trades, has_more = await client.get_trades_chunk(instrument, start_seq, end_seq)
    in_range = [row for row in trades if start_seq <= int(row["trade_seq"]) <= end_seq]
    seen = {int(row["trade_seq"]) for row in in_range}
    expected_count = end_seq - start_seq + 1

    if len(seen) == expected_count:
        if has_more:
            logger.warning(
                f"{instrument} [{start_seq}, {end_seq}] reported has_more=true "
                "but every requested sequence was present; accepting exact coverage."
            )
        return in_range

    if not in_range or start_seq == end_seq:
        raise IncompleteTradeRangeError(
            f"Incomplete response for {instrument} [{start_seq}, {end_seq}]: "
            f"covered {len(seen)}/{expected_count} unique sequences, has_more={has_more}"
        )

    midpoint = (start_seq + end_seq) // 2
    logger.warning(
        f"Incomplete response for {instrument} [{start_seq}, {end_seq}] "
        f"({len(seen)}/{expected_count}, has_more={has_more}); splitting at {midpoint}."
    )
    left = await _fetch_complete_range(
        client,
        instrument,
        start_seq,
        midpoint,
        request_count,
    )
    right = await _fetch_complete_range(
        client,
        instrument,
        midpoint + 1,
        end_seq,
        request_count,
    )
    return left + right


async def fetch_future_chunk(tasking: dict, *, client: _FetchClientProtocol) -> dict:
    """Fetch a single chunk of future trades and return it with metadata."""
    instrument = tasking["instrument"]
    start_seq = tasking["chunk_no"]
    end_seq = tasking.get("end_seq")

    if end_seq is None:
        end_seq = start_seq + settings.CHUNK_SIZE - 1
        trades, has_more = await client.get_trades_chunk(instrument, start_seq, end_seq)
    else:
        trades = await _fetch_complete_range(client, instrument, start_seq, end_seq, [0])
        has_more = False
    return {
        "instrument": instrument,
        "chunk_no": start_seq,
        "has_more": has_more,
        "data": trades if trades else None,
    }


async def sync_future_db(
    buffers: dict[str, list[dict]],
    *,
    sink: _SyncSinkProtocol,
    repo: _SyncRepoProtocol,
) -> None:
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


async def finalize_future(repo: FutureProgressRepo) -> None:
    """Mark completed chunks and instruments so they're skipped on restart."""
    await repo.finalize_chunks()
    await repo.finalize_future_meta()


# ---------------------------------------------------------------------------
# Task preparation
# ---------------------------------------------------------------------------


async def _fetch_all_sequences(incompleted_futures, deribit_client):
    """Fetch the latest trade_seq for each incomplete future in parallel."""
    tasks = [deribit_client.get_last_trade_seq(f["instrument"]) for f in incompleted_futures]
    results = await tqdm.gather(*tasks, desc="Fetching last seq")

    for future, seq in zip(incompleted_futures, results, strict=False):
        future["last_seq"] = seq

    return incompleted_futures


async def prepare_future_tasks(
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
    if refresh_chunks:
        last_seq_by_instrument = {
            f["instrument"]: int(f["last_seq"])
            for f in incompleted_futures
            if (f.get("last_seq") or 0) > 0
        }
        bounded_chunks = []
        for chunk in pending_chunks:
            last_seq = last_seq_by_instrument.get(chunk["instrument"])
            if last_seq is None or last_seq < chunk["chunk_no"]:
                logger.warning(
                    f"Skipping {chunk['instrument']} chunk {chunk['chunk_no']} because "
                    "a trustworthy end_seq is unavailable; it remains pending."
                )
                continue
            chunk["end_seq"] = min(
                chunk["chunk_no"] + settings.CHUNK_SIZE - 1,
                last_seq,
            )
            bounded_chunks.append(chunk)
        pending_chunks = bounded_chunks

    if not pending_chunks:
        logger.info("No bounded pending chunks are ready this run.")
        return []
    logger.info(f"Found {len(pending_chunks)} pending chunks.")
    return pending_chunks


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def run(stop_event: asyncio.Event):
    """Fetch all future trades using a producer-consumer engine for concurrent chunk fetching."""
    await run_fetcher(
        db_path=settings.future_db_path,
        data_dir=settings.data_future_dir,
        repo_cls=FutureProgressRepo,
        prepare_fn=prepare_future_tasks,
        fetch_fn=fetch_future_chunk,
        sync_fn=sync_future_db,
        stop_event=stop_event,
        finalize_fn=finalize_future,
        engine_kwargs={
            "worker_count": MAX_WORKER_TASKS,
            "write_batch_size": WRITE_BATCH_SIZE,
            "task_queue_size": TASK_QUEUE_SIZE,
            "storage_queue_size": STORAGE_QUEUE_SIZE,
        },
        pbar_desc="Downloading Trades",
    )


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the future fetcher."""
    parser = argparse.ArgumentParser(description="Fetch Deribit historical future trades.")
    parser.add_argument(
        "--base-dir",
        type=str,
        default=None,
        help="Override the data directory (default: ./data/<CURRENCY>).",
    )
    return parser.parse_args()


async def main():
    """Parse CLI args, set up signal handlers for graceful shutdown, and run the fetcher."""
    args = _parse_args()
    set_base_dir(args.base_dir)
    await run_main(run)


if __name__ == "__main__":
    asyncio.run(main())
