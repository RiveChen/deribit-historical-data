"""Shared fetcher skeleton used by both future.py and option.py."""

import asyncio
import functools
from collections.abc import Awaitable, Callable

from deribit_fetcher.client import DeribitClient
from deribit_fetcher.engine import FetcherEngine
from deribit_fetcher.log import setup_logging
from deribit_fetcher.progress import DatabaseClient
from deribit_fetcher.storage import JSONLinesSink


async def run_fetcher(
    *,
    db_path,
    data_dir,
    repo_cls,
    prepare_fn: Callable[..., Awaitable[list[dict] | None]],
    fetch_fn: Callable,
    sync_fn: Callable,
    stop_event: asyncio.Event,
    on_success: Callable | None = None,
    custom_pbar_updater: Callable | None = None,
    pbar_desc: str = "Fetching",
    pbar_unit: str = "chunk",
    engine_kwargs: dict | None = None,
    finalize_fn: Callable | None = None,
):
    """Shared run skeleton for both future and option fetchers.

    Opens DB, repo, sink, and client; prepares tasks; creates an engine;
    runs with the given callbacks; and optionally finalizes.
    """
    setup_logging()

    async with DatabaseClient(db_path) as db_conn:
        repo = repo_cls(db_conn)
        sink = JSONLinesSink(data_dir)

        async with DeribitClient() as client:
            tasks = await prepare_fn(repo, client)
            if stop_event.is_set() or not tasks:
                return

            engine = FetcherEngine(**(engine_kwargs or {}))

            await engine.run(
                initial_tasks=tasks,
                fetch_func=functools.partial(fetch_fn, client=client),
                sync_db_func=functools.partial(sync_fn, sink=sink, repo=repo),
                stop_event=stop_event,
                on_success=functools.partial(on_success, engine=engine, stop_event=stop_event)
                if on_success
                else None,
                custom_pbar_updater=(
                    functools.partial(custom_pbar_updater, engine=engine)
                    if custom_pbar_updater
                    else None
                ),
                pbar_desc=pbar_desc,
                pbar_unit=pbar_unit,
            )

            if finalize_fn:
                await finalize_fn(repo)
