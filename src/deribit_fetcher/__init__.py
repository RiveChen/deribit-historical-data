"""Deribit Historical Data Fetcher."""

import asyncio
import signal
from collections.abc import Awaitable, Callable

from deribit_fetcher.config import logger

__version__ = "0.1.0"


async def run_main(
    run_func: Callable[[asyncio.Event], Awaitable[None]],
) -> None:
    """Common entry point: sets up signal handlers for graceful shutdown.

    Both future.py and option.py share this same main() pattern.

    ``run_func`` is an async callable that takes a stop_event and runs
    the fetch routine (e.g. ``future.run``, ``option.run``).
    """
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def signal_handler() -> None:
        logger.warning("Received signal, starting graceful shutdown...")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await run_func(shutdown_event)
    finally:
        logger.info("Exited.")
