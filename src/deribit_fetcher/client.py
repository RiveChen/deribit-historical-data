"""Async HTTP client for the Deribit History API v2."""

import asyncio

import httpx
from aiolimiter import AsyncLimiter
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from deribit_fetcher.config import logger, settings


class DeribitRateLimitWait:
    """Custom wait strategy preferring Retry-After header, falling back to exponential backoff."""

    def __init__(self, fallback_wait):
        """Initialize with a fallback wait function."""
        self.fallback_wait = fallback_wait

    def __call__(self, retry_state: RetryCallState) -> float:
        """Compute wait time, preferring server's Retry-After header."""
        if retry_state.outcome is None:
            return self.fallback_wait(retry_state)

        exc = retry_state.outcome.exception()
        if isinstance(exc, httpx.HTTPStatusError):
            # Deribit may return Retry-After (seconds) on 429 responses
            retry_after = exc.response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                wait_time = float(retry_after) + 0.5  # Small buffer for safety
                logger.warning(f"Rate limit hit. Server requested wait: {wait_time}s")
                return wait_time

        # Fall back to random exponential backoff if no Retry-After header
        return self.fallback_wait(retry_state)


RETRY_EXCEPTIONS = (
    httpx.TimeoutException,
    httpx.ConnectError,
    httpx.HTTPStatusError,
)


class DeribitClient:
    """Async HTTP client for the Deribit History API v2."""

    def __init__(self):
        """Initialize the client with rate limiter and HTTP session."""
        # Strict RPS limiter: max settings.MAX_RPS requests per second
        self.limiter = AsyncLimiter(settings.MAX_RPS, 1)
        self.client = self._create_client()
        logger.info(f"Deribit client initialized with {settings.MAX_RPS} RPS limit.")

    def _create_client(self) -> httpx.AsyncClient:
        proxy = settings.PROXY if settings.PROXY else None
        return httpx.AsyncClient(
            base_url=settings.BASE_URL,
            proxy=proxy,
            # For 20 RPS, slightly oversize the connection pool to avoid contention
            limits=httpx.Limits(
                max_connections=settings.MAX_WORKERS,
                max_keepalive_connections=20,
                keepalive_expiry=30.0,
            ),
            timeout=httpx.Timeout(60.0, connect=10.0),
        )

    # Retry decorator: hybrid strategy — prefer server's Retry-After, else exponential backoff
    @retry(
        retry=retry_if_exception_type(RETRY_EXCEPTIONS),
        wait=DeribitRateLimitWait(
            fallback_wait=wait_random_exponential(multiplier=1, min=1, max=60)
        ),
        stop=stop_after_attempt(10),
        reraise=True,
        before_sleep=lambda retry_state: logger.warning(
            f"Retrying (Attempt {retry_state.attempt_number}): "
            f"Next wait {getattr(retry_state.next_action, 'sleep', 'unknown')}s"
        ),
    )
    async def _fetch(self, endpoint: str, params: dict):
        # Rate-limit gate: acquire token before issuing request
        async with self.limiter:
            response = await self.client.get(endpoint, params=params)

            # Log rate-limit info on 429 (Deribit specific header)
            if response.status_code == 429:
                limit_reset = response.headers.get("x-ratelimit-reset")
                logger.error(f"429 Too Many Requests. Reset at: {limit_reset}")

            response.raise_for_status()
            return response.json()

    async def get_instruments(self, currency: str, kind: str) -> list:
        """Fetch all instruments (both expired and active) for a given currency and kind."""
        import json

        instruments = []
        tasks = []
        for expired in ["true", "false"]:
            params = {"currency": currency, "kind": kind, "expired": expired}
            tasks.append(self._fetch("/get_instruments", params))

        results = await asyncio.gather(*tasks)
        for data in results:
            instruments.extend(data["result"])

        logger.info(f"Fetched {len(instruments)} {currency} {kind} instruments.")

        save_dir = settings.base_dir / kind
        save_dir.mkdir(parents=True, exist_ok=True)
        save_path = save_dir / "instruments.json"

        try:
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(instruments, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved instrument list to {save_path}")
        except Exception as e:
            logger.error(f"Failed to save {kind} instruments: {e}")

        return instruments

    async def get_last_trade_seq(self, instrument: str) -> int | None:
        """Get the latest trade_seq for an instrument.

        Returns:
            0    — request succeeded and the instrument genuinely has no trades.
            > 0  — the latest trade_seq.
            None — could not determine (request still failing after all retries).
                   Callers MUST treat this as "unknown, retry next run" and must
                   NOT mark the instrument complete, otherwise a transient failure
                   would silently drop the instrument's entire history.

        """
        try:
            params = {"instrument_name": instrument, "count": 1}
            data = await self._fetch("/get_last_trades_by_instrument", params)
            trades = data.get("result", {}).get("trades", [])
            return trades[0]["trade_seq"] if trades else 0
        except Exception as e:
            logger.error(f"Failed to get last trade seq for {instrument}: {e}")
            return None

    async def get_trades_chunk(
        self, instrument: str, start_seq: int, end_seq: int
    ) -> tuple[list, bool]:
        """Fetch a chunk of trades within [start_seq, end_seq]. Returns (trades, has_more)."""
        params = {
            "instrument_name": instrument,
            "start_seq": start_seq,
            "end_seq": end_seq,
            "count": settings.CHUNK_SIZE,
        }
        data = await self._fetch("/get_last_trades_by_instrument", params)
        return (data["result"]["trades"], data["result"]["has_more"])

    async def __aenter__(self):
        """Enter async context manager."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit async context manager and close the HTTP client."""
        await self.client.aclose()
        logger.info("Deribit client closed.")

    async def close(self):
        """Close the HTTP client explicitly."""
        await self.client.aclose()
