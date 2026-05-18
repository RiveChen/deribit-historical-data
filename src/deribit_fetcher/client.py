import asyncio
import httpx
from aiolimiter import AsyncLimiter
from tenacity import (
    retry,
    wait_random_exponential,
    stop_after_attempt,
    retry_if_exception_type,
    BaseRetrying,
    RetryCallState,
)
from deribit_fetcher.config import settings, logger


# 自定义等待策略：优先读取 Deribit 的 Retry-After Header
class DeribitRateLimitWait:
    def __init__(self, fallback_wait):
        self.fallback_wait = fallback_wait

    def __call__(self, retry_state: RetryCallState) -> float:
        if retry_state.outcome is None:
            return self.fallback_wait(retry_state)

        # 检查最后一次尝试是否是 HTTPStatusError
        exc = retry_state.outcome.exception()
        if isinstance(exc, httpx.HTTPStatusError):
            # Deribit 在 429 时可能会提供 Retry-After (秒)
            retry_after = exc.response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                wait_time = float(retry_after) + 0.5  # 多加 0.5s 缓冲
                logger.warning(f"Rate limit hit. Server requested wait: {wait_time}s")
                return wait_time

        # 如果没有 Header，使用默认的指数退避策略
        return self.fallback_wait(retry_state)


RETRY_EXCEPTIONS = (
    httpx.TimeoutException,
    httpx.ConnectError,
    httpx.HTTPStatusError,
)


class DeribitClient:
    def __init__(self):
        # 1. 严格 RPS 控制器：每秒最多 settings.MAX_RPS 次
        self.limiter = AsyncLimiter(settings.MAX_RPS, 1)
        self.client = self._create_client()
        logger.info(f"Deribit client initialized with {settings.MAX_RPS} RPS limit.")

    def _create_client(self) -> httpx.AsyncClient:
        proxy = settings.PROXY if settings.PROXY else None
        return httpx.AsyncClient(
            base_url=settings.BASE_URL,
            proxy=proxy,
            # 对于 20 RPS，连接池需要稍微放大，防止竞争
            limits=httpx.Limits(
                max_connections=settings.MAX_WORKERS,
                max_keepalive_connections=20,
                keepalive_expiry=30.0,
            ),
            timeout=httpx.Timeout(60.0, connect=10.0),
        )

    # 2. 增强型重试装饰器
    @retry(
        retry=retry_if_exception_type(RETRY_EXCEPTIONS),
        # 混合策略：优先听服务器的，服务器没说就用随机指数退避
        wait=DeribitRateLimitWait(
            fallback_wait=wait_random_exponential(multiplier=1, min=1, max=60)
        ),
        stop=stop_after_attempt(10),
        reraise=True,
        before_sleep=lambda retry_state: logger.warning(
            f"Retrying {retry_state.fn.__name__} (Attempt {retry_state.attempt_number}): "
            f"Next wait {retry_state.next_action.sleep}s"
        ),
    )
    async def _fetch(self, endpoint: str, params: dict):
        # 3. 频率限制控制
        async with self.limiter:
            response = await self.client.get(endpoint, params=params)

            # 如果是限流，记录剩余权重（Deribit 专属 Header）
            if response.status_code == 429:
                limit_reset = response.headers.get("x-ratelimit-reset")
                logger.error(f"429 Too Many Requests. Reset at: {limit_reset}")

            response.raise_for_status()
            return response.json()

    async def get_instruments(self, currency: str, kind: str) -> list:
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
        
        # save to json
        save_dir = settings.BASE_DIR / kind
        save_dir.mkdir(parents=True, exist_ok=True)
        save_path = save_dir / "instruments.json"
        
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(instruments, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved instrument list to {save_path}")
        except Exception as e:
            logger.error(f"Failed to save {kind} instruments: {e}")

        return instruments

    async def get_last_trade_seq(self, instrument: str) -> int:
        try:
            params = {"instrument_name": instrument, "count": 1}
            data = await self._fetch("/get_last_trades_by_instrument", params)
            trades = data.get("result", {}).get("trades", [])
            return trades[0]["trade_seq"] if trades else 0
        except Exception as e:
            logger.error(f"Failed to get last trade seq for {instrument}: {e}")
            return 0

    async def get_trades_chunk(
        self, instrument: str, start_seq: int, end_seq: int
    ) -> tuple[list, bool]:
        params = {
            "instrument_name": instrument,
            "start_seq": start_seq,
            "end_seq": end_seq,
            "count": settings.CHUNK_SIZE,
        }
        data = await self._fetch("/get_last_trades_by_instrument", params)
        return (data["result"]["trades"], data["result"]["has_more"])

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
        logger.info("Deribit client closed.")

    async def close(self):
        await self.client.aclose()
