import time
import random
import logging
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, Any, Optional
from tenacity import (
    retry,
    stop_never,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

# 配置日志，用于追踪重试状态
logger = logging.getLogger(__name__)


class RateLimiter:
    """令牌桶限流器：确保多线程环境下请求速率不超过限制"""

    def __init__(self, max_rps: int):
        self.interval = 1.0 / max_rps
        self.next_run = time.time()
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            if now < self.next_run:
                sleep_time = self.next_run - now
                time.sleep(sleep_time)
                self.next_run += self.interval
            else:
                self.next_run = now + self.interval


class DeribitClient:
    """
    Deribit 客户端
    - 自动处理 HTTP 429 (Rate Limit)
    - 指数退避无限重试 (Must succeed)
    - 令牌桶严格限流 (20 RPS)
    """

    def __init__(
        self,
        api_base: str = "https://history.deribit.com/api/v2/public",
        max_rps: int = 20,
        timeout: int = 15,
    ):
        self.api_base = api_base
        self.timeout = timeout
        self.limiter = RateLimiter(max_rps)  # 设置令牌桶限流
        self.session = requests.Session()  # 持久化 session

        retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retries)

        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # 设置默认 Header 减少被拦截风险
        self.session.headers.update(
            {
                "User-Agent": "DeribitDataFetcher/2.0 (Python/Requests)",
                "Accept": "application/json",
            }
        )

    @retry(
        stop=stop_never,  # 永不停止，直到成功
        wait=wait_exponential(multiplier=1, min=2, max=60),  # 2s, 4s, 8s...
        retry=retry_if_exception_type((requests.RequestException, Exception)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        发起网络请求，内置限流与重试逻辑
        """
        # 1. 进入限流等待
        self.limiter.wait()

        url = f"{self.api_base}/{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)

            # 2. 处理频率限制 (Deribit 返回 429)
            if response.status_code == 429:
                logger.warning(
                    f"Rate limit exceeded (429) on {endpoint}. Backing off..."
                )
                # 抛出异常以触发 tenacity 的 retry 逻辑
                raise requests.exceptions.RequestException(
                    "Deribit 429: Too Many Requests"
                )

            # 3. 处理其他 HTTP 错误 (500, 502, 503, 504 等)
            response.raise_for_status()

            # 4. 验证 JSON 响应完整性
            data = response.json()
            if "result" not in data:
                logger.error(f"Invalid API response structure: {data}")
                raise ValueError("API response missing 'result' field")

            return data["result"]

        except (requests.Timeout, requests.ConnectionError) as e:
            # 网络层错误
            logger.error(f"Network error on {endpoint}: {str(e)}")
            raise
        except Exception as e:
            # 其他不可预见的错误
            logger.error(f"Unexpected error: {str(e)}")
            raise
