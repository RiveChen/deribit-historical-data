import os
import logging
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    # API
    BASE_URL: str = "https://history.deribit.com/api/v2/public"
    CURRENCY: str = "BTC"
    CHUNK_SIZE: int = 10000

    # Paths
    BASE_DIR: Path = Path("./data") / CURRENCY
    DATA_FUTURE_DIR: Path = BASE_DIR / "future"
    DATA_OPTION_DIR: Path = BASE_DIR / "option"
    FUTURE_DB_PATH: Path = BASE_DIR / "future.db"
    OPTION_DB_PATH: Path = BASE_DIR / "option.db"

    # Concurrency & Limits
    MAX_RPS: int = 20
    MAX_WORKERS: int = 40

    # Network
    PROXY: str | None = None

    def __post_init__(self):
        self.PROXY = (
            os.environ.get("HTTP_PROXY")
            or os.environ.get("HTTPS_PROXY")
            or os.environ.get("http_proxy")
            or os.environ.get("https_proxy")
        )
        if self.PROXY:
            self.PROXY = self.PROXY.strip()


logger = logging.getLogger("Deribit Fetcher")

settings = Config()
