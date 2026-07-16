"""Application configuration loaded from environment variables."""

import logging
import os
from dataclasses import dataclass
from pathlib import Path


def _resolve_proxy() -> str | None:
    """Resolve proxy from environment (try uppercase and lowercase variants)."""
    proxy = (
        os.environ.get("HTTP_PROXY")
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("http_proxy")
        or os.environ.get("https_proxy")
    )
    return proxy.strip() if proxy else None


@dataclass
class Config:
    """Application configuration loaded from environment variables."""

    # API
    BASE_URL: str = "https://history.deribit.com/api/v2/public"
    CURRENCY: str = "BTC"
    CHUNK_SIZE: int = 10000

    # Concurrency & Limits
    MAX_RPS: int = 20
    MAX_WORKERS: int = 40

    # Network
    PROXY: str | None = None

    # --- derived path properties ---

    @property
    def base_dir(self) -> Path:
        """Root data directory based on current CURRENCY."""
        return Path("./data") / self.CURRENCY

    @property
    def data_future_dir(self) -> Path:
        """Directory for future JSONL files."""
        return self.base_dir / "future"

    @property
    def data_option_dir(self) -> Path:
        """Directory for option JSONL files."""
        return self.base_dir / "option"

    @property
    def future_db_path(self) -> Path:
        """SQLite database path for future progress tracking."""
        return self.base_dir / "future.db"

    @property
    def option_db_path(self) -> Path:
        """SQLite database path for option progress tracking."""
        return self.base_dir / "option.db"

    @classmethod
    def from_env(cls) -> "Config":
        """Factory: read environment variables and return a Config instance.

        Reads CURRENCY, HTTP_PROXY / HTTPS_PROXY / http_proxy / https_proxy
        from the environment. All other fields keep their default values.
        """
        currency = os.environ.get("CURRENCY", "").strip()
        if not currency:
            currency = "BTC"
        return cls(CURRENCY=currency, PROXY=_resolve_proxy())


logger = logging.getLogger("Deribit Fetcher")

settings = Config.from_env()
