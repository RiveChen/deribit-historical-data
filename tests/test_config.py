"""Tests for config.py and general functionality."""

from pathlib import Path

import pytest

from deribit_fetcher.config import Config


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Ensure a hermetic environment for testing.

    Clears ambient proxy/currency vars so tests don't depend on the host
    (e.g. a CI runner or shell with HTTP_PROXY set).
    """
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "CURRENCY"):
        monkeypatch.delenv(var, raising=False)


class TestConfig:
    """Test suite for Config dataclass."""

    def test_default_values(self, monkeypatch):
        """Config should have sensible defaults."""
        monkeypatch.delenv("HTTP_PROXY", raising=False)
        monkeypatch.delenv("HTTPS_PROXY", raising=False)
        monkeypatch.delenv("http_proxy", raising=False)
        monkeypatch.delenv("https_proxy", raising=False)
        monkeypatch.delenv("CURRENCY", raising=False)
        c = Config()
        assert c.BASE_URL == "https://history.deribit.com/api/v2/public"
        assert c.CURRENCY == "BTC"
        assert c.CHUNK_SIZE == 10000
        assert c.MAX_RPS == 20
        assert c.MAX_WORKERS == 40
        assert c.PROXY is None
        assert c.BASE_DIR == Path("./data/BTC")
        assert c.DATA_FUTURE_DIR == Path("./data/BTC/future")
        assert c.DATA_OPTION_DIR == Path("./data/BTC/option")
        assert c.FUTURE_DB_PATH == Path("./data/BTC/future.db")
        assert c.OPTION_DB_PATH == Path("./data/BTC/option.db")

    def test_proxy_from_http_env(self, monkeypatch):
        """HTTP_PROXY should be picked up."""
        monkeypatch.setenv("HTTP_PROXY", "http://proxy.example.com:8080")
        c = Config()
        assert c.PROXY == "http://proxy.example.com:8080"

    def test_proxy_from_https_env(self, monkeypatch):
        """HTTPS_PROXY should be picked up."""
        monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example.com:8443")
        c = Config()
        assert c.PROXY == "http://proxy.example.com:8443"

    def test_proxy_lowercase_fallback(self, monkeypatch):
        """Lowercase http_proxy should be picked up when uppercase not set."""
        monkeypatch.delenv("HTTP_PROXY", raising=False)
        monkeypatch.delenv("HTTPS_PROXY", raising=False)
        monkeypatch.setenv("http_proxy", "http://lowercase.proxy:8080")
        c = Config()
        assert c.PROXY == "http://lowercase.proxy:8080"

    def test_proxy_strips_whitespace(self, monkeypatch):
        """Proxy value should have whitespace stripped."""
        monkeypatch.setenv("HTTP_PROXY", "  http://proxy.com:8080  ")
        c = Config()
        assert c.PROXY == "http://proxy.com:8080"

    def test_proxy_precedence(self, monkeypatch):
        """HTTP_PROXY should take precedence over http_proxy."""
        monkeypatch.setenv("HTTP_PROXY", "http://upper.proxy:8080")
        monkeypatch.setenv("http_proxy", "http://lower.proxy:8080")
        c = Config()
        assert c.PROXY == "http://upper.proxy:8080"

    def test_currency_from_env(self, monkeypatch):
        """CURRENCY env var should override the default and affect derived paths."""
        monkeypatch.delenv("HTTP_PROXY", raising=False)
        monkeypatch.delenv("HTTPS_PROXY", raising=False)
        monkeypatch.delenv("http_proxy", raising=False)
        monkeypatch.delenv("https_proxy", raising=False)
        monkeypatch.setenv("CURRENCY", "ETH")
        c = Config()
        assert c.CURRENCY == "ETH"
        assert c.BASE_DIR == Path("./data/ETH")
        assert c.DATA_FUTURE_DIR == Path("./data/ETH/future")
        assert c.DATA_OPTION_DIR == Path("./data/ETH/option")
        assert c.FUTURE_DB_PATH == Path("./data/ETH/future.db")
        assert c.OPTION_DB_PATH == Path("./data/ETH/option.db")

    def test_currency_env_strips_whitespace(self, monkeypatch):
        """CURRENCY value should have whitespace stripped."""
        monkeypatch.delenv("HTTP_PROXY", raising=False)
        monkeypatch.delenv("HTTPS_PROXY", raising=False)
        monkeypatch.setenv("CURRENCY", "  ETH  ")
        c = Config()
        assert c.CURRENCY == "ETH"
        assert c.BASE_DIR == Path("./data/ETH")
