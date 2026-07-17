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

    def test_default_values(self):
        """Config should have sensible defaults."""
        c = Config()
        assert c.BASE_URL == "https://history.deribit.com/api/v2/public"
        assert c.CURRENCY == "BTC"
        assert c.CHUNK_SIZE == 10000
        assert c.MAX_RPS == 20
        assert c.MAX_WORKERS == 40
        assert c.PROXY is None
        # Derived path properties
        assert c.base_dir == Path("./data/BTC")
        assert c.data_future_dir == Path("./data/BTC/future")
        assert c.data_option_dir == Path("./data/BTC/option")
        assert c.future_db_path == Path("./data/BTC/future.db")
        assert c.option_db_path == Path("./data/BTC/option.db")

    def test_proxy_from_http_env(self, monkeypatch):
        """HTTP_PROXY should be picked up."""
        monkeypatch.setenv("HTTP_PROXY", "http://proxy.example.com:8080")
        c = Config.from_env()
        assert c.PROXY == "http://proxy.example.com:8080"

    def test_proxy_from_https_env(self, monkeypatch):
        """HTTPS_PROXY should be picked up."""
        monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example.com:8443")
        c = Config.from_env()
        assert c.PROXY == "http://proxy.example.com:8443"

    def test_proxy_lowercase_fallback(self, monkeypatch):
        """Lowercase http_proxy should be picked up when uppercase not set."""
        monkeypatch.delenv("HTTP_PROXY", raising=False)
        monkeypatch.delenv("HTTPS_PROXY", raising=False)
        monkeypatch.setenv("http_proxy", "http://lowercase.proxy:8080")
        c = Config.from_env()
        assert c.PROXY == "http://lowercase.proxy:8080"

    def test_proxy_strips_whitespace(self, monkeypatch):
        """Proxy value should have whitespace stripped."""
        monkeypatch.setenv("HTTP_PROXY", "  http://proxy.com:8080  ")
        c = Config.from_env()
        assert c.PROXY == "http://proxy.com:8080"

    def test_proxy_precedence(self, monkeypatch):
        """HTTP_PROXY should take precedence over http_proxy."""
        monkeypatch.setenv("HTTP_PROXY", "http://upper.proxy:8080")
        monkeypatch.setenv("http_proxy", "http://lower.proxy:8080")
        c = Config.from_env()
        assert c.PROXY == "http://upper.proxy:8080"

    def test_currency_from_env(self, monkeypatch):
        """CURRENCY env var should override the default and affect derived paths."""
        monkeypatch.delenv("HTTP_PROXY", raising=False)
        monkeypatch.delenv("HTTPS_PROXY", raising=False)
        monkeypatch.delenv("http_proxy", raising=False)
        monkeypatch.delenv("https_proxy", raising=False)
        monkeypatch.setenv("CURRENCY", "ETH")
        c = Config.from_env()
        assert c.CURRENCY == "ETH"
        assert c.base_dir == Path("./data/ETH")
        assert c.data_future_dir == Path("./data/ETH/future")
        assert c.data_option_dir == Path("./data/ETH/option")
        assert c.future_db_path == Path("./data/ETH/future.db")
        assert c.option_db_path == Path("./data/ETH/option.db")

    def test_currency_env_strips_whitespace(self, monkeypatch):
        """CURRENCY value should have whitespace stripped."""
        monkeypatch.delenv("HTTP_PROXY", raising=False)
        monkeypatch.delenv("HTTPS_PROXY", raising=False)
        monkeypatch.setenv("CURRENCY", "  ETH  ")
        c = Config.from_env()
        assert c.CURRENCY == "ETH"
        assert c.base_dir == Path("./data/ETH")


class TestBaseDirOverride:
    """Test suite for the --base-dir override on Config."""

    def test_override_replaces_default_base_dir(self):
        """base_dir_override should replace ./data/<CURRENCY> for all derived paths."""
        c = Config(base_dir_override=Path("/mnt/deribit"))
        assert c.base_dir == Path("/mnt/deribit")
        assert c.data_future_dir == Path("/mnt/deribit/future")
        assert c.data_option_dir == Path("/mnt/deribit/option")
        assert c.future_db_path == Path("/mnt/deribit/future.db")
        assert c.option_db_path == Path("/mnt/deribit/option.db")

    def test_no_override_uses_currency_default(self):
        """With no override, base_dir still derives from CURRENCY."""
        c = Config(CURRENCY="ETH")
        assert c.base_dir == Path("./data/ETH")

    def test_set_base_dir_helper_mutates_settings(self, monkeypatch):
        """set_base_dir() should override the global settings; falsy input is a no-op."""
        from deribit_fetcher import config as config_mod

        monkeypatch.setattr(config_mod.settings, "base_dir_override", None)

        config_mod.set_base_dir(None)
        assert config_mod.settings.base_dir_override is None

        config_mod.set_base_dir("/tmp/deribit-data")
        assert config_mod.settings.base_dir == Path("/tmp/deribit-data")
        assert config_mod.settings.future_db_path == Path("/tmp/deribit-data/future.db")
