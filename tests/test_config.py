"""Tests for config.py and general functionality."""

import os
import pytest
from deribit_fetcher.config import Config


class TestConfig:
    def test_default_values(self, monkeypatch):
        """Config should have sensible defaults."""
        monkeypatch.delenv("HTTP_PROXY", raising=False)
        monkeypatch.delenv("HTTPS_PROXY", raising=False)
        monkeypatch.delenv("http_proxy", raising=False)
        monkeypatch.delenv("https_proxy", raising=False)
        c = Config()
        assert c.BASE_URL == "https://history.deribit.com/api/v2/public"
        assert c.CURRENCY == "BTC"
        assert c.CHUNK_SIZE == 10000
        assert c.MAX_RPS == 20
        assert c.MAX_WORKERS == 40
        assert c.PROXY is None

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
