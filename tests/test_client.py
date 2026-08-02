"""Tests for client.py: rate-limit wait strategy, retry logic, and None semantics.

All tests use httpx.MockTransport (no real network requests).
"""

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt, wait_fixed

from deribit_fetcher.client import (
    DeribitAPIError,
    DeribitClient,
    DeribitRateLimitWait,
    is_retryable_exception,
)
from deribit_fetcher.config import settings

# =============================================================================
# Tests for DeribitRateLimitWait (pure logic, no network)
# =============================================================================


class TestDeribitRateLimitWait:
    """Test the custom wait strategy in isolation."""

    def _make_retry_state(self, exception=None):
        """Helper to construct a mock RetryCallState."""
        retry_object = MagicMock()
        state = RetryCallState(retry_object=retry_object, fn=lambda: None, args=(), kwargs={})
        state.outcome = MagicMock()
        if exception is not None:
            state.outcome.exception.return_value = exception
        return state

    def test_retry_after_header_preferred(self):
        """A 429 with Retry-After: 2 should wait 2.5s."""
        response = httpx.Response(429, headers={"Retry-After": "2"})
        exc = httpx.HTTPStatusError("Too Many", request=MagicMock(), response=response)
        state = self._make_retry_state(exception=exc)

        fallback = MagicMock(return_value=10.0)
        wait = DeribitRateLimitWait(fallback_wait=fallback)
        result = wait(state)

        assert result == 2.5, "Should use Retry-After + 0.5 buffer, not fallback"
        fallback.assert_not_called()

    def test_no_header_falls_back(self):
        """A 429 without Retry-After header should fall back to exponential backoff."""
        response = httpx.Response(429, headers={})
        exc = httpx.HTTPStatusError("Too Many", request=MagicMock(), response=response)
        state = self._make_retry_state(exception=exc)

        fallback = MagicMock(return_value=3.0)
        wait = DeribitRateLimitWait(fallback_wait=fallback)
        result = wait(state)

        assert result == 3.0, "Should fall back to fallback_wait"
        fallback.assert_called_once_with(state)

    def test_non_digit_retry_after_falls_back(self):
        """A malformed Retry-After header should fall back."""
        response = httpx.Response(429, headers={"Retry-After": "abc"})
        exc = httpx.HTTPStatusError("Too Many", request=MagicMock(), response=response)
        state = self._make_retry_state(exception=exc)

        fallback = MagicMock(return_value=2.0)
        wait = DeribitRateLimitWait(fallback_wait=fallback)
        result = wait(state)

        assert result == 2.0

    def test_timeout_exception_falls_back(self):
        """A non-429 exception (e.g. TimeoutException) should fall back."""
        exc = httpx.TimeoutException("Timed out")
        state = self._make_retry_state(exception=exc)

        fallback = MagicMock(return_value=1.5)
        wait = DeribitRateLimitWait(fallback_wait=fallback)
        result = wait(state)

        assert result == 1.5

    def test_no_outcome_falls_back(self):
        """When retry_state.outcome is None, should fall back."""
        state = self._make_retry_state()
        state.outcome = None

        fallback = MagicMock(return_value=0.5)
        wait = DeribitRateLimitWait(fallback_wait=fallback)
        result = wait(state)

        assert result == 0.5
        fallback.assert_called_once_with(state)


# =============================================================================
# Tests for _fetch retry behavior via MockTransport
#
# We create a thin DeribitClient subclass with a much faster retry config so
# tests don't wait for real exponential backoff.  The retry *logic* (stop after
# N attempts, reraise, which exceptions trigger retry) is identical.
# =============================================================================


class _FastRetryClient(DeribitClient):
    """DeribitClient variant with accelerated retry for testing."""

    def __init__(self, transport: httpx.MockTransport):
        """Skip normal init — build client with mock transport and fast retry."""
        self.limiter = MagicMock()
        self.limiter.__aenter__ = AsyncMock(return_value=None)
        self.limiter.__aexit__ = AsyncMock(return_value=None)
        self.client = httpx.AsyncClient(transport=transport, base_url=settings.BASE_URL)

    # Re-apply @retry with fast wait so tests don't sleep for seconds
    @retry(
        retry=retry_if_exception(is_retryable_exception),
        wait=wait_fixed(0.001),  # Nearly instant retries
        stop=stop_after_attempt(3),
        reraise=True,
    )
    async def _fetch(self, endpoint: str, params: dict):
        return await self._request_once(endpoint, params)


class TestFetchRetry:
    """Test _fetch retry logic using MockTransport."""

    async def test_success_on_first_try(self):
        """A 200 response should return immediately."""

        def handler(request):
            return httpx.Response(200, json={"result": {"trades": []}})

        client = _FastRetryClient(httpx.MockTransport(handler))
        result = await client._fetch("/test", {})
        assert result == {"result": {"trades": []}}

    async def test_429_retry_then_success(self):
        """429 → 429 → 200: should retry twice, succeed on third."""
        call_count = 0

        def handler(request):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return httpx.Response(429)
            return httpx.Response(200, json={"result": {"trades": [{"trade_seq": 1}]}})

        client = _FastRetryClient(httpx.MockTransport(handler))
        result = await client._fetch("/test", {})
        assert call_count == 3, "Should have retried 2 times"
        assert result["result"]["trades"][0]["trade_seq"] == 1

    async def test_all_attempts_fail(self):
        """All 3 attempts return 429 → should raise HTTPStatusError."""

        def handler(request):
            return httpx.Response(429)

        client = _FastRetryClient(httpx.MockTransport(handler))
        with pytest.raises(httpx.HTTPStatusError):
            await client._fetch("/test", {})

    @pytest.mark.parametrize("status_code", [400, 401, 403, 404])
    async def test_deterministic_client_errors_are_not_retried(self, status_code):
        """Request/auth/not-found errors should fail on the first response."""
        call_count = 0

        def handler(request):
            nonlocal call_count
            call_count += 1
            return httpx.Response(status_code)

        client = _FastRetryClient(httpx.MockTransport(handler))
        with pytest.raises(httpx.HTTPStatusError):
            await client._fetch("/test", {})

        assert call_count == 1

    @pytest.mark.parametrize("status_code", [408, 500, 503])
    async def test_retryable_http_errors_retry_then_succeed(self, status_code):
        """Timeout/rate-limit/server statuses should retry within the budget."""
        call_count = 0

        def handler(request):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(status_code)
            return httpx.Response(200, json={"result": "ok"})

        client = _FastRetryClient(httpx.MockTransport(handler))
        assert await client._fetch("/test", {}) == {"result": "ok"}
        assert call_count == 2

    async def test_json_rpc_error_preserves_code_without_retry(self):
        """An HTTP-200 Deribit error body is explicit and deterministic."""
        call_count = 0

        def handler(request):
            nonlocal call_count
            call_count += 1
            return httpx.Response(
                200,
                json={"error": {"code": 11050, "message": "bad_request", "data": "detail"}},
            )

        client = _FastRetryClient(httpx.MockTransport(handler))
        with pytest.raises(DeribitAPIError) as error:
            await client._fetch("/test", {})

        assert error.value.code == 11050
        assert error.value.data == "detail"
        assert call_count == 1


# =============================================================================
# Tests for get_last_trade_seq None semantics
# =============================================================================


class TestGetLastTradeSeq:
    """Test the None/0 return semantics of get_last_trade_seq."""

    async def test_returns_zero_when_no_trades(self):
        """A successful response with empty trades should return 0."""

        def handler(request):
            return httpx.Response(200, json={"result": {"trades": []}})

        client = _FastRetryClient(httpx.MockTransport(handler))
        result = await client.get_last_trade_seq("BTC-EMPTY")
        assert result == 0

    async def test_returns_trade_seq_when_trades_exist(self):
        """A successful count=1 response should return its sole trade_seq."""

        def handler(request):
            return httpx.Response(200, json={"result": {"trades": [{"trade_seq": 42}]}})

        client = _FastRetryClient(httpx.MockTransport(handler))
        result = await client.get_last_trade_seq("BTC-HAS-TRADES")
        assert result == 42

    async def test_returns_none_on_total_failure(self):
        """All 3 attempts fail → should return None (not 0)."""

        def handler(request):
            return httpx.Response(429)

        client = _FastRetryClient(httpx.MockTransport(handler))
        result = await client.get_last_trade_seq("BTC-ALWAYS-429")
        assert result is None, "Must return None, not 0, when all retries exhausted"

    async def test_returns_none_on_timeout(self):
        """Timeout on all attempts → should return None."""

        def handler(request):
            raise httpx.TimeoutException("Connection timed out")

        client = _FastRetryClient(httpx.MockTransport(handler))
        result = await client.get_last_trade_seq("BTC-TIMEOUT")
        assert result is None


# =============================================================================
# Tests for get_trades_chunk / get_instruments
# =============================================================================


class TestGetTradesChunk:
    """Test that get_trades_chunk parses (trades, has_more) correctly."""

    async def test_returns_expected_tuple(self):
        """Should return (trades_list, has_more_bool)."""

        def handler(request):
            return httpx.Response(
                200,
                json={"result": {"trades": [{"trade_seq": 1}, {"trade_seq": 2}], "has_more": True}},
            )

        client = _FastRetryClient(httpx.MockTransport(handler))
        trades, has_more = await client.get_trades_chunk("BTC-PERPETUAL", 1, 10000)
        assert trades == [{"trade_seq": 1}, {"trade_seq": 2}]
        assert has_more is True


class TestGetInstruments:
    """Test get_instruments parses and returns instrument list."""

    async def test_returns_instruments(self, tmp_path):
        """Should return merged instrument list from both expired/active requests."""

        def handler(request):
            expired = request.url.params.get("expired")
            if expired == "true":
                return httpx.Response(
                    200,
                    json={"result": [{"instrument_name": "BTC-EXPIRED", "is_active": False}]},
                )
            return httpx.Response(
                200,
                json={"result": [{"instrument_name": "BTC-ACTIVE", "is_active": True}]},
            )

        client = _FastRetryClient(httpx.MockTransport(handler))
        result = await client.get_instruments("BTC", "future")
        assert len(result) == 2
        names = {r["instrument_name"] for r in result}
        assert names == {"BTC-EXPIRED", "BTC-ACTIVE"}
