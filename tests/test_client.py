import time
import pytest
import requests
from unittest.mock import MagicMock, call
from deribit_historical_data.client import DeribitClient, RateLimiter


class TestRateLimiter:
    def test_wait(self, mocker):
        """Test that wait sleeps correctly."""
        mock_sleep = mocker.patch("time.sleep")
        limiter = RateLimiter(max_rps=10)

        # First call should set next_run
        limiter.wait()
        assert mock_sleep.call_count == 0

        # Immediate second call should trigger sleep
        limiter.wait()
        assert mock_sleep.call_count == 1
        # Expect sleep approx 0.1s
        args, _ = mock_sleep.call_args
        assert 0.09 < args[0] < 0.11


class TestDeribitClient:
    @pytest.fixture
    def client(self):
        return DeribitClient(max_rps=100)

    def test_request_success(self, client, mocker):
        """Test successful request processing."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": ["trade1", "trade2"]}

        mocker.patch.object(client.session, "get", return_value=mock_response)

        result = client.request("test_endpoint", {"param": 1})
        assert result == ["trade1", "trade2"]
        client.session.get.assert_called_once()

    def test_request_429_retry(self, client, mocker):
        """Test retry logic on 429."""
        # First 429, then 200
        mock_429 = MagicMock()
        mock_429.status_code = 429

        mock_200 = MagicMock()
        mock_200.status_code = 200
        mock_200.json.return_value = {"result": "success"}

        # Mock session.get to return 429 twice then 200
        mocker.patch.object(
            client.session, "get", side_effect=[mock_429, mock_429, mock_200]
        )
        # Mock sleep to speed up test
        mocker.patch("time.sleep")

        result = client.request("test_endpoint")
        assert result == "success"
        assert client.session.get.call_count == 3

    def test_request_missing_result(self, client, mocker):
        """Test handling of malformed response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"error": "something wrong"}

        mocker.patch.object(client.session, "get", return_value=mock_response)

        # Should retry eventually, but here we expect it to raise or retry indefinitely.
        # Since we use tenacity, it might retry certain exceptions.
        # But ValueError (raised when 'result' missing) is NOT in retry_if_exception_type
        # (which only catches RequestException and Exception, wait... Exception includes ValueError).
        # Actually in the code: retry=retry_if_exception_type((requests.RequestException, Exception))
        # So it WILL retry on ValueError.
        # To avoid infinite loop in test, we can mock it to succeed eventually or just assert it retries specific times.
        # Let's mock it to fail once then succeed.

        mock_success = MagicMock()
        mock_success.status_code = 200
        mock_success.json.return_value = {"result": "ok"}

        mocker.patch.object(
            client.session, "get", side_effect=[mock_response, mock_success]
        )

        result = client.request("test_endpoint")
        assert result == "ok"
        assert client.session.get.call_count == 2

    def test_request_abort_on_stop_event(self, client, mocker):
        """Test that retries are aborted when stop_event is set."""
        import threading

        stop_event = threading.Event()
        client.set_stop_event(stop_event)

        # Mock requests to fail continuously
        mocker.patch.object(
            client.session,
            "get",
            side_effect=requests.exceptions.ConnectionError("Fail"),
        )

        # Mock sleep to set the event after a few tries
        original_sleep = time.sleep

        def side_effect_sleep(seconds):
            # Set stop event during sleep
            stop_event.set()
            original_sleep(0.01)  # Short sleep

        mocker.patch("time.sleep", side_effect=side_effect_sleep)

        with pytest.raises(KeyboardInterrupt, match="Stop event detected"):
            client.request("test_endpoint")
