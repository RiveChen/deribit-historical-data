import pytest
from unittest.mock import MagicMock, call
from deribit_historical_data.fetcher import FutureFetcher, OptionFetcher


class TestFutureFetcher:
    def test_sync_chunk(self, mock_client, mock_storage, mocker):
        fetcher = FutureFetcher(mock_client, mock_storage)

        # Mock storage says chunk does not exist
        # mocker.patch.object must be used because mock_storage is a real object
        mocker.patch.object(mock_storage, "is_future_chunk_exists", return_value=False)
        mocker.patch.object(mock_storage, "save_future_chunk")

        # Mock client returns trades
        trades = [{"trade_seq": 1}]
        mock_client.request.return_value = trades

        # Mock tqdm
        mock_pbar = MagicMock()

        fetcher.sync_chunk("BTC-FUTURE", 1, 10000, mock_pbar)

        # Verify request
        mock_client.request.assert_called_with(
            "get_last_trades_by_instrument",
            {
                "instrument_name": "BTC-FUTURE",
                "start_seq": 1,
                "end_seq": 10000,
                "count": 10000,
            },
        )

        # Verify save
        mock_storage.save_future_chunk.assert_called_with(
            "BTC-FUTURE", 1, 10000, trades
        )
        mock_pbar.update.assert_called_once()


class TestOptionFetcher:
    def test_sync_instrument(self, mock_client, mock_storage, mocker):
        fetcher = OptionFetcher(mock_client, mock_storage)

        inst = {
            "instrument_name": "BTC-OPT",
            "creation_timestamp": 1000,
            "expiration_timestamp": 5000,
        }
        cur_ts = 6000  # Expired

        # Mock status not exists
        mocker.patch.object(
            mock_storage.tracker, "get_instrument_status", return_value=None
        )
        mocker.patch.object(mock_storage, "save_option_trades")
        mocker.patch.object(mock_storage, "mark_instrument_completed")

        # Create a large batch to avoid early break (len < 10000)
        # We need 10000 items to make the loop continue to the next iteration
        trades_batch1 = [{"trade_seq": i, "timestamp": 1500 + i} for i in range(10000)]
        # Ensure max timestamp is set correctly for cursor update: max(1500...11499) -> 11499
        # But we want to control it, let's just let it be.
        # The next cursor will be max_ts + 1 = 11500
        # If end_ts is 5000, 11500 is > 5000, loop terminates by timestamp check!
        # Ah, logic: while current_cursor < end_ts.
        # So if we want another loop, current_cursor must be < end_ts.
        # But if max_ts > end_ts, it will stop.
        # So trades timestamps must be within range [1000, 5000].
        # But we are mocking, so we can set timestamps to stay within range.

        # Let's adjust timestamps to be small.
        # All timestamps = 1000. max_ts = 1000. next = 1001. < 5000. Continue.
        for t in trades_batch1:
            t["timestamp"] = 1000

        mock_client.request.side_effect = [{"trades": trades_batch1}, {"trades": []}]

        mock_pbar = MagicMock()

        fetcher.sync_instrument(inst, cur_ts, mock_pbar)

        # Check interactions
        # It should call twice: once for batch1, once for empty batch (and then break)
        assert mock_client.request.call_count == 2

        # Verify save called for the first batch
        assert mock_storage.save_option_trades.call_count == 1
        mock_storage.save_option_trades.assert_called_with(
            "BTC-OPT", trades_batch1, True
        )

        mock_storage.mark_instrument_completed.assert_called_with("BTC-OPT", "option")
