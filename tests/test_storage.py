import pytest
import pandas as pd
from deribit_historical_data.storage import StorageManager


class TestStorageManager:
    def test_future_storage(self, temp_data_dir):
        storage = StorageManager(base_dir=str(temp_data_dir))
        name = "BTC-FUTURE"
        start, end = 1, 100
        trades = [
            {"trade_seq": i, "timestamp": 1000 + i, "price": 50000}
            for i in range(start, end + 1)
        ]

        # Test Save
        storage.save_future_chunk(name, start, end, trades)

        # Test Exists
        assert storage.is_future_chunk_exists(name, start, end)
        assert not storage.is_future_chunk_exists(name, 101, 200)

        # Verify Content
        file_path = temp_data_dir / "future" / name / f"chunk_{start}_{end}.parquet"
        df = pd.read_parquet(file_path)
        assert len(df) == 100
        assert df.iloc[0]["trade_seq"] == 1

        # Verify DB Update
        status = storage.tracker.get_instrument_status(name)
        assert status is not None
        assert status["kind"] == "future"
        assert status["last_seq"] == end
        assert status["status"] == "syncing"

    def test_option_storage_incremental(self, temp_data_dir):
        storage = StorageManager(base_dir=str(temp_data_dir))
        name = "BTC-OPTION"

        # Batch 1
        trades1 = [
            {"trade_seq": 1, "timestamp": 1000},
            {"trade_seq": 2, "timestamp": 1001},
        ]
        storage.save_option_trades(name, trades1, is_expired=False)

        # Batch 2 (with overlap)
        trades2 = [
            {"trade_seq": 2, "timestamp": 1001},
            {"trade_seq": 3, "timestamp": 1002},
        ]
        storage.save_option_trades(name, trades2, is_expired=True)

        # Verify File
        file_path = temp_data_dir / "option" / name / f"{name}.parquet"
        df = pd.read_parquet(file_path)

        # Should have 3 unique trades sorted
        assert len(df) == 3
        assert df["trade_seq"].tolist() == [1, 2, 3]

        # Verify DB Status
        status = storage.tracker.get_instrument_status(name)
        assert status["status"] == "completed"
        assert status["is_expired"] == 1
