"""Tests for storage.py: JSONLinesSink."""

import orjson
import pytest

from deribit_fetcher.storage import JSONLinesSink

pytestmark = pytest.mark.asyncio


class TestJSONLinesSink:
    """Test suite for JSONLinesSink."""

    async def test_flush_creates_file(self, tmp_path):
        """Flush should create a .jsonl file with the correct data."""
        sink = JSONLinesSink(tmp_path)
        buffers = {
            "BTC-TEST": [
                {
                    "instrument": "BTC-TEST",
                    "data": [
                        {"trade_seq": 1, "price": 50000, "amount": 1.0},
                        {"trade_seq": 2, "price": 50001, "amount": 0.5},
                    ],
                }
            ]
        }
        await sink.flush(buffers)

        file_path = tmp_path / "BTC-TEST.jsonl"
        assert file_path.exists()

        with open(file_path, "rb") as f:
            lines = f.read().splitlines()
        assert len(lines) == 2
        row1 = orjson.loads(lines[0])
        assert row1["trade_seq"] == 1
        assert row1["price"] == 50000
        row2 = orjson.loads(lines[1])
        assert row2["trade_seq"] == 2

    async def test_flush_appends_to_existing(self, tmp_path):
        """Flush should append to an existing .jsonl file."""
        sink = JSONLinesSink(tmp_path)
        # First flush
        await sink.flush(
            {"BTC-TEST": [{"instrument": "BTC-TEST", "data": [{"trade_seq": 1}]}]}
        )
        # Second flush
        await sink.flush(
            {"BTC-TEST": [{"instrument": "BTC-TEST", "data": [{"trade_seq": 2}]}]}
        )

        file_path = tmp_path / "BTC-TEST.jsonl"
        with open(file_path, "rb") as f:
            lines = f.read().splitlines()
        assert len(lines) == 2
        assert orjson.loads(lines[0])["trade_seq"] == 1
        assert orjson.loads(lines[1])["trade_seq"] == 2

    async def test_flush_empty_buffer(self, tmp_path):
        """Flush with empty buffer should not create any files."""
        sink = JSONLinesSink(tmp_path)
        await sink.flush({})
        assert len(list(tmp_path.iterdir())) == 0

    async def test_flush_empty_data(self, tmp_path):
        """Flush with items that have empty data should not create files."""
        sink = JSONLinesSink(tmp_path)
        await sink.flush({"BTC-TEST": [{"instrument": "BTC-TEST", "data": None}]})
        file_path = tmp_path / "BTC-TEST.jsonl"
        assert not file_path.exists()

    async def test_multiple_instruments(self, tmp_path):
        """Flush should create separate files for different instruments."""
        sink = JSONLinesSink(tmp_path)
        await sink.flush(
            {
                "BTC-A": [{"instrument": "BTC-A", "data": [{"seq": 1}]}],
                "BTC-B": [{"instrument": "BTC-B", "data": [{"seq": 2}]}],
            }
        )

        assert (tmp_path / "BTC-A.jsonl").exists()
        assert (tmp_path / "BTC-B.jsonl").exists()
