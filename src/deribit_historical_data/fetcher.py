import time
import logging
import threading
import concurrent.futures
from typing import List, Tuple, Dict
from tqdm import tqdm
from .client import DeribitClient
from .storage import StorageManager

logger = logging.getLogger(__name__)


class BaseFetcher:
    def __init__(
        self, client: DeribitClient, storage: StorageManager, max_workers: int = 10
    ):
        self.client = client
        self.storage = storage
        self.max_workers = max_workers
        self.stop_event = threading.Event()

    def get_instruments(self, currency: str, kind: str) -> List[Dict]:
        """获取所有品种（包括已过期的）"""
        results = []
        for expired in ["true", "false"]:
            data = self.client.request(
                "get_instruments",
                {"currency": currency, "kind": kind, "expired": expired},
            )
            results.extend(data)
        return results


class FutureFetcher(BaseFetcher):
    """期货抓取器：基于序列号 (trade_seq) 进行分片同步"""

    def _get_latest_seq(self, name: str, end_ts: int) -> int:
        data = self.client.request(
            "get_last_trades_by_instrument_and_time",
            {"instrument_name": name, "end_timestamp": end_ts, "count": 1},
        )
        return data["trades"][0]["trade_seq"] if data["trades"] else 0

    def sync_chunk(self, name: str, start: int, end: int, pbar: tqdm):
        if self.stop_event.is_set():
            return

        if not self.storage.is_future_chunk_exists(name, start, end):
            trades = self.client.request(
                "get_last_trades_by_instrument",
                {
                    "instrument_name": name,
                    "start_seq": start,
                    "end_seq": end,
                    "count": 10000,
                },
            )
            self.storage.save_future_chunk(name, start, end, trades)

        pbar.update(1)

    def run(self, currency: str = "BTC"):
        instruments = self.get_instruments(currency, "future")
        cur_ts = int(time.time() * 1000)

        # 1. 汇总所有分片任务
        all_tasks = []
        print(f"[{currency}] Preparing Future tasks...")
        for inst in instruments:
            name = inst["instrument_name"]
            end_ts = min(inst["expiration_timestamp"], cur_ts)
            latest_seq = self._get_latest_seq(name, end_ts)

            # 生成分片逻辑
            for s in range(1, latest_seq, 10000):
                all_tasks.append((name, s, min(s + 9999, latest_seq)))

        # 2. 执行并发抓取
        with tqdm(
            total=len(all_tasks), desc=f"Future Chunks ({currency})", unit="chunk"
        ) as pbar:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_workers
            ) as executor:
                futures = [
                    executor.submit(self.sync_chunk, *t, pbar) for t in all_tasks
                ]
                try:
                    for f in concurrent.futures.as_completed(futures):
                        f.result()
                except KeyboardInterrupt:
                    self.stop_event.set()
                    executor.shutdown(wait=True, cancel_futures=True)
                    raise


class OptionFetcher(BaseFetcher):
    """期权抓取器：基于时间戳 (timestamp) 进行增量同步"""

    def sync_instrument(self, inst: Dict, cur_ts: int, pbar: tqdm):
        if self.stop_event.is_set():
            return

        name = inst["instrument_name"]
        start_ts = inst["creation_timestamp"]
        end_ts = min(inst["expiration_timestamp"], cur_ts)
        is_expired = inst["expiration_timestamp"] <= cur_ts

        # 从 SQLite 获取断点
        status = self.storage.tracker.get_instrument_status(name)
        if status and status["status"] == "completed":
            pbar.update(1)
            return

        current_cursor = max(start_ts, (status["last_ts"] if status else 0) + 1)

        while current_cursor < end_ts and not self.stop_event.is_set():
            data = self.client.request(
                "get_last_trades_by_instrument_and_time",
                {
                    "instrument_name": name,
                    "start_timestamp": current_cursor,
                    "end_timestamp": end_ts,
                    "count": 10000,
                },
            )
            trades = data.get("trades", [])
            if not trades:
                break

            self.storage.save_option_trades(name, trades, is_expired)
            current_cursor = max(t["timestamp"] for t in trades) + 1
            if len(trades) < 10000:
                break

        if not self.stop_event.is_set() and is_expired:
            self.storage.mark_instrument_completed(name, "option")

        pbar.update(1)
        pbar.set_postfix_str(f"Last: {name[:15]}")

    def run(self, currency: str = "BTC"):
        instruments = self.get_instruments(currency, "option")
        cur_ts = int(time.time() * 1000)

        with tqdm(
            total=len(instruments), desc=f"Option Sync ({currency})", unit="inst"
        ) as pbar:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_workers
            ) as executor:
                futures = [
                    executor.submit(self.sync_instrument, inst, cur_ts, pbar)
                    for inst in instruments
                ]
                try:
                    for f in concurrent.futures.as_completed(futures):
                        f.result()
                except KeyboardInterrupt:
                    self.stop_event.set()
                    executor.shutdown(wait=True, cancel_futures=True)
                    raise
