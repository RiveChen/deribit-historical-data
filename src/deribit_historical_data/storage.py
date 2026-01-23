import sqlite3
import threading
import time
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime


class StatusTracker:
    """使用 SQLite 管理同步状态，确保断点续传的原子性和可靠性"""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._local = threading.local()
        # 初始化数据库结构（在主线程执行一次）
        self._init_db()

    @property
    def conn(self):
        """
        为每个线程提供独立的数据库连接 (Thread-Local Storage)
        解决 sqlite3.ProgrammingError: SQLite objects created in a thread can only be used in that same thread.
        """
        if not hasattr(self._local, "connection"):
            # 建立物理连接
            conn = sqlite3.connect(
                str(self.db_path), check_same_thread=False, timeout=30.0
            )
            conn.row_factory = sqlite3.Row
            # 性能优化：开启 WAL 模式，允许并发读写，大幅减少 'database is locked' 错误
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            self._local.connection = conn
        return self._local.connection

    def _init_db(self):
        """创建表结构和索引"""
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_status (
                    instrument_name TEXT PRIMARY KEY,
                    kind TEXT,
                    last_seq INTEGER DEFAULT 0,
                    last_ts INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending', 
                    is_expired BOOLEAN DEFAULT 0,
                    updated_at TIMESTAMP
                )
            """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_status ON sync_status(status)"
            )

    def get_instrument_status(self, name: str) -> Optional[Dict]:
        """获取特定合约的同步记录"""
        cursor = self.conn.execute(
            "SELECT * FROM sync_status WHERE instrument_name = ?", (name,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def update_status(
        self, name: str, kind: str, seq: int, ts: int, status: str, is_expired: bool
    ):
        """原子化更新同步进度"""
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO sync_status (instrument_name, kind, last_seq, last_ts, status, is_expired, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_name) DO UPDATE SET
                    last_seq = excluded.last_seq,
                    last_ts = excluded.last_ts,
                    status = excluded.status,
                    is_expired = excluded.is_expired,
                    updated_at = excluded.updated_at
            """,
                (name, kind, seq, ts, status, int(is_expired), datetime.now()),
            )

    def mark_instrument_completed(self, name: str, kind: str):
        """标记任务为完成状态"""
        status_info = self.get_instrument_status(name)
        if status_info:
            self.update_status(
                name,
                kind,
                status_info["last_seq"],
                status_info["last_ts"],
                "completed",
                status_info["is_expired"],
            )


class StorageManager:
    """管理数据落地：Future 分片存储，Option 累积合并存储"""

    def __init__(self, base_dir: str = "data"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        # 状态数据库放在 data 根目录
        self.tracker = StatusTracker(self.base_dir / "sync_status.db")

    def _get_folder(self, kind: str, name: str) -> Path:
        folder = self.base_dir / kind / name
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    # --- Future 逻辑 ---
    def is_future_chunk_exists(self, name: str, start: int, end: int) -> bool:
        file_path = self._get_folder("future", name) / f"chunk_{start}_{end}.parquet"
        return file_path.exists() and file_path.stat().st_size > 0

    def save_future_chunk(self, name: str, start: int, end: int, trades: List[Dict]):
        if not trades:
            return
        df = pd.DataFrame(trades)
        file_path = self._get_folder("future", name) / f"chunk_{start}_{end}.parquet"
        df.to_parquet(file_path, engine="pyarrow", compression="zstd", index=False)

        # 更新数据库断点
        max_ts = int(df["timestamp"].max())
        self.tracker.update_status(name, "future", end, max_ts, "syncing", False)

    # --- Option 逻辑 ---
    def save_option_trades(self, name: str, trades: List[Dict], is_expired: bool):
        if not trades:
            return

        df = pd.DataFrame(trades)
        file_path = self._get_folder("option", name) / f"{name}.parquet"

        # 合并已有数据去重
        if file_path.exists():
            old_df = pd.read_parquet(file_path)
            df = (
                pd.concat([old_df, df])
                .drop_duplicates(subset=["trade_seq"])
                .sort_values("trade_seq")
            )

        df.to_parquet(file_path, engine="pyarrow", compression="zstd", index=False)

        # 更新进度
        max_seq = int(df["trade_seq"].max())
        max_ts = int(df["timestamp"].max())
        status = "completed" if is_expired else "syncing"
        self.tracker.update_status(name, "option", max_seq, max_ts, status, is_expired)

    def mark_instrument_completed(self, name: str, kind: str):
        self.tracker.mark_instrument_completed(name, kind)
