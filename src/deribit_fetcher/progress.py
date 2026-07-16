"""Progress tracking repositories using SQLite for checkpointing fetch progress."""

from pathlib import Path

import aiosqlite

from deribit_fetcher.config import logger, settings


class DatabaseClient:
    """Manages the SQLite connection and schema initialization.

    Designed to be used as an async context manager. Creates all required
    tables on first connection and configures appropriate PRAGMAs (WAL mode,
    NORMAL synchronous) for concurrent read/write performance.
    """

    def __init__(self, db_path: Path):
        """Initialize the database client and ensure the parent directory exists."""
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db: aiosqlite.Connection | None = None

    async def __aenter__(self):
        """Enter async context manager, connect to SQLite and initialize schema."""
        self.db = await aiosqlite.connect(self.db_path)
        self.db.row_factory = aiosqlite.Row
        await self.db.execute("PRAGMA journal_mode=WAL;")
        await self.db.execute("PRAGMA synchronous=NORMAL;")

        # Future checkpoint tables
        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS future_meta (
                instrument TEXT PRIMARY KEY,
                is_expired INTEGER NOT NULL DEFAULT 0,
                is_completed INTEGER NOT NULL DEFAULT 0
            )
        """
        )
        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS future_chunk (
                instrument TEXT,
                chunk_no INTEGER,
                count INTEGER DEFAULT 0,
                has_more INTEGER,
                is_done INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (instrument, chunk_no)
            )
        """
        )
        # Option checkpoint tables
        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS option_meta (
                instrument TEXT PRIMARY KEY,
                last_no INTEGER DEFAULT 0,
                is_expired INTEGER NOT NULL DEFAULT 0,
                is_completed INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        await self.db.commit()
        logger.info("Database schema initialized and connected.")
        return self.db

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit async context manager and close the database connection."""
        if self.db:
            await self.db.close()
            logger.info("Database connection closed.")


class FutureProgressRepo:
    """Repository for future fetch progress tracking."""

    def __init__(self, db: aiosqlite.Connection):
        """Initialize repository with a database connection."""
        self.db = db

    async def upsert_future_list(self, instruments: list[dict]):
        """Insert or update the future instrument list. Marks completed=0 for new entries."""
        data = [(i["instrument_name"], int(not i["is_active"])) for i in instruments]
        sql = """
            INSERT INTO future_meta (instrument, is_expired, is_completed)
            VALUES (?, ?, 0)
            ON CONFLICT(instrument) DO UPDATE SET
                is_expired = excluded.is_expired
        """
        await self.db.executemany(sql, data)
        await self.db.commit()
        logger.info("Future list upserted.")

    async def get_incomplete_future_list(self) -> list[dict]:
        """Get all futures that haven't been fully fetched yet."""
        async with self.db.execute(
            "SELECT instrument, is_expired FROM future_meta WHERE is_completed = 0"
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                {"instrument": row["instrument"], "is_expired": row["is_expired"]} for row in rows
            ]

    async def mark_future_complete(self, instrument: str):
        """Mark a future instrument as fully fetched (no trades or all chunks done)."""
        await self.db.execute(
            "UPDATE future_meta SET is_completed = 1 WHERE instrument = ?",
            (instrument,),
        )
        await self.db.commit()

    async def upsert_chunks(self, chunks: list[tuple]):
        """Pre-allocate chunk records for a future. Uses INSERT OR IGNORE for idempotency."""
        await self.db.executemany(
            "INSERT OR IGNORE INTO future_chunk (instrument, chunk_no, is_done) VALUES (?, ?, 0)",
            chunks,
        )
        await self.db.commit()

    async def get_pending_chunks(self) -> list[dict]:
        """Get all chunks that haven't been fetched yet."""
        cursor = await self.db.execute(
            "SELECT instrument, chunk_no FROM future_chunk WHERE is_done = 0"
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def update_chunks(self, chunks: list[tuple[int, bool, str, int]]):
        """Record fetch results (count, has_more) for completed chunks."""
        await self.db.executemany(
            "UPDATE future_chunk SET count = ?, has_more = ? WHERE instrument = ? AND chunk_no = ?",
            chunks,
        )
        await self.db.commit()

    async def finalize_chunks(self):
        """Mark chunks as done if they meet either finalize condition.

        A chunk is finalized when:
        - count >= CHUNK_SIZE (chunk was full — all data in range was returned)
        - has_more = 0 (no more data remaining in this range)
        """
        sql = """
            UPDATE future_chunk
            SET is_done = 1
            WHERE is_done = 0
            AND (count >= ? OR has_more = 0)
        """
        await self.db.execute(sql, (settings.CHUNK_SIZE,))
        await self.db.commit()
        logger.debug("Checked and finalized completed future chunks.")

    async def finalize_future_meta(self):
        """Mark expired futures as completed once all their chunks are done.

        An expired future with no pending chunks and at least one chunk
        recorded is considered complete.
        """
        sql = """
            UPDATE future_meta
            SET is_completed = 1
            WHERE is_completed = 0
            AND is_expired = 1
            AND instrument NOT IN (
                SELECT DISTINCT instrument
                FROM future_chunk
                WHERE is_done = 0
            )
            AND instrument IN (
                SELECT DISTINCT instrument FROM future_chunk
            )
        """
        await self.db.execute(sql)
        await self.db.commit()
        logger.debug("Checked and finalized completed future meta records.")


class OptionProgressRepo:
    """Repository for option fetch progress tracking."""

    def __init__(self, db: aiosqlite.Connection):
        """Initialize repository with a database connection."""
        self.db = db

    async def upsert_option_list(self, instruments: list[dict]):
        """Insert or update the option instrument list. Resets progress for new entries."""
        data = [(i["instrument_name"], int(not i["is_active"])) for i in instruments]

        sql = """
            INSERT INTO option_meta (instrument, last_no, is_expired, is_completed)
            VALUES (?, 0, ?, 0)
            ON CONFLICT(instrument) DO UPDATE SET
                is_expired = excluded.is_expired
        """
        await self.db.executemany(sql, data)
        await self.db.commit()
        logger.info(f"Option list upserted: {len(instruments)} items.")

    async def get_incomplete_option_list(self) -> list[dict]:
        """Get all options that haven't been fully fetched yet, including their progress offset."""
        async with self.db.execute(
            "SELECT instrument, last_no, is_expired FROM option_meta WHERE is_completed = 0"
        ) as cursor:
            rows = await cursor.fetchall()
            return [
                {
                    "instrument": row["instrument"],
                    "last_no": row["last_no"],
                    "is_expired": row["is_expired"],
                }
                for row in rows
            ]

    async def mark_options_complete(self, instruments: list[str]):
        """Mark options as fully fetched."""
        if not instruments:
            return
        await self.db.executemany(
            "UPDATE option_meta SET is_completed = 1 WHERE instrument = ?",
            [(i,) for i in instruments],
        )
        await self.db.commit()

    async def update_option_last_no(self, updates: list[tuple[int, str]]):
        """Batch update option progress.

        Uses MAX(last_no, ?) to ensure progress never rolls back — even if
        crash recovery causes a re-fetch of already-processed data, the
        checkpoint will not regress.

        updates: List of (new_last_no, instrument_name)
        """
        sql = """
            UPDATE option_meta
            SET last_no = MAX(last_no, ?)
            WHERE instrument = ?
        """
        try:
            await self.db.executemany(sql, updates)
            await self.db.commit()
        except Exception as e:
            logger.error(f"Failed to update option progress: {e}")
