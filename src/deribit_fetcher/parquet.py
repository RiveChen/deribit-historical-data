"""Parquet generation and validation.

Merges Deribit JSONL files into a single Parquet file, and validates
the output by checking trade_seq continuity for each instrument.
"""

import concurrent.futures
import io
import logging
import mmap
import multiprocessing as mp
import os
import tempfile
from collections.abc import Generator
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

COMPREHENSIVE_SCHEMA = pl.Schema(
    {
        "trade_seq": pl.Int64,
        "trade_id": pl.String,
        "timestamp": pl.Int64,
        "tick_direction": pl.Int64,
        "price": pl.Float64,
        "mark_price": pl.Float64,
        "iv": pl.Float64,
        "instrument_name": pl.String,
        "index_price": pl.Float64,
        "direction": pl.String,
        "amount": pl.Float64,
        "contracts": pl.Float64,
        "block_trade_id": pl.String,
        "block_rfq_id": pl.String,
        "block_trade_leg_count": pl.Int64,
        "combo_id": pl.String,
        "combo_trade_id": pl.String,
        "liquidation": pl.String,
    }
)

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

BATCH_SIZE = 200_000
MAX_PENDING_TABLES = 50
LARGE_FILE_THRESHOLD = 100 * 1024 * 1024  # 100 MB
DEFAULT_BLOCK_BYTES = 100 * 1024 * 1024  # 100 MB per block for parallel reading
DEFAULT_STREAM_WORKERS = 4


# ---------------------------------------------------------------------------
# Pure dedup functions
# ---------------------------------------------------------------------------


def dedup_intra(df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """Remove intra-file (or intra-batch) duplicate rows.

    Deduplicates on (instrument_name, trade_seq) keeping the first occurrence.
    Returns (deduplicated_df, removed_count).
    """
    before = len(df)
    df = df.unique(subset=["instrument_name", "trade_seq"], keep="first")
    return df, before - len(df)


def dedup_cross_batch(df: pl.DataFrame, max_seen: int) -> tuple[pl.DataFrame, int]:
    """Remove rows with trade_seq <= max_seen from a proven ascending stream.

    This helper is retained for callers that have independently established an
    ascending-order invariant.  Raw Deribit JSONL does *not* provide that
    invariant and the production merge path must use ``dedup_exact`` instead.
    Returns (filtered_df, removed_count).
    """
    before = len(df)
    df = df.filter(pl.col("trade_seq").cast(pl.Int64) > max_seen)
    return df, before - len(df)


class SeenTradeSeqs:
    """Compact exact membership tracker for dense, non-negative trade sequences.

    A Python ``set[int]`` stores a full Python object per sequence and becomes
    prohibitively expensive for large instruments.  Deribit ``trade_seq``
    values are dense non-negative integers, so one bit per observed sequence is
    both exact and substantially smaller.  Unlike a single high-water mark, the
    bitmap is correct for descending and arbitrarily ordered input.
    """

    def __init__(self) -> None:
        """Initialize an empty sequence bitmap."""
        self._bits = bytearray()

    def add(self, trade_seq: int) -> bool:
        """Record ``trade_seq`` and return True only when it was not seen before."""
        if trade_seq < 0:
            raise ValueError(f"trade_seq must be non-negative, got {trade_seq}")

        byte_index, bit_index = divmod(trade_seq, 8)
        if byte_index >= len(self._bits):
            self._bits.extend(b"\x00" * (byte_index + 1 - len(self._bits)))

        mask = 1 << bit_index
        if self._bits[byte_index] & mask:
            return False

        self._bits[byte_index] |= mask
        return True


def dedup_exact(
    df: pl.DataFrame,
    seen_by_instrument: dict[str, SeenTradeSeqs],
) -> tuple[pl.DataFrame, int]:
    """Exactly deduplicate rows regardless of their input order.

    Membership is tracked by ``(instrument_name, trade_seq)``.  The function is
    safe for ascending, descending, shuffled, and mixed-instrument batches.
    """
    if df.is_empty():
        return df, 0

    keep: list[bool] = []
    for instrument, trade_seq in zip(
        df["instrument_name"].to_list(),
        df["trade_seq"].cast(pl.Int64).to_list(),
        strict=True,
    ):
        if instrument is None or trade_seq is None:
            raise ValueError("instrument_name and trade_seq must not be null")
        instrument_key = str(instrument)
        tracker = seen_by_instrument.get(instrument_key)
        if tracker is None:
            tracker = SeenTradeSeqs()
            seen_by_instrument[instrument_key] = tracker
        keep.append(tracker.add(int(trade_seq)))

    removed = len(df) - sum(keep)
    if removed == 0:
        return df, 0
    return df.filter(pl.Series("keep", keep)), removed


def dedup_cross_file(df: pl.DataFrame, seen_keys: set[int] | None) -> tuple[pl.DataFrame, int]:
    """Remove rows whose trade_seq has already been seen in another file.

    Used for cross-file dedup among small files. Returns (filtered_df, removed_count).
    """
    if not seen_keys:
        return df, 0
    before = len(df)
    df = df.filter(~pl.col("trade_seq").cast(pl.Int64).is_in(seen_keys))
    return df, before - len(df)


# ---------------------------------------------------------------------------
# Worker: small files  (thread pool)
# ---------------------------------------------------------------------------


def read_and_dedup_file(
    f: Path,
) -> tuple[str, pl.DataFrame | None, int, str | None, set[int] | None]:
    """Read a single JSONL file, deduplicate intra-file, return metadata."""
    try:
        df = pl.read_ndjson(f, schema=COMPREHENSIVE_SCHEMA)
        if df.is_empty():
            return (f.name, None, 0, None, None)

        df, intra_dup = dedup_intra(df)
        if intra_dup:
            logger.debug(f"{f.name}: removed {intra_dup} intra-file dups")

        instr_name = str(df["instrument_name"][0])
        seq_set = set(int(v) for v in df["trade_seq"].to_list())

        return (f.name, df, intra_dup, instr_name, seq_set)
    except Exception as e:
        raise RuntimeError(f"Failed to process JSONL file {f}") from e


# ---------------------------------------------------------------------------
# Worker: large files — parallel reading with ProcessPoolExecutor
#
# A large file is split into \n-aligned byte blocks; each block is processed
# in a separate subprocess (true parallelism, not GIL-bound).  Results are
# sorted by block offset to restore file order before cross-block dedup.
# ---------------------------------------------------------------------------


def _split_blocks(file_path: Path, block_bytes: int) -> list[tuple[int, int]]:
    r"""Split a file into \n-aligned byte ranges.

    Each block starts at a \n boundary (first complete line) so that no worker
    receives a partial line.  Returns list of (start_offset, end_offset).
    """
    size = file_path.stat().st_size
    if size == 0:
        return []

    blocks: list[tuple[int, int]] = []
    start = 0
    while start < size:
        end = min(start + block_bytes, size)
        if end < size:
            # Rewind to next \n so the block starts at a line boundary.
            # We search forward from end (not backward) to find the break point.
            with open(file_path, "rb") as f:
                f.seek(end)
                # Read up to 1 MB looking for \n
                chunk = f.read(min(1024 * 1024, size - end))
                nl = chunk.find(b"\n")
                if nl != -1:
                    end = end + nl + 1
                else:
                    # No newline found in lookahead — this IS the last block
                    end = size
        blocks.append((start, end))
        start = end
    return blocks


def _process_block(args: tuple) -> tuple[int, pl.DataFrame | None, int, str | None]:
    """Worker function for ProcessPoolExecutor: read one byte block and dedup.

    This is a module-level function (required for pickling across processes).
    """
    file_path, start, end = args
    try:
        with open(file_path, "rb") as f:
            f.seek(start)
            chunk = f.read(end - start)

        # _split_blocks guarantees block boundaries are \n-aligned, so every
        # chunk starts at a complete line.  No need to strip a partial line.
        if not chunk.strip():
            return (start, None, 0, None)

        df = pl.read_ndjson(io.BytesIO(chunk), schema=COMPREHENSIVE_SCHEMA)
        if df.is_empty():
            return (start, None, 0, None)

        df, intra_dup = dedup_intra(df)
        instr_name = str(df["instrument_name"][0])
        return (start, df, intra_dup, instr_name)

    except Exception as e:
        raise RuntimeError(f"Failed to process {file_path} block [{start}:{end})") from e


def parallel_read_large_file(
    file_path: Path,
    block_bytes: int = DEFAULT_BLOCK_BYTES,
    workers: int = DEFAULT_STREAM_WORKERS,
) -> Generator[tuple[str, pl.DataFrame, int, str | None, int], None, None]:
    r"""Read a large JSONL file in parallel using a process pool.

    Splits the file into \n-aligned blocks, processes each in a subprocess,
    then yields results in file-order (by block offset) with cross-block
    dedup applied.

    Yields same format as stream_batches: (filename, df, intra_dup, instr_name, pos).
    """
    blocks = _split_blocks(file_path, block_bytes)
    if not blocks:
        return

    fname = file_path.name

    # Process blocks — use spawn context for Linux/macOS safety
    ctx = mp.get_context("spawn")
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as pool:
        args = [(file_path, start, end) for start, end in blocks]
        results = list(pool.map(_process_block, args))

    # Sort by block start offset to restore file order
    results.sort(key=lambda r: r[0])

    # Cross-block dedup must be exact: API responses are descending within a
    # chunk and concurrently-written future chunks may appear in any order.
    seen_by_instrument: dict[str, SeenTradeSeqs] = {}

    for start_offset, df, intra_dup, instr in results:
        if df is None or df.is_empty():
            continue

        df, cross_dup = dedup_exact(df, seen_by_instrument)
        if cross_dup:
            logger.debug(
                f"{fname}[block offset={start_offset}]: removed {cross_dup} cross-block dups"
            )
        if df.is_empty():
            continue

        yield (fname, df, intra_dup, instr, start_offset)


# ---------------------------------------------------------------------------
# Worker: large files  (streaming, main thread) — unchanged single-thread fallback
# ---------------------------------------------------------------------------


def stream_batches(
    f: Path,
    batch_size: int = BATCH_SIZE,
) -> Generator[tuple[str, pl.DataFrame, int, str, int], None, None]:
    """Stream-read a large JSONL file in fixed-size batches via mmap.

    Yields (filename, df, intra_dup_count, instrument_name, pos).
    *intra-batch dedup* is done here via ``unique()``.
    *cross-batch dedup* is the caller's responsibility via ``max_trade_seq``.
    """
    fd = os.open(f, os.O_RDONLY)
    try:
        with mmap.mmap(fd, 0, access=mmap.ACCESS_READ) as mm:
            file_size = len(mm)
            batch_id = 0
            pos = 0  # current byte offset in mmap
            lines_bytes: list[bytes] = []

            def _parse_bytes() -> tuple[pl.DataFrame, int, str] | None:
                nonlocal lines_bytes, batch_id
                if not lines_bytes:
                    return None
                chunk = b"\n".join(lines_bytes)
                lines_bytes.clear()
                df = pl.read_ndjson(io.BytesIO(chunk), schema=COMPREHENSIVE_SCHEMA)
                if df.is_empty():
                    return None
                df, intra_dup = dedup_intra(df)
                if intra_dup:
                    logger.debug(
                        f"{f.name}[batch {batch_id}]: removed {intra_dup} intra-batch dups"
                    )
                instr_name = str(df["instrument_name"][0])
                batch_id += 1
                return (df, intra_dup, instr_name)

            while pos < file_size:
                nl = mm.find(b"\n", pos)
                if nl == -1:
                    line = mm[pos:]
                    if line:
                        lines_bytes.append(line)
                    break
                line = mm[pos:nl]
                pos = nl + 1
                if not line:
                    continue
                lines_bytes.append(line)

                if len(lines_bytes) >= batch_size:
                    result = _parse_bytes()
                    if result:
                        df, intra, instr = result
                        yield (f.name, df, intra, instr, pos)

            result = _parse_bytes()
            if result:
                df, intra, instr = result
                yield (f.name, df, intra, instr, pos)
    finally:
        os.close(fd)


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


def generate_parquet(
    data_dir: Path,
    output_file: Path,
    dedup: bool = True,
    workers: int = 4,
    fast: bool = False,
    large_file_threshold: int = LARGE_FILE_THRESHOLD,
    stream_batch_size: int = BATCH_SIZE,
    stream_workers: int = 0,
    block_bytes: int = DEFAULT_BLOCK_BYTES,
) -> None:
    """Merge JSONL files in data_dir into a single Parquet file.

    Detects small and large files, processes them accordingly, deduplicates
    intra-file and cross-file, and writes the result to output_file.

    When stream_workers > 0, large files are read in parallel using a process
    pool (true parallelism, not GIL-bound).  Otherwise the single-threaded
    mmap-based streaming path is used.
    """
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
    if not data_dir.is_dir():
        raise NotADirectoryError(f"Data path is not a directory: {data_dir}")

    all_files = sorted(data_dir.glob("*.jsonl"))
    if not all_files:
        raise ValueError(f"No JSONL files found in {data_dir}")

    large_files = [f for f in all_files if f.stat().st_size >= large_file_threshold]
    small_files = [f for f in all_files if f.stat().st_size < large_file_threshold]
    n_total = len(all_files)
    n_large = len(large_files)
    n_small = len(small_files)
    effective_workers = min(workers, n_small) if n_small > 0 else 0
    compression = "lz4" if fast else "zstd"

    logger.info(f"Found {n_total} JSONL files in {data_dir}")
    if effective_workers:
        logger.info(f"  - {n_small} small files  -> thread pool ({effective_workers} workers)")
    if stream_workers:
        eff_workers = stream_workers
        logger.info(f"  - {n_large} large files -> parallel process pool ({eff_workers} workers)")
    else:
        logger.info(f"  - {n_large} large files -> stream batches ({stream_batch_size} rows)")
    logger.info(f"Merging into {output_file}... (compression={compression})")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    temp_fd, temp_name = tempfile.mkstemp(
        dir=output_file.parent,
        prefix=f".{output_file.name}.",
        suffix=".tmp",
    )
    os.close(temp_fd)
    temp_output = Path(temp_name)
    writer: pq.ParquetWriter | None = None

    try:
        from tqdm import tqdm

        total_rows = 0
        total_duplicates = 0

        seen_by_instrument: dict[str, SeenTradeSeqs] = {}

        pending: list[pa.Table] = []
        processed_count = 0

        def _flush_pending() -> None:
            nonlocal pending, writer
            if not pending:
                return
            combined = pa.concat_tables(pending)
            assert writer is not None
            writer.write_table(combined)
            pending.clear()

        def _init_writer(table: pa.Table) -> None:
            nonlocal writer
            if writer is None:
                writer = pq.ParquetWriter(temp_output, table.schema, compression=compression)

        # Phase 1 - large files (parallel or streaming)
        if large_files:
            logger.info("Phase 1/2: processing large files...")
            with tqdm(total=n_large, desc=f"Processing {output_file.name}", unit="file") as pbar:
                for f in large_files:
                    if stream_workers > 0:
                        iterable = parallel_read_large_file(f, block_bytes, stream_workers)
                    else:
                        iterable = stream_batches(f, stream_batch_size)

                    file_size = f.stat().st_size
                    with tqdm(
                        total=file_size, unit="B", unit_scale=True, desc=f.name, leave=False
                    ) as inner_pbar:
                        last_pos = 0
                        for fname, df, intra, _instr, pos in iterable:
                            total_duplicates += intra
                            if df.is_empty():
                                inner_pbar.update(pos - last_pos)
                                last_pos = pos
                                continue

                            if dedup:
                                df, cross_dup = dedup_exact(df, seen_by_instrument)
                                if cross_dup:
                                    logger.debug(
                                        f"{fname}: removed {cross_dup} exact cross-batch dups"
                                    )
                                total_duplicates += cross_dup

                            total_rows += len(df)
                            if df.is_empty():
                                inner_pbar.update(pos - last_pos)
                                last_pos = pos
                                continue

                            table = df.to_arrow()
                            _init_writer(table)
                            pending.append(table)

                            if (
                                sum(len(t) for t in pending) >= BATCH_SIZE
                                or len(pending) >= MAX_PENDING_TABLES
                            ):
                                _flush_pending()

                            inner_pbar.update(pos - last_pos)
                            last_pos = pos
                    processed_count += 1
                    pbar.update(1)

        # Phase 2 - small files (thread pool)
        if small_files:
            logger.info("Phase 2/2: processing small files in parallel...")

            def _consume_small(
                fname: str,
                df: pl.DataFrame | None,
                intra_dup: int,
                instr_name: str | None,
                seq_set: set[int] | None,
            ) -> None:
                nonlocal total_rows, total_duplicates
                total_duplicates += intra_dup
                if df is None or df.is_empty():
                    return

                if dedup:
                    df, cross_dup = dedup_exact(df, seen_by_instrument)
                    if cross_dup:
                        logger.debug(f"{fname}: removed {cross_dup} cross-file dups")
                    total_duplicates += cross_dup

                total_rows += len(df)
                if df.is_empty():
                    return
                table = df.to_arrow()
                _init_writer(table)
                pending.append(table)

                if sum(len(t) for t in pending) >= BATCH_SIZE or len(pending) >= MAX_PENDING_TABLES:
                    _flush_pending()

            with concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers) as executor:
                futures = {executor.submit(read_and_dedup_file, f): f for f in small_files}
                with tqdm(total=n_small, desc=f"Writing {output_file.name}", unit="file") as pbar:
                    for future in concurrent.futures.as_completed(futures):
                        result = future.result()
                        _consume_small(*result)
                        processed_count += 1
                        pbar.update(1)

        if writer is None:
            raise ValueError(f"No trade rows found in {data_dir}")

        _flush_pending()
        writer.close()
        writer = None
        os.replace(temp_output, output_file)

        summary = f"Successfully loaded {total_rows} rows from {processed_count} files"
        if total_duplicates > 0:
            summary += f", removed {total_duplicates} duplicates"
        logger.info(summary)
        logger.info(
            f"Successfully wrote Parquet file: {output_file} "
            f"(Size: {output_file.stat().st_size / (1024 * 1024):.2f} MB)"
        )

    except Exception:
        logger.exception("Failed to generate parquet")
        raise
    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                logger.exception("Failed to close partial Parquet writer")
        temp_output.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Data validation
# ---------------------------------------------------------------------------

N_BUCKETS = 10


def _show_gap_histogram(lf: pl.LazyFrame, gapped: list) -> None:
    """For each instrument with gaps, show a per-bucket histogram.

    Each instrument's seq range is divided into N_BUCKETS equal intervals.
    """
    print(f"\n{'─' * 80}")
    print(f"Gap Distribution ({N_BUCKETS} equal-sized buckets per instrument)")
    print(f"{'─' * 80}")

    for instr_name, count, seq_min, seq_max in gapped:
        expected_total = seq_max - seq_min + 1
        gap_total = expected_total - count
        b_expected_base = expected_total // N_BUCKETS
        remainder = expected_total % N_BUCKETS

        bucket_counts = (
            lf.filter(pl.col("instrument_name") == instr_name)
            .with_columns(
                (pl.col("trade_seq") - seq_min)
                .cast(pl.Int64)
                .mul(N_BUCKETS)
                .truediv(expected_total)
                .floor()
                .cast(pl.UInt32)
                .clip(0, N_BUCKETS - 1)
                .alias("bucket")
            )
            .group_by("bucket")
            .len()
            .collect(engine="streaming")
        )

        counts_map = dict(bucket_counts.iter_rows())

        print(f"\n{instr_name} — {gap_total:,} gaps (expected {expected_total:,}, got {count:,})")
        print(f"  {'Bucket':>8s}  {'Rows':>10s}  {'Expected':>10s}  {'Deficit':>10s}")
        print(f"  {'─' * 8}  {'─' * 10}  {'─' * 10}  {'─' * 10}")

        for b in range(N_BUCKETS):
            b_rows = counts_map.get(b, 0)
            b_expected = b_expected_base + (1 if b < remainder else 0)
            deficit = b_expected - b_rows
            marker = " ⚠️" if deficit > 0 else "   "
            print(f"  {b + 1:>8d}  {b_rows:>10,}  {b_expected:>10,}  {deficit:>+10,}{marker}")


def validate_parquet(parquet_path: Path) -> None:
    """Validate a generated parquet file, checking each instrument for gaps."""
    if not parquet_path.exists():
        logger.warning(f"Parquet file not found: {parquet_path}")
        return

    print(f"\n{'=' * 80}")
    print(f"Validating Parquet: {parquet_path}")
    print(f"{'=' * 80}")

    lf = pl.scan_parquet(parquet_path)

    schema = lf.collect_schema()
    print(f"Columns: {len(schema)}")
    print("Schema:")
    for col, dtype in schema.items():
        print(f"  {col:25s}  {str(dtype):12s}")

    instr_stats = (
        lf.group_by("instrument_name")
        .agg(
            [
                pl.len().alias("count"),
                pl.min("trade_seq").alias("seq_min"),
                pl.max("trade_seq").alias("seq_max"),
                pl.min("timestamp").alias("ts_min"),
                pl.max("timestamp").alias("ts_max"),
            ]
        )
        .collect(engine="streaming")
    )

    print(f"\n{'Instrument':35s} {'Rows':>10s} {'Seq Range':>24s} {'Status':20s}")
    print("-" * 93)

    total_rows = 0
    ok_count = 0
    gap_count = 0
    gapped_instruments = []

    ts_min_global = instr_stats[0, "ts_min"]
    ts_max_global = instr_stats[0, "ts_max"]
    instr_stats = instr_stats.sort("instrument_name")

    for row in instr_stats.iter_rows():
        instr, count, seq_min, seq_max, ts_min, ts_max = row
        total_rows += count
        expected = seq_max - seq_min + 1

        if ts_min < ts_min_global:
            ts_min_global = ts_min
        if ts_max > ts_max_global:
            ts_max_global = ts_max

        if count < expected:
            gap = expected - count
            status = f"⚠️  {gap} gaps"
            gap_count += 1
            gapped_instruments.append((instr, count, seq_min, seq_max))
        else:
            status = "✅"
            ok_count += 1

        print(f"{instr:35s} {count:>10,} {seq_min:>12,}..{seq_max:<12,} {status:20s}")

    if gapped_instruments:
        _show_gap_histogram(lf, gapped_instruments)

    size_mb = parquet_path.stat().st_size / (1024 * 1024)
    t_min = datetime.fromtimestamp(int(ts_min_global) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    t_max = datetime.fromtimestamp(int(ts_max_global) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

    print(f"\n{'=' * 80}")
    print(f"Total rows: {total_rows:,}")
    print(f"Files with gaps: {gap_count}    Files OK: {ok_count}")
    print(f"Time range: {t_min} ~ {t_max}")
    print(f"File size: {size_mb:.2f} MB")
    print(f"{'=' * 80}")
