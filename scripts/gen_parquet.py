"""Merge Deribit JSONL files into a single Parquet file.

Uses parallel reading with intra-file dedup in workers and streaming
cross-file dedup in the main thread.

Performance characteristics:
- Small files (<100 MB, typical option data): thread-pool parallel, one file per worker.
- Large files (>=100 MB, typical perpetual futures): stream-read in batches in the
  main thread to avoid OOM and thread-pool starvation.
- Bottleneck (small files): CPU (zstd compression); use --fast for lz4.
- Bottleneck (large files): NDJSON line-by-line parsing; mitigated by streaming batches.
"""

import argparse
import concurrent.futures
import io
import logging
import mmap
import os
from collections.abc import Generator
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from deribit_fetcher.config import settings
from deribit_fetcher.log import setup_logging

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

_BATCH_SIZE = 200_000
_MAX_PENDING_TABLES = 50
_LARGE_FILE_THRESHOLD = 100 * 1024 * 1024  # 100 MB

# ---------------------------------------------------------------------------
# Worker: small files  (thread pool)
# ---------------------------------------------------------------------------


def _read_and_dedup_file(
    f: Path,
) -> tuple[str, pl.DataFrame | None, int, str | None, set[int] | None]:
    try:
        df = pl.read_ndjson(f, schema=COMPREHENSIVE_SCHEMA)
        if df.is_empty():
            return (f.name, None, 0, None, None)

        before = len(df)
        df = df.unique(subset=["instrument_name", "trade_seq"], keep="first")
        intra_dup = before - len(df)
        if intra_dup:
            logger.debug(f"{f.name}: removed {intra_dup} intra-file dups")

        instr_name = str(df["instrument_name"][0])
        seq_set = set(int(v) for v in df["trade_seq"].to_list())

        return (f.name, df, intra_dup, instr_name, seq_set)
    except Exception as e:
        logger.error(f"Error processing {f.name}: {e}")
        return (f.name, None, 0, None, None)


# ---------------------------------------------------------------------------
# Worker: large files  (streaming, main thread)
#
# Uses mmap for zero-copy batch splitting — finds batch boundaries via
# ``find(b'\n')`` and hands each chunk directly to ``pl.read_ndjson``
# without per-line Python decode/append overhead.
#
# trade_seq within a single file is monotonic, so cross-batch dedup simply
# filters "trade_seq <= max_seen" — no need for a set or is_in.
# ---------------------------------------------------------------------------


def _stream_batches(
    f: Path,
    batch_size: int = _BATCH_SIZE,
) -> Generator[tuple[str, pl.DataFrame, int, str, int], None, None]:
    """Stream-read a **large** JSONL file in fixed-size batches via mmap.

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
                # Use BytesIO — Polars reads directly without copying
                chunk = b"\n".join(lines_bytes)
                lines_bytes.clear()
                df = pl.read_ndjson(io.BytesIO(chunk), schema=COMPREHENSIVE_SCHEMA)
                if df.is_empty():
                    return None
                before = len(df)
                df = df.unique(subset=["instrument_name", "trade_seq"], keep="first")
                intra_dup = before - len(df)
                if intra_dup:
                    logger.debug(
                        f"{f.name}[batch {batch_id}]: removed {intra_dup} intra-batch dups"
                    )
                instr_name = str(df["instrument_name"][0])
                batch_id += 1
                return (df, intra_dup, instr_name)

            while pos < file_size:
                # Find the end of the current line
                nl = mm.find(b"\n", pos)
                if nl == -1:
                    # Last line (no trailing newline)
                    line = mm[pos:]
                    if line:
                        lines_bytes.append(line)
                    break
                line = mm[pos:nl]
                pos = nl + 1  # skip past '\n'

                if not line:
                    continue  # skip empty lines

                lines_bytes.append(line)

                if len(lines_bytes) >= batch_size:
                    result = _parse_bytes()
                    if result:
                        df, intra, instr = result
                        yield (f.name, df, intra, instr, pos)

            # tail
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
    large_file_threshold: int = _LARGE_FILE_THRESHOLD,
    stream_batch_size: int = _BATCH_SIZE,
) -> None:
    """Merge JSONL files in data_dir into a single Parquet file.

    Detects small and large files, processes them accordingly, deduplicates
    intra-file and cross-file, and writes the result to output_file.
    """
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        return

    all_files = list(data_dir.glob("*.jsonl"))
    if not all_files:
        logger.warning(f"No JSONL files found in {data_dir}")
        return

    large_files = [f for f in all_files if f.stat().st_size >= large_file_threshold]
    small_files = [f for f in all_files if f.stat().st_size < large_file_threshold]
    n_total = len(all_files)
    n_large = len(large_files)
    n_small = len(small_files)
    effective_workers = min(workers, n_small) if n_small > 0 else 0
    compression = "lz4" if fast else "zstd"

    logger.info(f"Found {n_total} JSONL files in {data_dir}")
    logger.info(f"  - {n_small} small files  -> thread pool ({effective_workers} workers)")
    logger.info(f"  - {n_large} large files -> stream batches ({stream_batch_size} rows)")
    logger.info(f"Merging into {output_file}... (compression={compression})")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        from tqdm import tqdm

        writer: pq.ParquetWriter | None = None
        total_rows = 0
        total_duplicates = 0

        # Dedup structures
        max_seqs: dict[str, int] = {}
        prev_keys: dict[str, set[int]] = {}

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
                writer = pq.ParquetWriter(output_file, table.schema, compression=compression)

        # ═══════════════════════════════════════════════════════════════
        # Phase 1 - large files (streaming, main thread)
        # ═══════════════════════════════════════════════════════════════
        if large_files:
            logger.info("Phase 1/2: streaming large files...")
            with tqdm(
                total=n_large,
                desc=f"Streaming {output_file.name}",
                unit="file",
            ) as pbar:
                for f in large_files:
                    file_size = f.stat().st_size
                    with tqdm(
                        total=file_size,
                        unit="B",
                        unit_scale=True,
                        desc=f.name,
                        leave=False,
                    ) as inner_pbar:
                        last_pos = 0
                        for fname, df, intra, instr, pos in _stream_batches(f, stream_batch_size):
                            total_duplicates += intra

                            if df.is_empty():
                                inner_pbar.update(pos - last_pos)
                                last_pos = pos
                                continue

                            if dedup and instr is not None:
                                max_seen = max_seqs.get(instr, -1)
                                if max_seen >= 0:
                                    before = len(df)
                                    df = df.filter(pl.col("trade_seq").cast(pl.Int64) > max_seen)
                                    cross_dup = before - len(df)
                                    if cross_dup:
                                        logger.debug(
                                            f"{fname}: removed {cross_dup} "
                                            f"cross-batch dups (max={max_seen})"
                                        )
                                    total_duplicates += cross_dup

                                batch_max = df.select(
                                    pl.col("trade_seq").cast(pl.Int64).max()
                                ).item()
                                if batch_max is not None and batch_max > max_seen:
                                    max_seqs[instr] = batch_max

                            total_rows += len(df)
                            if df.is_empty():
                                inner_pbar.update(pos - last_pos)
                                last_pos = pos
                                continue

                            table = df.to_arrow()
                            _init_writer(table)
                            pending.append(table)

                            if (
                                sum(len(t) for t in pending) >= _BATCH_SIZE
                                or len(pending) >= _MAX_PENDING_TABLES
                            ):
                                _flush_pending()

                            inner_pbar.update(pos - last_pos)
                            last_pos = pos
                    processed_count += 1
                    pbar.update(1)

        # ═══════════════════════════════════════════════════════════════
        # Phase 2 - small files (thread pool)
        # ═══════════════════════════════════════════════════════════════
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

                if dedup and instr_name is not None and seq_set is not None:
                    seen_seqs = prev_keys.get(instr_name)
                    if seen_seqs:
                        before = len(df)
                        df = df.filter(~pl.col("trade_seq").cast(pl.Int64).is_in(seen_seqs))
                        cross_dup = before - len(df)
                        if cross_dup:
                            logger.debug(f"{fname}: removed {cross_dup} cross-file dups")
                        total_duplicates += cross_dup

                    if instr_name in prev_keys:
                        prev_keys[instr_name].update(seq_set)
                    else:
                        prev_keys[instr_name] = seq_set

                total_rows += len(df)
                if df.is_empty():
                    return

                table = df.to_arrow()
                _init_writer(table)
                pending.append(table)

                if (
                    sum(len(t) for t in pending) >= _BATCH_SIZE
                    or len(pending) >= _MAX_PENDING_TABLES
                ):
                    _flush_pending()

            with concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers) as executor:
                futures = {executor.submit(_read_and_dedup_file, f): f for f in small_files}

                with tqdm(
                    total=n_small,
                    desc=f"Writing {output_file.name}",
                    unit="file",
                ) as pbar:
                    for future in concurrent.futures.as_completed(futures):
                        result = future.result()
                        _consume_small(*result)
                        processed_count += 1
                        pbar.update(1)

        if writer is not None:
            _flush_pending()
            writer.close()

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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point: parse args and run gen_parquet."""
    parser = argparse.ArgumentParser(
        description="Merge Deribit JSONL files into a single Parquet file."
    )
    parser.add_argument(
        "--type",
        type=str,
        choices=["future", "option"],
        required=True,
        help="Type of data to merge (future or option).",
    )
    parser.add_argument(
        "--no-dedup",
        action="store_true",
        help="Skip deduplication (faster, but may contain duplicate rows).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=os.cpu_count() or 4,
        help=f"Thread-pool workers for small files (default: {os.cpu_count() or 4}).",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Trade ~10-15%% larger Parquet for ~20%% lower CPU (lz4 instead of zstd).",
    )
    parser.add_argument(
        "--large-threshold-mb",
        type=float,
        default=100.0,
        help="Files larger than this (MB) are streamed to avoid OOM (default: 100).",
    )
    parser.add_argument(
        "--stream-batch-size",
        type=int,
        default=_BATCH_SIZE,
        help=f"Rows per streaming batch for large files (default: {_BATCH_SIZE}).",
    )

    args = parser.parse_args()
    setup_logging()

    data_type = args.type
    input_dir: Path
    if data_type == "future":
        input_dir = settings.DATA_FUTURE_DIR
    else:
        input_dir = settings.DATA_OPTION_DIR

    output_file = settings.BASE_DIR / f"{data_type}.parquet"

    logger.info(f"Starting Parquet generation for {data_type.upper()} data...")
    generate_parquet(
        data_dir=input_dir,
        output_file=output_file,
        dedup=not args.no_dedup,
        workers=args.workers,
        fast=args.fast,
        large_file_threshold=int(args.large_threshold_mb * 1024 * 1024),
        stream_batch_size=args.stream_batch_size,
    )
    logger.info("Done.")


if __name__ == "__main__":
    main()
