"""
Merge Deribit JSONL files into a single Parquet file with parallel reading,
intra-file dedup in workers, and streaming cross-file dedup in the main thread.

Performance characteristics (measured on WSL / CPU-bound workloads):
- Bottleneck is CPU (zstd compression + Python set construction from to_list()).
- Disk I/O rarely stalls (wai < 2% in typical runs).
- Use --fast to trade ~15% file size for ~20% lower CPU (lz4 vs zstd).
"""
import argparse
import logging
import os
import concurrent.futures
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from deribit_fetcher.config import settings
from deribit_fetcher.log import setup_logging

logger = logging.getLogger(__name__)

# Comprehensive schema of a Deribit Trade.
# This ensures rare fields are not dropped during schema inference
# and standardizes the output Parquet schema for all instrument types.
# Fields marked with ¹ appear only on specific trade types:
#   combo/block-trades → combo_id, combo_trade_id, block_trade_id, block_rfq_id, block_trade_leg_count
#   perpetual futures  → liquidation
#   options only       → iv, contracts
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

# Accumulate this many Arrow rows before flushing a Row Group.
# Tuned to balance writer overhead vs memory: 200k rows × ~200 bytes ≈ 40 MB per batch.
_BATCH_SIZE = 200_000
# Hard cap on pending tables to avoid OOM on giant single files.
_MAX_PENDING_TABLES = 50


def _read_and_dedup_file(
    f: Path,
) -> tuple[str, pl.DataFrame | None, int, str | None, set[int] | None]:
    """
    Read a single JSONL file, apply intra-file dedup, and extract dedup keys.

    Runs in a thread pool worker to offload CPU work from the main thread:
      - NDJSON parsing (Polars, Rust, no GIL)
      - intra-file unique()
      - key set construction via to_list() (pure Python, worker thread absorbs GIL cost)

    Returns (filename, df_or_None, intra_dup_count, instrument_name_or_None, seq_set_or_None).
    Returns df=None if the file is empty or errored.
    """
    try:
        df = pl.read_ndjson(f, schema=COMPREHENSIVE_SCHEMA)
        if df.is_empty():
            return (f.name, None, 0, None, None)

        before = len(df)
        df = df.unique(subset=["instrument_name", "trade_seq"], keep="first")
        intra_dup = before - len(df)
        if intra_dup:
            logger.debug(f"{f.name}: removed {intra_dup} intra-file dups")

        # Extract keys in the worker thread to avoid Python loop in the main thread
        instr_name = str(df["instrument_name"][0])
        seq_set = set(int(v) for v in df["trade_seq"].to_list())

        return (f.name, df, intra_dup, instr_name, seq_set)
    except Exception as e:
        logger.error(f"Error processing {f.name}: {e}")
        return (f.name, None, 0, None, None)


def generate_parquet(
    data_dir: Path,
    output_file: Path,
    dedup: bool = True,
    workers: int = 4,
    fast: bool = False,
) -> None:
    """
    Scan a directory for JSONL files and merge them into a single Parquet file.

    Uses a two-phase pipeline:
      Phase 1 (ThreadPool, N workers): parallel NDJSON parsing + intra-file dedup + key extraction.
      Phase 2 (main thread, sequential): cross-file dedup + batch write.

    When dedup=True, removes duplicate rows by (instrument_name, trade_seq)
    both within each JSONL file and across files (handles chunk boundary overlap).

    Performance note: cross-file dedup uses per-instrument key tracking
    (dict[str, set[int]]) instead of a flat global set. Since each JSONL file
    contains trades for exactly one instrument, this avoids O(n) scan over
    millions of unrelated keys on every file.
    """
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        return

    jsonl_files = list(data_dir.glob("*.jsonl"))
    if not jsonl_files:
        logger.warning(f"No JSONL files found in {data_dir}")
        return

    n_files = len(jsonl_files)
    effective_workers = min(workers, n_files)
    compression = "lz4" if fast else "zstd"
    logger.info(f"Found {n_files} JSONL files in {data_dir}")
    logger.info(
        f"Merging into {output_file}... "
        f"(workers={effective_workers}, compression={compression})"
    )

    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        from tqdm import tqdm

        writer = None
        total_rows = 0
        total_duplicates = 0
        # Per-instrument key tracking for cross-file dedup.
        prev_keys: dict[str, set[int]] = {}
        # Pending Arrow tables for batched write
        pending: list[pa.Table] = []

        def _flush_pending():
            """Write accumulated pending tables as a single Row Group."""
            nonlocal pending
            if not pending:
                return
            combined = pa.concat_tables(pending)
            writer.write_table(combined)  # type: ignore[union-attr]
            pending.clear()

        # Phase 1: parallel read + intra-file dedup + key extraction
        # Phase 2 (main thread): sequential cross-file dedup + batch write
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=effective_workers
        ) as executor:
            futures = {
                executor.submit(_read_and_dedup_file, f): f for f in jsonl_files
            }

            with tqdm(
                total=n_files, desc=f"Writing {output_file.name}", unit="file"
            ) as pbar:
                for future in concurrent.futures.as_completed(futures):
                    fname, df, intra_dup, instr_name, seq_set = future.result()
                    total_duplicates += intra_dup

                    if df is None or df.is_empty():
                        pbar.update(1)
                        continue

                    if dedup and instr_name is not None and seq_set is not None:
                        # Cross-file dedup: only check against this instrument's seen keys
                        seen_seqs = prev_keys.get(instr_name)

                        if seen_seqs:
                            before = len(df)
                            df = df.filter(
                                ~pl.col("trade_seq")
                                .cast(pl.Int64)
                                .is_in(seen_seqs)
                            )
                            cross_dup = before - len(df)
                            if cross_dup:
                                logger.debug(
                                    f"{fname}: removed {cross_dup} cross-file dups"
                                )
                            total_duplicates += cross_dup

                        # Record keys (already unioned inside the worker)
                        if instr_name in prev_keys:
                            prev_keys[instr_name].update(seq_set)
                        else:
                            prev_keys[instr_name] = seq_set

                    total_rows += len(df)
                    if df.is_empty():
                        pbar.update(1)
                        continue

                    table = df.to_arrow()

                    # Initialize PyArrow writer with the schema from the first valid table
                    if writer is None:
                        writer = pq.ParquetWriter(
                            output_file,
                            table.schema,
                            compression=compression,
                        )

                    pending.append(table)

                    # Flush when accumulated rows or table count exceeds thresholds
                    if (
                        sum(len(t) for t in pending) >= _BATCH_SIZE
                        or len(pending) >= _MAX_PENDING_TABLES
                    ):
                        _flush_pending()

                    pbar.update(1)

        # Flush remaining tables
        if writer is not None:
            _flush_pending()
            writer.close()

        summary = f"Successfully loaded {total_rows} rows"
        if total_duplicates > 0:
            summary += f", removed {total_duplicates} duplicates"
        logger.info(summary)
        logger.info(
            f"Successfully wrote Parquet file: {output_file} "
            f"(Size: {output_file.stat().st_size / (1024 * 1024):.2f} MB)"
        )

    except Exception as e:
        logger.exception(f"Failed to generate parquet: {e}")


def main() -> None:
    """CLI entry point for JSONL to Parquet conversion."""
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
        help=f"Number of parallel read workers (default: {os.cpu_count() or 4}, capped by file count).",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Trade ~10-15%% larger Parquet for ~20%% lower CPU (lz4 instead of zstd)."
        "  Recommended when CPU is the bottleneck.",
    )

    args = parser.parse_args()

    setup_logging()

    data_type = args.type

    if data_type == "future":
        input_dir = settings.DATA_FUTURE_DIR
    else:
        input_dir = settings.DATA_OPTION_DIR

    output_file = settings.BASE_DIR / f"{data_type}.parquet"

    logger.info(f"Starting Parquet generation for {data_type.upper()} data...")
    generate_parquet(
        input_dir,
        output_file,
        dedup=not args.no_dedup,
        workers=args.workers,
        fast=args.fast,
    )
    logger.info("Done.")


if __name__ == "__main__":
    main()
