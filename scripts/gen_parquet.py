#!/usr/bin/env python3
"""CLI for merging Deribit JSONL files into a single Parquet file."""

import argparse
import logging
import os
from pathlib import Path

from deribit_fetcher.config import set_base_dir, settings
from deribit_fetcher.log import setup_logging
from deribit_fetcher.parquet import (
    BATCH_SIZE,
    DEFAULT_BLOCK_BYTES,
    generate_parquet,
)

logger = logging.getLogger(__name__)


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
        default=BATCH_SIZE,
        help=f"Rows per streaming batch for large files (default: {BATCH_SIZE}).",
    )
    parser.add_argument(
        "--stream-workers",
        type=int,
        default=0,
        help="Parallel worker count for large-file block reading "
        "(0 = single-threaded streaming; default: 0).",
    )
    parser.add_argument(
        "--block-bytes",
        type=int,
        default=DEFAULT_BLOCK_BYTES,
        help=f"Block size in bytes for parallel large-file reading "
        f"(default: {DEFAULT_BLOCK_BYTES}).",
    )
    parser.add_argument(
        "--base-dir",
        type=str,
        default=None,
        help="Override the data directory (default: ./data/<CURRENCY>).",
    )

    args = parser.parse_args()
    setup_logging()
    set_base_dir(args.base_dir)

    data_type = args.type
    input_dir: Path
    if data_type == "future":
        input_dir = settings.data_future_dir
    else:
        input_dir = settings.data_option_dir

    output_file = settings.base_dir / f"{data_type}.parquet"

    logger.info(f"Starting Parquet generation for {data_type.upper()} data...")
    generate_parquet(
        data_dir=input_dir,
        output_file=output_file,
        dedup=not args.no_dedup,
        workers=args.workers,
        fast=args.fast,
        large_file_threshold=int(args.large_threshold_mb * 1024 * 1024),
        stream_batch_size=args.stream_batch_size,
        stream_workers=args.stream_workers,
        block_bytes=args.block_bytes,
    )
    logger.info("Done.")


if __name__ == "__main__":
    main()
