"""
Post-download data integrity validation.

Validates generated Parquet files by checking trade_seq continuity for each
instrument individually. Uses streaming-only execution: group_by on ~357
instruments with count/min/max accumulators, all in a single streaming pass.
"""

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from deribit_fetcher.config import settings
from deribit_fetcher.log import setup_logging

logger = logging.getLogger(__name__)


def validate_parquet(parquet_path: Path) -> None:
    """Validate a generated parquet file, checking each instrument for gaps."""
    if not parquet_path.exists():
        logger.warning(f"Parquet file not found: {parquet_path}")
        return

    print(f"\n{'=' * 80}")
    print(f"Validating Parquet: {parquet_path}")
    print(f"{'=' * 80}")

    lf = pl.scan_parquet(parquet_path)

    # Schema info (metadata only — no data scan)
    schema = lf.collect_schema()
    print(f"Columns: {len(schema)}")
    print(f"Schema:")
    for col, dtype in schema.items():
        print(f"  {col:25s}  {str(dtype):12s}")

    # Single streaming pass: group_by on ~357 keys is safe (tiny hash table),
    # streaming=True forces Polars to use the streaming engine so only a few
    # row groups are held in memory at a time.
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
        .collect(streaming=True)
    )

    print(f"\n{'Instrument':35s} {'Rows':>10s} {'Seq Range':>24s} {'Status':20s}")
    print("-" * 93)

    total_rows = 0
    ok_count = 0
    gap_count = 0
    ts_min_global = instr_stats[0, "ts_min"]
    ts_max_global = instr_stats[0, "ts_max"]

    # Sort by instrument name for deterministic output
    instr_stats = instr_stats.sort("instrument_name")

    for row in instr_stats.iter_rows():
        instr, count, seq_min, seq_max, ts_min, ts_max = row
        total_rows += count
        expected = seq_max - seq_min + 1

        # Track global time range across instruments
        if ts_min < ts_min_global:
            ts_min_global = ts_min
        if ts_max > ts_max_global:
            ts_max_global = ts_max

        if count < expected:
            gap = expected - count
            status = f"⚠️  {gap} gaps"
            gap_count += 1
        else:
            status = "✅"
            ok_count += 1

        print(
            f"{instr:35s} {count:>10,} {seq_min:>12,}..{seq_max:<12,} {status:20s}"
        )

    # File size
    size_mb = parquet_path.stat().st_size / (1024 * 1024)

    t_min = datetime.fromtimestamp(
        int(ts_min_global) / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%d")
    t_max = datetime.fromtimestamp(
        int(ts_max_global) / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%d")

    print(f"\n{'=' * 80}")
    print(f"Total rows: {total_rows:,}")
    print(f"Files with gaps: {gap_count}    Files OK: {ok_count}")
    print(f"Time range: {t_min} ~ {t_max}")
    print(f"File size: {size_mb:.2f} MB")
    print(f"{'=' * 80}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate generated Deribit trade Parquet files."
    )
    parser.add_argument(
        "--type",
        type=str,
        choices=["future", "option", "both"],
        default="both",
        help="Type of data to validate (default: both).",
    )

    args = parser.parse_args()
    setup_logging()

    types = ["future", "option"] if args.type == "both" else [args.type]

    for data_type in types:
        if data_type == "future":
            parquet_file = settings.BASE_DIR / "future.parquet"
        else:
            parquet_file = settings.BASE_DIR / "option.parquet"

        validate_parquet(parquet_file)


if __name__ == "__main__":
    main()
