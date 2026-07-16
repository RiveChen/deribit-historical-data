"""Post-download data integrity validation.

Validates generated Parquet files by checking trade_seq continuity for each
instrument individually. For instruments with gaps, a histogram overlay shows
the gap distribution across equal-sized buckets of the seq range.

Memory safety: all group_by operations use engine="streaming" so only a few
row groups are held in memory at a time.
"""

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from deribit_fetcher.config import settings
from deribit_fetcher.log import setup_logging

logger = logging.getLogger(__name__)

N_BUCKETS = 10


def _show_gap_histogram(lf: pl.LazyFrame, gapped: list) -> None:
    """For each instrument with gaps, show a per-bucket histogram.

    Each instrument's seq range is divided into N_BUCKETS equal intervals.
    A separate streaming pass is made per instrument (typically 1-2, at most ~357).
    """
    print(f"\n{'─' * 80}")
    print(f"Gap Distribution ({N_BUCKETS} equal-sized buckets per instrument)")
    print(f"{'─' * 80}")

    for instr_name, count, seq_min, seq_max in gapped:
        expected_total = seq_max - seq_min + 1
        gap_total = expected_total - count

        # Distribute rows evenly across buckets: remainder goes to first R buckets
        b_expected_base = expected_total // N_BUCKETS
        remainder = expected_total % N_BUCKETS

        # One streaming pass per gapped instrument — safe because:
        # 1. streaming=True means only a few row groups in memory
        # 2. Typically only 1-2 instruments have gaps
        bucket_counts = (
            lf.filter(pl.col("instrument_name") == instr_name)
            .with_columns(
                # Map trade_seq to bucket via integer arithmetic:
                # bucket = (trade_seq - seq_min) * N_BUCKETS // expected_total
                # This avoids floating-point rounding issues entirely.
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

        # Build a lookup: bucket -> actual row count
        counts_map = dict(bucket_counts.iter_rows())

        print(f"\n{instr_name} — {gap_total:,} gaps (expected {expected_total:,}, got {count:,})")
        print(f"  {'Bucket':>8s}  {'Rows':>10s}  {'Expected':>10s}  {'Deficit':>10s}")
        print(f"  {'─' * 8}  {'─' * 10}  {'─' * 10}  {'─' * 10}")

        for b in range(N_BUCKETS):
            b_rows = counts_map.get(b, 0)
            b_expected = b_expected_base + (1 if b < remainder else 0)
            deficit = b_expected - b_rows

            if deficit > 0:
                marker = " ⚠️"
            else:
                marker = "   "
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

    # Schema info (metadata only — no data scan)
    schema = lf.collect_schema()
    print(f"Columns: {len(schema)}")
    print("Schema:")
    for col, dtype in schema.items():
        print(f"  {col:25s}  {str(dtype):12s}")

    # Pass 1: per-instrument stats (streaming-safe)
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

    # Pass 2: gap histogram for instruments with gaps
    if gapped_instruments:
        _show_gap_histogram(lf, gapped_instruments)

    # File size
    size_mb = parquet_path.stat().st_size / (1024 * 1024)

    t_min = datetime.fromtimestamp(int(ts_min_global) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    t_max = datetime.fromtimestamp(int(ts_max_global) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

    print(f"\n{'=' * 80}")
    print(f"Total rows: {total_rows:,}")
    print(f"Files with gaps: {gap_count}    Files OK: {ok_count}")
    print(f"Time range: {t_min} ~ {t_max}")
    print(f"File size: {size_mb:.2f} MB")
    print(f"{'=' * 80}")


def main() -> None:
    """Entry point: parse args and run validation."""
    parser = argparse.ArgumentParser(description="Validate generated Deribit trade Parquet files.")
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
            parquet_file = settings.base_dir / "future.parquet"
        else:
            parquet_file = settings.base_dir / "option.parquet"

        validate_parquet(parquet_file)


if __name__ == "__main__":
    main()
