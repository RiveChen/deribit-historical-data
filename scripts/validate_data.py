"""
Post-download data integrity validation.

Only validates generated Parquet files, using Polars LazyFrame (streaming)
aggregations to avoid OOM on large files (future.parquet ~90 GB).
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
    """Validate a generated parquet file using streaming LazyFrame to avoid OOM.

    Only streaming-safe aggregations are used — no hash-based operations
    (group_by, unique, struct.n_unique) that would OOM on 90 GB files.
    """
    if not parquet_path.exists():
        logger.warning(f"Parquet file not found: {parquet_path}")
        return

    print(f"\n{'=' * 80}")
    print(f"Validating Parquet: {parquet_path}")
    print(f"{'=' * 80}")

    lf = pl.scan_parquet(parquet_path)

    # Streaming aggregations — never materializes the full file.
    # All these ops are streaming-safe (no hash table needed):
    #   len, min, max, n_unique
    stats = (
        lf.select(
            [
                pl.len().alias("count"),
                pl.min("timestamp").alias("ts_min"),
                pl.max("timestamp").alias("ts_max"),
                pl.min("trade_seq").alias("seq_min"),
                pl.max("trade_seq").alias("seq_max"),
                pl.col("trade_seq").n_unique().alias("seq_unique"),
            ]
        )
        .collect()
    )

    rows = int(stats[0, "count"])

    print(f"Rows: {rows:,}")

    # Schema info (metadata only — no data scan)
    schema = lf.collect_schema()
    print(f"Columns: {len(schema)}")
    print(f"Schema:")
    for col, dtype in schema.items():
        print(f"  {col:25s}  {str(dtype):12s}")

    # Dedup estimate: if trade_seq is globally unique within the parquet,
    # seq_unique == count means no duplicates.
    seq_unique = int(stats[0, "seq_unique"])
    if rows == seq_unique:
        print(f"\n✅ Dedup check passed: {rows:,} unique trade_seq values")
    else:
        print(
            f"\n⚠️  Found {rows - seq_unique} duplicate trade_seq values "
            f"({seq_unique:,} unique out of {rows:,})"
        )

    # Time range
    t_min = datetime.fromtimestamp(
        int(stats[0, "ts_min"]) / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%d")
    t_max = datetime.fromtimestamp(
        int(stats[0, "ts_max"]) / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%d")
    print(f"Time range: {t_min} ~ {t_max}")

    # trade_seq range
    seq_min = int(stats[0, "seq_min"])
    seq_max = int(stats[0, "seq_max"])
    expected = seq_max - seq_min + 1
    if seq_unique < expected:
        print(f"Gap estimate: {expected - seq_unique} missing trade_seq values")
    else:
        print(f"trade_seq range: {seq_min:,} ~ {seq_max:,} ({expected:,} expected)")

    # File size
    size_mb = parquet_path.stat().st_size / (1024 * 1024)
    print(f"\nFile size: {size_mb:.2f} MB")
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
