"""
Post-download data integrity validation.

All file reads use Polars LazyFrame (streaming) to avoid OOM on large files
(e.g. BTC-PERPETUAL.jsonl ~several GB, future.parquet ~90 GB).
"""

import argparse
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from deribit_fetcher.config import settings
from deribit_fetcher.log import setup_logging

# Shared schema — must match gen_parquet.py's COMPREHENSIVE_SCHEMA
_TRADE_SCHEMA = pl.Schema(
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

logger = logging.getLogger(__name__)


def validate_jsonl(data_dir: Path) -> None:
    """
    Validate all JSONL files in data_dir using streaming LazyFrame queries.

    For each instrument file:
      - Count rows
      - Check trade_seq continuity (no gaps)
      - Report time range
    """
    jsonl_files = sorted(data_dir.glob("*.jsonl"))
    if not jsonl_files:
        logger.warning(f"No JSONL files found in {data_dir}")
        return

    print(f"\n{'=' * 80}")
    print(f"Validating {len(jsonl_files)} JSONL files in {data_dir}")
    print(f"{'=' * 80}")

    total_rows = 0
    total_with_gaps = 0
    total_with_overlaps = 0
    reports = []

    for f in jsonl_files:
        instr = f.stem

        # Streaming scan — never loads the full file into memory
        lf = pl.scan_ndjson(f, schema=_TRADE_SCHEMA)

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

        if stats.is_empty() or stats[0, "count"] == 0:
            reports.append((instr, 0, "N/A", "N/A", "-"))
            continue

        rows = int(stats[0, "count"])
        total_rows += rows

        ts_min = stats[0, "ts_min"]
        ts_max = stats[0, "ts_max"]
        t_min = datetime.fromtimestamp(ts_min / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d"
        )
        t_max = datetime.fromtimestamp(ts_max / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d"
        )

        min_seq = int(stats[0, "seq_min"])
        max_seq = int(stats[0, "seq_max"])
        unique_seqs = int(stats[0, "seq_unique"])
        expected_count = max_seq - min_seq + 1

        if unique_seqs < expected_count:
            gaps = expected_count - unique_seqs
            status = f"⚠️  {gaps} gaps"
            total_with_gaps += 1
        elif unique_seqs < rows:
            duplicates = rows - unique_seqs
            status = f"⚠️  {duplicates} dup(s)"
            total_with_overlaps += 1
        else:
            status = "✅"

        fields = list(lf.collect_schema())
        reports.append((instr, rows, t_min, t_max, status, fields))

    # Print summary table
    print(f"\n{'Instrument':35s} {'Rows':>10s} {'From':14s} {'To':14s} {'Status':20s}")
    print("-" * 93)
    for r in reports:
        instr, rows, t_min, t_max, status = r[:5]
        print(f"{instr:35s} {rows:>10,} {t_min:14s} {t_max:14s} {status:20s}")

    print(f"\n{'=' * 80}")
    print(f"Total rows: {total_rows:,}")
    print(f"Files with gaps: {total_with_gaps}")
    print(f"Files with duplicates: {total_with_overlaps}")
    print(f"{'=' * 80}")

    # Field union across all instruments
    all_fields = set()
    field_sources = defaultdict(set)
    for r in reports:
        instr = r[0]
        for f in r[5]:
            all_fields.add(f)
            field_sources[f].add(instr.split("-")[0])

    print(f"\nFields ({len(all_fields)}):")
    for f in sorted(all_fields):
        sources = field_sources[f]
        print(f"  {f:25s}  from {len(sources)} instrument(s)")


def validate_parquet(parquet_path: Path) -> None:
    """Validate a generated parquet file using streaming LazyFrame to avoid OOM."""
    if not parquet_path.exists():
        logger.warning(f"Parquet file not found: {parquet_path}")
        return

    print(f"\n{'=' * 80}")
    print(f"Validating Parquet: {parquet_path}")
    print(f"{'=' * 80}")

    lf = pl.scan_parquet(parquet_path)

    # Streaming aggregations — never materializes the full file
    stats = (
        lf.select(
            [
                pl.len().alias("count"),
                pl.min("timestamp").alias("ts_min"),
                pl.max("timestamp").alias("ts_max"),
                pl.col("instrument_name").n_unique().alias("n_instruments"),
            ]
        )
        .collect()
    )

    rows = int(stats[0, "count"])
    del stats

    print(f"Rows: {rows:,}")

    # Schema info
    schema = lf.collect_schema()
    print(f"Columns: {len(schema)}")
    print(f"Schema:")
    for col, dtype in schema.items():
        print(f"  {col:25s}  {str(dtype):12s}")

    # Check dedup effectiveness via streaming unique
    dedup_check = (
        lf.select(
            pl.struct(["instrument_name", "trade_seq"])
            .alias("pair")
            .n_unique()
        )
        .collect()
    )
    unique_pairs = int(dedup_check[0, "pair"])
    if rows == unique_pairs:
        print(
            f"\n✅ Dedup check passed: {rows:,} unique (instrument, trade_seq) pairs"
        )
    else:
        print(
            f"\n⚠️  Found {rows - unique_pairs} duplicate (instrument, trade_seq) rows remaining"
        )

    # Time range
    ts_stats = (
        lf.select(
            [
                pl.min("timestamp").alias("ts_min"),
                pl.max("timestamp").alias("ts_max"),
            ]
        )
        .collect()
    )
    t_min = datetime.fromtimestamp(
        int(ts_stats[0, "ts_min"]) / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%d")
    t_max = datetime.fromtimestamp(
        int(ts_stats[0, "ts_max"]) / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%d")
    print(f"Time range: {t_min} ~ {t_max}")

    # Top instruments (limited to 10 rows via head, streaming-friendly)
    instr_counts = (
        lf.group_by("instrument_name")
        .len()
        .sort("len", descending=True)
        .head(10)
        .collect()
    )
    print(f"\nTop 10 instruments by row count:")
    for row in instr_counts.iter_rows():
        print(f"  {row[0]:35s}  {row[1]:>12,}")

    # File size
    size_mb = parquet_path.stat().st_size / (1024 * 1024)
    print(f"\nFile size: {size_mb:.2f} MB")
    print(f"{'=' * 80}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate downloaded Deribit trade data."
    )
    parser.add_argument(
        "--type",
        type=str,
        choices=["future", "option", "both"],
        default="both",
        help="Type of data to validate (default: both).",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["jsonl", "parquet", "all"],
        default="all",
        help="Validation mode (default: all).",
    )

    args = parser.parse_args()
    setup_logging()

    types = ["future", "option"] if args.type == "both" else [args.type]
    modes = ["jsonl", "parquet"] if args.mode == "all" else [args.mode]

    for data_type in types:
        if data_type == "future":
            jsonl_dir = settings.DATA_FUTURE_DIR
            parquet_file = settings.BASE_DIR / "future.parquet"
        else:
            jsonl_dir = settings.DATA_OPTION_DIR
            parquet_file = settings.BASE_DIR / "option.parquet"

        if "jsonl" in modes:
            validate_jsonl(jsonl_dir)
        if "parquet" in modes:
            validate_parquet(parquet_file)


if __name__ == "__main__":
    main()
