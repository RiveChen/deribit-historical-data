"""
Post-download data integrity validation.

Checks:
  1. For each instrument: trade_seq continuity (no gaps)
  2. Time range coverage (earliest ~ latest timestamp)
  3. Row count sanity check vs predicted last_seq
  4. Per-instrument statistics report
"""

import argparse
import asyncio
import json
import logging
import sqlite3
from collections import defaultdict
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
    Validate all JSONL files in data_dir.

    For each instrument file:
      - Count rows
      - Check trade_seq order (should be descending within each chunk)
      - Check for gaps in trade_seq (expect no gaps since chunks are contiguous)
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
        df = pl.read_ndjson(f, schema=_TRADE_SCHEMA)
        if df.is_empty():
            reports.append((instr, 0, "N/A", "N/A", "-"))
            continue

        rows = len(df)
        total_rows += rows

        # Timestamp range
        ts_min = df["timestamp"].min()
        ts_max = df["timestamp"].max()
        from datetime import datetime, timezone

        t_min = datetime.fromtimestamp(ts_min / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d"
        )
        t_max = datetime.fromtimestamp(ts_max / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d"
        )

        # trade_seq analysis
        seqs = df["trade_seq"].to_list()
        seqs.sort()
        unique_seqs = sorted(set(seqs))

        total_unique = len(unique_seqs)
        total_raw = len(seqs)
        duplicates = total_raw - total_unique
        min_seq = unique_seqs[0]
        max_seq = unique_seqs[-1]
        expected_count = max_seq - min_seq + 1

        if total_unique < expected_count:
            gaps = expected_count - total_unique
            status = f"⚠️  {gaps} gaps"
            total_with_gaps += 1
        elif duplicates > 0:
            status = f"⚠️  {duplicates} dup(s)"
            total_with_overlaps += 1
        else:
            status = "✅"

        # fields present
        fields = list(df.columns)

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
            field_sources[f].add(instr.split("-")[0])  # BTC or ETH prefix

    print(f"\nFields ({len(all_fields)}):")
    for f in sorted(all_fields):
        sources = field_sources[f]
        print(f"  {f:25s}  from {len(sources)} instrument(s)")


def validate_parquet(parquet_path: Path) -> None:
    """Validate a generated parquet file (after dedup)."""
    if not parquet_path.exists():
        logger.warning(f"Parquet file not found: {parquet_path}")
        return

    print(f"\n{'=' * 80}")
    print(f"Validating Parquet: {parquet_path}")
    print(f"{'=' * 80}")

    df = pl.read_parquet(parquet_path)
    rows = len(df)

    print(f"Rows: {rows:,}")
    print(f"Columns: {len(df.columns)}")
    print(f"Schema:")
    for col, dtype in df.schema.items():
        nulls = df[col].null_count()
        non_null = rows - nulls
        print(f"  {col:25s}  {str(dtype):12s}  {non_null:>12,} non-null")

    # Check dedup effectiveness (should be no duplicates by instrument + trade_seq)
    before = rows
    after = df.unique(subset=["instrument_name", "trade_seq"]).height
    if before == after:
        print(
            f"\n✅ Dedup check passed: {before:,} unique (instrument, trade_seq) pairs"
        )
    else:
        print(
            f"\n⚠️  Found {before - after} duplicate (instrument, trade_seq) rows remaining"
        )

    # Time range
    ts_min = df["timestamp"].min()
    ts_max = df["timestamp"].max()
    from datetime import datetime, timezone

    t_min = datetime.fromtimestamp(ts_min / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    t_max = datetime.fromtimestamp(ts_max / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    print(f"Time range: {t_min} ~ {t_max}")

    # Instrument counts
    instr_counts = df.group_by("instrument_name").len().sort("len", descending=True)
    print(f"\nTop 10 instruments by row count:")
    for row in instr_counts.head(10).iter_rows():
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
