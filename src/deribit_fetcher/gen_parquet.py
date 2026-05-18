import argparse
import logging
from pathlib import Path

import polars as pl

from deribit_fetcher.config import settings
from deribit_fetcher.log import setup_logging

logger = logging.getLogger(__name__)


def generate_parquet(
    data_dir: Path,
    output_file: Path,
    dedup: bool = True,
) -> None:
    """
    Scan a directory for JSONL files and merge them into a single Parquet file.

    Uses PyArrow's ParquetWriter for streaming write (memory efficient with
    many large input files). A comprehensive schema is applied to ensure all
    optional fields (like liquidation, block_trade_id) are captured uniformly.

    When dedup=True, removes duplicate rows by (instrument_name, trade_seq)
    both within each JSONL file and across files (handles chunk boundary overlap).
    """
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        return

    jsonl_files = list(data_dir.glob("*.jsonl"))
    if not jsonl_files:
        logger.warning(f"No JSONL files found in {data_dir}")
        return

    logger.info(f"Found {len(jsonl_files)} JSONL files in {data_dir}")
    logger.info(f"Merging into {output_file}...")

    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Define the comprehensive schema of a Deribit Trade.
        # This ensures rare fields are not dropped during schema inference
        # and standardizes the output Parquet schema for all instrument types.
        # Fields marked with ¹ appear only on specific trade types:
        #   combo/block-trades → combo_id, combo_trade_id, block_trade_id, block_rfq_id, block_trade_leg_count
        #   perpetual futures  → liquidation
        #   options only       → iv, contracts
        comprehensive_schema = pl.Schema(
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

        import pyarrow.parquet as pq
        from tqdm import tqdm

        writer = None
        total_rows = 0
        total_duplicates = 0
        # Track seen (instrument, trade_seq) keys across all files for cross-file dedup
        prev_keys: set[tuple[str, int]] = set()

        for f in tqdm(jsonl_files, desc=f"Writing {output_file.name}", unit="file"):
            try:
                # Read each file using the guaranteed schema
                df = pl.read_ndjson(f, schema=comprehensive_schema)
                if df.is_empty():
                    continue

                if dedup:
                    # Intra-file dedup: remove duplicates within a single JSONL file
                    before = len(df)
                    df = df.unique(
                        subset=["instrument_name", "trade_seq"], keep="first"
                    )
                    intra_dup = before - len(df)
                    if intra_dup:
                        logger.debug(f"{f.name}: removed {intra_dup} intra-file dups")

                    # Cross-file dedup: drop rows already seen in earlier files
                    if prev_keys:
                        # Build a struct column for is_in comparison
                        key_struct = pl.struct(
                            pl.col("instrument_name"),
                            pl.col("trade_seq").cast(pl.Int64),
                        )
                        mask = ~key_struct.is_in(
                            pl.Series(
                                "prev",
                                [
                                    {"instrument_name": k[0], "trade_seq": k[1]}
                                    for k in prev_keys
                                ],
                            )
                        )
                        before = len(df)
                        df = df.filter(mask)
                        cross_dup = before - len(df)
                        if cross_dup:
                            logger.debug(
                                f"{f.name}: removed {cross_dup} cross-file dups"
                            )
                        total_duplicates += cross_dup
                    total_duplicates += intra_dup

                    # Record keys from this file for future cross-file checks
                    new_keys = set(
                        zip(
                            df["instrument_name"].to_list(),
                            (int(v) for v in df["trade_seq"].to_list()),
                        )
                    )
                    prev_keys.update(new_keys)

                total_rows += len(df)
                if df.is_empty():
                    continue

                table = df.to_arrow()

                # Initialize PyArrow writer with the schema from the first valid table
                if writer is None:
                    writer = pq.ParquetWriter(
                        output_file, table.schema, compression="zstd"
                    )

                writer.write_table(table)
            except Exception as e:
                logger.error(f"Error processing {f.name}: {e}")

        if writer:
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

    args = parser.parse_args()

    setup_logging()

    data_type = args.type

    if data_type == "future":
        input_dir = settings.DATA_FUTURE_DIR
    else:
        input_dir = settings.DATA_OPTION_DIR

    output_file = settings.BASE_DIR / f"{data_type}.parquet"

    logger.info(f"Starting Parquet generation for {data_type.upper()} data...")
    generate_parquet(input_dir, output_file, dedup=not args.no_dedup)
    logger.info("Done.")


if __name__ == "__main__":
    main()
