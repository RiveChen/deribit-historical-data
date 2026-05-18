import argparse
import logging
from pathlib import Path

import polars as pl

from deribit_fetcher.config import settings
from deribit_fetcher.log import setup_logging

logger = logging.getLogger(__name__)


def generate_parquet(data_dir: Path, output_file: Path) -> None:
    """
    Scans a directory for JSONL files and merges them into a single Parquet file.
    Uses polars lazy evaluation for memory efficiency.
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
        # Use scan_ndjson to process multiple files lazily
        # Polars accepts glob patterns directly
        glob_pattern = str(data_dir / "*.jsonl")
        
        # Define the comprehensive schema of a Deribit Trade object.
        # This ensures rare fields (like liquidation, block_trade_id) are not dropped during inference
        # and standardizes the output Parquet schema for all types.
        comprehensive_schema = pl.Schema({
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
            "combo_id": pl.String,
            "combo_trade_id": pl.String,
            "fee_currency": pl.String,
            "liquidation": pl.String,
            "mmp": pl.Boolean,
        })
        
        import pyarrow.parquet as pq
        from tqdm import tqdm
        
        writer = None
        total_rows = 0
        
        for f in tqdm(jsonl_files, desc=f"Writing {output_file.name}", unit="file"):
            try:
                # Read single file using the guaranteed schema
                df = pl.read_ndjson(f, schema=comprehensive_schema)
                if df.is_empty():
                    continue
                    
                total_rows += len(df)
                table = df.to_arrow()
                
                # Initialize PyArrow writer with the schema from the first valid table
                if writer is None:
                    writer = pq.ParquetWriter(output_file, table.schema, compression="zstd")
                    
                writer.write_table(table)
            except Exception as e:
                logger.error(f"Error processing {f.name}: {e}")
                
        if writer:
            writer.close()
            
        logger.info(f"Successfully loaded {total_rows} rows.")
        logger.info(f"Successfully wrote Parquet file: {output_file} (Size: {output_file.stat().st_size / (1024 * 1024):.2f} MB)")

    except Exception as e:
        logger.exception(f"Failed to generate parquet: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge Deribit JSONL files into a single Parquet file.")
    parser.add_argument(
        "--type", 
        type=str, 
        choices=["future", "option"], 
        required=True,
        help="Type of data to merge (future or option)."
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
    generate_parquet(input_dir, output_file)
    logger.info("Done.")


if __name__ == "__main__":
    main()
