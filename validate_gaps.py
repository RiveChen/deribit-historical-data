import argparse
import polars as pl
from pathlib import Path

def validate_gaps(file_path: str):
    p = Path(file_path)
    if not p.exists():
        print(f"File not found: {file_path}")
        return
        
    print(f"\n--- Checking Gaps: {p.name} ---")
    
    # Use read_parquet instead of scan_parquet to avoid hanging
    df = pl.read_parquet(file_path)
    
    # Filter out potential nulls or zero trade_seq if any exist (though rare)
    # Group by instrument_name
    stats = (
        df.filter(pl.col("trade_seq").is_not_null())
        .group_by("instrument_name")
        .agg([
            pl.len().alias("actual_count"),
            pl.col("trade_seq").min().alias("min_seq"),
            pl.col("trade_seq").max().alias("max_seq"),
        ])
        .with_columns(
            expected_count=(pl.col("max_seq") - pl.col("min_seq") + 1)
        )
        .with_columns(
            gap=(pl.col("expected_count") - pl.col("actual_count"))
        )
        .sort(by="gap", descending=True)
    )
    
    gaps = stats.filter(pl.col("gap") > 0)
    
    if len(gaps) == 0:
        print(f"✅ Awesome! All {len(stats)} instruments are perfectly contiguous. Zero gaps found.")
    else:
        print(f"❌ Found gaps in {len(gaps)} out of {len(stats)} instruments.")
        pl.Config.set_tbl_rows(20)
        print("\nInstruments with the largest missing sequence count:")
        print(gaps.head(20).select([
            "instrument_name", "actual_count", "expected_count", "gap", "min_seq", "max_seq"
        ]))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, help="Path to parquet file")
    args = parser.parse_args()
    
    if args.file:
        validate_gaps(args.file)
    else:
        validate_gaps("data/BTC/future.parquet")
        validate_gaps("data/BTC/option.parquet")
