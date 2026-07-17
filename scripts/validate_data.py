#!/usr/bin/env python3
"""CLI for post-download data integrity validation."""

import argparse

from deribit_fetcher.config import set_base_dir, settings
from deribit_fetcher.log import setup_logging
from deribit_fetcher.parquet import validate_parquet


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
    parser.add_argument(
        "--base-dir",
        type=str,
        default=None,
        help="Override the data directory (default: ./data/<CURRENCY>).",
    )

    args = parser.parse_args()
    setup_logging()
    set_base_dir(args.base_dir)

    types = ["future", "option"] if args.type == "both" else [args.type]

    for data_type in types:
        if data_type == "future":
            parquet_file = settings.base_dir / "future.parquet"
        else:
            parquet_file = settings.base_dir / "option.parquet"

        validate_parquet(parquet_file)


if __name__ == "__main__":
    main()
