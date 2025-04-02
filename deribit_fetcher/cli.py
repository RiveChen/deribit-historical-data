import argparse

from .config import AVAILABLE_CONFIGS, DEFAULT_CONFIG


def get_parser():
    parser = argparse.ArgumentParser("Deribit Historical Data Fetcher")
    parser.add_argument(
        "--currency",
        type=str,
        default=DEFAULT_CONFIG["currency"],
        choices=AVAILABLE_CONFIGS["currency"],
        help="Currency to fetch",
    )
    parser.add_argument(
        "--instrument",
        type=str,
        default=DEFAULT_CONFIG["instrument"],
        choices=AVAILABLE_CONFIGS["instrument"],
        help="Instrument type to fetch",
    )
    parser.add_argument(
        "--expired",
        type=str,
        default=DEFAULT_CONFIG["expired"],
        choices=AVAILABLE_CONFIGS["expired"],
        help="Fetch expired or active instruments",
    )
    parser.add_argument(
        "--base_dir",
        type=str,
        default=DEFAULT_CONFIG["base_dir"],
        help="Base directory for saving data",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose logging",
    )
    parser.add_argument(
        "--save_parquet",
        action="store_true",
        help="Save data to a single Parquet file",
    )
    return parser
