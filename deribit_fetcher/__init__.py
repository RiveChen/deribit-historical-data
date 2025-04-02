"""
Deribit Historical Data Fetcher

A Python package for fetching historical trade data from Deribit exchange.
"""

__version__ = "0.1.0"

from .logger import configure_global_logger, get_global_logger
from .common import get_all_instruments, do_request
from .fetch_futures import fetch_futures_trades
from .fetch_options import fetch_options_trades
from .config import CONFIG, AVAILABLE_CONFIGS, DEFAULT_CONFIG

# Initialize with default configuration
configure_global_logger(verbose=False)

# Make main components available at package level
__all__ = [
    "get_global_logger",
    "get_all_instruments",
    "fetch_futures_trades",
    "fetch_options_trades",
    "CONFIG",
    "AVAILABLE_CONFIGS",
    "DEFAULT_CONFIG",
]
