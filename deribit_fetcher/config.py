AVAILABLE_CONFIGS = {
    "currency": ["BTC", "ETH", "USDC", "USDT"],
    "instrument": ["option", "future", "all"],
    "expired": ["true", "false", "all"],
}

DEFAULT_CONFIG = {
    "currency": "BTC",
    "instrument": "option",
    "expired": "true",
    "base_dir": "./data",
    "save_parquet": False,
    "verbose": False,
}

CONFIG = DEFAULT_CONFIG

BASE_URL = "https://history.deribit.com/api/v2"

MAX_WORKERS = 200  # API rate limit constraint
MAX_COUNT = 10000  # Maximum results per API request
