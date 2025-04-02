import sys
from datetime import datetime
from .cli import get_parser
from .config import CONFIG
from .common import get_all_instruments
from .fetch_futures import fetch_futures_trades
from .fetch_options import fetch_options_trades
from .utils import prepare_dir
from .logger import configure_global_logger


def main():
    # Parse command line arguments
    parser = get_parser()
    args = parser.parse_args()

    # Setup logging
    configure_global_logger(verbose=args.verbose)

    # Update config with command line arguments
    CONFIG.update(
        {
            "currency": args.currency,
            "instrument": args.instrument,
            "expired": args.expired == "true",
            "base_dir": args.base_dir,
        }
    )

    # Prepare directory structure
    prepare_dir(CONFIG["base_dir"], CONFIG["currency"], CONFIG["instrument"])

    # Get all instruments based on type
    instruments = get_all_instruments(
        currency=CONFIG["currency"],
        kind=CONFIG["instrument"],
        expired=CONFIG["expired"],
    )

    if len(instruments) == 0:
        print(f"No {CONFIG['instrument']} instruments found for {CONFIG['currency']}")
        sys.exit(1)

    # Fetch trades based on instrument type
    if CONFIG["instrument"] == "future":
        fetch_futures_trades(
            instruments,
            expired=CONFIG["expired"],
            frozen_time=datetime.now() if not CONFIG["expired"] else None,
        )
    elif CONFIG["instrument"] == "option":
        fetch_options_trades(
            instruments,
            expired=CONFIG["expired"],
            frozen_time=datetime.now() if not CONFIG["expired"] else None,
        )
    else:
        print(f"Unsupported instrument type: {CONFIG['instrument']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
