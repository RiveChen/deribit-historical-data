import requests
import argparse
import os
import pandas as pd
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from datetime import datetime

DEFAULT_CURRENCY = "BTC"
DEFAULT_INSTRUMENT = "option"
DEFAULT_SAVE_PATH = "./data"  # Change this to your preferred data storage folder

AVAILABLE_CURRENCIES = ["BTC", "ETH", "USDC", "USDT"]  # Supported settlement currencies
AVAILABLE_INSTRUMENTS = [
    "option",
    "future",
]  # Note: Historical API doesn't support spot markets

CURRENCY = DEFAULT_CURRENCY
INSTRUMENT = DEFAULT_INSTRUMENT
EXPIRED = "true"

SAVE_PATH = os.path.join(DEFAULT_SAVE_PATH, CURRENCY)
INSTRUMENTS_FILE = f"{CURRENCY}-{INSTRUMENT}-list.csv"

BASE_URL = "https://history.deribit.com/api/v2"

MAX_WORKERS = 200  # API rate limit constraint
MAX_COUNT = 10000  # Maximum results per API request


class TqdmLoggingHandler(logging.Handler):
    """Custom logging handler that integrates with tqdm progress bars.

    Prevents log messages from interfering with progress bar display.
    """

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


logging.basicConfig(
    level=logging.ERROR,  # Set to INFO for detailed logging
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        # logging.FileHandler(os.path.join(SAVE_PATH, "deribit_fetcher.log")),
        TqdmLoggingHandler(),
    ],
)
logger = logging.getLogger(__name__)


def get_trades(instrument: str, start_time_ms, end_time_ms):
    """Recursively fetch trades for a given instrument within time range.

    Args:
        instrument: Deribit instrument name (e.g. BTC-OPTION)
        start_time_ms: Start timestamp in milliseconds
        end_time_ms: End timestamp in milliseconds

    Returns:
        pd.DataFrame: Combined DataFrame of all trades in requested time range
    """

    url = f"{BASE_URL}/public/get_last_trades_by_instrument_and_time"
    params = {
        "instrument_name": instrument,
        "start_timestamp": start_time_ms,
        "end_timestamp": end_time_ms,
        "count": MAX_COUNT,
        "include_old": EXPIRED,
    }

    res = pd.DataFrame()
    try:
        response = requests.get(url, params)
        data = response.json()

        if data.get("result", {}).get("trades"):
            trades = data.get("result", {}).get("trades")
            res = pd.DataFrame(trades)
            # Sort by timestamp to get earliest entries first
            res.sort_values(
                by=["timestamp"], ascending=True, ignore_index=True, inplace=True
            )

            if len(trades) == 10000:
                # Incomplete data, fetch remaining trades
                new_end_ms = res["timestamp"].iloc[0] - 1
                prev_res = get_trades(instrument, start_time_ms, new_end_ms)
                res = pd.concat([prev_res, res], ignore_index=True)

            return res
        else:
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        logger.info(f"Rate limit exceeded, retrying after 1 second... {e}")
        logger.debug(e)
        sleep(1)
        return get_trades(instrument, start_time_ms, end_time_ms)
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        exit(-1)


def get_all_trades_by_instrument(instrument: str, start_time_ms, end_time_ms):
    """Fetch and save all trades for a single instrument within time range.

    Args:
        instrument: Deribit instrument name
        start_time_ms: Start timestamp in milliseconds
        end_time_ms: End timestamp in milliseconds
    """

    logger.debug(f"Processing {instrument}")
    df = get_trades(instrument, start_time_ms, end_time_ms)
    if not df.empty:
        filename = f"{instrument}.csv"
        df.to_csv(os.path.join(SAVE_PATH, filename), index=False)
        logger.debug(f"{instrument} fetched, saved to {filename}")
    else:
        logger.debug(f"{instrument} has no trades.")


def get_all_instruments():
    """Fetch and save all instruments from Deribit.

    Makes API call to retrieve instrument data and saves to CSV file.

    Returns:
        pd.DataFrame: DataFrame containing instrument data if successful, None otherwise
    """
    url = f"{BASE_URL}/public/get_instruments"
    params = {"currency": CURRENCY, "kind": INSTRUMENT, "expired": EXPIRED}

    try:
        response = requests.get(url, params)
        data = response.json()

        if "result" in data:
            df = pd.DataFrame(data["result"])
            df.to_csv(os.path.join(SAVE_PATH, INSTRUMENTS_FILE), index=False)
            return df
        else:
            logger.error(f"Instrument list retrieval failed: {data}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Connection error during instrument list fetch:: {e}")
        print("Retrying...")
        return get_all_instruments()


def main():
    """Main entry point that coordinates fetching of all instrument trades."""
    now = datetime.now()
    if EXPIRED == "false":
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"Retrieving active instruments: All trades are capped at current time ({now_str})"
        )

    instrument_list_file = os.path.join(SAVE_PATH, INSTRUMENTS_FILE)
    if not os.path.exists(instrument_list_file):
        print("Initializing instrument list download (approx. 30 seconds)...")
        get_all_instruments()

    instrument_list = pd.read_csv(instrument_list_file)
    print(
        f"Discovered {len(instrument_list)} {CURRENCY} {INSTRUMENT} instruments to process."
    )

    print("Initiating trade data download (approx. 20 minutes)...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_instrument = {}
        for row in instrument_list.itertuples():
            instrument_name = row.instrument_name
            start_time_ms = row.creation_timestamp
            # if fetching non-expired instruments, limit end_time_ms
            end_time_ms = row.expiration_timestamp
            if EXPIRED == "false":
                end_time_ms = min(end_time_ms, int(now.timestamp() * 1000))

            future = executor.submit(
                get_all_trades_by_instrument,
                instrument=instrument_name,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            future_to_instrument[future] = instrument_name

        with tqdm(total=len(future_to_instrument)) as pbar:
            for future in as_completed(future_to_instrument):
                instrument = future_to_instrument[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing {instrument}: {e}")
                finally:
                    pbar.update(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch historical data from Deribit.")
    parser.add_argument(
        "--currency",
        type=str,
        default=DEFAULT_CURRENCY,
        choices=AVAILABLE_CURRENCIES,
        help=f"Settlement currency for instruments (default: BTC). Options: {AVAILABLE_CURRENCIES}",
    )
    parser.add_argument(
        "--kind",
        type=str,
        default=DEFAULT_INSTRUMENT,
        choices=AVAILABLE_INSTRUMENTS,
        help=f"Instrument type (default: option). Options: {AVAILABLE_INSTRUMENTS}",
    )
    parser.add_argument(
        "--expired",
        type=int,
        default=1,
        choices=[0, 1],
        help="Fetch expired or non-expired instruments (default: 1)",
    )
    parser.add_argument(
        "--folder",
        type=str,
        default=SAVE_PATH,
        help=f"Output directory for data storage (default: {SAVE_PATH})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Enable detailed logging output",
    )
    args = parser.parse_args()
    CURRENCY = args.currency
    INSTRUMENT = args.kind
    EXPIRED = "true" if args.expired == 1 else "false"
    INSTRUMENTS_FILE = (
        f"{CURRENCY}-{INSTRUMENT}-expired-list.csv"
        if args.expired == 1
        else f"{CURRENCY}-{INSTRUMENT}-non-expired-list.csv"
    )
    SAVE_PATH = os.path.join(args.folder, CURRENCY)
    os.makedirs(SAVE_PATH, exist_ok=True)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    main()
