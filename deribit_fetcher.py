import requests
import os
import pandas as pd
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

CURRENCY = "BTC"  # Change to "ETH" for Ethereum
SAVE_PATH = f"./data/{CURRENCY}"  # Change to the folder you want to save data
INSTRUMENTS_FILE = f"{CURRENCY}-option-list.csv"

BASE_URL = "https://history.deribit.com/api/v2"

MAX_WORKERS = 200  # Limited by API rate
MAX_COUNT = 10000  # Limited by API param


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
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        # logging.FileHandler(os.path.join(SAVE_PATH, "deribit_fetcher.log")),
        TqdmLoggingHandler(),
    ],
)
logger = logging.getLogger(__name__)


def do_request(instrument: str, start_time_ms, end_time_ms):
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
        "include_old": "true",
    }

    res = pd.DataFrame()
    try:
        response = requests.get(url, params)
        data = response.json()

        if data.get("result", {}).get("trades"):
            trades = data.get("result", {}).get("trades")
            res = pd.DataFrame(trades)
            # sort to get earliest timestamp
            res.sort_values(
                by=["timestamp"], ascending=True, ignore_index=True, inplace=True
            )

            if len(trades) == 10000:
                # not complete, fetching previous trades
                new_end_ms = res["timestamp"].iloc[0] - 1
                prev_res = do_request(instrument, start_time_ms, new_end_ms)
                res = pd.concat([prev_res, res], ignore_index=True)

            return res
        else:
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        logger.info(f"Reaching rate limit, retry after 1s... {e}")
        logger.debug(e)
        sleep(1)
        return do_request(instrument, start_time_ms, end_time_ms)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        exit(-1)


def get_all_instruments():
    f"""Fetch and save all expired {CURRENCY} option instruments from Deribit.

    Makes API call to retrieve instrument data and saves to CSV file.

    Returns:
        pd.DataFrame: DataFrame containing instrument data if successful, None otherwise
    """
    url = f"{BASE_URL}/public/get_instruments"
    params = {"currency": CURRENCY, "kind": "option", "expired": "true"}

    try:
        response = requests.get(url, params)
        data = response.json()

        if "result" in data:
            df = pd.DataFrame(data["result"])
            df.to_csv(os.path.join(SAVE_PATH, INSTRUMENTS_FILE), index=False)
            return df
        else:
            logger.error(f"Failed in fetching option list: {data}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}")
        exit(-1)


def get_all_trades_by_instrument(instrument: str, start_time_ms, end_time_ms):
    """Fetch and save all trades for a single instrument within time range.

    Args:
        instrument: Deribit instrument name
        start_time_ms: Start timestamp in milliseconds
        end_time_ms: End timestamp in milliseconds
    """

    logger.debug(f"Fetching {instrument}")
    df = do_request(instrument, start_time_ms, end_time_ms)
    if not df.empty:
        filename = f"{instrument}.csv"
        df.to_csv(os.path.join(SAVE_PATH, filename), index=False)
        logger.debug(f"{instrument} fetched, saved to {filename}")
    else:
        logger.debug(f"{instrument} has no trades.")


def main():
    """Main entry point that coordinates fetching of all instrument trades.

    Uses ThreadPoolExecutor for parallel processing of instruments.
    """
    os.makedirs(SAVE_PATH, exist_ok=True)
    option_list_file = os.path.join(SAVE_PATH, INSTRUMENTS_FILE)
    if not os.path.exists(option_list_file):
        print("Fetching instruments, it should take about 30s...")
        get_all_instruments()

    option_list = pd.read_csv(option_list_file)
    logger.info(f"Found {len(option_list)} instruments to process.")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_instrument = {}
        for row in option_list.itertuples():
            instrument_name = row.instrument_name
            start_time_ms = row.creation_timestamp
            end_time_ms = row.expiration_timestamp

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
                    logger.error(f"Unknown error in fetching {instrument}: {e}")
                finally:
                    pbar.update(1)


if __name__ == "__main__":
    main()
