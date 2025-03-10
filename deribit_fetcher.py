import requests
import argparse
import os
import pandas as pd
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from datetime import datetime
import pyarrow
import glob

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
FROZEN_TIME = datetime.now()

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
        TqdmLoggingHandler(),
    ],
)
logger = logging.getLogger(__name__)


def bool_to_str(value: bool) -> str:
    return "true" if value else "false"


def str_to_bool(value: str) -> bool:
    return value == "true"


def bool_to_filename(value: bool) -> str:
    return "expired" if value else "active"


def get_all_instruments(currency: str, kind: str, expired: bool) -> pd.DataFrame:
    """Fetch all instruments for a given currency and type from Deribit API.

    Args:
        currency (str): The currency to fetch instruments for (e.g., "BTC", "ETH").
        kind (str): The type of instrument ("option" or "future").
        expired (bool): Whether to fetch expired or active instruments.

    Returns:
        pd.DataFrame: DataFrame containing the instrument information, or None if the request fails.
    """
    url = f"{BASE_URL}/public/get_instruments"
    params = {
        "currency": currency,
        "kind": kind,
        "expired": bool_to_str(expired),
    }

    try:
        response = requests.get(url, params)
        data = response.json()

        if "result" in data:
            df = pd.DataFrame(data["result"])
            df.to_csv(
                os.path.join(
                    CONFIG["base_dir"],
                    currency,
                    kind,
                    f"{currency}-{bool_to_filename(expired)}-{kind}-list.csv",
                ),
                index=False,
            )
            return df
        else:
            logger.error(f"Instrument list retrieval failed: {data}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Connection error during instrument list fetch: {e}")
        print("Retrying...")
        return get_all_instruments(currency, kind, expired)


def do_request(instrument: str, start_ms: int, end_ms: int, expired: bool) -> dict:
    """Make an API request return until success.

    Args:
        instrument (str): The name of the instrument to fetch trades for.
        start_ms (int): Start timestamp in milliseconds.
        end_ms (int): End timestamp in milliseconds.
        expired (bool): Whether to include expired trades.

    Returns:
        dict: The JSON response from the API.
    """
    url = f"{BASE_URL}/public/get_last_trades_by_instrument_and_time"
    params = {
        "instrument_name": instrument,
        "start_timestamp": start_ms,
        "end_timestamp": end_ms,
        "count": MAX_COUNT,
        "include_old": expired,
    }

    try:
        response = requests.get(url, params)
        res = response.json()
        return res
    except requests.exceptions.RequestException as e:
        logger.info(f"Rate limit exceeded, retrying after 1 second... {e}")
        sleep(1)
        return do_request(instrument, start_ms, end_ms, expired)
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        exit(-1)


def get_option_trades(
    instrument: str, start_ms: int, end_ms: int, expired: bool
) -> pd.DataFrame:
    """Fetch all trades for an option instrument within a time range.

    Args:
        instrument (str): The name of the option instrument.
        start_ms (int): Start timestamp in milliseconds.
        end_ms (int): End timestamp in milliseconds.
        expired (bool): Whether to include expired trades.

    Returns:
        pd.DataFrame: DataFrame containing all trades, or None if no trades found.
    """
    dfs = []
    current_end_ms = end_ms

    while True:
        res = do_request(instrument, start_ms, current_end_ms, expired)
        if not res.get("result", {}).get("trades"):
            break

        df = pd.DataFrame(res["result"]["trades"])
        if len(df) == 0:
            break

        dfs.append(df)

        if len(df) < MAX_COUNT:
            break

        current_end_ms = df["timestamp"].iloc[0] - 1
        if current_end_ms < start_ms:
            break

    if not dfs:
        return None

    df = pd.concat(dfs, ignore_index=True)
    df.sort_values(by="timestamp", inplace=True)
    return df


def get_all_option_trades(
    instrument: str, start_ms: int, end_ms: int, expired: bool
) -> None:
    """Fetch and save all trades for an option instrument.

    Args:
        instrument (str): The name of the option instrument.
        start_ms (int): Start timestamp in milliseconds.
        end_ms (int): End timestamp in milliseconds.
        expired (bool): Whether to include expired trades.
    """
    df = get_option_trades(instrument, start_ms, end_ms, expired)
    if df is None:
        logger.info(f"No trades found for {instrument} in the time range")
        return
    logger.info(f"Saving {len(df)} trades to {instrument}.csv")
    df.to_csv(
        os.path.join(
            CONFIG["base_dir"],
            CONFIG["currency"],
            "option",
            bool_to_filename(expired),
            f"{instrument}.csv",
        ),
    )


def get_future_trades(
    instrument: str, start_ms: int, end_ms: int, expired: bool
) -> pd.DataFrame:
    """Fetch all trades for a future instrument within a time range.

    Args:
        instrument (str): The name of the future instrument.
        start_ms (int): Start timestamp in milliseconds.
        end_ms (int): End timestamp in milliseconds.
        expired (bool): Whether to include expired trades.

    Returns:
        pd.DataFrame: DataFrame containing all trades, or None if no trades found.
    """
    dfs = []
    current_end_ms = end_ms

    while True:
        logger.info(f"Fetching {instrument} from {start_ms} to {current_end_ms}")
        res = do_request(instrument, start_ms, current_end_ms, expired)
        if not res.get("result", {}).get("trades"):
            break

        df = pd.DataFrame(res["result"]["trades"])
        if len(df) == 0:
            break

        dfs.append(df)

        if len(df) < MAX_COUNT:
            break

        current_end_ms = df["timestamp"].iloc[0] - 1
        if current_end_ms < start_ms:
            break

    if not dfs:
        return None

    df = pd.concat(dfs, ignore_index=True)
    df.sort_values(by="timestamp", inplace=True)
    return df


def mark_future(instrument: str, start_ms: int, end_ms: int, expired: bool) -> list:
    """A wrapper to memorize the start and end timestamps.

    Args:
        instrument (str): The name of the future instrument.
        start_ms (int): Start timestamp in milliseconds.
        end_ms (int): End timestamp in milliseconds.
        expired (bool): Whether to include expired trades.

    Returns:
        list: List containing [DataFrame, start_ms, end_ms].
    """
    logger.info(f"Handling {instrument} from {start_ms} to {end_ms}")
    return [
        get_future_trades(instrument, start_ms, end_ms, expired),
        start_ms,
        end_ms,
    ]


def get_all_future_trades(
    instrument: str, start_ms: int, end_ms: int, expired: bool
) -> None:
    """Fetch and save all trades for a future instrument, splitting the time range into daily chunks.

    Args:
        instrument (str): The name of the future instrument.
        start_ms (int): Start timestamp in milliseconds.
        end_ms (int): End timestamp in milliseconds.
        expired (bool): Whether to include expired trades.
    """
    # split the time range into multiple requests
    # use parrallel requests for multiple days
    time_range = end_ms - start_ms
    # ! TODO: make this adjustable, for some active futures, trades number can exceed 10000 in 1 minute
    chunk_size = 4 * 3600000  # 4 hours in ms
    chunks = []
    i = 0
    while True:
        chunks.append(start_ms + i * chunk_size)
        if start_ms + i * chunk_size > end_ms:
            break
        i += 1
    logger.info(f"Time range: {time_range}, start_ms: {start_ms}, end_ms: {end_ms}")
    logger.info(f"Fetching {len(chunks)} chunks for {instrument}")

    # Process chunks in smaller batches to manage memory
    batch_size = 20
    for i in range(0, len(chunks), batch_size):
        batch_chunks = chunks[i : i + batch_size]
        logger.info(
            f"Processing {instrument} batch {i // batch_size + 1} of {len(chunks) // batch_size}"
        )
        base_dir = CONFIG["base_dir"]
        currency = CONFIG["currency"]
        with ThreadPoolExecutor(
            max_workers=min(len(batch_chunks), MAX_WORKERS // 5)
        ) as executor:
            futures = [
                executor.submit(
                    mark_future,
                    instrument,
                    chunk,
                    min(chunk + chunk_size - 1, end_ms),
                    expired,
                )
                for chunk in batch_chunks
            ]
            for future in as_completed(futures):
                df, chunk_start_ms, chunk_end_ms = future.result()
                logger.info(
                    f"Processing {instrument} chunk {chunk_start_ms} to {chunk_end_ms}"
                )
                if df is not None:
                    output_path = os.path.join(
                        base_dir,
                        currency,
                        "future",
                        bool_to_filename(expired),
                        f"{instrument}-{chunk_start_ms}-{chunk_end_ms}.csv",
                    )
                    df.to_csv(output_path, index=False)
                    del df  # Explicitly delete DataFrame to free memory
                del future  # Clean up future object


def for_instruments(currency: str, kind: str, expired: bool) -> None:
    """Process all instruments of a given type for a currency.

    Args:
        currency (str): The currency to process instruments for.
        kind (str): The type of instrument ("option" or "future").
        expired (bool): Whether to process expired or active instruments.
    """
    if not expired:
        print(
            f"Notice: fetching active instruments, all trades after {FROZEN_TIME} are not included."
        )
    instument_list = get_all_instruments(currency, kind, expired)
    print(f"Discovered {len(instument_list)} {currency} {kind} instruments to process.")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_instrument = {}
        for row in instument_list.itertuples():
            instrument_name = row.instrument_name
            start_time_ms = row.creation_timestamp
            # if fetching non-expired instruments, limit end_time_ms
            end_time_ms = row.expiration_timestamp
            if not expired:
                end_time_ms = min(end_time_ms, int(FROZEN_TIME.timestamp() * 1000))

            future = executor.submit(
                get_all_option_trades if kind == "option" else get_all_future_trades,
                instrument=instrument_name,
                start_ms=start_time_ms,
                end_ms=end_time_ms,
                expired=expired,
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

    if CONFIG["save_parquet"]:
        print(f"Saving {CONFIG['currency']} {kind} data to parquet...")
        save_to_parquet(
            os.path.join(
                CONFIG["base_dir"],
                CONFIG["currency"],
                kind,
                bool_to_filename(expired),
            )
        )


def prepare_dir(base_dir: str, currency: str) -> None:
    """Create the directory structure for storing data.

    Args:
        base_dir (str): The base directory for storing data.
        currency (str): The currency to create directories for.
    """
    # directory structure:
    # base_dir/
    #   currency/
    #     option/
    #       expired/
    #       active/
    #     future/
    #       expired/
    #       active/
    # ...
    if CONFIG["instrument"] == "all":
        os.makedirs(
            os.path.join(base_dir, currency, "option", "expired"), exist_ok=True
        )
        os.makedirs(os.path.join(base_dir, currency, "option", "active"), exist_ok=True)
        os.makedirs(
            os.path.join(base_dir, currency, "future", "expired"), exist_ok=True
        )
        os.makedirs(os.path.join(base_dir, currency, "future", "active"), exist_ok=True)
    else:
        os.makedirs(
            os.path.join(base_dir, currency, CONFIG["instrument"], "expired"),
            exist_ok=True,
        )
        os.makedirs(
            os.path.join(base_dir, currency, CONFIG["instrument"], "active"),
            exist_ok=True,
        )


def save_to_parquet(dir_path: str) -> None:
    """Save all CSV files in the directory to a single Parquet file.

    Args:
        dir_path (str): The path to the directory containing the CSV files.
    """
    # read all csv files in the directory
    files = glob.glob(os.path.join(dir_path, "*.csv"))
    if not files:
        logger.warning(f"No CSV files found in {dir_path}")
        return

    # Read first file to get common columns
    first_df = pd.read_csv(files[0])
    common_columns = set(first_df.columns)

    # Find common columns across all files
    for file in files[1:]:
        df = pd.read_csv(file)
        common_columns &= set(df.columns)

    common_columns = list(common_columns)

    # Create parquet writer
    parquet_path = os.path.join(dir_path, "data.parquet")
    writer = None

    try:
        # Process each file in chunks
        for file in files:
            for chunk in pd.read_csv(file, usecols=common_columns, chunksize=100000):
                if writer is None:
                    writer = pd.io.parquet.PyArrowWriter(
                        parquet_path,
                        engine="pyarrow",
                        schema=chunk[common_columns].schema,
                    )
                writer.write(chunk[common_columns])

        if writer is not None:
            writer.close()

    except Exception as e:
        logger.error(f"Error saving parquet file: {e}")
        if writer is not None:
            writer.close()
        if os.path.exists(parquet_path):
            os.remove(parquet_path)
        raise


def main(args) -> None:
    """Main function to orchestrate the data fetching process.

    Args:
        args: Command line arguments containing configuration parameters.
    """
    CONFIG["currency"] = args.currency
    CONFIG["instrument"] = args.instrument
    CONFIG["expired"] = args.expired
    CONFIG["base_dir"] = args.base_dir
    CONFIG["save_parquet"] = args.save_parquet
    CONFIG["verbose"] = args.verbose
    prepare_dir(CONFIG["base_dir"], CONFIG["currency"])

    if CONFIG["verbose"]:
        logger.setLevel(logging.INFO)
        # add file handler
        logger.addHandler(
            logging.FileHandler(os.path.join(CONFIG["base_dir"], "deribit-fetcher.log"))
        )

    if CONFIG["instrument"] == "all" and CONFIG["expired"] == "all":
        for_instruments(CONFIG["currency"], "option", True)
        for_instruments(CONFIG["currency"], "future", True)
        for_instruments(CONFIG["currency"], "option", False)
        for_instruments(CONFIG["currency"], "future", False)
    elif CONFIG["instrument"] == "all":
        for_instruments(CONFIG["currency"], "option", str_to_bool(CONFIG["expired"]))
        for_instruments(CONFIG["currency"], "future", str_to_bool(CONFIG["expired"]))
    elif CONFIG["expired"] == "all":
        for_instruments(CONFIG["currency"], CONFIG["instrument"], True)
        for_instruments(CONFIG["currency"], CONFIG["instrument"], False)
    else:
        for_instruments(
            CONFIG["currency"], CONFIG["instrument"], str_to_bool(CONFIG["expired"])
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
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
    args = parser.parse_args()
    main(args)
