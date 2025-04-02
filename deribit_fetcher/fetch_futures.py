import os
from tqdm import tqdm
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from deribit_fetcher.config import MAX_WORKERS, MAX_COUNT, CONFIG
from deribit_fetcher.logger import get_global_logger
from deribit_fetcher.utils import bool_to_filename
from deribit_fetcher.common import do_request

logger = get_global_logger()


def fetch_futures_trades(
    future_list: pd.DataFrame, expired: bool, frozen_time: int = None
):
    with tqdm(total=len(future_list)) as pbar:
        for row in future_list.itertuples():
            instrument_name = row.instrument_name
            start_time_ms = row.creation_timestamp
            end_time_ms = row.expiration_timestamp
            if not expired:
                end_time_ms = min(end_time_ms, int(frozen_time.timestamp() * 1000))
            __get_all_future_trades(
                instrument_name, start_time_ms, end_time_ms, expired
            )
            pbar.update(1)


def __get_all_future_trades(
    instrument: str, start_ms: int, end_ms: int, expired: bool
) -> None:
    # 1. split the time range into chunks of day
    chunks = []
    delta = 60 * 60 * 1000
    tmp_end_ms = start_ms
    while tmp_end_ms < end_ms:
        chunks.append((tmp_end_ms, min(tmp_end_ms + delta - 1, end_ms)))
        tmp_end_ms += delta

    # 2. fetch the trades for each chunk parrallelly
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_info = {}
        for start_ms, end_ms in chunks:
            future = executor.submit(
                __get_future_trades, instrument, start_ms, end_ms, expired
            )
            future_info[future] = (instrument, start_ms, end_ms)

        with tqdm(total=len(future_info)) as pbar:
            for future in as_completed(future_info):
                instrument, start_ms, end_ms = future_info[future]
                try:
                    df = future.result()
                    # 3. save the trades of each chunk to a csv file
                    if df is not None:
                        df.to_csv(
                            os.path.join(
                                CONFIG["base_dir"],
                                CONFIG["currency"],
                                "future",
                                bool_to_filename(expired),
                                f"{instrument}_{start_ms}_{end_ms}.csv",
                            ),
                            index=False,
                        )
                except Exception as e:
                    logger.error(f"Error processing {instrument}: {e}")
                finally:
                    pbar.update(1)


def __get_future_trades(
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


if __name__ == "__main__":
    from deribit_fetcher.config import *
    from deribit_fetcher.common import get_all_instruments
    from deribit_fetcher.utils import prepare_dir
    from datetime import datetime

    prepare_dir(CONFIG["base_dir"], CONFIG["currency"], "future")

    future_list = get_all_instruments(currency="BTC", kind="future", expired=False)
    fetch_futures_trades(future_list, expired=False, frozen_time=datetime.now())
