import pandas as pd
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from deribit_fetcher.config import MAX_WORKERS, MAX_COUNT, CONFIG
from deribit_fetcher.logger import get_global_logger
from deribit_fetcher.utils import bool_to_filename
from deribit_fetcher.common import do_request

logger = get_global_logger()


def fetch_options_trades(
    option_list: pd.DataFrame, expired: bool, frozen_time: int = None
):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_info = {}
        for row in option_list.itertuples():
            instrument_name = row.instrument_name
            start_time_ms = row.creation_timestamp
            end_time_ms = row.expiration_timestamp
            if not expired:
                end_time_ms = min(end_time_ms, int(frozen_time.timestamp() * 1000))

            future = executor.submit(
                __get_all_option_trades,
                instrument=instrument_name,
                start_ms=start_time_ms,
                end_ms=end_time_ms,
                expired=expired,
            )
            future_info[future] = (instrument_name, start_time_ms, end_time_ms)

        with tqdm(total=len(future_info)) as pbar:
            for future in as_completed(future_info):
                instrument, start_ms, end_ms = future_info[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing {instrument}: {e}")
                finally:
                    pbar.update(1)


def __get_all_option_trades(
    instrument: str, start_ms: int, end_ms: int, expired: bool
) -> None:
    """Fetch and save all trades for an option instrument.

    Args:
        instrument (str): The name of the option instrument.
        start_ms (int): Start timestamp in milliseconds.
        end_ms (int): End timestamp in milliseconds.
        expired (bool): Whether to include expired trades.
    """
    df = __get_option_trades(instrument, start_ms, end_ms, expired)
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


def __get_option_trades(
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


if __name__ == "__main__":
    from deribit_fetcher.config import *
    from deribit_fetcher.common import get_all_instruments
    from deribit_fetcher.utils import prepare_dir

    prepare_dir(CONFIG["base_dir"], CONFIG["currency"], "option")

    option_list = get_all_instruments(currency="BTC", kind="option", expired=True)
    fetch_options_trades(option_list, expired=True)
