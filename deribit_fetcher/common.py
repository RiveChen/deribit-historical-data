import os
import pandas as pd
import requests
from time import sleep

from deribit_fetcher.config import BASE_URL, MAX_COUNT, CONFIG
from deribit_fetcher.utils import bool_to_str, bool_to_filename
from deribit_fetcher.logger import get_global_logger

logger = get_global_logger()


def get_all_instruments(currency: str, kind: str, expired: bool) -> pd.DataFrame:
    """Fetch all instruments for a given currency and type from Deribit API.

    Args:
        currency (str): The currency to fetch instruments for (e.g., "BTC", "ETH").
        kind (str): The type of instrument ("option" or "future").
        expired (bool): Whether to fetch expired or active instruments.

    Returns:
        dict: Dictionary containing the instrument information, or None if the request fails.
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
            # df.sort_values(by="timestamp", inplace=True)
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
