# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "requests",
#     "tqdm",
# ]
# ///

import os
import time
import glob
import concurrent.futures

import requests
import pandas as pd
from tqdm import tqdm

BASE_DIR = "data"
TRADES_DIR = "option"
LIST_FILE = "option-list.csv"
MERGED_FILE = "options_merged.parquet"
JOINED_FILE = "options.parquet"


def get_list():
    if os.path.exists(f"{BASE_DIR}/{LIST_FILE}"):
        df = pd.read_csv(f"{BASE_DIR}/{LIST_FILE}")
    else:
        url = f"https://history.deribit.com/api/v2/public/get_instruments"
        params = {
            "currency": "BTC",
            "kind": "option",
            "expired": "true",
        }
        response = requests.get(url, params=params)
        expired_df = pd.DataFrame(response.json()["result"])

        params["expired"] = "false"
        response = requests.get(url, params=params)
        active_df = pd.DataFrame(response.json()["result"])

        df = pd.concat([expired_df, active_df])
        df.to_csv(f"{BASE_DIR}/{LIST_FILE}", index=False)

    df = df[["instrument_name", "creation_timestamp", "expiration_timestamp"]]
    result = list(df.itertuples(index=False, name=None))
    return result


def get_trades(url, params):
    while True:
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                break
            else:
                # 429 Too Many Requests
                # print(f"{response.status_code}: {response.text}")
                time.sleep(1)
        except Exception as e:
            # Max retries exceeded with url [SSL: UNEXPECTED_EOF_WHILE_READING]
            # print(e)
            time.sleep(1)

    res = response.json()["result"]
    # res["has_more"] is ignored because it does not return the correct value for some instruments
    return res["trades"]


def get_data_by_seq_recur(name, start_seq=1, end_seq=10000, count=10000):
    url = f"https://history.deribit.com/api/v2/public/get_last_trades_by_instrument"
    params = {
        "instrument_name": name,
        "start_seq": start_seq,
        "end_seq": end_seq,
        "count": count,
    }

    trades = get_trades(url, params)
    if len(trades) == count:
        trades += get_data_by_seq_recur(name, end_seq + 1, end_seq + count, count)
    return trades


def get_data_by_ts_recur(name, start_ts, end_ts, count=10000):
    url = f"https://history.deribit.com/api/v2/public/get_last_trades_by_instrument_and_time"
    params = {
        "instrument_name": name,
        "start_timestamp": start_ts,
        "end_timestamp": end_ts,
        "count": count,
    }

    trades = get_trades(url, params)
    if len(trades) == count:
        new_end_ts = min(trade["timestamp"] for trade in trades) - 1
        trades += get_data_by_ts_recur(name, start_ts, new_end_ts, count)
    return trades


def process_trades(name, trades):
    if len(trades) == 0:
        return

    df = pd.DataFrame(trades)
    df.set_index("trade_seq", inplace=True)
    df.sort_index(inplace=True)
    df.to_csv(f"{BASE_DIR}/{TRADES_DIR}/{name}.csv", index=True)


TRADE_DTYPE_SPEC = {
    "trade_seq": "int64",
    "trade_id": "int64",
    "timestamp": "int64",
    "tick_direction": "int8",
    "price": "float64",
    "mark_price": "float64",
    "iv": "float32",
    "instrument_name": "str",
    "index_price": "float64",
    "direction": "category",
    "contracts": "float64",
    "amount": "float64",
    "block_trade_leg_count": "float32",
    "block_trade_id": "str",
    "block_rfq_id": "float32",
    "combo_id": "str",
    "combo_trade_id": "Int32",
}

LIST_DTYPE_SPEC = {
    "tick_size_steps": "category",
    "tick_size": "category",
    "taker_commission": "category",
    "strike": "float64",
    "settlement_period": "category",
    "settlement_currency": "category",
    "rfq": "bool",
    "quote_currency": "category",
    "price_index": "category",
    "option_type": "category",
    "min_trade_amount": "category",
    "maker_commission": "category",
    "kind": "category",
    "is_active": "bool",
    "instrument_name": "str",
    "instrument_id": "int64",
    "expiration_timestamp": "int64",
    "creation_timestamp": "int64",
    "counter_currency": "category",
    "contract_size": "float64",
    "block_trade_tick_size": "float64",
    "block_trade_min_trade_amount": "float64",
    "block_trade_commission": "float64",
    "base_currency": "category",
    "base_currency_conversion_rate": "float64",
    "base_currency_conversion_rate_exponent": "Int64",
    "base_currency_conversion_rate_precision": "Int64",
}


def merge_all_csv():
    print("option merge started")

    # Find all trade CSV files in the directory
    csv_files = glob.glob(f"{BASE_DIR}/{TRADES_DIR}/*.csv")

    dfs = []
    for file in tqdm(csv_files, desc="Reading CSV files"):
        try:
            df = pd.read_csv(file, dtype=TRADE_DTYPE_SPEC, index_col=0)
            dfs.append(df)
        except Exception as e:
            print(f"error reading {file}: {e}")
            print(df.info())
            exit()

    if not dfs:
        print("no data to merge")
        return

    print("concatenating dataframes")
    merged_df = pd.concat(dfs, axis=0, ignore_index=True)
    merged_df.to_parquet(f"{BASE_DIR}/{MERGED_FILE}", compression="zstd")

    print("option merge completed")


def join_with_list():
    print("merging with list")

    list_df = pd.read_csv(f"{BASE_DIR}/{LIST_FILE}", dtype=LIST_DTYPE_SPEC)
    merged_df = pd.read_parquet(f"{BASE_DIR}/{MERGED_FILE}")
    joined_df = merged_df.merge(list_df, on="instrument_name", how="left")
    joined_df.set_index("trade_id", inplace=True)
    joined_df.sort_index(inplace=True)
    joined_df.to_parquet(f"{BASE_DIR}/{JOINED_FILE}", compression="zstd")

    print("join completed")


def fetch():
    print("option fetch started")

    if not os.path.exists(f"{BASE_DIR}/{TRADES_DIR}"):
        os.makedirs(f"{BASE_DIR}/{TRADES_DIR}")

    list = get_list()
    # list.reverse()
    print("Total options: ", len(list))

    MAX_WORKERS = 20  # Set your desired concurrency limit here

    def fetch_and_process(item):
        name = item[0]
        trades = get_data_by_ts_recur(name, item[1], item[2])
        process_trades(name, trades)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_and_process, item) for item in list]

        for future in tqdm(concurrent.futures.as_completed(futures), total=len(list)):
            future.result()

    # print(len(get_data_by_seq("BTC-28JUN24-100000-C")))
    # print(len(get_data_by_seq("BTC-27DEC24-100000-C")))
    # btc_tuple = next((item for item in list if item[0] == "BTC-27DEC24-100000-C"), None)
    # print(btc_tuple)
    # print(len(get_data_by_ts(btc_tuple[0], btc_tuple[1], btc_tuple[2])))

    print("option fetch completed")


if __name__ == "__main__":
    fetch()
    # merge_all_csv()
    # join_with_list()
