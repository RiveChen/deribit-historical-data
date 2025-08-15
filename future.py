# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pandas",
#     "requests",
#     "tqdm",
# ]
# ///

import os
import time
import concurrent.futures

import requests
import pandas as pd
from tqdm import tqdm


BASE_DIR = "data"
TRADES_DIR = "future"
LIST_FILE = "future-list.csv"


def get_list():
    if os.path.exists(f"{BASE_DIR}/{LIST_FILE}"):
        df = pd.read_csv(f"{BASE_DIR}/{LIST_FILE}")
    else:
        url = f"https://history.deribit.com/api/v2/public/get_instruments"
        params = {
            "currency": "BTC",
            "kind": "future",
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
    cur_ts = int(time.time()) * 1000
    df.loc[df["expiration_timestamp"] > cur_ts, "expiration_timestamp"] = cur_ts
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


def get_data_by_seq(name, start_seq=1, end_seq=10000, count=10000):
    url = f"https://history.deribit.com/api/v2/public/get_last_trades_by_instrument"
    params = {
        "instrument_name": name,
        "start_seq": start_seq,
        "end_seq": end_seq,
        "count": count,
    }

    trades = get_trades(url, params)
    return trades


def get_latest_seq_by_ts(name, end_ts):
    url = f"https://history.deribit.com/api/v2/public/get_last_trades_by_instrument_and_time"
    params = {
        "instrument_name": name,
        "end_timestamp": end_ts,
        "count": 1,
    }

    trades = get_trades(url, params)
    if len(trades) == 0:
        return 0
    return trades[0]["trade_seq"]


def process_trades(name, trades, start_seq, end_seq):
    if len(trades) == 0:
        return

    df = pd.DataFrame(trades)
    df.set_index("trade_seq", inplace=True)
    df.sort_index(inplace=True)
    df.to_csv(f"{BASE_DIR}/{TRADES_DIR}/{name}_{start_seq}-{end_seq}.csv", index=True)


def main() -> None:
    print("future fetch started")

    if not os.path.exists(f"{BASE_DIR}/{TRADES_DIR}"):
        os.makedirs(f"{BASE_DIR}/{TRADES_DIR}")

    list = get_list()
    # list.reverse()
    print("Total futures: ", len(list))

    for i, item in enumerate(list):
        latest_seq = get_latest_seq_by_ts(item[0], item[2])
        if latest_seq == 0:
            print(f"( {i+1}/{len(list)}) No data for {item[0]}")
            continue
        ranges = [
            (item[0], start, min(start + 9999, latest_seq))
            for start in range(1, latest_seq, 10000)
        ]
        print(f"( {i+1}/{len(list)}) Fetching {item[0]}")

        MAX_WORKERS = 100

        def fetch_and_process(item):
            name = item[0]
            trades = get_data_by_seq(name, item[1], item[2])
            process_trades(name, trades, item[1], item[2])

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(fetch_and_process, item) for item in ranges]
            for future in tqdm(
                concurrent.futures.as_completed(futures), total=len(ranges)
            ):
                future.result()

    # print(get_latest_seq_by_ts("BTC-21MAR25", 1755222207000))

    print("future fetch completed")


if __name__ == "__main__":
    main()
