import deribit_fetcher

deribit_fetcher.DEFAULT_CONFIG = {
    "currency": "BTC",
    "instrument": "future",
    "expired": "false",
    "base_dir": "/Volumes/Extend/tmp/",
    "save_parquet": True,
    "verbose": True,
}

deribit_fetcher.main()
