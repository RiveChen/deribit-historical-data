# Deribit Historical Data Fetcher

This project fetches historical trade data for all expired option instruments from the Deribit API. The data is saved as CSV files for further analysis.

## TL;DR

Clone, install requirements and run.

```sh
git clone https://github.com/RiveChen/deribit-historical-data.git
cd deribit-historical-data
pip install -r requirements.txt
python deribit_fetcher.py
```

All you need is ~20min and ~3GB disk space.
If this helps, a star would be appreciated!

## Features

- Fetches all expired option instruments for a specified currency.
- Retrieves all trades for each instrument within its active time range.
- Saves the fetched data as CSV files.
- Utilizes multi-threading for efficient data fetching.

## Requirements

- Python 3.7+
- `requests` library
- `pandas` library
- `tqdm` library

## Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/RiveChen/deribit-historical-data.git
    cd deribit-historical-data
    ```

2. Install the required Python packages:

    ```sh
    pip install -r requirements.txt
    ```

## Usage

1. (optional) Set the desired currency in the `deribit_fetcher.py` file:

    ```python
    CURRENCY = "BTC"  # Change to your desired currency, e.g., "ETH"
    ```

2. Run the script (It will take about 20 minutes):

    ```sh
    python deribit_fetcher.py
    ```

3. The fetched data will be saved in the `./data/{CURRENCY}` directory.

## Todo

- [ ] Save as a single `.parquet` file.
- [x] Add a CLI interface.
- [x] Retrieve non-expired options.
- [x] Retrieve other instruments.
- [ ] Make it a package.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## Acknowledgements

- [Deribit API](https://docs.deribit.com/)
