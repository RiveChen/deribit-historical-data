# Deribit Historical Data Fetcher

A Python tool for fetching historical trade data from Deribit API, supporting BTC/ETH, options/futures, and expired/non-expired instruments.

If you encounter any bugs, please report them!

Stars are appreciated if you find this tool helpful!

## Quick Start

```sh
# Clone the repository
git clone https://github.com/RiveChen/deribit-historical-data.git
cd deribit-historical-data

# Install dependencies
pip install -r requirements.txt

# Run with default settings (BTC expired options)
python -m deribit_fetcher
```

## Features

- **Multiple Currency Support**: Fetch data for BTC, ETH
- **Instrument Types**: Support for both options and futures
- **Data Organization**:
  - Separate directories for each currency and instrument type
  - Organized by expired/active status
  - CSV format for easy analysis
- **Efficient Data Collection**:
  - Multi-threaded data fetching
  - Progress tracking with tqdm
- **Flexible Configuration**:
  - Command-line interface
  - Verbose logging option

## Requirements

- Python 3.7+
- Dependencies:
  - requests
  - pandas
  - tqdm
  - pyarrow

## Usage

### Command Line Arguments

```text
python -m deribit_fetcher [options]

Options:
  --currency {BTC,ETH,USDC,USDT}   Currency to fetch (default: BTC)
  --instrument {option,future,all} Instrument type to fetch (default: option)
  --expired {true,false,all}       Fetch expired or active instruments (default: true)
  --base_dir DIR                   Base directory for saving data (default: ./data)
  --verbose                        Enable verbose logging
  -h, --help                       Show this help message
```

### Examples

```sh
# Fetch all expired ETH options
python -m deribit_fetcher --currency ETH --instrument option --expired true

# Fetch active BTC futures
python -m deribit_fetcher --currency BTC --instrument future --expired false

# Enable verbose logging
python -m deribit_fetcher --verbose
```

## Data Structure

The fetched data is organized in the following structure:

```text
data/
├── {currency}/
│   ├── option/
│   │   ├── expired/
│   │   │   └── {instrument}.csv
│   │   └── active/
│   │       └── {instrument}.csv
│   └── future/
│       ├── expired/
│       │   └── {instrument}-{start_ms}-{end_ms}.csv
│       └── active/
│           └── {instrument}-{start_ms}-{end_ms}.csv
└── deribit-fetcher.log
```

## Todo

- [x] Add a CLI interface
- [x] Retrieve non-expired options
- [x] Retrieve futures
- [x] Make it a package
- [ ] Sort `.csv` files into a `.parquet` file

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request or create an Issue.

## Acknowledgements

- [Deribit API](https://docs.deribit.com/)
