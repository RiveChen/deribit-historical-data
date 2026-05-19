# Deribit Historical Data Fetcher

> An async scraper for downloading full historical trade data from the [Deribit History API v2](https://docs.deribit.com/#public-get_last_trades_by_instrument) for both **Futures** and **Options**.

## Features

- **Full history download** — fetches every single trade, not just recent ones, using `trade_seq`-based chunking
- **Async & fast** — up to 20 RPS (limited by API) with configurable concurrency via `asyncio`
- **Resumable** — SQLite checkpoint database tracks progress, so partial downloads can be resumed
- **Graceful shutdown** — handles `SIGINT`/`SIGTERM` cleanly, preserving all data collected so far
- **JSONL output** — raw data saved as newline-delimited JSON, one trade per line
- **Parquet export** — utility script to merge all JSONL files into a single compressed Parquet file (with dedup)
- **Data validation** — post-download integrity checks (gap detection, duplicates, schema analysis)
- **Both currency & instrument kinds** — supports BTC and ETH, Futures and Options

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Installation

```bash
# Clone the repository
git clone https://github.com/RiveChen/deribit-historical-data.git
cd deribit-historical-data

# Create virtual environment and install dependencies with uv
uv sync

# Or with pip
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Configuration

All settings are managed via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `CURRENCY` | `BTC` | Currency to fetch (`BTC` or `ETH`) |
| `CHUNK_SIZE` | `10000` | Trades per API request (Deribit max is 10000) |
| `MAX_RPS` | `20` | Requests per second limit |
| `MAX_WORKERS` | `40` | Max concurrent HTTP connections |
| `HTTP_PROXY` / `HTTPS_PROXY` | (none) | Proxy URL, e.g. `http://127.0.0.1:7890` |

You can set them inline or export beforehand:

```bash
CURRENCY=ETH MAX_RPS=10 uv run python -m deribit_fetcher.future
```

## Usage

You will need ~10 GB for BTC option and ~90 GB for BTC future trades raw data (as of May 2026). Make sure you have enough disk space with the `data/` directory.

It will take about 1 hours to fetching BTC option trades and about 4 hours to fetching all BTC future trades, please be patient.

### 1. Fetch Future Trades

```bash
# Fetch all BTC futures (default)
uv run python -m deribit_fetcher.future

# Fetch ETH futures with custom settings
CURRENCY=ETH uv run python -m deribit_fetcher.future
```

### 2. Fetch Option Trades

```bash
uv run python -m deribit_fetcher.option
```

### 3. Export to Parquet

The Parquet generator merges all JSONL files into a single compressed Parquet file with dedup support. The JSONL source files are kept and only a new `.parquet` file is created as output, so you need enough free disk space for both the raw JSONL and the resulting Parquet (roughly 1:1 ratio, e.g. ~10 GB for options or ~90 GB for futures as of May 2026).

```bash
# Merge all BTC future JSONL files into a single Parquet
uv run python scripts/gen_parquet.py --type future

# Merge all BTC option JSONL files
uv run python scripts/gen_parquet.py --type option

# Use lz4 compression (faster, slightly larger file)
uv run python scripts/gen_parquet.py --type future --fast

# Parallel block processing (default: all CPU cores)
uv run python scripts/gen_parquet.py --type future --stream-workers 8

# Skip deduplication (faster, but may contain duplicate rows)
uv run python scripts/gen_parquet.py --type future --no-dedup
```

The generator uses a two-phase strategy:
- **Small files** (<100 MB, typical options): processed in parallel using a thread pool
- **Large files** (>=100 MB, typical perpetuals): split into `\n`-aligned byte blocks and processed in parallel using a process pool (`--stream-workers`), achieving near-SSD read speeds by saturating disk queue depth. The single-threaded fallback (`--stream-workers 1`) uses mmap for zero-copy batch splitting.

Performance tips:
- For a single large perpetual file: `--stream-workers <N>` defaults to all CPU cores
- Block size can be tuned: `--block-bytes 268435456` (256 MB default, smaller = finer granularity)
- Trade space for speed: `--fast` (lz4 instead of zstd, ~10-15% larger file)

### 4. Validate Data

Check data integrity after download — detects gaps, duplicates, and reports per-instrument statistics.

```bash
# Validate both future and option data (JSONL + Parquet)
uv run python scripts/validate_data.py

# Validate only JSONL files
uv run python scripts/validate_data.py --mode jsonl

# Validate only a specific type
uv run python scripts/validate_data.py --type future
```

### Output Structure

```
data/
└── {CURRENCY}/
    ├── future/
    │   ├── BTC-27MAR26.jsonl     # One file per instrument
    │   └── ...
    ├── option/
    │   ├── BTC-27MAR26-70000-C.jsonl
    │   └── ...
    ├── future.db                  # Progress checkpoint (SQLite)
    ├── option.db
    ├── future.parquet             # Generated by gen_parquet.py
    └── option.parquet
```

## Project Structure

```
src/deribit_fetcher/
├── __init__.py          # Package version
├── client.py            # Deribit API client (rate limiting, retries)
├── config.py            # Configuration (dataclass + env vars)
├── engine.py            # Generic async producer-consumer engine
├── future.py            # Future data fetcher (entry point)
├── option.py            # Option data fetcher (entry point)
├── progress.py          # SQLite checkpoint database
├── storage.py           # JSONL file writer
└── log.py               # Logging setup (tqdm-compatible)

scripts/
├── gen_parquet.py       # JSONL → Parquet conversion
├── validate_data.py     # Post-download integrity validation
└── test_real_api.py     # Deribit API behavior testing tool
```

## How It Works

### Future Fetch Strategy

1. Fetch all future instruments via `get_instruments`
2. Get the latest `trade_seq` for each instrument
3. Partition the seq range [1, last_seq] into fixed chunks of `CHUNK_SIZE`
4. Concurrently fetch all chunks using a producer-consumer pattern
5. Each completed chunk is written to JSONL and its progress is recorded in SQLite
6. On completion, chunks and instrument metadata are finalized (skipped on restart)

### Option Fetch Strategy

1. Fetch all option instruments
2. For each incomplete option, start from `last_no + 1` (resume offset)
3. Fetch chunks sequentially via an `on_success` callback that enqueues the next range
4. Write to JSONL, update DB progress with `MAX(last_no, ?)` to prevent rollback
5. Mark as complete when there are no more trades left (expired instruments only)

### Resumability

- **SQLite checkpoint database** tracks which chunks are done
- On restart, already-completed chunks/instruments are skipped
- `MAX(last_no, ?)` guard in option progress prevents regression on crash recovery

For detailed API behavior (e.g. `has_more` semantics, chunk boundary overlap), see [api-reference.md](api-reference.md).

## Data Notes

- **Chunk boundary overlap**: Occasionally Deribit may return 1 overlapping trade at chunk boundaries. This is tolerated — duplicates can be removed during Parquet conversion by `(instrument_name, trade_seq)` dedup.
- **No-trade instruments**: Some early expired instruments have zero trades and are skipped automatically.
- **Trade schema**: Future and Option trades share the same fields (17-field union schema confirmed by real API testing). The Parquet generator uses a comprehensive schema to capture all fields, including rare ones like `liquidation`, `block_trade_id`, `block_rfq_id`, `combo_id`, etc. Missing fields are automatically filled as null.
