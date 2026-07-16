# Deribit Historical Data Fetcher

> An async scraper for downloading full historical trade data from the [Deribit History API v2](https://docs.deribit.com/#public-get_last_trades_by_instrument) for both **Futures** and **Options**.

*If it helps, stars are appreciated!* ⭐

## tl;dr

``` shell
git clone https://github.com/RiveChen/deribit-historical-data.git
cd deribit-historical-data

# install `uv` if you haven't already, then
uv sync

# for all BTC option trades data:
uv run python -m deribit_fetcher.option
# note: it may take 1-2 hours and ~10GB disk space for BTC options

# for all BTC future trades data:
uv run python -m deribit_fetcher.future
# note: it may take 3-4 hours and ~90GB disk space for BTC futures 


# if you want to merge the downloaded JSONL to a single parquet file:
uv run python scripts/gen_parquet.py --type option
uv run python scripts/gen_parquet.py --type future
```

## Features

- **Full history download** — fetches every single trade, not just recent ones, using `trade_seq`-based chunking
- **Async & fast** — up to 20 RPS (the API's limit) via `asyncio`, with a bounded producer-consumer engine and per-second rate limiting
- **Resumable** — SQLite checkpoint database tracks progress, so partial downloads can be resumed
- **Graceful shutdown** — handles `SIGINT`/`SIGTERM` cleanly, preserving all data collected so far
- **JSONL output** — raw data saved as newline-delimited JSON, one trade per line
- **Parquet export** — utility script to merge all JSONL files into a single compressed Parquet file (with dedup)
- **Data validation** — streaming Parquet validation (per-instrument `trade_seq` gap detection with a gap-distribution histogram, plus schema & time-range summary) without loading the full file into memory
- **Both currency & instrument kinds** — supports BTC and ETH, Futures and Options

## Requirements

- Python 3.10+
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

Two settings are read from **environment variables**:

| Variable | Default | Description |
| ---------- | --------- | ------------- |
| `CURRENCY` | `BTC` | Currency to fetch (`BTC` or `ETH`) |
| `HTTP_PROXY` / `HTTPS_PROXY` | (none) | Proxy URL, e.g. `http://127.0.0.1:7890` |

Set them inline or export beforehand:

```bash
CURRENCY=ETH uv run python -m deribit_fetcher.future
```

The remaining tuning knobs live in [`src/deribit_fetcher/config.py`](./src/deribit_fetcher/config.py) — edit the source to change them:

| Setting | Default | Description |
| ---------- | --------- | ------------- |
| `CHUNK_SIZE` | `10000` | Trades per API request (Deribit max is 10000) |
| `MAX_RPS` | `20` | Requests per second limit |
| `MAX_WORKERS` | `40` | Max concurrent HTTP connections |

## Usage

You will need ~10 GB for BTC option and ~90 GB for BTC future trades raw data (as of May 2026). Make sure you have enough disk space with the `data/` directory.

It will take about 1 hour to fetch BTC option trades and about 4 hours to fetch all BTC future trades, please be patient.

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

The Parquet generator merges all JSONL files into a single compressed Parquet file with dedup support. The JSONL source files are kept and only a new `.parquet` file is created, so you need free space for the raw JSONL plus the (much smaller) Parquet output. Parquet + zstd is typically several times smaller than the source JSONL — run [`scripts/benchmark.py`](#benchmarks) to measure the exact ratio on your data.

```bash
# Merge all BTC future JSONL files into a single Parquet
uv run python scripts/gen_parquet.py --type future

# Merge all BTC option JSONL files
uv run python scripts/gen_parquet.py --type option

# Use lz4 compression (faster, ~10-15% larger file)
uv run python scripts/gen_parquet.py --type future --fast

# Skip deduplication (faster, but may contain duplicate rows)
uv run python scripts/gen_parquet.py --type future --no-dedup

# Tune the small-file thread pool (default: all CPU cores)
uv run python scripts/gen_parquet.py --type future --workers 8
```

The generator uses a two-phase strategy:

- **Small files** (`< --large-threshold-mb`, default 100 MB; typical options): read in parallel with a thread pool (`--workers`), one file per worker, deduped per file.
- **Large files** (`>= --large-threshold-mb`; typical perpetuals): stream-read in the main thread in fixed-size batches (`--stream-batch-size` rows, default 200000) using `mmap` for zero-copy line splitting. Because `trade_seq` is monotonic within a file, cross-batch dedup is a simple `trade_seq > max_seen` filter, so memory stays bounded regardless of file size (no OOM on 90 GB perpetual files).

All flags:

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--type` | (required) | `future` or `option` |
| `--workers` | CPU count | Thread-pool workers for the small-file phase |
| `--fast` | off | Use lz4 instead of zstd (faster, ~10-15% larger) |
| `--no-dedup` | off | Skip `(instrument_name, trade_seq)` dedup |
| `--large-threshold-mb` | `100` | Files at or above this size use the streaming path |
| `--stream-batch-size` | `200000` | Rows per streaming batch for large files |

### 4. Validate Data

Streaming Parquet validation — detects per-instrument `trade_seq` gaps and prints a gap-distribution histogram, using streaming-safe aggregations so the full file is never loaded into memory (avoids OOM on a 90 GB `future.parquet`).

```bash
# Validate both future and option Parquet files
uv run python scripts/validate_data.py

# Validate only a specific type
uv run python scripts/validate_data.py --type future
```

### Output Structure

``` txt
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

``` txt
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
├── gen_parquet.py       # JSONL → Parquet conversion (dedup, streaming)
├── validate_data.py     # Post-download integrity validation (gap detection)
└── benchmark.py         # Reproducible throughput / memory / compression benchmarks
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

For detailed API behavior, see: [api-reference.md](./api-reference.md)

## Benchmarks

Performance numbers here are reproducible — run the benchmark yourself:

```bash
# Synthetic quick smoke test (seconds)
uv run python scripts/benchmark.py --quick

# Benchmark your real downloaded data (most credible)
uv run python scripts/benchmark.py --data-dir data/BTC/option
uv run python scripts/benchmark.py --data-dir data/BTC/future --large-threshold-mb 100
```

It measures input rows/s and MB/s, thread-pool scaling, dedup cost, zstd vs lz4, streaming-batch size, JSONL→Parquet compression ratio, and peak RSS (each case runs in an isolated subprocess). Results are written to `benchmark_results/BENCHMARK.md`.

_Replace the placeholders below with numbers from your machine (`benchmark_results/BENCHMARK.md`):_

| Case | Rows/s | MB/s in | Compress× | Peak RSS (MB) |
|------|-------:|--------:|----------:|--------------:|
| small files, workers=1, zstd | — | — | — | — |
| small files, workers=N, zstd | — | — | — | — |
| dedup off | — | — | — | — |
| lz4 (`--fast`) | — | — | — | — |
| large file, streaming | — | — | — | — |

## Data Notes

- **Chunk boundary overlap**: Occasionally Deribit may return 1 overlapping trade at chunk boundaries. This is tolerated — duplicates can be removed during Parquet conversion by `(instrument_name, trade_seq)` dedup.
- **No-trade instruments**: Some early expired instruments have zero trades and are skipped automatically.
- **Trade schema**: Future and Option trades share the same fields. The Parquet generator uses a comprehensive 18-field union schema to capture every field seen in real API responses, including rare ones like `liquidation`, `block_trade_id`, `block_rfq_id`, `combo_id`, etc. Missing fields are automatically filled as null.
