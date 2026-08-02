# Deribit Historical Data Fetcher

[![CI](https://github.com/RiveChen/deribit-historical-data/actions/workflows/ci.yml/badge.svg)](https://github.com/RiveChen/deribit-historical-data/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](https://www.python.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

> A resumable async downloader for historical **Futures** and **Options** trades from the [Deribit History API v2](https://docs.deribit.com/#public-get_last_trades_by_instrument).

> **Verification status:** correctness regressions, a 10000-row BTC-PERPETUAL sample, and one complete 10593-row expired BTC option pass. Full currency/kind datasets and 90 GB peak RSS are not yet certified. Use checkpoint-aware validation before treating an export as complete.

*If it helps, stars are appreciated!* ⭐

## tl;dr

``` shell
git clone https://github.com/RiveChen/deribit-historical-data.git
cd deribit-historical-data

# install `uv` if you haven't already, then
uv sync

# download available BTC option trade history:
uv run python -m deribit_fetcher.option

# download available BTC future trade history:
uv run python -m deribit_fetcher.future

# if you want to merge the downloaded JSONL to a single parquet file:
uv run python scripts/gen_parquet.py --type option
uv run python scripts/gen_parquet.py --type future
```

## Features

- **Checkpointed range download** — uses `trade_seq`-based chunking and final checkpoints so completeness can be checked explicitly
- **Bounded async fetch engine** — uses `asyncio`, bounded queues, and a configurable rate cap (20 RPS by default)
- **Resumable** — SQLite checkpoint database tracks progress, so partial downloads can be resumed
- **Signal-aware shutdown** — handles `SIGINT`/`SIGTERM` and flushes safely persisted batches before returning
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
| `CHUNK_SIZE` | `10000` | Trades requested per sequence range; confirm the history host's current limit before a long run |
| `MAX_RPS` | `20` | Requests per second limit |
| `MAX_WORKERS` | `40` | Max concurrent HTTP connections |

Data is stored under `./data/<CURRENCY>` by default. Pass `--base-dir PATH` to any command (`future`, `option`, `gen_parquet.py`, `validate_data.py`) to relocate it, e.g. `uv run python -m deribit_fetcher.future --base-dir /mnt/disk/deribit`.

## Usage

Capacity and duration depend on the currency, instruments, network, and the API's current dataset. The original README estimated roughly 10 GB for BTC options and 90 GB for BTC futures in May 2026; treat these only as planning inputs, not measured current results. Run on a volume with headroom and validate the resulting Parquet against its checkpoint database.

### 1. Fetch Future Trades

```bash
# Fetch available BTC futures history (default)
uv run python -m deribit_fetcher.future

# Fetch ETH futures with custom settings
CURRENCY=ETH uv run python -m deribit_fetcher.future
```

### 2. Fetch Option Trades

```bash
uv run python -m deribit_fetcher.option
```

### 3. Export to Parquet

The Parquet generator merges all JSONL files into a single compressed Parquet file with optional deduplication. The JSONL source files are kept and only a new `.parquet` file is created, so you need free space for both raw input and Parquet output. Run [`scripts/benchmark.py`](#benchmarks) to measure the compression ratio and peak RSS on your data.

```bash
# Merge all BTC future JSONL files into a single Parquet
uv run python scripts/gen_parquet.py --type future

# Merge all BTC option JSONL files
uv run python scripts/gen_parquet.py --type option

# Use lz4 compression (measure the speed/size trade-off on your data)
uv run python scripts/gen_parquet.py --type future --fast

# Skip deduplication and preserve duplicate input rows
uv run python scripts/gen_parquet.py --type future --no-dedup

# Tune the small-file thread pool (default: all CPU cores)
uv run python scripts/gen_parquet.py --type future --workers 8
```

The generator uses a two-phase strategy:

- **Small files** (`< --large-threshold-mb`, default 100 MB; typical options): read through a bounded thread-pool window (`--workers`), with deduplication applied only when enabled.
- **Large files** (`>= --large-threshold-mb`; typical perpetuals): default to single-process `mmap` streaming in fixed-size batches. Set `--stream-workers` to use a bounded process-pool block reader. Cross-batch dedup uses an exact per-instrument bitmap, so descending API responses and concurrently appended out-of-order chunks are handled without a Python object per trade.

Both paths bound outstanding reader results, but peak RSS still depends on batch/block size, worker count, schema, and sequence range. Measure the target dataset before choosing production settings.

All flags:

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--type` | (required) | `future` or `option` |
| `--workers` | CPU count | Thread-pool workers for the small-file phase |
| `--fast` | off | Use lz4 instead of zstd |
| `--no-dedup` | off | Skip `(instrument_name, trade_seq)` dedup |
| `--large-threshold-mb` | `100` | Files at or above this size use the streaming path |
| `--stream-batch-size` | `200000` | Rows per streaming batch for large files |
| `--stream-workers` | `0` | Process workers for large-file block reads; `0` selects single-process streaming |
| `--block-bytes` | `104857600` | Approximate block size for parallel large-file reads |

### 4. Validate Data

Checkpoint-aware streaming validation compares each instrument's unique `trade_seq` range with `future.db` / `option.db`, so it detects internal gaps, duplicates, missing heads/tails, and entirely missing instruments. It reports `COMPLETE` only for an exact match against a final checkpoint; active or otherwise non-final checkpoints are `UNKNOWN` rather than a false success. Exit codes are `0` complete, `1` incomplete, and `2` unknown. Aggregations remain streaming-safe, so the full Parquet file is not loaded into memory.

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

For detailed API behavior, see: [docs/api-reference.md](./docs/api-reference.md)

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

## Documentation

Design & developer docs live in [docs/](./docs/): [overview](./docs/overview.md), [architecture](./docs/architecture.md), [design decisions](./docs/design-decisions.md), [data model](./docs/data-model.md), [Deribit API notes](./docs/deribit-api.md), [operations](./docs/operations.md), and [development](./docs/development.md).

## Data Notes

- **Chunk boundary overlap**: Occasionally Deribit may return 1 overlapping trade at chunk boundaries. This is tolerated — duplicates can be removed during Parquet conversion by `(instrument_name, trade_seq)` dedup.
- **No-trade instruments**: Some early expired instruments have zero trades and are skipped automatically.
- **Trade schema**: Future and Option trades share the same fields. The Parquet generator uses a comprehensive 18-field union schema to capture every field seen in real API responses, including rare ones like `liquidation`, `block_trade_id`, `block_rfq_id`, `combo_id`, etc. Missing fields are automatically filled as null.

## Acknowledgments

Portions of the tooling, tests, and documentation were developed with assistance from [Claude](https://www.anthropic.com/claude) (Anthropic).
