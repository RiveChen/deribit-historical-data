# Operations

How to install, configure, run, and troubleshoot the tool. For user-facing quick start, see the root [README](../README.md); this page is the operational detail.

## Install

```bash
git clone https://github.com/RiveChen/deribit-historical-data.git
cd deribit-historical-data
uv sync            # recommended
# or: python -m venv .venv && source .venv/bin/activate && pip install -e .
```

Requires Python 3.10+.

## Configuration

### Environment variables (runtime)

| Variable | Default | Description |
|----------|---------|-------------|
| `CURRENCY` | `BTC` | Currency to fetch: `BTC` or `ETH`. Determines all `data/<CCY>/…` paths. |
| `HTTP_PROXY` / `HTTPS_PROXY` | (none) | Proxy URL, e.g. `http://127.0.0.1:7890`. Lowercase variants also honored. SOCKS needs the `httpx[socks]` extra (already included). |

```bash
CURRENCY=ETH uv run python -m deribit_fetcher.future
```

### Source-level knobs (`src/deribit_fetcher/config.py`)

These are `Config` dataclass fields, **not** environment variables — edit the source to change them:

| Setting | Default | Description |
|---------|---------|-------------|
| `CHUNK_SIZE` | `10000` | Trades per API request (Deribit max). |
| `MAX_RPS` | `20` | Requests/second cap. |
| `MAX_WORKERS` | `40` | Max concurrent HTTP connections in the pool. |

Engine worker counts and queue sizes are module-level constants in `future.py` / `option.py` (`MAX_WORKER_TASKS`, `WRITE_BATCH_SIZE`, `TASK_QUEUE_SIZE`, `STORAGE_QUEUE_SIZE`).

### Data directory (`--base-dir`)

By default all data lives under `./data/<CURRENCY>`. Every command (`future`, `option`, `gen_parquet.py`, `validate_data.py`) accepts `--base-dir PATH` to relocate that root — e.g. onto an external drive. When set, the JSONL directories, `*.db`, and `*.parquet` all live directly under `PATH`.

```bash
uv run python -m deribit_fetcher.future --base-dir /mnt/disk/deribit
uv run python scripts/gen_parquet.py --type future --base-dir /mnt/disk/deribit
uv run python scripts/validate_data.py --base-dir /mnt/disk/deribit
```

## Running

### 1. Fetch trades → JSONL

```bash
uv run python -m deribit_fetcher.future     # futures (default BTC)
uv run python -m deribit_fetcher.option     # options
CURRENCY=ETH uv run python -m deribit_fetcher.future
```

Rough cost (BTC, as of 2026): options ≈ 1 h and ~10 GB; futures ≈ 4 h and ~90 GB. Both are **resumable** — re-run after an interruption and it continues from the SQLite checkpoint. `Ctrl-C` (SIGINT) triggers a graceful shutdown that flushes buffered data first.

### 2. Merge JSONL → Parquet

```bash
uv run python scripts/gen_parquet.py --type future
uv run python scripts/gen_parquet.py --type option
```

Flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--type` | (required) | `future` or `option`. |
| `--no-dedup` | off | Skip `(instrument_name, trade_seq)` dedup. |
| `--workers` | CPU count | Thread-pool workers for the small-file phase. |
| `--fast` | off | lz4 instead of zstd (~20% less CPU, ~10–15% larger). |
| `--large-threshold-mb` | `100` | Files at/above this size use the streaming/parallel path. |
| `--stream-batch-size` | `200000` | Rows per streaming batch for large files. |
| `--stream-workers` | `0` | `>0` reads large files in parallel via a **process pool** (true parallelism); `0` uses single-thread mmap streaming. |
| `--block-bytes` | `100 MB` | Byte-block size for parallel large-file reading. |
| `--base-dir` | (none) | Override the data directory (default `./data/<CURRENCY>`). |

JSONL source files are kept; you need room for both JSONL and the (smaller) Parquet.

### 3. Validate

```bash
uv run python scripts/validate_data.py                 # both kinds
uv run python scripts/validate_data.py --type future   # future | option | both
```

Prints per-instrument row counts, `trade_seq` ranges, a gap flag, and — for instruments with gaps — a bucketed gap histogram. Streaming aggregations keep memory bounded even on a 90 GB Parquet.

### 4. Benchmark (optional)

```bash
uv run python scripts/benchmark.py --quick                         # synthetic smoke test
uv run python scripts/benchmark.py --data-dir data/BTC/option      # real data
```

Writes `benchmark_results/BENCHMARK.md`. See the root README's Benchmarks section.

## Disk planning

- Budget for **JSONL + Parquet simultaneously** during the merge (JSONL dominates).
- WAL mode creates `*.db-wal` / `*.db-shm` alongside each `.db`; they're checkpointed automatically.

## Troubleshooting

| Symptom | Likely cause / action |
|---------|----------------------|
| Frequent `429` / "Rate limit hit" warnings | Expected under load; the client honors `Retry-After`. Lower `MAX_RPS` in `config.py` if persistent. |
| Run stops but data looks partial | Normal after Ctrl-C; just re-run — it resumes from the checkpoint. |
| "N futures could not resolve last_seq this run" | Transient API failures for those instruments; they stay incomplete and retry on the next run (by design — no data lost). |
| Merge is slow on a huge perpetual file | Use `--stream-workers <N>` to parallelize; tune `--block-bytes`. Compare with `benchmark.py`. |
| Out-of-memory during merge | Ensure large files exceed `--large-threshold-mb` so they take the streaming path; reduce `--stream-batch-size`. |
| Duplicate rows in output | Shouldn't happen after dedup; if using `--no-dedup`, that's expected. |
| Behind a corporate proxy | Set `HTTP_PROXY` / `HTTPS_PROXY`. |

## Resuming & re-running semantics

- Re-running fetch skips finished chunks (futures) / advances from `last_no` (options).
- Deleting a `*.db` forces a full re-fetch of that kind.
- Deleting a `.parquet` and re-running `gen_parquet.py` rebuilds it from the JSONL (idempotent).
