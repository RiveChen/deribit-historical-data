# README ↔ Code Reconciliation

_Checked the published `main` README against the published `main` code (fetched from GitHub raw), 2026-07-04._

**Headline:** the README documents a `gen_parquet` that uses a **process pool with `--stream-workers` / `--block-bytes`**, but the actual code uses a **single-threaded mmap streamer** with completely different flags. Anyone who runs the README's commands verbatim gets an `argparse` error. Several env vars in the Configuration table are also silently ignored by the code. These are exactly the mismatches an interviewer catches by opening one file.

## Critical — following the README breaks or silently no-ops

| # | README says | Code actually does | Effect |
|---|-------------|--------------------|--------|
| 1 | Large files "processed in parallel using a **process pool** (`--stream-workers`)"; `--stream-workers 8`; "`--stream-workers 1` fallback uses mmap" | `_stream_batches` runs **single-threaded in the main thread** via mmap. There is **no process pool** and **no `ProcessPoolExecutor`**. | `gen_parquet.py --stream-workers 8` → `error: unrecognized arguments`. Core perf claim is fictional. |
| 2 | `--block-bytes 268435456` (256 MB default) | No such flag. Real flag is `--stream-batch-size` (rows, default **200000**) and `--large-threshold-mb`. | `--block-bytes` → argparse error. |
| 3 | Config table lists `CHUNK_SIZE`, `MAX_RPS`, `MAX_WORKERS` as env vars; example `CURRENCY=ETH MAX_RPS=10 uv run …` | `config.py::__post_init__` reads **only** `CURRENCY` and `HTTP(S)_PROXY` from env. `CHUNK_SIZE` / `MAX_RPS` / `MAX_WORKERS` are hardcoded defaults. | `MAX_RPS=10` is silently ignored — user thinks they throttled to 10 RPS, still runs at 20. |
| 4 | Project Structure lists `scripts/test_real_api.py` — "Deribit API behavior testing tool" | File does not exist in the repo (`scripts/` has only `gen_parquet.py`, `validate_data.py`). | Phantom file; also undercuts the "17-field union schema confirmed by real API testing" claim, since the tool that supposedly confirmed it isn't there. |

## Medium — unsupported or contradictory claims

| # | README says | Reality | Fix |
|---|-------------|---------|-----|
| 5 | "achieving **near-SSD read speeds by saturating disk queue depth**" | No parallel I/O exists to saturate queue depth; single-threaded read. No benchmark backs this. | Delete the phrase, or implement it and back it with the benchmark numbers. |
| 6 | Requirements: **Python 3.12+** | `pyproject.toml` = `requires-python >=3.10`; `.python-version` = `3.10`. | Pick one. If you use 3.10 features only, say 3.10+. |
| 7 | Features: "Data validation … **dedup estimate**"; Usage §4: "detects gaps **and duplicates**" | `validate_data.py` detects **gaps** (trade_seq continuity), schema, and timestamp range. It does **not** estimate or detect duplicates. | Reword to "gap detection + schema/time-range summary". |
| 8 | Features: "Async & fast — **configurable concurrency** via asyncio" | Worker counts (`MAX_WORKER_TASKS`, etc.) are module-level constants in `future.py`/`option.py`; not configurable via env or CLI. | Say "concurrency tunable in source", or actually wire it to config. |

## Low — polish

| # | Item | Note |
|---|------|------|
| 9 | Real flags `--large-threshold-mb`, `--stream-batch-size` are **undocumented** in the README. | Document the flags that actually exist. |
| 10 | "roughly **1:1 ratio**" for Parquet vs JSONL disk size | Parquet+zstd is typically **much** smaller than JSONL (often 5–10×). The benchmark measures the real ratio — likely your files need far less space than the README warns. |
| 11 | Install: `pip install -e .` | Build backend is `uv_build`; a plain `pip install -e .` may fail without uv. Test it or drop the pip path. |

## Root cause & the honest fix

This is classic "docs written ahead of (or by a different pass than) the code" drift — most likely an LLM drafted an aspirational README describing a process-pool design that was never merged. Two clean options:

1. **Make the README true** (fast, recommended for job-hunting): remove `--stream-workers`/`--block-bytes`/process-pool/near-SSD language, fix the env-var table to only `CURRENCY` + proxy, fix Python version, remove `test_real_api.py`, document the real flags. ~1 hour.
2. **Make the code match the README** (more impressive, more work): actually implement parallel block-aligned reading with a process pool, wire `MAX_RPS`/`MAX_WORKERS` to env. Then the benchmark proves the numbers. ~1–2 days — a legitimately good portfolio story if you can defend it.

Either way, run the benchmark (`scripts/benchmark.py`) and paste **real** numbers into the README so no claim is unbacked.
