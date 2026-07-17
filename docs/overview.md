# Overview

## What this project is

**Deribit Historical Data Fetcher** is a Python tool that downloads the **complete** historical trade (transaction) record for every Deribit **futures** and **options** instrument, then merges it into a single compressed Parquet file for analysis.

Deribit's public API is well known for real-time data, but it also exposes a separate **history** endpoint (`https://history.deribit.com/api/v2/public`) that can return trades far back in time via `trade_seq`-based paging. This project wraps that endpoint with the concurrency, rate-limiting, checkpointing, and post-processing needed to pull *all* of it reliably.

## Why it exists (motivation)

- Getting *recent* trades from Deribit is easy; getting the **full history** for thousands of instruments is not — it requires paging through millions of sequential trades per instrument, respecting rate limits, and surviving interruptions over multi-hour runs.
- Naive scripts break on: rate limits (429), transient network errors, process kills mid-run (losing progress), and out-of-memory when merging tens of GB of JSONL.
- This tool solves those with an async producer-consumer engine, a SQLite checkpoint database (resumable), graceful shutdown, and a streaming/parallel Parquet merge.

## Scope

**In scope**

- Currencies: **BTC** and **ETH** (via `CURRENCY`).
- Kinds: **futures** and **options**.
- Full-history download → JSONL (one file per instrument) → merged, deduplicated Parquet.
- Post-download integrity validation (per-instrument `trade_seq` gap detection).

**Non-goals**

- Not a real-time / streaming market-data feed (use Deribit's WebSocket API for that).
- No data cleaning, normalization into factors, or analytics — output is faithful raw trades.
- No order book / quotes / index data — only trades.
- Not a hosted service; it's a CLI tool you run locally against your own disk.

## Typical workflow

1. **Fetch** trades → JSONL (`python -m deribit_fetcher.future` / `.option`).
2. **Merge** JSONL → a single Parquet with dedup (`scripts/gen_parquet.py`).
3. **Validate** the Parquet for gaps (`scripts/validate_data.py`).
4. (Optional) **Benchmark** the merge step (`scripts/benchmark.py`).

Runs are resumable — re-running after an interruption picks up where it left off.

## Terminology

| Term | Meaning |
|------|---------|
| **instrument** | A tradable contract, e.g. `BTC-27MAR26` (future) or `BTC-27MAR26-70000-C` (option). One JSONL file per instrument. |
| **trade_seq** | Per-instrument, monotonically increasing trade sequence number. The paging cursor for full-history download. |
| **chunk** | A `[start_seq, end_seq]` range of `CHUNK_SIZE` (10 000) trades — the unit of one API request and one task. |
| **checkpoint** | Progress state persisted in SQLite (`future.db` / `option.db`) so runs can resume. |
| **expired / active** | An instrument that has settled (`is_active=false`, no new trades) vs. one still trading. Expired instruments can be marked *complete*; active ones never are (new trades keep arriving). |
| **has_more** | API flag meaning "more trades exist **within the requested range**" — not "beyond it". See [deribit-api.md](./deribit-api.md). |

## Where to go next

- How the pieces fit together → [architecture.md](./architecture.md)
- Why it's built this way → [design-decisions.md](./design-decisions.md)
- Data formats & schema → [data-model.md](./data-model.md)
- Running it → [operations.md](./operations.md)
