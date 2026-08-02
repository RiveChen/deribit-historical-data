# Architecture

## Layered view

The package is layered by concern. Higher layers depend on lower ones, never the reverse.

```
 CLI / entry points      future.py   option.py        scripts/{gen_parquet,validate_data,benchmark}.py
                              │           │                         │
 orchestration          fetcher.run_fetcher (shared skeleton)       │
                              │           │                         │
 concurrency            engine.FetcherEngine (producer/consumer)    │
                              │           │                         │
 domain callbacks       prepare_/fetch_/sync_ functions            parquet.generate_parquet / validate_parquet
                              │           │                         │
 I/O + persistence      client   progress(SQLite)   storage(JSONL)  polars / pyarrow
                              │
 cross-cutting          config (settings)   log (tqdm-aware logging)
```

## Module responsibilities

| Module | Responsibility |
|--------|----------------|
| `config.py` | `Config` dataclass + `from_env()` factory + derived path **properties** (`base_dir`, `future_db_path`, …). Exposes the `settings` singleton and the shared `logger`. |
| `client.py` | `DeribitClient` — async httpx client for the history API. Owns rate limiting (`AsyncLimiter`), retry/backoff (`tenacity` + custom `Retry-After` wait), and the three endpoints used (`get_instruments`, `get_last_trade_seq`, `get_trades_chunk`). |
| `engine.py` | `FetcherEngine` — generic async producer-consumer. Producers pull tasks, call a `fetch_func`, push results; a single consumer batches results and calls a `sync_db_func`. Bounded queues provide backpressure; supports graceful shutdown and dynamic task enqueue. |
| `fetcher.py` | `run_fetcher(...)` — the shared run skeleton: open DB → repo → sink → client → prepare tasks → build engine → run with injected callbacks → finalize. Both fetchers call it with different callbacks. |
| `future.py` | Future strategy: instrument-level callbacks (`prepare_future_tasks`, `fetch_future_chunk`, `sync_future_db`, `finalize_future`) + `run()` entry point. |
| `option.py` | Option strategy: streaming callbacks (`prepare_option_tasks`, `fetch_option_chunk`, `on_option_success`, `sync_option_db`, `option_pbar_updater`) + `run()` entry point. |
| `progress.py` | `DatabaseClient` (schema + PRAGMAs) and the repositories `FutureProgressRepo` / `OptionProgressRepo` — all SQLite checkpoint logic (repository pattern). |
| `storage.py` | `JSONLinesSink` — appends batched trades to per-instrument `.jsonl` files, offloading disk I/O to a thread pool to keep the event loop responsive. |
| `parquet.py` | JSONL → Parquet merge (`generate_parquet`), the extracted pure dedup functions (`dedup_intra`, `dedup_cross_batch`, `dedup_cross_file`), the single-thread (`stream_batches`) and parallel (`parallel_read_large_file`) large-file readers, plus validation (`validate_parquet`, `_show_gap_histogram`). |
| `log.py` | `setup_logging()` + a `tqdm`-compatible logging handler so log lines don't corrupt progress bars. |
| `__init__.py` | `run_main()` — installs SIGINT/SIGTERM handlers that set a shared `stop_event` for graceful shutdown. |

## Data flow (end to end)

```
Deribit history API
        │  (httpx, rate-limited, retried)
        ▼
   DeribitClient ──► FetcherEngine ──► JSONLinesSink ──► data/<CCY>/<kind>/*.jsonl
        │             (producers)        (consumer,                    │
        │                                 batched writes)              │
        └──► progress.*Repo ◄────────────────┘                        │
                (SQLite checkpoints: what's done, resume offsets)      │
                                                                       ▼
                                              scripts/gen_parquet.py → parquet.generate_parquet
                                                                       │  (dedup, streaming/parallel)
                                                                       ▼
                                                        data/<CCY>/<kind>.parquet
                                                                       │
                                              scripts/validate_data.py → parquet.validate_parquet
                                                                       ▼
                                                     gap report + histogram (stdout)
```

## Concurrency model

The download side is a classic **bounded producer-consumer** pipeline built on `asyncio`:

- **Producers** (`worker_count` coroutines) pull tasks off `task_queue`, call the injected `fetch_func`, run an optional `on_success` callback (options use it to enqueue one next chunk), and push results onto `storage_queue`. On error they retry the same logical task within a fixed retry budget.
- **Consumer** (single coroutine) drains `storage_queue`, buffers results per instrument, and flushes to disk+DB when the batch fills or on idle timeout.
- **Backpressure**: both queues are bounded. Initial work occupies at most `task_queue_size` slots; the task queue reserves one additional slot per producer for a follow-up or retry. This keeps a strict `task_queue_size + worker_count` upper bound while preventing producers from deadlocking on the queue they consume.
- **Failure supervision**: the main coroutine watches the storage consumer during both initial task distribution and steady-state execution. A disk/DB flush error cancels producers and propagates to the caller instead of leaving full queues hanging.
- **Rate limiting**: `AsyncLimiter(MAX_RPS, 1)` gates every request to ≤ `MAX_RPS` per second, independent of worker count.
- **Graceful shutdown**: SIGINT/SIGTERM set a `stop_event`; producers stop pulling, the consumer flushes buffered data (poison-pill), and progress already persisted in SQLite makes the next run resumable.

### Future vs. Option: two paces on one engine

The engine is generic; the two fetchers differ only in callbacks:

- **Future** — *pre-allocated* chunks. `last_seq` is known up front, so the whole `[1, last_seq]` range is partitioned into fixed chunks and fed as initial tasks. No `on_success`.
- **Option** — *streaming*. Each instrument starts one task at `last_no + 1`; `on_option_success` enqueues the next chunk only if `should_continue`, so the task set grows dynamically as the progress bar total expands.

See [design-decisions.md](./design-decisions.md) for *why* the two strategies differ.

## Post-processing side

`parquet.py` is independent of the async download stack (pure `polars`/`pyarrow`/stdlib), which makes its dedup logic unit-testable in isolation. It classifies files by size:

- **Small files** (`< --large-threshold-mb`) → thread pool with a fixed `2 × workers` inflight window, one file per task, optional per-file dedup.
- **Large files** (`≥ threshold`) → either single-thread `mmap` streaming in row batches, or a **process pool** (`--stream-workers`) that reads `\n`-aligned byte blocks in true parallel. The process path also uses a fixed `2 × workers` ordered result window instead of retaining every block DataFrame. Cross-batch dedup uses an exact per-instrument sequence bitmap, so correctness does not depend on JSONL row order.
