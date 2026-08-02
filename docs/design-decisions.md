# Design Decisions

Short ADR-style records: **context → options → decision → consequences**. Each captures *why* a choice was made and what was given up.

---

## 1. Page by `trade_seq`, not by time

**Context.** Full history must be pulled per instrument without gaps or duplicates.
**Options.** (a) Time-window paging (`start_timestamp`/`end_timestamp`); (b) `trade_seq` range paging.
**Decision.** Use `trade_seq`. `trade_seq` is dense, monotonic, and unique per instrument, so `[1, last_seq]` can be partitioned into exact, non-overlapping ranges.
**Consequences.** Deterministic chunk boundaries and trivial resume math. Time-based analytics must be derived from the `timestamp` field afterwards. Requires one extra call per instrument to learn `last_seq`.

---

## 2. Two fetch strategies: pre-allocated (future) vs. streaming (option)

**Context.** Futures number in the hundreds; options in the hundreds of thousands, most with few or zero trades.
**Decision.**
- **Future** — fetch `last_seq`, pre-allocate all chunks `[1, last_seq]` up front, feed them as initial tasks (uniform, highly parallel).
- **Option** — start one task per instrument at `last_no + 1` and let `on_option_success` enqueue the next chunk only when `should_continue`. The task set grows lazily.
**Why not pre-allocate options too.** Pre-allocating chunks for ~115k mostly-tiny instruments would create huge numbers of empty tasks and bloat the checkpoint DB. Streaming spends work proportional to actual data.
**Consequences.** Two code paths, but both ride the same engine via injected callbacks. Option progress is a single resumable offset (`last_no`) rather than a chunk table.

---

## 3. JSONL as an intermediate layer, Parquet built afterward

**Context.** We want columnar Parquet for analysis, but also crash-safe incremental writes during a multi-hour download.
**Options.** (a) Write Parquet directly during fetch; (b) append newline-delimited JSON, convert later.
**Decision.** Append **JSONL** during fetch; merge to Parquet in a separate step.
**Why.** Parquet is columnar and immutable-ish — appending mid-stream is awkward and a crash can corrupt a half-written file. JSONL is append-only, human-inspectable, and a killed process simply leaves a valid prefix.
**Consequences.** Needs disk for both JSONL and Parquet transiently, and a second pass. Dedup and schema unification are deferred to that pass (see #6).

---

## 4. SQLite checkpoint database for resumability

**Context.** Runs take hours and can be interrupted; re-downloading everything is unacceptable.
**Decision.** Persist progress in SQLite (`future.db`, `option.db`) with WAL mode + `synchronous=NORMAL`. Futures track per-chunk `is_done`; options track a per-instrument `last_no` offset.
**Consequences.** Restart skips finished chunks/instruments. WAL gives good concurrent read/write; `NORMAL` trades a tiny durability window for speed (acceptable because JSONL is the source of truth and dedup is idempotent). Adds a schema and repository layer.

---

## 5. Write order on option sync: disk first, DB second

**Context.** A crash can happen between writing trades and recording progress.
**Decision.** In `sync_option_db`, **flush JSONL first, then update the DB** `last_no`.
**Why.** If we crash after the flush but before the DB update, the next run re-fetches from a slightly older `last_no` → at worst a few duplicate rows in JSONL (removed at Parquet dedup). The reverse order could *advance* `last_no` past data that never hit disk → permanent gap. Prefer duplicates over loss.
**Consequences.** Occasional boundary duplicates by design; harmless given #6. Reinforced by the `MAX(last_no, ?)` guard so progress never rolls back.

---

## 6. Deduplicate at the Parquet stage, not during fetch

**Context.** Deribit occasionally returns 1 overlapping `trade_seq` at chunk boundaries, and the crash-safety choices above intentionally allow duplicates.
**Decision.** Tolerate duplicates in JSONL; dedup by `(instrument_name, trade_seq)` when building Parquet.
**Consequences.** The fetch path stays simple and fast (no dedup bookkeeping in the hot loop). All correctness for duplicates lives in one well-tested place (`dedup_intra` / `dedup_cross_file` / `dedup_cross_batch`).

---

## 7. Size-tiered Parquet merge; order-independent exact dedup

**Context.** Option files are many and small; a perpetual future file can be tens of GB — too big to load at once.
**Decision.** Small files → thread pool (one file per worker). Large files → streaming in row batches (`mmap`) or a **process pool** over `\n`-aligned byte blocks (`--stream-workers`). Cross-batch and cross-file dedup use an exact bitmap keyed by `(instrument_name, trade_seq)`, consuming one bit per sequence position instead of one Python object per trade.
**Why not a high-water mark.** API responses are broadly descending but their default order is not guaranteed, and concurrently completed future chunks are appended in completion order. Therefore the JSONL file is not guaranteed to be ascending; filtering on `trade_seq > max_seen` can delete unique rows.
**Consequences.** Dedup is correct for ascending, descending, and shuffled chunks. Bitmap memory grows with the largest sequence span for each instrument. The parallel reader still has separate memory-scaling work tracked as technical debt.

---

## 8. `get_last_trade_seq` returns a three-state result (`0 / >0 / None`)

**Context.** The method must try hard to succeed (tenacity retries 10× with backoff), but network calls can still ultimately fail. A future with `last_seq == 0` is treated as "no trades" and marked complete.
**Problem avoided.** If failure also returned `0`, a transient error would mark an instrument complete and **permanently drop its entire history** — invisibly (it never enters Parquet, so validation can't flag it).
**Decision.** Return `0` for a confirmed-empty instrument, `>0` for a real `last_seq`, and **`None` when it could not be determined**. `prepare_future_tasks` leaves `None` instruments incomplete (no chunks, logged) so they retry next run.
**Consequences.** Distinguishes "empty" from "unknown"; failures self-heal on the next run instead of causing silent loss. Callers must handle `None` (enforced by types + regression tests).

---

## 9. Prefer server `Retry-After` over blind backoff

**Context.** On 429, guessing a backoff can be slower or more aggressive than the server wants.
**Decision.** A custom `tenacity` wait (`DeribitRateLimitWait`) uses the `Retry-After` header (plus a small buffer) when present, else falls back to random exponential backoff (1–60 s), capped at 10 attempts.
**Consequences.** Respects the server's own guidance, reducing wasted retries and ban risk; still robust when the header is absent.

---

## 10. Config: `from_env()` factory with derived-path properties; env limited to `CURRENCY` + proxy

**Context.** An earlier version computed paths twice (class body then `__post_init__`) and implied more env vars than it read.
**Decision.** `Config.from_env()` reads only `CURRENCY` and `HTTP(S)_PROXY`; all paths are `@property` derived from `CURRENCY` (computed once, always consistent). `CHUNK_SIZE`, `MAX_RPS`, `MAX_WORKERS` are dataclass fields tuned in source, not env.
**Consequences.** No path-derivation duplication; the config surface is honest. Changing throughput knobs means editing `config.py` (documented in [operations.md](./operations.md)), a deliberate trade of runtime flexibility for a smaller, less error-prone surface.

---

## 11. Extract engine callbacks from closures into module-level functions

**Context.** Fetch/sync logic originally lived as closures inside `run()`, capturing `client`/`engine`/`repo`/`sink` — impossible to unit-test in isolation.
**Decision.** Promote them to module-level functions with dependencies passed as keyword-only args; bind them with `functools.partial` in the shared `run_fetcher`.
**Consequences.** Each callback is independently testable (see `test_future.py`, `test_option.py`); future and option share one run skeleton. Slightly more explicit wiring at the call site.
