# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Reconciled the README and operations guide with tested behavior: completeness now requires checkpoint-aware validation; historic time/disk figures are labeled as unverified estimates; Parquet flags cover both large-file reader paths; unmeasured performance and memory guarantees were removed.
- CI now enforces Ruff lint/format, Pyrefly, and an 80% coverage floor; Ruff is a development-only dependency.

### Fixed

- Correctness, supervision, validation, retry, and bounded-reader defects identified by the 2026-08-02 audit. See `docs/internal/CODE_AUDIT_2026-08-02_ZH.md` for the commit-level evidence and remaining full-dataset acceptance gaps.

## [0.1.0] - 2026-07-17

Initial public release.

### Added

- Historical trade download for Deribit **futures** and **options** (BTC/ETH) via `trade_seq`-based chunking.
- Async producer-consumer engine with per-second rate limiting and bounded-queue backpressure.
- Resumable runs via a SQLite checkpoint database; graceful `SIGINT`/`SIGTERM` shutdown.
- JSONL output (one file per instrument) and a `gen_parquet.py` merge step with `(instrument_name, trade_seq)` deduplication.
- Streaming Parquet validation with per-instrument `trade_seq` gap detection and a gap histogram.
- Parallel large-file reading in the Parquet merge via a process pool (`--stream-workers`, `--block-bytes`), alongside the single-thread `mmap` streaming path.
- `--base-dir` CLI flag on all entry points (`future`, `option`, `gen_parquet.py`, `validate_data.py`) to relocate the data directory (default `./data/<CURRENCY>`); all derived paths (JSONL, `*.db`, `*.parquet`) follow it.
- `scripts/benchmark.py` — reproducible throughput / memory / compression benchmarks for the Parquet merge.
- GitHub Actions CI: `ruff` lint job + `pytest` across Python 3.10 / 3.11 / 3.12.
- Test suite for config, client (retry / `Retry-After`), engine, progress, storage, and the future/option/parquet logic.
- Developer & design documentation under `docs/` (overview, architecture, design-decisions, data-model, deribit-api, operations, development) with an index, plus `CONTRIBUTING.md` and this changelog.

### Changed

- Extracted the engine callbacks (`fetch_*`, `sync_*`, `on_*`) from closures into module-level, unit-testable functions; future and option share one `run_fetcher` skeleton.
- `Config` uses a `from_env()` factory with derived-path **properties** (paths computed once), reading only `CURRENCY` + proxy from the environment.
- Rewrote both READMEs to match the actual code (removed non-existent flags and unsupported performance claims) and documented the real CLI flags.
- Organized the repo: `api-reference.md`/`.zh.md` under `docs/`, audit/roadmap notes under `docs/internal/`, and `ruff` + coverage configured in `pyproject.toml`.

### Fixed

- `get_last_trade_seq` now returns `None` (not `0`) when a lookup can't be resolved after retries, so a transient failure no longer silently marks a future complete and drops its entire history. Undetermined instruments are retried on the next run.
- Made the config tests hermetic (clear ambient proxy/currency env) so they don't fail on hosts with a proxy set.

[Unreleased]: https://github.com/RiveChen/deribit-historical-data/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/RiveChen/deribit-historical-data/releases/tag/v0.1.0
