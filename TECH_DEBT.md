# Tech Debt Audit — deribit-historical-data

_Audit date: 2026-06-28 · Scope: `src/deribit_fetcher/`, `scripts/`, `tests/` (~2,450 LOC)_

## Summary

The codebase is small, well-documented, and reasonably structured (clean producer/consumer engine, repository pattern for SQLite, async I/O offloading). The biggest risks are **operational, not structural**: there is no CI to run the existing test suite, one error-handling path can silently drop an entire instrument's data, and the highest-value logic (Parquet dedup, HTTP retry/rate-limit) has no test coverage.

Verification done for this audit: ran the suite (**31 passed, 1 failed** — the failure is a non-hermetic test, item T1), confirmed no `.github/workflows`, no `asyncio_mode` config, 5 broad `except Exception` blocks, and an inline `import json`.

Priority score = **(Impact + Risk) × (6 − Effort)**, each rated 1–5.

| # | Item | Category | Impact | Risk | Effort | **Priority** |
|---|------|----------|:------:|:----:|:------:|:------------:|
| C1 | `get_last_trade_seq` swallows all errors → instrument silently marked complete | Code / correctness | 3 | 5 | 2 | **32** |
| I1 | No CI pipeline — tests exist but never run automatically | Infrastructure | 4 | 4 | 2 | **32** |
| T1 | Non-hermetic config tests + missing `asyncio_mode` config | Test | 2 | 3 | 1 | **25** |
| T2 | No tests for `gen_parquet` dedup or `client` retry/rate-limit | Test | 4 | 4 | 3 | **24** |
| A1 | Engine re-queues failed tasks with no retry cap → possible infinite loop | Architecture | 3 | 4 | 3 | **21** |
| D1 | Dependencies have no upper bounds; Polars API churn already broke once | Dependency | 2 | 3 | 2 | **20** |
| C2 | Duplicated path derivation in config + duplicated `run()` scaffolding/constants | Code | 3 | 2 | 3 | **15** |
| Doc1 | Doc duplication (EN/ZH × README + api-reference); no lint/coverage gate | Documentation | 2 | 1 | 2 | **12** |
| A2 | `gen_parquet.py` is a 482-line monolith, one function, untestable | Architecture | 3 | 2 | 4 | **10** |
| C3 | Inline imports (`import json`, `from tqdm import tqdm`) | Code | 1 | 1 | 1 | **10** |

---

## Detail & business justification

### C1 — Silent data loss in `get_last_trade_seq` (Priority 32)
`client.py` catches every exception and returns `0`. In `future.py::_prepare_tasks`, any future with `last_seq == 0` is immediately marked complete via `mark_future_complete`. So a single transient network/HTTP error while fetching the last sequence permanently skips that instrument — and because it's marked complete, a re-run won't retry it. For a tool whose entire purpose is "full history download," silent permanent gaps undermine the core guarantee.

_Fix:_ distinguish "genuinely no trades" (empty result) from "request failed" (exception). Let the existing tenacity retry handle transient errors and let real failures propagate rather than masquerade as `0`. ~half a day including a regression test.

### I1 — No CI (Priority 32)
There is a working 32-test suite and a `pyrefly.toml` type-check config, but no `.github/workflows`. Nothing enforces that tests/type-checks pass before merge. The git history shows several `fix:` commits for regressions (Polars `engine` arg, float rounding in buckets) — exactly what CI catches.

_Fix:_ one GitHub Actions workflow: `uv sync`, `pytest`, `pyrefly check`. ~half a day.

### T1 — Non-hermetic tests + asyncio config (Priority 25)
`test_proxy_from_https_env` sets `HTTPS_PROXY` but never clears `HTTP_PROXY`; since config reads `HTTP_PROXY` first, the test fails in any environment that has an ambient proxy (it failed in this audit's sandbox). Several proxy tests are order-dependent on ambient env. Also `pytest-asyncio` is a dependency but `asyncio_mode` isn't set in `pyproject.toml`, producing `PytestUnknownMarkWarning` and relying on implicit plugin behavior.

_Fix:_ `delenv` all proxy vars at the top of each proxy test (or a fixture/`monkeypatch` that clears the full set); add `[tool.pytest.ini_options] asyncio_mode = "auto"`. ~1–2 hours.

### T2 — Core logic untested (Priority 24)
No tests exist for `client.py` (retry strategy, `Retry-After` handling, rate limiting), `gen_parquet.py` (intra-file, cross-file, and cross-batch dedup — the module's main value), `validate_data.py`, or `log.py`. Tested modules are config, engine, progress, storage. The dedup code is intricate (mmap streaming, monotonic-seq filtering, set-based cross-file) and is precisely where a subtle bug would corrupt output without an obvious crash.

_Fix:_ unit tests for the three dedup paths against small fixtures; mock-transport tests for the client's `Retry-After`/backoff branch. ~1–2 days; pairs naturally with extracting functions in A2.

### A1 — Unbounded engine-level retry (Priority 21)
`engine._producer_worker` re-queues any task whose `fetch_func` raises (`await self.task_queue.put(tasking)`), with no attempt counter. The client already retries 10× then re-raises; the engine then loops the same task forever. A permanently-failing chunk (e.g. a server-side 4xx that isn't transient) stalls a worker indefinitely and never surfaces as a hard failure.

_Fix:_ track per-task attempts; after N engine-level retries, log and drop (or route to a dead-letter list) instead of re-queueing. ~half a day.

### D1 — Dependency bounds (Priority 20)
Deps in `pyproject.toml` use `>=` lower bounds with no upper caps; `polars>=1.38.0`, `pyarrow>=23.0.0`, etc. The history already contains a Polars-API-break fix (`collect(engine=...)`). With `uv.lock` committed, local runs are reproducible, but a fresh `uv sync --upgrade` can silently pull a breaking major. No automated dependency/security scanning.

_Fix:_ add conservative upper bounds for the fast-moving libs (Polars/PyArrow), and a scheduled `uv lock --upgrade` + test job (or Dependabot). ~2–3 hours.

### C2 — Duplication (Priority 15)
Config recomputes `BASE_DIR` and its four derived paths twice (class-body defaults, then again in `__post_init__` after the env override) — the derivation logic is copy-pasted and must be kept in sync. Separately, `future.py` and `option.py` repeat the same module-level tuning constants (`MAX_WORKER_TASKS`, `WRITE_BATCH_SIZE`, `STORAGE_QUEUE_SIZE`, `TASK_QUEUE_SIZE`) and near-identical `run()` scaffolding (open DB → repo → sink → client → build engine → define `fetch_chunk`/`sync_db` → `engine.run`).

_Fix:_ make the derived paths `@property` (or a single `_recompute_paths()` helper) so derivation lives once; lift shared `run()` scaffolding into a helper. ~half to one day.

### Doc1 — Doc duplication & no quality gate (Priority 12)
Four parallel docs (`README.md`/`README_ZH.md`, `api-reference.md`/`api-reference.zh.md`) drift independently — a change must be made in both languages. No linter/formatter (ruff/black) or coverage threshold is enforced anywhere.

_Fix:_ add ruff + a coverage report to the CI from I1; note in CONTRIBUTING that EN is source-of-truth for docs. ~2–3 hours (mostly on top of I1).

### A2 — `gen_parquet.py` monolith (Priority 10)
482 lines dominated by one `generate_parquet` function with several nested closures (`_flush_pending`, `_init_writer`, `_consume_small`) sharing `nonlocal` state. This is the reason T2's dedup logic can't be unit-tested in isolation.

_Fix:_ extract pure functions for the three dedup strategies and the writer/flush bookkeeping; keep orchestration thin. ~1 day; do it as the enabler for T2.

### C3 — Inline imports (Priority 10)
`client.py` does `import json` inside `get_instruments`; `gen_parquet.py` imports `tqdm` inside the function body. Minor, but obscures dependencies and is inconsistent with the rest of the codebase.

_Fix:_ move to module top. ~15 minutes.

---

## Phased remediation plan

Designed to run alongside feature work — each phase is independently shippable.

**Phase 1 — Stop the bleeding (~1.5 days).** I1 (CI running pytest + pyrefly) and C1 (fix silent data loss). CI lands first so the C1 fix and its regression test are gated from then on. Highest risk reduction per hour.

**Phase 2 — Trustworthy tests (~2 days).** T1 (hermetic tests + `asyncio_mode`), then A2→T2 together: extract the dedup functions and cover them, plus client retry/`Retry-After`. After this, the suite actually protects the data-integrity guarantees, and coverage can be added as a CI gate (Doc1).

**Phase 3 — Resilience & hygiene (~1.5 days).** A1 (retry cap / dead-letter), D1 (dependency bounds + scheduled upgrade job), C2 (de-duplicate config paths and run scaffolding), C3 (inline imports), and the remainder of Doc1 (ruff, CONTRIBUTING note).

Total ≈ 5 engineering-days, fully incremental. Phase 1 alone removes both Priority-32 risks.
