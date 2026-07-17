# Development

## Environment

```bash
uv sync                     # install runtime + dev dependencies
```

Dev dependencies (`[dependency-groups].dev`): `pytest`, `pytest-asyncio`, `pytest-cov`. Lint/format via `ruff` (run with `uvx ruff`).

## Project layout

```
src/deribit_fetcher/   # the package (importable, tested)
scripts/               # thin CLI wrappers around package functions
tests/                 # pytest suite
docs/                  # this documentation
```

Guiding rule: **logic lives in the package, `scripts/` only parses args and calls it.** `gen_parquet.py` / `validate_data.py` / `benchmark.py` are thin wrappers over `deribit_fetcher.parquet`.

## Tests

```bash
uv run pytest                 # full suite (+ coverage, configured in pyproject)
uv run pytest tests/test_parquet.py -q
uv run pytest -k dedup
```

- Async tests run under `asyncio_mode = "auto"` (`[tool.pytest.ini_options]`), so `async def test_*` needs no per-test marker.
- Coverage is on by default: `addopts = "--cov=deribit_fetcher --cov-report=term-missing"`. The `fail_under` threshold lives in `[tool.coverage.report]` — raise it as coverage improves so it can't regress.

### Current test modules

| File | Covers |
|------|--------|
| `test_config.py` | env parsing, proxy precedence, derived paths (hermetic via an autouse env-clearing fixture) |
| `test_client.py` | retry / `Retry-After` / backoff, endpoint parsing, `None` failure semantics |
| `test_engine.py` | producer/consumer, graceful shutdown, retry-on-error, buffering |
| `test_progress.py` | finalize/resume SQL logic for both repos |
| `test_storage.py` | JSONL sink write/append/empty handling |
| `test_future.py` | task prep (`0/>0/None` semantics) + extracted `fetch_future_chunk` / `sync_future_db` |
| `test_option.py` | streaming callbacks (`fetch_option_chunk`, `on_option_success`, `sync_option_db`) |
| `test_parquet.py` | dedup paths (intra / cross-file / cross-batch) and merge behavior |

### Writing tests

- Prefer testing **module-level callbacks** directly (they take dependencies as keyword args) over driving a full `run()`.
- Use `httpx.MockTransport` for client tests — no real network calls.
- For SQLite, use the `DatabaseClient` context manager against a `tmp_path` DB (see `test_progress.py` / `test_future.py`).

## Lint & format (ruff)

```bash
uvx ruff check .            # lint
uvx ruff format .          # apply formatting
uvx ruff format --check .  # verify only (what CI runs)
```

Config in `[tool.ruff]`: line length 100, rule sets `E,W,F,I,N,D,UP,B`, google docstring convention, double-quote style. Fix findings or add a justified `# noqa: <code>`.

## Continuous integration

`.github/workflows/ci.yml` runs on push/PR to `main`:

- **lint** job: `ruff check .` (Python 3.12).
- **test** job: `uv sync` + `pytest` across Python **3.10 / 3.11 / 3.12** (`fail-fast: false`).

Keep the suite green before merging.

## Conventions

- Type hints throughout; new failure-y functions should make "unknown" states explicit (e.g. `int | None`) rather than overloading a sentinel like `0`.
- Google-style docstrings (enforced by ruff `D`).
- Commit messages: state **what** and **why**, not "misc fixes".

## Release (when tagging)

1. Ensure CI is green.
2. Bump `version` in `pyproject.toml` and `__version__` in `__init__.py`.
3. Update a `CHANGELOG`.
4. Tag (`git tag vX.Y.Z`) and create a GitHub Release.

## Related internal docs

Engineering process notes live in [`internal/`](./internal/): [`TECH_DEBT.md`](./internal/TECH_DEBT.md), [`DOC_AUDIT.md`](./internal/DOC_AUDIT.md), [`ROADMAP.md`](./internal/ROADMAP.md).
