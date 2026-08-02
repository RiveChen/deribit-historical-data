# Contributing

Thanks for your interest in improving **Deribit Historical Data Fetcher**! This guide covers the essentials; see [docs/development.md](./docs/development.md) for more depth.

## Ways to contribute

- Report bugs or unexpected API behavior (include the instrument, currency, and a minimal repro).
- Improve docs (`docs/`, English is the source language).
- Add tests, fix bugs, or implement items from [docs/internal/ROADMAP.md](./docs/internal/ROADMAP.md).

## Development setup

```bash
git clone https://github.com/RiveChen/deribit-historical-data.git
cd deribit-historical-data
uv sync            # installs runtime + dev dependencies
```

Requires Python 3.10+ and [uv](https://docs.astral.sh/uv/).

## Before you open a PR

Run these locally — CI runs the same checks on Python 3.10 / 3.11 / 3.12.

```bash
uv run pytest              # tests (+ coverage, configured in pyproject.toml)
uv run ruff check .        # lint
uv run ruff format --check .
uv run pyrefly check --search-path . --search-path src
```

All four must pass.

## Project conventions

- **Logic lives in the package, `scripts/` only wraps it.** New functionality goes in `src/deribit_fetcher/`; a script should just parse args and call a package function.
- **Testable by design.** Prefer module-level functions that take dependencies as keyword args over closures, so they can be unit-tested (see `tests/test_future.py`, `tests/test_option.py`). Use `httpx.MockTransport` for client tests — no real network calls.
- **Make unknown states explicit.** Don't overload a sentinel (e.g. return `int | None`, not `0`, for "couldn't determine"). See the `get_last_trade_seq` decision in [docs/design-decisions.md](./docs/design-decisions.md).
- **Style**: type hints throughout; Google-style docstrings; `ruff` rule sets `E,W,F,I,N,D,UP,B`; line length 100; double quotes. Fix findings or add a justified `# noqa: <code>`.

## Commit & PR guidelines

- Write commit messages that state **what** and **why**, not "misc fixes". A `type: summary` prefix (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`) is encouraged.
- Keep PRs focused. Update the relevant `docs/` page **in the same PR** that changes behavior.
- Add a note to the `[Unreleased]` section of [CHANGELOG.md](./CHANGELOG.md) for user-visible changes.
- Add or update tests for any behavior change; don't lower coverage.

## Reporting security or data-integrity issues

Data-correctness bugs (silent gaps, dedup errors, resume regressions) are high priority — please flag them clearly. See [docs/internal/TECH_DEBT.md](./docs/internal/TECH_DEBT.md) for known items.

## License

By contributing, you agree that your contributions are licensed under the project's [MIT License](./LICENSE).
