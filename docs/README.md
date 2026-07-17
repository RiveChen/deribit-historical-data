# Documentation

Developer & design documentation for **Deribit Historical Data Fetcher**. For quick-start usage, see the root [README](../README.md).

## Contents

| Doc | What's inside |
|-----|---------------|
| [overview.md](./overview.md) | Purpose, motivation, scope & non-goals, terminology. Start here. |
| [architecture.md](./architecture.md) | Layered module map, data flow, concurrency model. |
| [design-decisions.md](./design-decisions.md) | ADR-style records of the key trade-offs and *why* they were made. |
| [data-model.md](./data-model.md) | On-disk layout, 18-field trade schema, Parquet output, SQLite tables. |
| [deribit-api.md](./deribit-api.md) | Practical notes on the Deribit history API + the behavioral gotchas. |
| [operations.md](./operations.md) | Install, configure, run, CLI flags, disk planning, troubleshooting. |
| [development.md](./development.md) | Dev setup, tests, lint, CI, conventions, release. |

## Also in the repo

- [api-reference.md](./api-reference.md) — exhaustive, test-verified Deribit API reference.
- [internal/ROADMAP.md](./internal/ROADMAP.md) — engineering roadmap toward a production-grade OSS repo.
- [internal/TECH_DEBT.md](./internal/TECH_DEBT.md) / [internal/DOC_AUDIT.md](./internal/DOC_AUDIT.md) — audit records.

## Reading paths

- **New contributor** → overview → architecture → development.
- **Understanding a design choice** → design-decisions (cross-links to the rest).
- **Just running it** → operations (+ root README).
- **Working with the data** → data-model → deribit-api.

## Maintenance

Keep docs in sync with code — update the relevant page in the same PR that changes behavior. English is the source language for `docs/`.
