# Data Model

This page documents the data at rest: on-disk layout, the trade schema, the Parquet output, and the SQLite checkpoint tables. For the live API contract, see [deribit-api.md](./deribit-api.md).

## On-disk layout

```
data/
└── {CURRENCY}/                 # e.g. BTC or ETH (from the CURRENCY env var)
    ├── future/
    │   ├── BTC-27MAR26.jsonl       # one JSONL file per instrument
    │   └── ...
    ├── option/
    │   ├── BTC-27MAR26-70000-C.jsonl
    │   └── ...
    ├── future.db                   # SQLite checkpoint (futures)
    ├── option.db                   # SQLite checkpoint (options)
    ├── future.parquet              # produced by gen_parquet.py
    └── option.parquet
```

All paths derive from `base_dir` via the `Config` properties (`base_dir`, `data_future_dir`, `future_db_path`, …). `base_dir` defaults to `./data/<CURRENCY>` but can be relocated with `--base-dir` on any command (see [operations.md](./operations.md#data-directory---base-dir)).

## JSONL (raw fetch output)

- One file per instrument, named `<instrument_name>.jsonl`.
- One trade per line, newline-delimited JSON (serialized with `orjson`).
- **Append-only**; a killed process leaves a valid prefix (see [design-decisions.md](./design-decisions.md#3-jsonl-as-an-intermediate-layer-parquet-built-afterward)).
- May contain a small number of duplicate rows by design (chunk-boundary overlap and crash-recovery re-fetch); these are removed at the Parquet stage.

## Trade schema (18-field union)

Future and option trades share one structure. The Parquet generator writes a fixed **18-field union schema** (`parquet.COMPREHENSIVE_SCHEMA`) so any trade type reads correctly; fields absent for a given type are filled with `null`.

| Field | Parquet type | Presence | Meaning |
|-------|--------------|----------|---------|
| `trade_seq` | Int64 | always | Per-instrument monotonic sequence number (paging cursor + dedup key). |
| `trade_id` | String | always | Global trade ID. |
| `timestamp` | Int64 | always | Trade time, epoch **milliseconds**. |
| `tick_direction` | Int64 | always | Price-move indicator (0–3). |
| `price` | Float64 | always | Trade price. |
| `mark_price` | Float64 | always | Mark price at fetch time (may be null for very old data). |
| `iv` | Float64 | options only | Implied volatility. |
| `instrument_name` | String | always | Instrument (also the JSONL filename); dedup key with `trade_seq`. |
| `index_price` | Float64 | always | Index price at fetch time. |
| `direction` | String | always | Taker side, `buy` / `sell`. |
| `amount` | Float64 | always | Trade amount (contract count). |
| `contracts` | Float64 | futures & options | Contract count (not applicable to all types). |
| `block_trade_id` | String | block trades | Block trade ID. |
| `block_rfq_id` | String | block trades | Block RFQ ID. |
| `block_trade_leg_count` | Int64 | block trades | Number of legs. |
| `combo_id` | String | combo/spread | Combo/spread identifier. |
| `combo_trade_id` | String | combo/spread | Combo trade ID. |
| `liquidation` | String | perpetual | Liquidation flag. |

> The ~10 "always present" fields form the core; the rest appear only for specific trade types. See the full field-by-field notes in [api-reference.md](./api-reference.md#43-trade-structurefull-field-union).

## Parquet (analysis output)

- A single file per kind: `future.parquet`, `option.parquet`.
- Schema = the 18-field union above.
- **Deduplicated** by `(instrument_name, trade_seq)`.
- Compression: **zstd** by default, or **lz4** with `--fast`; benchmark the speed/size trade-off on the target data.
- Written incrementally with a `pyarrow.parquet.ParquetWriter`; row batches are accumulated and flushed to keep memory bounded.
- Compression ratio depends on the input and codec — measure it with `scripts/benchmark.py`.

## SQLite checkpoint tables

Created by `DatabaseClient` on first connect, with `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=NORMAL`.

### `future_meta` — per-instrument future state

| Column | Type | Notes |
|--------|------|-------|
| `instrument` | TEXT | PRIMARY KEY |
| `is_expired` | INTEGER | 1 = expired (settled), 0 = active |
| `is_completed` | INTEGER | 1 = fully fetched (skipped on restart) |

### `future_chunk` — per-chunk progress

| Column | Type | Notes |
|--------|------|-------|
| `instrument` | TEXT | part of PRIMARY KEY |
| `chunk_no` | INTEGER | = chunk `start_seq`; part of PRIMARY KEY |
| `count` | INTEGER | trades fetched for the chunk (default 0) |
| `has_more` | INTEGER | post-recovery range status persisted for the chunk |
| `is_done` | INTEGER | 1 once finalized |

Each production task carries its exact expected `[start_seq, end_seq]`. The history host can shift a full response across the requested boundary, so the downloader filters out-of-range rows and checks the complete unique sequence set. An incomplete response is split into smaller ranges under a bounded recovery budget; only exact coverage is persisted with `has_more=0`. A chunk is then finalized (`is_done=1`) when its exact range is full or when it is the partial tail of an expired future. An active future's partial tail remains pending because new trades can grow into the same range; re-fetching may append duplicates, which the Parquet stage removes exactly.

### `option_meta` — per-instrument option state + resume offset

| Column | Type | Notes |
|--------|------|-------|
| `instrument` | TEXT | PRIMARY KEY |
| `last_no` | INTEGER | highest fetched `trade_seq`; resume from `last_no + 1` |
| `is_expired` | INTEGER | 1 = expired, 0 = active |
| `is_completed` | INTEGER | 1 = fully fetched |

`last_no` is advanced with `SET last_no = MAX(last_no, ?)` so a crash-recovery re-fetch can never roll progress backward. Options are marked complete only when expired and no more trades remain.

## Data integrity notes

- **Completeness proof**: `validate_data.py` compares unique per-instrument `trade_seq` ranges with the SQLite checkpoint inventory. Missing heads/tails, missing instruments, internal gaps, and duplicates are `INCOMPLETE`; a continuous range backed only by a non-final checkpoint is `UNKNOWN`; only an exact final-checkpoint match is `COMPLETE`. A final checkpoint with `last_no = 0` explicitly proves that an absent instrument has no trades.
- **Duplicates**: expected in JSONL, removed in Parquet. `generate_parquet` reports how many were removed.
- **Timestamps** are epoch-ms UTC; convert with `datetime.fromtimestamp(ts/1000, tz=timezone.utc)`.
