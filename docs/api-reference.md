# Deribit Historical Data API Reference

> Deribit History API behavior documentation verified against real endpoint testing (2025-05-18).

---

## Table of Contents

1. [Basic Information](#1-basic-information)
2. [API Endpoints](#2-api-endpoints)
   - [get_instruments](#21-get_instruments)
   - [get_last_trades_by_instrument](#22-get_last_trades_by_instrument)
3. [Request Parameters](#3-request-parameters)
4. [Response Structure](#4-response-structure)
   - [get_instruments Response](#41-get_instruments-response)
   - [get_last_trades_by_instrument Response](#42-get_last_trades_by_instrument-response)
   - [Trade Structure (Future & Option Unified)](#43-trade-structurefuture--option-unified)
5. [Core Behavior Semantics](#5-core-behavior-semantics)
   - [The True Meaning of has_more](#51-the-true-meaning-of-has_more)
   - [trade_seq Ordering](#52-trade_seq-ordering)
   - [Chunk Boundary Continuity](#53-chunk-boundary-continuity)
   - [count Parameter and Pagination](#54-count-parameter-and-pagination)
6. [Rate Limiting and Error Handling](#6-rate-limiting-and-error-handling)
7. [Usage Patterns in Code](#7-usage-patterns-in-code)
   - [Future Fetch Strategy](#71-future-fetch-strategy)
   - [Option Fetch Strategy](#72-option-fetch-strategy)
8. [Verified Assumptions](#8-verified-assumptions)
9. [Known Issues and Caveats](#9-known-issues-and-caveats)

---

## 1. Basic Information

| Item | Value |
|------|-------|
| **Base URL** | `https://history.deribit.com/api/v2/public` |
| **Auth** | Public API, no token required |
| **Protocol** | HTTP/HTTPS |
| **Data Format** | JSON |
| **Default RPS Limit** | 20 requests/sec |
| **CHUNK_SIZE** | 10,000 |
| **Max Retries** | 10 |
| **Timeout Settings** | Request 60s, Connection 10s |

---

## 2. API Endpoints

### 2.1 `get_instruments`

Retrieves the full list of instruments (both expired and active) for a given currency and kind.

```
GET /get_instruments
```

#### Request Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `currency` | string | Yes | Currency, e.g. `"BTC"`, `"ETH"` |
| `kind` | string | Yes | Instrument kind: `"future"` or `"option"` |
| `expired` | string | Yes | `"true"` or `"false"` |

#### Code Usage (client.py:93)

```python
# Fetch both expired=true and expired=false concurrently, merge results
params = {"currency": currency, "kind": kind, "expired": expired}
data = await fetch_json("/get_instruments", params)
```

#### Test Data

| Currency | kind | expired | Count |
|----------|------|---------|-------|
| BTC | future | true | 379 |
| BTC | future | false | 5 |
| BTC | option | true | 114,851 |
| BTC | option | false | 406 |

---

### 2.2 `get_last_trades_by_instrument`

Retrieves historical trade data for a specific instrument. Supports **range-based queries** (by `trade_seq`) and **cursor-based queries** (by `count`).

```
GET /get_last_trades_by_instrument
```

#### Request Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `instrument_name` | string | Yes | Instrument name, e.g. `"BTC-27MAR26"` |
| `start_seq` | integer | No | Starting trade_seq (inclusive). Omitted = start from most recent trades |
| `end_seq` | integer | No | Ending trade_seq (inclusive). Omitted = return `count` trades |
| `count` | integer | Yes | Max trades to return (1 ~ 10,000) |

#### Query Mode Comparison

| Mode | Usage | Returns | Typical Use Case |
|------|-------|---------|-----------------|
| **Range Query** | `start_seq` + `end_seq` + `count` | Trades within the range, limited by `count`; default row order is not guaranteed | Chunked full history download |
| **Cursor Query** | `count` only, no `start_seq`/`end_seq` | The most recent `count` trades | Getting the latest trade seq |

#### Code Usage

```python
# 1. Get latest trade_seq for an instrument (client.py:121)
params = {"instrument_name": instr, "count": 1}

# 2. Chunked fetch (client.py:131)
params = {
    "instrument_name": instr,
    "start_seq": start_seq,
    "end_seq": end_seq,
    "count": CHUNK_SIZE,  # 10000
}
```

---

## 3. Request Parameters

### `count` Parameter

- **Observed history-host range**: 1 ~ 10,000 (`count=10,000` accepted on 2026-08-02)
- **Intended behavior**: `has_more` reflects a count-limited response within the requested range
- **Observed caveat**: a 10,000-wide future range can return 10,000 unique rows shifted across the boundary with `has_more = true`; exact in-range key coverage is authoritative
- **Note**: The current generic Deribit API reference states a 1,000 maximum, while `history.deribit.com` accepted and returned 10,000 rows in the 2026-08-02 production probe. Treat this as a history-host compatibility contract and monitor it for drift.

### `start_seq` and `end_seq` Parameters

- **`start_seq`**: Inclusive lower bound
- **`end_seq`**: Inclusive upper bound
- **Default result order is not guaranteed**: explicitly compute min/max; do not infer a cursor from the first row
- **Requested as a closed interval**: `[start_seq, end_seq]`; the history host can occasionally return a boundary overlap, so `has_more` and exact key validation remain authoritative

---

## 4. Response Structure

### 4.1 `get_instruments` Response

```json
{
    "jsonrpc": "2.0",
    "id": null,
    "result": [
        {
            "kind": "future",
            "is_active": false,
            "instrument_name": "BTC-17FEB17",
            "expiry": 1487318400000,
            "creation_timestamp": 1486483200000,
            "contract_size": 10,
            "min_trade_amount": 1,
            "tick_size": 0.01,
            "option_type": null,
            "strike": null,
            "settlement_period": "week",
            "base_currency": "BTC",
            "quote_currency": "USD"
        }
    ]
}
```

#### Key Fields

| Field | Description |
|-------|-------------|
| `instrument_name` | Instrument name, used as the JSONL filename |
| `is_active` | `true` = not yet expired, `false` = expired (no longer producing trades) |
| `settlement_period` | `"week"`, `"month"`, `"perpetual"`, etc. |

### 4.2 `get_last_trades_by_instrument` Response

```json
{
    "jsonrpc": "2.0",
    "id": null,
    "result": {
        "trades": [ ... ],
        "has_more": false
    }
}
```

#### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `result.trades` | array\[Trade\] | Array of trades; default ordering may contain local inversions |
| `result.has_more` | boolean | **Whether there is more data within the requested range** (not "whether there is more data beyond the range") |

### 4.3 Trade Structure (Full Field Union)

The following 17 fields represent the union across all trade types. Core 10 fields are always present; the remaining 7 appear only in specific trade types.

| Field | Type | Example | Occurrence |
|-------|------|---------|------------|
| `trade_seq` | integer | `655` | **Always present**. Monotonically increasing unique sequence number, used as pagination cursor |
| `trade_id` | string | `"59348"` | **Always present**. Trade ID |
| `timestamp` | integer (ms) | `1487318049023` | **Always present**. Trade timestamp (milliseconds) |
| `amount` | float | `6000.0` | **Always present**. Trade amount (contract count) |
| `price` | float | `1041.86` | **Always present**. Trade price |
| `direction` | string | `"buy"` / `"sell"` | **Always present**. Taker direction |
| `tick_direction` | integer | `0`, `1`, `2`, `3` | **Always present**. Price movement direction indicator |
| `index_price` | float | `1042.56` | **Always present**. Index price at time of fetch |
| `mark_price` | float \| null | `null` | **Always present**. Mark price at time of fetch (may be null for early data) |
| `instrument_name` | string | `"BTC-17FEB17"` | **Always present**. Instrument name |
| `contracts` | float | `10.0` | Appears for both Future and Option, but not applicable to all types |
| `iv` | float | `0.7549` | **Option only**. Implied volatility |
| `liquidation` | string | `"..."` | **Perpetual only**. Liquidation flag |
| `combo_id` | string | `"BTC-FS-26JUN26_27MAR26"` | **Combo/spread trades only** |
| `combo_trade_id` | string | `"376192167"` | **Combo/spread trades only** |
| `block_trade_id` | string | `"..."` | **Block trades only** |
| `block_rfq_id` | string | `"..."` | **Block trades only** |
| `block_trade_leg_count` | integer | `2` | **Block trades only**. Number of legs |

> **Note**: `gen_parquet.py` uses the full Union Schema above to ensure any trade type is read correctly. Missing fields are automatically filled as null.

---

## 5. Core Behavior Semantics

### 5.1 The True Meaning of `has_more`

**`has_more` is intended to signal that the count limit omitted rows within `[start_seq, end_seq]`, but it is not a completeness proof.**

⚠️ It does **NOT** indicate "there is more data to fetch beyond start_seq".

#### Verified by Testing

Using `BTC-26JUN26` (last_seq = 1,912,937) as an example:

| Query | Result | has_more Meaning |
|-------|--------|-----------------|
| `[1,10000] count=10000` → 10000 trades | `has_more=false` | ✅ All 10000 trades within range returned |
| `[1,10000] count=100` → 100 trades | `has_more=true` | ⚠️ 9900 more trades within range not yet returned |
| No start/end, count=100 → 100 trades | `has_more=true` | ⚠️ Older data available |
| Last chunk → <10,000 trades | `has_more=false` | ✅ All data within range returned |

**Impact on Code**:

- **Future.py**: Carries the exact expected end bound, filters boundary leakage, and compares the unique sequence set with the full interval. Incomplete pages are split and re-fetched under a 32-request budget; only exact coverage is persisted as complete. ✅
- **Option.py**: Uses `next_seq = max(trade_seq) + 1` to advance, independent of response ordering. A full response or `has_more=true` schedules another range. ✅

### 5.2 `trade_seq` Ordering

- **Monotonically increasing**: New trades always have a larger `trade_seq` than older trades
- **Response order is unspecified by default**: real history responses are broadly descending but contain local inversions
- **Cursor rule**: use the maximum `trade_seq` across all returned rows, never `trades[0]`

### 5.3 Chunk Boundary Continuity

#### Test Results

```text
BTC-10SEP21 request [30001,40000]:
  returned unique range [30003,40003], has_more=true
  missing requested keys: 30001, 30002, 30004
  leaked later keys:       40001, 40002, 40003
```

- Responses do not always respect the requested closed interval, even when 10,000 unique rows are returned.
- **Recovery strategy**: filter out-of-range rows, split an incomplete interval, and require the exact sequence set before checkpointing.
- **Duplicate strategy**: restart-safe re-fetches may append duplicates to JSONL; Parquet removes them by `(instrument_name, trade_seq)`.

### 5.4 `count` Parameter and Pagination

- The `count` parameter is a **per-request upper limit**, not a total count
- A CHUNK_SIZE-wide range can still be shifted across boundaries; neither row count nor `has_more` proves coverage
- **When actual data < count**: returns all current data, `has_more=false`. The chunk is final only if the instrument is expired; an active future's partial tail remains pending because the same range can grow later.

---

## 6. Rate Limiting and Error Handling

### Retry Strategy (client.py)

```python
@retry(
    retry=retry_if_exception(is_retryable_exception),
    wait=DeribitRateLimitWait(fallback_wait=wait_random_exponential(multiplier=1, min=1, max=60)),
    stop=stop_after_attempt(10),
    reraise=True,
)
```

| Exception Type | Retry Behavior |
|----------------|----------------|
| `TransportError` (timeout/network/protocol) | Exponential backoff, max wait 60s |
| `HTTPStatusError` (429) | Prefer `Retry-After` Header; fallback to exponential backoff |
| `HTTPStatusError` (408 or 5xx) | Exponential backoff, max wait 60s |
| `HTTPStatusError` (other 4xx) | No retry; fail immediately |
| HTTP 200 JSON-RPC `error` | Raise `DeribitAPIError` with code/message/data; no retry |

### Deribit-specific Headers

| Header | Description |
|--------|-------------|
| `Retry-After` | Suggested wait time in seconds on 429 (code uses this first) |
| `x-ratelimit-reset` | Rate limit reset time (logged only, not used in logic) |

### Connection Pool Configuration

```python
limits=httpx.Limits(
    max_connections=settings.MAX_WORKERS,  # 40
    max_keepalive_connections=20,
    keepalive_expiry=30.0,
)
```

---

## 7. Usage Patterns in Code

### 7.1 Future Fetch Strategy

```
Steps:
  1. get_instruments(currency="BTC", kind="future") → get all Future instruments
  2. get_last_trades_by_instrument(count=1) → get latest trade_seq for each Future
  3. Partition into chunks by CHUNK_SIZE: [(instr, 1), (instr, 10001), (instr, 20001), ...]
  4. Concurrently fetch all chunks using producer-consumer pattern
  5. Each completed chunk: write to JSONL + update SQLite
  6. On completion: finalize_chunks() + finalize_future_meta()
```

**Chunk Generation** (future.py:59-63):

```python
for i in range(1, f.get("last_seq") + 1, CHUNK_SIZE):
    chunks.append((f["instrument"], i))  # chunk_no = start_seq
```

**Chunk Request** (future.py:92-105):

```python
end_seq = start_seq + CHUNK_SIZE - 1  # exactly matches range
trades, has_more = await client.get_trades_chunk(instrument, start_seq, end_seq)
```

**Finalize Logic** (progress.py:120-128):

```python
UPDATE future_chunk 
SET is_done = 1 
WHERE is_done = 0 
AND has_more = 0
AND (count >= ? OR instrument_is_expired)
-- has_more = 0 + count >= CHUNK_SIZE → fixed range is complete
-- has_more = 0 + expired → final partial range can no longer grow
```

### 7.2 Option Fetch Strategy

```
Steps:
  1. get_instruments(currency="BTC", kind="option") → all Option instruments
  2. Filter incomplete options, start from DB last_no
  3. Streaming fetch: get CHUNK_SIZE trades each time
  4. Use on_success callback to dynamically enqueue next chunk (if should_continue)
  5. Each chunk: write to JSONL + update DB (MAX(last_no, ?) prevents rollback)
```

**Streaming Advance** (option.py:89-101):

```python
last_seq_in_chunk = max(trade["trade_seq"] for trade in trades)
next_seq = last_seq_in_chunk + 1            # start seq for next chunk
should_continue = has_more or len(trades) >= CHUNK_SIZE  # more data available?
```

**DB Update Protection** (progress.py:191-204):

```sql
UPDATE option_meta 
SET last_no = MAX(last_no, ?)   -- MAX guard prevents rollback
WHERE instrument = ?
```

---

## 8. Verified Assumptions

The following assumptions combine the original 2025-05-18 endpoint test with a production-history recheck on 2026-08-02:

| # | Assumption | Result |
|---|-----------|--------|
| 1 | Response structure is `{ result: { trades: [...], has_more: bool } }` | ✅ Confirmed |
| 2 | trade_seq is monotonically increasing (new > old) | ✅ Confirmed |
| 3 | Default results are strictly descending | ❌ Refuted; local inversions observed |
| 4 | Future and Option trades share the same structure | ✅ Confirmed (10/10 fields) |
| 5 | `has_more` alone proves whether a range is complete | ❌ Refuted; exact in-range keys are required |
| 6 | count=CHUNK_SIZE with exact range → has_more=false | ❌ Refuted by shifted full future pages |
| 7 | `next_seq = max(trade_seq) + 1` can continue fetching | ✅ Confirmed |
| 8 | Expired last chunk satisfies `count<CHUNK_SIZE AND has_more=0` → finalizable | ✅ Confirmed |
| 9 | Starting from `start_seq=1` covers full history | ✅ Confirmed |
| 10 | Responses stay inside requested chunk boundaries | ❌ Refuted; boundary leakage observed |

---

## 9. Known Issues and Caveats

### 9.1 Chunk Boundary Leakage

- A BTC-10SEP21 `[30001,40000]` probe returned 10,000 unique rows spanning `30003..40003`, omitting three requested keys and leaking three later keys with `has_more=true`.
- **Handling**: filter to the requested range, split incomplete ranges, and verify the exact unique sequence set before checkpointing.
- **Restart behavior**: previously pending chunks can be re-fetched by the adaptive path; JSONL duplicates are removed during Parquet export.

### 9.2 Zero-Trade Instruments

- Approximately 3 early expired Futures (e.g., BTC-15JUL16) have zero trades
- Options have many zero-trade instruments (1,000+)
- **Code handling**: `get_last_trade_seq` returns 0 → `mark_future_complete` marks as done immediately
- Options with no trades return `has_more=False, trades=[]` → `last_seq = start_seq`, no DB update

### 9.3 count Parameter Limit

- The generic API reference documents a lower limit, while the history host accepted 10,000 in the 2026-08-02 probes.
- A 10,000-wide range does not guarantee `has_more=false` or correct boundaries.
- **The code does not trust this behavior**: future verifies exact range coverage; option advances from the maximum returned sequence.

### 9.4 Proxy Support

- Code defaults to using `HTTP_PROXY` / `HTTPS_PROXY` environment variables (including lowercase variants)
- For SOCKS proxy, install `httpx[socks]` dependency (already added in pyproject.toml)

---

*Documentation based on Deribit History API v2 and real endpoint testing conducted on 2025-05-18.*
