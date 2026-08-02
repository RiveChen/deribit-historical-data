# Deribit Historical API Notes

Developer-facing summary of the Deribit **history** API as this project uses it. The exhaustive, test-verified reference (request/response shapes, field tables, verified assumptions) lives in **[api-reference.md](./api-reference.md)** — this page is the short version plus the gotchas that shaped the design.

## Endpoint basics

| Item | Value |
|------|-------|
| Base URL | `https://history.deribit.com/api/v2/public` |
| Auth | none (public) |
| Format | JSON-RPC over HTTP GET |
| Rate limit | ~20 req/s (see `MAX_RPS`) |
| `count` max | 10 000 (= `CHUNK_SIZE`) |
| Timeouts | 60 s request, 10 s connect |

## Endpoints used (`client.py`)

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `get_instruments(currency, kind)` | `/get_instruments` | List all instruments; called twice per kind (`expired=true` and `false`) and merged. |
| `get_last_trade_seq(instrument)` | `/get_last_trades_by_instrument` (`count=1`) | Cursor query for the newest `trade_seq`. Returns `0` (no trades), `>0` (last seq), or `None` (undetermined after retries). |
| `get_trades_chunk(instrument, start_seq, end_seq)` | `/get_last_trades_by_instrument` (range + `count=CHUNK_SIZE`) | Fetch one chunk; returns `(trades, has_more)`. |

## The three gotchas that drive the design

### 1. `has_more` means "more **within the requested range**"

Not "more data beyond `start_seq`". The history host usually treats it as a count-limit signal within `[start_seq, end_seq]`, but real full-sized responses can leak rows across the boundary and still return `has_more=true`. Future correctness therefore comes from exact in-range `trade_seq` coverage: out-of-range rows are filtered and incomplete ranges are split and re-fetched under a bounded request budget. Active future tails remain pending and are safely re-fetched as they grow.

### 2. Trades come back **descending**; `trade_seq` is monotonic

`trade_seq` itself increases over time, but the endpoint's default response order is not guaranteed. Production history responses are broadly descending yet contain local inversions. Options therefore advance with `next_seq = max(trade["trade_seq"] for trade in trades) + 1`, and checkpoint updates use the same explicit maximum. The Parquet merge uses exact bitmap membership rather than any row-order assumption (see [design-decisions.md](./design-decisions.md#7-size-tiered-parquet-merge-order-independent-exact-dedup)).

### 3. Occasional 1-trade chunk-boundary overlap

Deribit sometimes returns one overlapping `trade_seq` at chunk boundaries (e.g. `[1,10000]` and `[10001,20000]` share one). Tolerated on purpose — removed at Parquet dedup by `(instrument_name, trade_seq)`. Severity: cosmetic redundancy, no correctness impact.

## Rate limiting & retries

- One request at a time is gated by `AsyncLimiter(MAX_RPS, 1)`.
- Retries via `tenacity`: up to **10 attempts**, `reraise=True`, for transport errors, HTTP 408/429, and 5xx. Deterministic 4xx responses fail immediately. HTTP-200 JSON-RPC error objects raise `DeribitAPIError` with their code/message/data preserved and are not retried.
- Wait strategy `DeribitRateLimitWait`: prefer the `Retry-After` header (on 429) + small buffer, else random exponential backoff (1–60 s).
- `x-ratelimit-reset` is logged for diagnostics but not used in logic.

## Zero-trade instruments

Some early expired futures and **many** options have no trades. `get_last_trade_seq` returns `0` → the future is marked complete immediately; empty option requests return `has_more=false, trades=[]` and simply don't advance `last_no`. This is normal and not a gap.

## Proxy

`HTTP_PROXY` / `HTTPS_PROXY` (and lowercase variants) are honored. SOCKS proxies need the `httpx[socks]` extra (already a dependency).

---

*For request/response JSON, full field semantics, and the 10-point list of test-verified assumptions (validated 2025-05-18), see [api-reference.md](./api-reference.md).*
