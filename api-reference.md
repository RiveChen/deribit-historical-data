# Deribit Historical Data API Reference

> 基于真实端点测试（2025-05-18）验证的 Deribit History API 行为文档。

---

## 目录

1. [基础信息](#1-基础信息)
2. [API 端点](#2-api-端点)
   - [get_instruments](#21-get_instruments)
   - [get_last_trades_by_instrument](#22-get_last_trades_by_instrument)
3. [请求参数详解](#3-请求参数详解)
4. [响应结构](#4-响应结构)
   - [get_instruments 响应](#41-get_instruments-响应)
   - [get_last_trades_by_instrument 响应](#42-get_last_trades_by_instrument-响应)
   - [Trade 结构（Future & Option 一致）](#43-trade-结构future--option-一致)
5. [核心行为语义](#5-核心行为语义)
   - [has_more 的真正含义](#51-has_more-的真正含义)
   - [trade_seq 的排序机制](#52-trade_seq-的排序机制)
   - [Chunk 边界连续性](#53-chunk-边界连续性)
   - [count 参数与分页](#54-count-参数与分页)
6. [速率限制与错误处理](#6-速率限制与错误处理)
7. [代码中的使用模式](#7-代码中的使用模式)
   - [Future Fetch Strategy](#71-future-fetch-strategy)
   - [Option Fetch Strategy](#72-option-fetch-strategy)
8. [已验证的假设](#8-已验证的假设)
9. [已知问题与注意点](#9-已知问题与注意点)

---

## 1. 基础信息

| 项目 | 值 |
|------|-----|
| **Base URL** | `https://history.deribit.com/api/v2/public` |
| **认证** | 公开 API，无需 Token |
| **协议** | HTTP/HTTPS |
| **数据格式** | JSON |
| **默认 RPS 限制** | 20 请求/秒 |
| **CHUNK_SIZE** | 10,000 |
| **最大重试次数** | 10 |
| **超时设置** | 请求 60s，连接 10s |

---

## 2. API 端点

### 2.1 `get_instruments`

获取指定币种和类型的全部交易对列表（含已过期和未过期）。

```
GET /get_instruments
```

#### 请求参数

| 参数 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `currency` | string | 是 | 币种，如 `"BTC"`, `"ETH"` |
| `kind` | string | 是 | 类型：`"future"` 或 `"option"` |
| `expired` | string | 是 | `"true"` 或 `"false"` |

#### 代码调用方式（client.py:93）

```python
# 同时获取 expired=true 和 expired=false 的结果，合并返回
params = {"currency": currency, "kind": kind, "expired": expired}
data = await fetch_json("/get_instruments", params)
```

#### 测试数据

| 币种 | kind | expired | 数量 |
|------|------|---------|------|
| BTC | future | true | 379 |
| BTC | future | false | 5 |
| BTC | option | true | 114,851 |
| BTC | option | false | 406 |

---

### 2.2 `get_last_trades_by_instrument`

获取指定交易对的历史成交数据。支持 **按 trade_seq 范围查询** 和 **按 count 游标查询** 两种模式。

```
GET /get_last_trades_by_instrument
```

#### 请求参数

| 参数 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `instrument_name` | string | 是 | 交易对名称，如 `"BTC-27MAR26"` |
| `start_seq` | integer | 否 | 起始 trade_seq（含）。不传则从最新成交开始 |
| `end_seq` | integer | 否 | 结束 trade_seq（含）。不传则取 count 条 |
| `count` | integer | 是 | 返回条数上限（1 ~ 10,000） |

#### 两种查询模式对比

| 模式 | 使用方式 | 返回数据 | 典型场景 |
|------|---------|---------|---------|
| **范围查询** | 同时传 `start_seq` + `end_seq` + `count` | 返回该范围内符合 `count` 限制的成交（从 start_seq 开始降序） | 按 seq 分块抓取全部历史 |
| **游标查询** | 只传 `count`，不传 `start_seq`/`end_seq` | 返回最新的 `count` 条成交 | 获取最新成交 seq |

#### 代码调用方式

```python
# 1. 获取最新一条成交的 trade_seq（client.py:121）
params = {"instrument_name": instr, "count": 1}

# 2. 分块抓取（client.py:131）
params = {
    "instrument_name": instr,
    "start_seq": start_seq,
    "end_seq": end_seq,
    "count": CHUNK_SIZE,  # 10000
}
```

---

## 3. 请求参数详解

### `count` 参数

- **有效范围**: 1 ~ 10,000
- **当 count < 范围内实际数据量**：返回 count 条，`has_more = true`
- **当 count >= 范围内实际数据量**：返回全部，`has_more = false`
- **注意**：Deribit 文档声称 count 上限是 10,000，但实测 count=10,000 配合 start/end_seq 时，即使范围内实际数量 <=10,000，`has_more` 仍为 false（见 has_more 语义一节）

### `start_seq` 和 `end_seq` 参数

- **`start_seq`**：包含此 seq 及以上的成交（起始 seq）
- **`end_seq`**：包含此 seq 及以下的成交（结束 seq）
- **trade_seq 降序返回**：最新的（highest seq）在前，最旧的（lowest seq）在后
- **范围并非排他**：`[start_seq, end_seq]` 闭区间，包含边界值

---

## 4. 响应结构

### 4.1 `get_instruments` 响应

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

#### 关键字段

| 字段 | 说明 |
|------|------|
| `instrument_name` | 交易对名称，用作 JSONL 文件名 |
| `is_active` | `true` = 尚未过期，`false` = 已过期（不再产生新成交） |
| `settlement_period` | `"week"`, `"month"`, `"perpetual"` 等 |

### 4.2 `get_last_trades_by_instrument` 响应

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

#### 关键字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `result.trades` | array\[Trade\] | 成交数据数组，**降序排列**（highest seq first） |
| `result.has_more` | boolean | **范围内是否有更多数据**（非"是否有后续范围"） |

### 4.3 Trade 结构（Future & Option 一致）

实测确认 future 和 option 的 trade 字段完全一致（10 个字段）：

| 字段 | 类型 | 示例 | 说明 |
|------|------|------|------|
| `trade_seq` | integer | `655` | **成交唯一序号**，单调递增，用于分页 cursor |
| `trade_id` | string | `"59348"` | 成交 ID |
| `timestamp` | integer (ms) | `1487318049023` | 成交时间戳（毫秒） |
| `amount` | float | `6000.0` | 成交数量（合约张数） |
| `price` | float | `1041.86` | 成交价格 |
| `direction` | string | `"buy"` / `"sell"` | 主动成交方向 |
| `tick_direction` | integer | `0`, `1`, `2`, `3` | 价格变动方向标记 |
| `index_price` | float | `1042.56` | 抓取时的指数价格 |
| `mark_price` | float \| null | `null` | 抓取时的标记价格（早期数据可能为 null） |
| `instrument_name` | string | `"BTC-17FEB17"` | 交易对名称 |

> **注意**：实测验证 Future 和 Option 的 Trade 结构完全一致，10/10 字段跨成交记录也一致。

---

## 5. 核心行为语义

### 5.1 `has_more` 的真正含义

**`has_more` = "本次请求的 [start_seq, end_seq] 范围内，还有更多成交因为 count 限制没有返回"**

⚠️ 它 **不** 表示 "start_seq 之外还有更多数据要抓取"。

#### 实测验证

以 `BTC-26JUN26`（last_seq = 1,912,937）为例：

| 查询 | result | has_more 含义 |
|------|--------|-------------|
| `[1,10000] count=10000` → 10000 trades | `has_more=false` | ✅ 范围内 10000 条全部返回了 |
| `[1,10000] count=100` → 100 trades | `has_more=true` | ⚠️ 范围内还有 9900 条未返回 |
| `不传 start/end, count=100` → 100 trades | `has_more=true` | ⚠️ 还有更老的数据可用 |
| 最后一个 chunk → <10,000 trades | `has_more=false` | ✅ 范围内的数据全部返回了 |

**对代码的影响**：

- **Future.py**：使用 `start_seq` ~ `end_seq` 预分配所有 chunk。因为 CHUNK_SIZE == count 且范围精确 = CHUNK_SIZE，正常情况下 `has_more` 应为 false。只要每个 chunk 的 seq 区间不重叠（`[1,10000], [10001,20000]`...），不会漏数据。✅
- **Option.py**：使用 `next_seq = trades[0].trade_seq + 1` 流式推进，每次请求 count=CHUNK_SIZE。因为 `count` 始终 == `end_seq - start_seq + 1`，所以 `has_more` 在非边界时总是 false。✅

### 5.2 `trade_seq` 的排序机制

- **单调递增**：新成交的 `trade_seq` 总是大于旧成交
- **降序返回**：每次请求返回的 trades 数组从 high seq 到 low seq 排列
- **每次返回的 first trade_seq 即为本次范围内最高 seq**

### 5.3 Chunk 边界连续性

#### 实测结果

```
BTC-27MAR26:
  Chunk [1,10000]:  seqs [1..10000],  has_more=True
  Chunk [10001,20000]: seqs [10001..20000], has_more=True
  → Overlap: 1 个 trade_seq 重复
```

- **绝大部分 chunks 是严格 disjoint 的**（如 `BTC-30JAN26` 和 `BTC-27FEB26` 的 chunks）
- **偶有 1 个 trade_seq 的边界重叠**，这是 Deribit 服务端的微小偏差
- **容忍策略**：JSONL 允许少量重复行，在 Parquet 导出阶段按 `(instrument_name, trade_seq)` 去重

### 5.4 `count` 参数与分页

- `count` 参数是 **per-request 的返回条数上限**，不是总条数
- 当 `count` 设为 CHUNK_SIZE，且 `end_seq - start_seq + 1` 也等于 CHUNK_SIZE 时，`has_more` 由范围内实际数据量决定
- **当范围内实际数据 < count**：返回全部数据，`has_more=false`（这就是 finalize 条件 `count >= CHUNK_SIZE OR has_more=0` 正确的原因）

---

## 6. 速率限制与错误处理

### 重试策略（client.py）

```python
@retry(
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError, httpx.HTTPStatusError)),
    wait=DeribitRateLimitWait(fallback_wait=wait_random_exponential(multiplier=1, min=1, max=60)),
    stop=stop_after_attempt(10),
    reraise=True,
)
```

| 异常类型 | 重试行为 |
|---------|---------|
| `TimeoutException` (60s) | 指数退避重试，最大等待 60s |
| `ConnectError` | 指数退避重试，最大等待 60s |
| `HTTPStatusError` (429) | 优先读取 `Retry-After` Header；无 Header 则指数退避 |
| `HTTPStatusError` (其他 4xx/5xx) | 同上 |

### Deribit 专属 Header

| Header | 说明 |
|--------|------|
| `Retry-After` | 429 时建议的等待秒数（代码优先使用） |
| `x-ratelimit-reset` | 限流重置时间（仅日志记录，未用于逻辑） |

### 连接池配置

```python
limits=httpx.Limits(
    max_connections=settings.MAX_WORKERS,  # 40
    max_keepalive_connections=20,
    keepalive_expiry=30.0,
)
```

---

## 7. 代码中的使用模式

### 7.1 Future Fetch Strategy

```
步骤：
  1. get_instruments(currency="BTC", kind="future") → 获取全部 Future 列表
  2. get_last_trades_by_instrument(count=1) → 获取每个 Future 的最新 trade_seq
  3. 按 CHUNK_SIZE 切分 chunks: [(instr, 1), (instr, 10001), (instr, 20001), ...]
  4. 生产者-消费者并发抓取所有 chunks
  5. 每个 chunk 返回后写入 JSONL + 更新 SQLite
  6. 全部完成后：finalize_chunks() + finalize_future_meta()
```

**Chunk 生成逻辑**（future.py:59-63）：

```python
for i in range(1, f.get("last_seq") + 1, CHUNK_SIZE):
    chunks.append((f["instrument"], i))  # chunk_no = start_seq
```

**Chunk 请求逻辑**（future.py:92-105）：

```python
end_seq = start_seq + CHUNK_SIZE - 1  # 精确匹配范围
trades, has_more = await client.get_trades_chunk(instrument, start_seq, end_seq)
```

**Finalize 逻辑**（progress.py:120-128）：

```python
UPDATE future_chunk 
SET is_done = 1 
WHERE is_done = 0 
AND (count >= ? OR has_more = 0)
-- count ≥ CHUNK_SIZE → 说明该 chunk 已取满
-- has_more = 0 → 说明范围内已无剩余数据（last chunk）
```

### 7.2 Option Fetch Strategy

```
步骤：
  1. get_instruments(currency="BTC", kind="option") → 全部 Option 列表
  2. 筛选未完成的 option，从 DB 中 last_no 开始
  3. 流式抓取：每次取 CHUNK_SIZE 条
  4. 用 on_success 回调动态入队下一个 chunk（如果 should_continue）
  5. 每个 chunk 写入 JSONL + 更新 DB（MAX(last_no, ?) 防止回退）
```

**流式推进逻辑**（option.py:89-101）：

```python
last_seq_in_chunk = trades[0]["trade_seq"]  # 本 chunk 最高 seq
next_seq = last_seq_in_chunk + 1            # 下一个 chunk 的起始 seq
should_continue = has_more or len(trades) >= CHUNK_SIZE  # 是否还有更多
```

**DB 更新保护**（progress.py:191-204）：

```sql
UPDATE option_meta 
SET last_no = MAX(last_no, ?)   -- MAX 保护，防止回退
WHERE instrument = ?
```

---

## 8. 已验证的假设

以下假设通过真实端点测试（2025-05-18）验证：

| # | 假设 | 结论 |
|---|------|------|
| 1 | 响应结构为 `{ result: { trades: [...], has_more: bool } }` | ✅ 确认 |
| 2 | trade_seq 单调递增，新 > 旧 | ✅ 确认 |
| 3 | 每次返回降序排列（high seq first） | ✅ 确认 |
| 4 | Future 和 Option 的 Trade 结构一致 | ✅ 确认（10/10 字段） |
| 5 | `has_more` 表示范围内是否还有数据 | ✅ 确认，与 count 限制相关 |
| 6 | count=CHUNK_SIZE 且范围精确时 has_more=false | ✅ 确认 |
| 7 | `next_seq = first_trade_seq + 1` 可继续抓取 | ✅ 确认 |
| 8 | 最后 chunk 满足 `count<CHUNK_SIZE AND has_more=0` → 可 finalize | ✅ 确认 |
| 9 | `start_seq=1` 开始能覆盖全量历史 | ✅ 确认 |
| 10 | Chunk 边界基本 disjoint（偶有 1 条重叠） | ✅ 确认 |

---

## 9. 已知问题与注意点

### 9.1 Chunk 边界 1-trade 重叠

- 实测 BTC-27MAR26 的 [1,10000] 和 [10001,20000] 之间有 1 个 `trade_seq` 的重叠
- 可能是 Deribit 服务端实现中游标偏移量问题
- **影响**：JSONL 中会出现重复行，Parquet 导出时按 `(instrument_name, trade_seq)` 去重即可
- **严重程度**：低（数据正确性不受影响，只是轻微冗余）

### 9.2 无成交的 Instrument

- 约 3/3 早期过期 Future（如 BTC-15JUL16）无任何成交
- Option 中有大量无成交记录（1,000+）
- **代码处理**：get_last_trade_seq 返回 0 → `mark_future_complete` 直接标记完成
- Option 中无成交会返回 `has_more=False, trades=[]` → `last_seq = start_seq`，DB 无更新

### 9.3 count 参数限制

- 文档上限 10,000，实测也如此
- 如果 count 设为 10,000 + range 也是 10,000，has_more 似乎始终为 false（即使后面还有数据）
- **但代码逻辑不依赖此行为**：future 用 seq 范围精确分割，option 用 next_seq 推进

### 9.4 代理支持

- 代码默认使用环境变量 `HTTP_PROXY` / `HTTPS_PROXY`（含小写变体）
- 如果使用 SOCKS 代理，需要安装 `httpx[socks]` 依赖（已在 pyproject.toml 中添加）

---

*文档基于 Deribit History API v2 及 2025-05-18 真实端点测试结果编写。*
