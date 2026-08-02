# P0 修复验收记录

日期：2026-08-02
范围：活跃 future 续传、顺序无关 Parquet 去重、转换失败传播与原子发布
环境：macOS arm64、Python 3.10.19、Deribit production history API（只读）

## 结论

三个原始 P0 已通过代码级回归与小规模真实 API 数据验收。当前证据足以证明已修复的具体失败路径不再复现，但**还不能替代 BTC/ETH、future/option 全量数据下载后的最终对账**。

| 验收项 | 结果 | 证据 |
|---|---|---|
| 活跃 future 尾块跨运行增长 | 通过 | 部分尾块保持 pending；第二次增长会重新调度原尾块 |
| `has_more=true` 的满响应 | 通过 | 即使 `count=CHUNK_SIZE` 也保持 pending |
| option 无序响应游标 | 通过 | `next_seq` 与 checkpoint 均取整批最大 `trade_seq` |
| 降序/乱序跨批去重 | 通过 | 精确位图只删除真实重复键 |
| 串行真实响应转换 | 通过 | 10000/10000 唯一键集合一致 |
| 2 进程真实响应转换 | 通过 | 10000/10000 唯一键集合一致 |
| 真实样本 Parquet 校验 | 通过 | 18 列、10000 行、序号范围连续、0 gap |
| 已过期 option 两页闭环 | 通过 | 10593/10593 唯一键集合一致；checkpoint `completed=1`；validator `COMPLETE` |
| 小文件解析失败 | 通过 | 异常上抛、旧产物保持、临时文件删除 |
| 多进程 block 解析失败 | 通过 | 子进程异常上抛、旧产物保持、临时文件删除 |
| 缺失/空输入目录 | 通过 | CLI 非零退出，不再打印假成功 |
| 完整自动化测试 | 通过 | 138 passed（最终门禁结果见本记录末尾） |
| 包构建 | 通过 | sdist 与 wheel 成功，生成物已清理 |

## 1. 线上 API 契约探针

请求目标：`history.deribit.com/api/v2/public/get_last_trades_by_instrument`，instrument 为 `BTC-PERPETUAL`。只读取公开成交，不使用认证信息。

### 1.1 最新 5 条

返回 `trade_seq`：

```text
295692526, 295692525, 295692524, 295692523, 295692522
```

该样本为降序，证明不能假设 JSONL 原始行按序号递增。

### 1.2 1001 序号范围

请求区间 `[295691526, 295692526]`、`count=1001`：

- HTTP 200；
- 返回 1001 行、1001 个唯一 `trade_seq`；
- `has_more=true`；
- 出现 143 次局部升序；
- 最小序号为 `295691525`，比请求起点小 1；请求范围内有一个预期序号未出现。

这条证据支持两项防御：

1. 去重不能使用单个 `max_seen`；
2. `count == CHUNK_SIZE` 不能覆盖服务器明确返回的 `has_more=true`。
3. option 游标不能使用 `trades[0]`，必须扫描整批取最大 `trade_seq`。

### 1.3 默认 10000 序号范围

请求区间 `[295682527, 295692526]`、`count=10000`：

```text
HTTP             200
rows             10000
unique keys      10000
min/max          295682527 / 295692526
has_more         false
missing expected 0
outside range    0
local inversions 1885
```

Deribit 当前通用 API 文档写明 `count` 最大 1000，但 production history host 在本次探针中接受并完整返回了 10000。两者存在接口文档差异，应保留一个低频契约测试，避免服务端未来收紧限制后静默影响下载器。

## 2. P0-1：future checkpoint 验收

SQLite 状态回归覆盖：

1. 活跃 instrument 第一次最新序号为 15000；
2. chunk 1 返回 10000 行且 `has_more=false`，完成；
3. chunk 10001 返回 5000 行且 `has_more=false`，保持 pending；
4. 第二次最新序号增长至 25000；
5. pending 集合为 `{10001, 20001}`，不会直接跳过 15001..20000；
6. 任意 `has_more=true` 的响应均不 finalize，即使返回行数等于 `CHUNK_SIZE`。

对应测试位于 `tests/test_progress.py`。

## 3. P0-2：真实响应端到端对账

把 10000 条线上响应转换成临时 JSONL（约 2.37 MB），分别执行：

### 串行流式路径

```text
large_file_threshold = 0
stream_batch_size     = 333
input rows            = 10000
input unique keys     = 10000
Parquet rows          = 10000
Parquet unique keys   = 10000
key sets equal        = true
schema columns        = 18
bitmap size           = 35.25 MiB at max trade_seq 295692526
```

### 多进程 block 路径

```text
stream_workers        = 2
block_bytes           = 100000
Parquet rows          = 10000
Parquet unique keys   = 10000
key sets equal        = true
```

临时线上样本只保存在 `/tmp/deribit-live-acceptance`，不提交到仓库。

对并行输出继续运行 `validate_data.py --type future`，结果为 18 列、10000 行、序号范围 `295682527..295692526`、0 gap，退出码 0。位图大小由最大序号跨度决定；BTC-PERPETUAL 当前探针约占 35.25 MiB，这一数值可接受但仍需在完整 90 GB 输入上测量总峰值 RSS。

同一线上证据还推动了 option 游标修复：`fetch_option_chunk` 不再假定第一行最大，而是取整批 `max(trade_seq)`；`sync_option_db` 也扫描每个 item 的全部成交后再更新 checkpoint。回归测试明确构造第一行不是最大值的响应。

### 3.1 已过期 option 两页闭环

选择公开历史 instrument `BTC-27JUN25-100000-C`。`count=1` 探针给出的最终 `trade_seq` 为 10593；随后按生产配置的 10000 序号区间读取：

- `[1, 10000]` 返回 10000 行、10000 个唯一序号、`has_more=false`；尽管服务端没有跨区间提示更多数据，`fetch_option_chunk` 仍因满页正确产生 `next_seq=10001`；
- `[10001, 20000]` 返回 593 行、593 个唯一序号、`has_more=false`，已过期 instrument 被标记完成；
- 两页分别出现 463 与 14 次局部升序，继续证明响应顺序不能作为游标或去重不变量。

将两页真实响应依次送入 `fetch_option_chunk`、`sync_option_db` 与 `JSONLinesSink` 后，SQLite 记录为 `last_no=10593, is_completed=1`。再运行项目原生 `gen_parquet.py` 和 checkpoint-aware `validate_data.py`：JSONL 与 Parquet 均为 10593 行/10593 个唯一键，键集合相等，序号范围 `1..10593`，validator 返回 `COMPLETE`（退出码 0）。临时验收数据位于 `/tmp/deribit-option-acceptance.PeiTNz`，不提交仓库。

## 4. P0-3：故障注入验收

自动化测试分别向小文件 reader 和多进程 block 注入非法 JSON，并预先放置一个“上一版正式产物”。两条路径都满足：

- 转换抛出异常；
- 正式输出字节不变；
- 同目录 `.tmp` 文件被清理；
- CLI 对缺失输入目录返回退出码 1。

## 5. 自动化门禁

最终交付前执行：

```text
pytest -q                 138 passed, coverage 83.54% (floor 80%)
ruff check .              passed
ruff format --check .     passed
pyrefly check             0 errors
git diff --check          passed
uv build --offline        passed
```

## 6. 尚未完成的最终验收

- [ ] 完整下载一个已过期 future，并将预期 `[1, last_seq]` 与 Parquet 唯一键集合对账；
- [ ] 对一个活跃 future 跨两个时间点运行，验证增量区间连续；
- [x] 完整下载一个有多页成交的已过期 option；
- [ ] BTC 与 ETH 各至少完成一个真实 instrument；
- [ ] 在接近目标数据规模的输入上记录峰值 RSS、吞吐和临时磁盘占用；
- [x] 处理审计报告中的 P1：动态队列死锁、consumer 失败监督、并行内存增长、校验盲区（代码级回归已通过；真实规模 RSS 单列在上一项）。

在以上清单完成前，可以确认“三个已知 P0 路径已修复”，但仍不应宣称整个数据集已经获得生产级全量认证。
