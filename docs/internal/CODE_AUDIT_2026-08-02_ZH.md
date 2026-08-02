# deribit-historical-data Python 工程审计报告

审计日期：2026-08-02
审计基线：`main` / `614ff5a`（`release: v0.1.0`）
审计范围：`src/deribit_fetcher/`、`scripts/`、`tests/`、构建配置、CI 与核心文档
审计方式：静态代码审查、现有测试/质量门禁、最小可复现实验；未连接 Deribit 线上 API，也未使用真实 90 GB 数据集做端到端核验。

> **审计后修复状态（2026-08-02）**：本报告识别的三个 P0 已在提交 `8d97623` 修复。P1-1/P1-2 已在提交 `ca523cd` 修复；P1-3 已在提交 `7e247b3` 修复；P1-4 已在提交 `62bec2c` 修复；P1-5 的代码级内存放大已在提交 `9de8870` 修复。P2 的 dead-letter 与 option checkpoint 假成功路径已在提交 `7619c34` 修复；HTTP 重试分类与 JSON-RPC 错误解析已在提交 `d099e57` 修复。本轮整改进一步建立 Ruff lint/format、Pyrefly 和 80% 覆盖率 CI 门禁，并把 Ruff 移出运行时依赖。完整回归现为 **138 passed**，总覆盖率 **84%**；真实 BTC-PERPETUAL 10000 条样本在串行与 2 进程路径均完成唯一键集合对账。下文保留修复前的发现、复现和验收依据；主要剩余证据缺口是完整 90 GB 数据的峰值 RSS 实测。

## 1. 结论

这是一个结构清楚、文档投入明显、已经具备基本工程化外形的 Python 项目，但当前版本**还不能把生成的 Parquet 视为可信的全量历史数据集**。

根因不是代码风格，而是三条核心正确性链路存在缺口：

1. 活跃期货跨多次运行增长时，会永久跳过上次“尾部未满 chunk”之后的一段成交；
2. 大文件流式去重建立在错误的文件内单调性假设上，会把唯一记录当成重复记录删除；
3. Parquet 转换会吞掉文件/数据块异常并以退出码 0 结束，自动化无法区分完整输出和部分输出。

因此建议当前状态定义为：

- 下载与转换工具：**可用于学习和受控实验**；
- 全量性保证：**未成立**；
- 生产或研究数据源：**整改 P0 并完成真实数据闭环验证前不建议使用**。

修复前综合评分：**5.2 / 10**。P0 修复后的生产可用性仍需真实 Deribit 数据闭环验证，暂不重新打分。

| 维度 | 评分 | 评价 |
|---|---:|---|
| 架构与可读性 | 7.5 | 分层合理，下载、持久化、编排、转换职责较清楚 |
| 数据正确性 | 3.0 | 存在可复现的静默漏数路径 |
| 并发与故障恢复 | 4.0 | 有限流、重试、检查点，但存在死锁和失败传播问题 |
| 测试有效性 | 6.0 | 103 个测试全绿，但关键测试使用了与真实输入相反的数据不变量 |
| 构建与 CI | 7.0 | lint、3 个 Python 版本测试、锁文件和可构建包均已具备 |
| 文档 | 8.0 | 文档完整，但若干关键正确性与内存承诺高于实际实现 |

## 2. 已验证基线

| 检查 | 结果 | 备注 |
|---|---|---|
| 完整测试 | `103 passed` | 沙箱外 Python 3.10.19；沙箱内两项多进程测试因 macOS semaphore 权限失败，不属于代码失败 |
| 覆盖率 | `61%` | `parquet.py` 仅 33%，`fetcher.py` 43%；`fail_under = 0`，CI 实际没有覆盖率门槛 |
| Ruff lint | 通过 | `ruff check .` |
| Ruff format | 通过 | `ruff format --check .` |
| 包构建 | 通过 | sdist 与 wheel 均成功生成；审计后已清理生成物 |
| 工作树 | 审计前干净 | 本报告是唯一预期新增文件 |

“测试全绿”与“数据正确”并不矛盾：当前测试主要验证函数在给定假设下工作，却没有验证这些假设是否与真实 API 返回顺序、并发写入顺序和跨运行增长一致。

## 3. 发现汇总

| ID | 级别 | 问题 | 主要后果 |
|---|---|---|---|
| P0-1 | P0 | 活跃期货的尾部 chunk 被过早永久完成 | 跨运行后出现不可自愈的数据缺口 |
| P0-2 | P0 | 大文件去重错误依赖 `trade_seq` 按文件顺序递增 | 唯一成交被静默删除 |
| P0-3 | P0 | Parquet 文件/块异常被吞掉，CLI 仍返回成功 | 部分数据被当成完整产物发布 |
| P1-1 | P1 | 动态续页回调向同一个有界队列阻塞入队 | 队列饱和时下载死锁 |
| P1-2 | P1 | consumer 异常未纳入主任务监督 | 写盘/DB 失败可能表现为永久挂起 |
| P1-3 | P1 | 校验只检查现有数据的 `min..max` | 缺首段、缺尾段、缺整个 instrument 仍可显示成功 |
| P1-4 | P1 | `--no-dedup` 实际仍执行文件内/批内去重 | CLI 契约与结果不一致，基准结果失真 |
| P1-5 | P1 | 并行/小文件路径保留全部中间结果 | 大数据下内存随输入规模增长 |
| P2-1 | P2 | 失败状态不能可靠传递给调用方 | dead-letter 或缺目录仍可能退出 0 |
| P2-2 | P2 | HTTP 重试范围过宽 | 4xx 参数错误也会重试 10 次 |
| P2-3 | P2 | 依赖与类型/覆盖率门禁仍偏弱 | 升级回归和边界类型错误不易提前发现 |

## 4. 原始发现与整改依据

### P0-1：活跃期货跨运行增长会产生永久缺口

**证据**

- `future.py:158-163` 每次按固定边界 `1, 10001, 20001...` 预分配 chunk，并通过 `INSERT OR IGNORE` 保留历史状态；
- `progress.py:137-151` 将 `has_more = 0` 的尾部 chunk 标为 `is_done = 1`，即使它只有部分数据；
- 活跃合约不会被标记为整个 instrument 完成，因此下次仍会刷新最新 `last_seq`，但已完成的尾部 chunk 不会重新打开。

最小状态实验：第一次最新序号为 15000，`[10001, 20000]` 只写入 10001..15000 后被标记完成；第二次最新序号增长到 25000，待处理列表只剩 `chunk_no=20001`。因此 15001..20000 永远不会再被请求。

**影响**

- `BTC-PERPETUAL` 以及任何跨运行继续增长的活跃 future 都可能漏数；
- 这是确定性的状态机错误，不依赖网络异常；
- JSONL 重跑、SQLite 续传和当前测试均不能自愈。

**整改建议**

优先把活跃 future 改为“连续高水位”模型，持久化实际写盘成功的最大 `trade_seq`，从 `max_seq + 1` 继续；或至少不完成未满的活跃尾部 chunk，并在下次重新抓取尾部范围。前者更清晰，也与 option 的续传模型一致。

**验收标准**

- 第一次目标 `last_seq=15000`、第二次 `last_seq=25000` 后，最终唯一序号严格覆盖 1..25000；
- 第二次调度必须包含 15001，而不是直接从 20001 开始；
- 在“写盘成功、DB 更新前崩溃”和“DB 更新成功后重启”两种注入故障下，无缺口，允许可去重的重复。

### P0-2：大文件流式去重会删除唯一记录

**证据**

- `parquet.py:77-85` 用 `trade_seq > max_seen` 判断跨批重复；
- `parquet.py:420-434` 对每个后续批次应用这一过滤；
- 本仓库自己的 `docs/deribit-api.md:30-32` 明确说明 API 每个响应按最高序号优先，即 chunk 内是降序；
- future chunk 并发完成后按到达顺序追加到同一 JSONL，文件级顺序更不可能保证递增；option 即便 chunk 之间向前推进，chunk 内仍为降序。

最小复现将唯一序号 `[5, 4, 3, 2, 1]` 按每批 2 行走“大文件”路径，生成的 Parquet 只剩 `[4, 5]`。3 条唯一记录被当作跨批重复删除。

测试未发现问题的原因是 `tests/test_parquet.py:430-439` 构造的是递增序列 1..50，正好把错误前提写进了 fixture。

**影响**

- 默认对大于等于 100 MB 的 JSONL 启用该路径，最重要的大型 perpetual 文件首当其冲；
- 输出仍是合法 Parquet，错误是静默的；
- 当前 gap 校验可能发现中间缺口，但无法证明首尾和 instrument 全量性，不能作为可靠兜底。

**整改建议**

立即移除基于单个 `max_seen` 的去重优化，直到写入顺序不变量得到代码级保证。可选方案是：

1. 先生成按 `trade_seq` 排序的磁盘中间 run，再做外部归并去重；
2. 下载阶段保存 chunk 边界/独立 chunk 文件，转换时按边界有序归并；
3. 使用可落盘的精确去重引擎，而不是用错误的单调性换内存。

**验收标准**

- 递增、降序、chunk 内降序但 chunk 间递增、future chunk 随机完成四种输入都保留相同的唯一键集合；
- 串行和多进程路径逐键一致；
- 用真实 JSONL 抽样同时比较输入 `n_unique(instrument_name, trade_seq)` 与 Parquet 唯一键数。

### P0-3：Parquet 转换会静默跳过坏文件/坏数据块并返回成功

**证据**

- `parquet.py:105-124` 捕获小文件的所有异常，返回 `df=None`；调用方仍增加 `processed_count`；
- `parquet.py:168-194` 对并行数据块采用同样策略；
- `parquet.py:516-517` 捕获顶层所有异常但不重新抛出；
- `parquet.py:342-349` 对输入目录不存在/无文件直接 `return`；
- `scripts/gen_parquet.py:94-106` 无论结果如何都会记录 `Done.`。

已验证：对不存在的 `--base-dir` 运行转换，日志先打印 `ERROR Data directory not found`，随后打印 `Done.`，进程退出码为 0。

**影响**

- 单个损坏 JSON 行可以让整个小文件或一个大文件 block 消失；
- CI、cron、数据流水线会把失败当成功；
- 若覆盖已有 Parquet，失败时还可能留下不完整的新文件。

**整改建议**

- 文件/块级失败必须汇总并最终抛出异常；
- 写入同目录临时文件，全部成功并完成行数/Schema 校验后再原子替换正式文件；
- 返回结构化结果（输入文件数、成功数、失败路径、输入/输出唯一键数），CLI 根据结果返回非零状态。

**验收标准**

- 任一 JSONL 含坏行时退出非 0，并打印具体文件与块偏移；
- 已存在的正式 Parquet 校验和保持不变；
- 空目录、目录不存在、writer 未初始化均返回非 0。

### P1-1：动态任务生成可使有界队列死锁

> **修复状态：已修复。** 初始任务仍最多占 `task_queue_size` 个槽位，物理队列额外预留 `worker_count` 个槽位，供每个正在执行的任务非阻塞地产生一个 follow-up 或 retry。逻辑任务计数覆盖动态任务，因此完成判断不再依赖可能瞬时归零的 `Queue.join()`。`worker_count=2`、`task_queue_size=1`、3 个初始任务及 3 个 follow-up 的确定性回归测试可在 1 秒内完成，队列上界为 3，不使用无限队列。

`engine.py:68-74` 在 worker 内先执行 `on_success`，option 的回调再通过 `engine.py:40-46` 阻塞写回同一个有界 `task_queue`。当队列已满且所有 worker 都在等待写入后续任务时，没有 worker 能继续消费队列，形成循环等待。

最小并发实验使用 `worker_count=2`、`task_queue_size=1`、3 个初始任务和每任务一个 follow-up，能够稳定超时并输出 `deadlocked`。

建议将“分页链”放在单个 instrument worker 内循环，或引入独立调度协程，使 producer 不承担可能阻塞的回写；不要简单改成无限队列。验收测试应在上述最小容量配置下于固定超时内完成。

### P1-2：consumer 失败没有被主任务及时监督

> **修复状态：已修复。** 初始任务投喂与运行完成等待都同时监督 `consumer_task`；首次 flush 抛出 `OSError` 后，主流程会取消 producer、回收后台任务并原样向调用方传播异常。20 个任务、容量均为 1 的压力回归在 1 秒内 fail-fast，不再超时，也不再出现 `Task exception was never retrieved`。

`engine.py:178-207` 创建 consumer 后，主流程只等待 `task_queue.join()` 或停止信号，没有同时等待 `consumer_task`。若 `sync_db_func` 在 `engine.py:122/138/147` 抛异常，consumer 会退出；producer 随后可能填满 `storage_queue`，再也无法完成 `task_queue.task_done()`，主任务永久等待。

建议使用 `TaskGroup` 或显式 `FIRST_EXCEPTION` 监督 producer、consumer、停止信号；任何持久化异常都应取消其余任务、保留未确认 checkpoint 并向 CLI 传播非零退出。新增 `sync_db_func` 第一次 flush 即抛错的超时回归测试。

### P1-3：当前校验不能证明“全量”

> **修复状态：已修复。** validator 现在统计唯一 `trade_seq`，并与对应 SQLite checkpoint 的完整 instrument inventory、已知下界及 completed 状态对账。缺首段、缺已知尾段、缺整个 instrument、重复掩盖缺口均判 `INCOMPLETE`（退出码 1）；覆盖当前已知范围但 checkpoint 尚未完成时判 `UNKNOWN`（退出码 2）；只有最终 checkpoint 精确匹配才判 `COMPLETE`（退出码 0）。future 与 option 两条路径均有回归测试。

`parquet.py:591-636` 只在 Parquet 中已存在的每个 instrument 内比较 `count` 与 `max_seq-min_seq+1`，因此以下情况仍可能显示成功：

- 缺少 1..`min_seq-1`；
- 缺少 `max_seq+1`..目标最新序号；
- 整个 instrument 缺失；
- 未去重数据中，重复行数量恰好掩盖缺口。

建议把“结构校验”和“全量校验”拆开。全量校验必须读取下载清单/检查点：对每个 instrument 比较预期起点、目标 `last_seq`、唯一键数与实际边界；无法取得目标边界时输出 `UNKNOWN`，不能输出成功。

### P1-4：`--no-dedup` 没有真正关闭去重

> **修复状态：已修复。** `dedup` 参数现已传入 `read_and_dedup_file`、`stream_batches`、`parallel_read_large_file` 及其子进程 block worker；关闭时不执行 intra-file、intra-batch、cross-block 或 merge 层去重。含 `[1, 1, 2]` 的同一输入在小文件、串行流式、2 进程 block 三条生成路径中均输出 3 行；默认开启去重的原有精确位图测试保持通过。

`read_and_dedup_file`、`stream_batches` 和 `_process_block` 都无条件调用 `dedup_intra`；多进程路径还无条件执行跨 block 去重。`dedup=False` 只关闭外层部分跨批/跨文件逻辑。

最小复现输入 `[1, 1, 2]` 并传 `dedup=False`，输出仍为 `[1, 2]`。这与 CLI 帮助和文档“跳过去重”的承诺不一致，也会使 benchmark 的“关闭去重”用例失真。

建议让 dedup 策略显式贯穿所有 reader，或移除该开关并把行为写清。验收应断言 `--no-dedup` 输出行数与原始有效 JSON 行数一致。

### P1-5：所谓“有界内存”没有覆盖并行路径

> **修复状态：代码级修复完成，真实规模基准待执行。** 大文件 reader 不再调用 `list(pool.map(...))`，而是按 block 输入顺序维护最多 `2 × workers` 个 Future，消费一个才提交一个；小文件线程池采用相同固定窗口，不再为全部文件一次性建 Future 字典；`read_and_dedup_file` 也不再构造从未使用的逐文件 `set[int]`。调度器测试验证 20 个任务、窗口 3 时最大 outstanding 始终为 3，现有串行/并行结果一致性测试保持通过。由于本次仍未运行真实 90 GB 数据，峰值 RSS 的工程结论保持“待实测”。

- `parquet.py:219-223` 用 `results = list(pool.map(...))` 一次保留大文件全部 block 的 DataFrame；90 GB 输入可能在父进程聚合数百个反序列化结果；
- `parquet.py:494-501` 的 `futures` 字典在阶段结束前保留每个 Future，而 Future 会持有其 DataFrame 结果；
- `prev_keys` 为小文件保存 Python `set[int]`，总大小随所有已处理键数增长。

这与“无论文件多大内存都保持有界”和并行路径的设计目标不一致。建议按 block 序号设置有限 reorder window，消费后立即释放 Future/结果；对小文件避免在一个字典中保留所有已完成 Future，并通过真实基准证明峰值 RSS 不随输入文件总量线性增长。

### P2：工程性问题

1. `engine.dead_letters` 只记录不向上抛；`run_fetcher` 仍执行 finalize 并正常返回。建议把 partial success 设计成显式状态，CLI 默认非零，必要时增加用户主动选择的 `--allow-partial`。
2. `client.py:43-47` 对所有 `HTTPStatusError` 重试，包括确定性的 400/401/403/404。建议只重试连接/超时、429、408 和约定的 5xx；解析 Deribit HTTP 200 错误体并保留错误码。
3. `progress.py:239-243` 吞掉 option checkpoint 更新异常，破坏统一的失败传播语义；应记录上下文后重新抛出。
4. `ruff` 位于运行时依赖而不是 dev dependency；生产安装不应携带代码检查器。
5. `coverage.fail_under = 0` 等于没有覆盖率门禁；CI 也未执行 `ruff format --check` 和静态类型检查。建议先以当前可稳定达到的阈值建立不回退基线，再重点提升编排、Parquet 和 CLI 失败分支。
6. `docs/internal/TECH_DEBT.md` 仍把已修复事项描述为当前状态，`ROADMAP.md` 的部分“完成”状态也未同步。内部审计文档应标注 archived/superseded，避免误导维护决策。

> **失败传播修复状态：已修复。** `engine.run()` 会先排空可安全持久化的成功结果，再在存在 dead-letter 时抛出带失败任务快照的 `FetchTasksFailedError`；`run_fetcher` 因而不会继续执行 future finalize，CLI 也会非零退出。`OptionProgressRepo.update_option_last_no` 的数据库异常现已记录堆栈并重新抛出。持久失败、健康任务并存和 checkpoint 写失败均有回归测试。

> **HTTP 重试修复状态：已修复。** 仅 `TransportError`、HTTP 408/429 与 5xx 进入 tenacity；400/401/403/404 均验证为首次响应即失败。HTTP 200 中的 JSON-RPC `error` 会转为保留 `code/message/data` 的 `DeribitAPIError`，不会被当成成功结果或瞬时故障重试。

> **工程门禁修复状态：已修复。** Ruff 已从运行时依赖移动到 dev group；覆盖率门禁由 0 提升到 80%（当前 83.54%）；CI 新增 `ruff format --check` 与 Pyrefly。与 CI 相同的命令在本地实测为 138 passed、Ruff 通过、Pyrefly 0 errors。旧 `TECH_DEBT` 与学习型 `ROADMAP` 已标注为历史材料，避免再被误读为当前状态。

## 5. 值得保留的设计

- 下载、存储、checkpoint、转换分层明确，模块规模总体可控；
- option 采用“先写盘、后推进 checkpoint”的方向正确，偏向重复而不是漏数；
- `get_last_trade_seq` 已区分 `0` 与 `None`，避免把网络失败伪装成空 instrument；
- 有界队列、API 限流、`Retry-After`、engine retry budget、dead-letter 等机制都说明项目已经开始处理真实运行问题；
- 测试、Ruff、CI、锁文件、双语文档和构建配置齐全，适合作为后续整改的可靠骨架。

这些优点值得保留，但不能抵消 P0 数据不变量错误；后续应优先修正状态机与数据算法，而不是继续扩展功能或优化吞吐。

## 6. 建议整改顺序

### 阶段 A：先恢复正确性（预计 2–4 天）

1. 修复活跃 future 连续高水位；
2. 删除错误的 `max_seen` 去重，建立对任意真实写入顺序正确的算法；
3. 转换失败向上抛、临时文件写入、原子发布；
4. 增加三类回归 fixture：降序、乱序 chunk、跨运行增长。

完成定义：所有 P0 验收通过；现有真实 JSONL 抽样重新转换后，输入与输出唯一键集合一致。

### 阶段 B：修复并发和可观测性（预计 1–2 天）

1. 消除动态队列循环等待；
2. 统一监督 producer/consumer，持久化失败立即 fail-fast；
3. dead-letter、缺目录、坏文件均返回非零状态；
4. 增加失败注入测试和结构化运行摘要。

完成定义：小队列压力测试、consumer 故障测试、SIGINT 恢复测试稳定通过，无超时和假成功。

### 阶段 C：再谈性能和工程门禁（预计 2–3 天）

1. 重写并行 reader 为有限 reorder window，并做峰值 RSS 基准；
2. 修正 `--no-dedup` 契约；
3. 全量校验关联 instrument inventory 与 checkpoint；
4. 建立覆盖率、format、类型检查门禁，整理依赖与过期内部文档。

完成定义：性能数字来自可复现实验；README 不再包含未经测量的内存/压缩/吞吐承诺。

## 7. 发布门槛建议

下一版本发布前至少满足：

- [x] P0-1、P0-2、P0-3 全部修复并有失败前/修复后的回归测试；
- [ ] 对 BTC future 与 option 各选一组真实数据，比较 JSONL 与 Parquet 的唯一键集合；
- [x] 校验能识别缺首段、缺尾段、缺 instrument 和重复掩盖缺口，并区分未知最终边界；
- [ ] 任一读取/写入/checkpoint 错误导致非零退出，旧产物不被覆盖；
- [x] 压力测试证明小容量队列不会死锁，consumer 失败会在固定超时内向上冒泡；
- [ ] CI 强制 test、lint、format，并设置非零覆盖率下限；
- [ ] README 中所有“全量、无 OOM、去重关闭、Done”语义与可执行证据一致。

在这些条件满足之前，README 和版本说明应把能力表述收敛为“实验性历史成交下载器”，避免使用未经验证的“全量”和“无论文件多大都不会 OOM”。
