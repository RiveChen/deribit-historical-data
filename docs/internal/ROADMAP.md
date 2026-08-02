# ROADMAP — 从「能用的工具」到「合格的开源 repo」

> 面向**自己手写实现**的学习路线。每个任务给出:目标 / 为什么 / 涉及文件 / 实现提示(不是完整答案)/ 验收标准 / 学习点 / 预估。
> 按依赖顺序做;每做完一项,确保 `pytest` 全绿再进下一项。用 `git commit` 小步提交,commit message 写清楚「做了什么、为什么」。
>
> 2026-08-02 更新：原路线中的 `trade_seq > max_seen` 跨批去重假设已被真实写入顺序反例推翻；当前实现使用顺序无关的精确位图。以下相关任务已同步为精确去重验收。

## 如何使用本文档

- **验收标准是可执行的**:大多是「某测试通过」或「某命令输出满足条件」。写不出验收标准的任务,说明还没想清楚。
- **先写测试或先想验收,再写实现**(古法但有效:红 → 绿 → 重构)。
- **学习点**列出该任务背后要真正理解的概念——面试被追问时,这些就是你的底气。
- 标 ⭐ 的是「投入产出 / 学习价值」最高的。

## 总览

| ID | 任务 | 类别 | 依赖 | 预估 | 优先级 |
|----|------|------|------|------|--------|
| P0-1 | CI 跑测试(已完成) | 基线 | — | — | ✅ 完成 |
| P0-2 | 引入 ruff(lint + format)并进 CI | 基线 | P0-1 | 1–2h | 高 |
| P0-3 | 覆盖率报告 pytest-cov + 阈值 | 基线 | P0-1 | 1h | 中 |
| P0-4 | pyrefly 类型检查进 CI | 基线 | P0-1 | 1h | 中 |
| P1-1 | `config.py` 改 `from_env()` 工厂,去掉双重路径推导 | 重构 | — | 2h | 中 |
| P1-2 | 把 `future`/`option` 的闭包提成可测函数,抽共享编排 | 重构 | — | 半天 | ⭐高 |
| P1-3 | 把 `gen_parquet` / `validate_data` 核心逻辑迁进包内 | 重构 | — | 半天 | ⭐高 |
| P2-1 | `client.py` 测试(重试 / Retry-After / 分页) | 测试 | — | 半天 | ⭐高 |
| P2-2 | 去重逻辑单元测试(文件内 / 跨文件 / 跨批) | 测试 | P1-3 | 半天 | ⭐高 |
| P2-3 | `validate_data` gap 直方图分桶测试 | 测试 | P1-3 | 2–3h | 中 |
| P2-4 | `option.py` 流式 `on_success` 测试 | 测试 | P1-2 | 3h | 中 |
| P3-1 | engine 重试上限 + dead-letter(修 A1) | 健壮性 | — | 半天 | 高 |
| P3-2 | ⭐拔高:真正实现大文件并行读取(把曾经的假 README 变真) | 健壮性 | P1-3, P2-2 | 1–2 天 | 拔高 |
| P4-1 | CONTRIBUTING + CHANGELOG + 打 tag 发布 v0.1.0 | 社区 | 全绿 | 2–3h | 中 |
| P4-2 | README badges(CI / license / python) | 社区 | P0-* | 20m | 低 |

建议节奏:**P0 一天 → P1 两天 → P2 两天 → P3-1 半天 → P4 半天**。约一周达到「合格开源 repo」。P3-2 作为拔高单独安排。

---

## Phase 0 — 工程基线(先立规矩)

### P0-2 引入 ruff ⭐
- **目标**:统一 lint + format,并在 CI 里强制。
- **为什么**:开源 repo 的第一印象;也帮你发现未用 import、可疑写法(还记得那个内联 `import json` 吗)。
- **涉及**:`pyproject.toml`、`.github/workflows/ci.yml`、可选 `.pre-commit-config.yaml`。
- **提示**:在 `pyproject.toml` 加 `[tool.ruff]`(选 `line-length`、`select` 规则集,如 `E,F,I,UP,B`)。本地 `uvx ruff check .` 和 `uvx ruff format --check .`。CI 里加一个 `lint` job。
- **验收**:
  - [ ] `ruff check .` 零报错(或显式 `# noqa` 并注明理由)
  - [ ] `ruff format --check .` 通过
  - [ ] CI 的 lint job 绿
- **学习点**:ruff 规则集含义;为什么 `I`(isort)能消除「内联/乱序 import」;pre-commit 钩子机制。

### P0-3 覆盖率
- **目标**:量化测试覆盖,设最低阈值防止倒退。
- **涉及**:dev 依赖加 `pytest-cov`;CI。
- **提示**:`uv run pytest --cov=deribit_fetcher --cov-report=term-missing`。先看当前覆盖率是多少(心里有数),再在 CI 里加 `--cov-fail-under=<现值>`,以后只许升不许降。
- **验收**:
  - [ ] 本地能打印 term-missing 覆盖
  - [ ] CI 设了 `--cov-fail-under`,且当前通过
- **学习点**:行覆盖 vs 分支覆盖;覆盖率高 ≠ 测得好(用它找**没测到的分支**,别刷数字)。

### P0-4 pyrefly 进 CI
- **目标**:已配置的类型检查真正强制。
- **提示**:CI 加一步 `uvx pyrefly check`(或项目约定的命令)。修掉暴露出的类型问题——尤其你刚把 `get_last_trade_seq` 改成 `int | None` 后,下游是否都处理了 `None`。
- **验收**:
  - [ ] CI 里 pyrefly 步骤绿
- **学习点**:`Optional`/union 类型如何逼你在编译期处理 `None`;渐进式类型。

---

## Phase 1 — 结构重构(让代码可测)

### P1-1 config 工厂化
- **目标**:消除「类体算一遍路径 + `__post_init__` 再算一遍」的重复;去掉全局单例的隐患。
- **涉及**:`config.py`,以及所有 `from ...config import settings` 的调用点。
- **提示**:
  - 加一个 `@classmethod def from_env(cls) -> "Config"`,只在这里读环境变量。
  - 派生路径改成 `@property`(`base_dir`、`data_future_dir` …),这样只推导一次、永远一致。
  - `settings` 可保留为 `Config.from_env()` 的模块级实例,但派生值走 property。
- **验收**:
  - [ ] `test_config.py` 原有断言仍通过(可能要改成调 `from_env()`)
  - [ ] 删除了重复的路径推导块
  - [ ] `grep` 确认没有第二处计算 `BASE_DIR`
- **学习点**:dataclass 默认值在**类定义时求值**的坑;`@property` 派生 vs 存储字段;全局可变单例为什么难测。

### P1-2 拆 future/option 的闭包 ⭐
- **目标**:把 `fetch_chunk` / `sync_db` / `on_success` 从 `run()` 里的闭包变成模块级(或类方法)可测函数;抽出 future/option 共享的 `run()` 骨架。
- **为什么**:这是「future.py 一直零测试」的根因。闭包捕获了 client/engine/repo/sink,无法独立调用。
- **涉及**:`future.py`、`option.py`,可能新增 `fetchers/_common.py` 或一个基类。
- **提示**:
  - 先只做 future:把 `fetch_chunk(tasking, client) -> dict`、`sync_db(buffers, sink, repo)` 提到模块级,`run()` 里用 `functools.partial` 或小闭包把依赖绑上去。
  - 观察 future 和 option 的 `run()`:开 DB → repo → sink → client → 建 engine → 定义回调 → `engine.run()`。把公共部分抽成 `_run_fetcher(repo_cls, sink_dir, prepare_fn, fetch_fn, sync_fn, ...)`。
  - 常量(`MAX_WORKER_TASKS` 等)集中到一处或 config。
- **验收**:
  - [ ] 新增 `test_future.py` 里能**直接调用** `fetch_chunk` / `sync_db` 并断言(不再只是 `_prepare_tasks`)
  - [ ] future 和 option 的 `run()` 共用同一骨架,重复代码明显减少
  - [ ] 全部旧测试仍绿
- **学习点**:闭包捕获 vs 依赖注入;`functools.partial`;为什么「可测性」几乎等价于「依赖显式传入」。

### P1-3 迁移 parquet / validate 核心进包 ⭐
- **目标**:把 `scripts/gen_parquet.py` 的 482 行拆开——纯逻辑进 `src/deribit_fetcher/parquet.py`,`scripts/gen_parquet.py` 只留 argparse + 调用。`validate_data` 同理。
- **为什么**:最值钱、最复杂的代码现在不可 import、不可测。
- **提示**:
  - 先识别**纯函数**:`_read_and_dedup_file`、`_stream_batches`、跨文件/跨批去重那几段。把去重从大函数里提成独立函数,签名类似:
    - `dedup_intra(df) -> tuple[df, int]`
    - `dedup_cross_file(df, seen: set[int]) -> tuple[df, int]`
    - `dedup_exact(df, seen_by_instrument) -> tuple[df, int]`
  - `generate_parquet` 变成编排这些纯函数的薄壳。
  - `scripts/gen_parquet.py` 变成 `from deribit_fetcher.parquet import generate_parquet` + CLI。
- **验收**:
  - [ ] `from deribit_fetcher.parquet import dedup_cross_batch` 能直接 import
  - [ ] `scripts/gen_parquet.py --type option` 行为不变(拿现有数据跑一遍对比)
  - [ ] 单函数行数明显下降
- **学习点**:「提炼纯函数」是可测性的核心手法;I/O 与计算分离;为什么把逻辑塞进 CLI 脚本是反模式。

---

## Phase 2 — 测试补全(覆盖难而值钱的)

### P2-1 client 测试 ⭐
- **目标**:测重试、`Retry-After` 优先、指数退避回退、`get_instruments`/`get_trades_chunk` 解析、以及你新加的 `None` 语义。
- **提示**:
  - 用 `httpx.MockTransport` 注入假响应(不发真请求):构造依次返回 429→429→200 的传输,断言最终成功且重试了 N 次。
  - 或用 `respx`。把 `AsyncLimiter` 的速率调高避免测试变慢。
  - 重点测:429 带 `Retry-After: 2` 时是否**优先用 header** 而非退避;10 次仍失败时 `get_last_trade_seq` 是否返回 `None`(而不是 0)。
- **验收**:
  - [ ] 覆盖「Retry-After 命中」「无 header 退避」「彻底失败返回 None」三条路径
  - [ ] 不产生真实网络请求(离线可跑)
- **学习点**:`httpx.MockTransport`/`respx`;tenacity 的 `wait`/`stop`/`reraise`;如何在测试里让退避不拖慢用例(patch sleep)。

### P2-2 去重测试 ⭐
- **目标**:三条去重路径各有针对性用例。
- **依赖**:P1-3(先能 import)。
- **提示**:用小 `pl.DataFrame` fixture,手工构造含重复 `trade_seq` 的数据。必须覆盖 API chunk 内降序、future chunk 并发乱序和跨批重复，不能假设 JSONL 按 seq 递增。
- **验收**:
  - [ ] 文件内重复被去掉、计数正确
  - [ ] 跨文件用 set 去重正确
  - [x] 降序与乱序跨批输入保留全部唯一键，仅删除真实重复
- **学习点**:算法优化必须建立在可验证的数据不变量上；API 返回顺序与并发落盘顺序是两层不同约束；位图如何以 1 bit/序号实现精确 membership。

### P2-3 gap 直方图测试
- **提示**:构造已知有缺口的 seq 序列,断言每个 bucket 的 deficit。重点覆盖之前修过 bug 的**整数分桶**逻辑(`(seq-min)*N // total`),验证不再有浮点漂移。
- **验收**:
  - [ ] 无缺口 → 全部 bucket deficit=0
  - [ ] 已知缺口 → 落在正确 bucket
- **学习点**:整数运算避免浮点误差;为已修 bug 补「回归测试」的意义。

### P2-4 option 流式测试
- **提示**:mock engine 的 `enqueue_task`,断言 `on_success` 在 `should_continue=True` 时用正确的 `next_seq` 入队、过期合约在结束时 `finished=True`。
- **验收**:
  - [ ] 续传链路(next_seq 计算)正确
  - [ ] 过期 vs 活跃合约的 `finished` 行为正确
- **学习点**:回调驱动的动态任务生成;如何测「递归入队」而不真的跑满。

---

## Phase 3 — 健壮性

### P3-1 engine 重试上限 + dead-letter
- **目标**:`_producer_worker` 现在对失败任务无限重入队,可能死循环。加尝试计数,超阈值则记录并丢弃(或进 dead-letter 列表)。
- **涉及**:`engine.py`,以及 `test_engine.py`。
- **提示**:在 task dict 里带 `_attempts`,或用 `dict[task_key, count]`。达到上限后 `logger.error` 并**不再**重入队;可暴露 `self.dead_letters` 供 run 结束后汇报。
- **验收**:
  - [ ] 新测试:一个永远抛异常的 `fetch_func`,验证重试有限次后停止、不死循环、任务进 dead-letter
  - [ ] 正常任务不受影响
- **学习点**:重试预算 / 退避 / dead-letter 模式;区分「瞬时失败(该重试)」与「持久失败(该放弃)」。

### P3-2 ⭐拔高:真正实现大文件并行读取
- **目标**:把 README 曾经吹过的「进程池 + 块对齐并行读取」**真的做出来**,并用 P2-2 的测试和 `benchmark.py` 证明加速。
- **提示**:
  - 大文件按字节切成 `\n` 对齐的 block,`ProcessPoolExecutor` 里每个 worker 读一个 block → 反序列化 → 局部去重 → 返回。
  - 难点:跨进程的**顺序**与**跨 block 去重**。block 可按起始偏移恢复输出顺序，但去重必须使用精确 membership，不能从文件偏移推断 seq 单调。
  - 加回 `--stream-workers` / `--block-bytes` 参数——这次是真的。跑 `benchmark.py --data-dir data/BTC/future` 对比单线程 vs 并行。
- **验收**:
  - [ ] 并行结果与单线程**逐行一致**(同样的去重输出)
  - [ ] benchmark 显示大文件吞吐提升(把数字填回 README)
  - [ ] 内存仍有界(峰值 RSS 不随文件线性增长)
- **学习点**:`ProcessPoolExecutor` vs 线程(GIL、pickle 开销);mmap 块切分;并行下如何保持确定性输出。这是整份路线里**最硬核、最值得写进简历**的一项。

---

## Phase 4 — 社区与发布

### P4-1 CONTRIBUTING + CHANGELOG + release
- **提示**:写 `CONTRIBUTING.md`(如何 `uv sync`、跑测试、lint 规范、EN 为文档源语言);建 `CHANGELOG.md`(Keep a Changelog 格式);`git tag v0.1.0` 并在 GitHub 建 Release。
- **验收**:
  - [ ] 有 CONTRIBUTING、CHANGELOG
  - [ ] GitHub Releases 至少一个 tag
- **学习点**:语义化版本(SemVer);变更日志纪律;开源协作礼仪。

### P4-2 badges
- **提示**:README 顶部加 CI 状态、License、Python 版本 badge(shields.io)。
- **验收**:[ ] 3 个 badge 正常显示且 CI badge 是绿的。

---

## 完成定义(Definition of Done —「合格开源 repo」)

全部满足即达标:

- [ ] CI 全绿(测试 + ruff + pyrefly + 覆盖率阈值),跨 3.10–3.12
- [ ] 核心模块(client / 去重 / validate / engine)都有针对性测试,覆盖率有下限
- [ ] 业务逻辑在包内、可 import、可测;`scripts/` 只是薄 CLI
- [ ] 无已知静默正确性 bug(P3-1 已修 A1)
- [ ] README/文档与代码一致(已完成),有 CONTRIBUTING、CHANGELOG、一个 release、badges
- [ ] 往后每个 PR 都过 CI,commit message 清晰

> 提示:每完成一个 Phase,回到本表把复选框打上 ✅,并在 CHANGELOG 记一笔。这份文档本身就是你学习轨迹的证据——面试时可以直接展示「我如何系统地把一个玩具项目提升为工程化 repo」。
