# Deribit 历史数据爬虫

[![CI](https://github.com/RiveChen/deribit-historical-data/actions/workflows/ci.yml/badge.svg)](https://github.com/RiveChen/deribit-historical-data/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](https://www.python.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

> 异步爬虫，从 [Deribit History API v2](https://docs.deribit.com/#public-get_last_trades_by_instrument) 下载全部历史成交数据，支持**期货（Future）**和**期权（Option）**。

## 功能特点

- **全量历史下载** — 基于 `trade_seq` 分块，下载每一笔成交，而不仅仅是近期数据
- **异步** — 最高 20 RPS（API 上限），基于 `asyncio`，采用有界生产者-消费者引擎并做每秒限流
- **断点续传** — SQLite 检查点数据库记录进度，部分下载可从中断处恢复
- **优雅关闭** — 处理 `SIGINT`/`SIGTERM` 信号，保留已收集的全部数据
- **JSONL 输出** — 原始数据保存为换行符分隔的 JSON，每行一笔成交
- **Parquet 导出** — 工具脚本可将所有 JSONL 文件合并为单个压缩 Parquet 文件（支持去重）
- **数据校验** — 流式 Parquet 校验（按交易对做 `trade_seq` 间隙检测并输出间隙分布直方图，附 Schema 与时间范围概览），无需将完整文件加载到内存
- **支持多种币种和品种** — 支持 BTC 和 ETH，期货和期权

## 环境要求

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)（推荐）或 pip

## 安装

```bash
# 克隆仓库
git clone https://github.com/RiveChen/deribit-historical-data.git
cd deribit-historical-data

# 使用 uv 创建虚拟环境并安装依赖
uv sync

# 或使用 pip
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## 配置

以下两项通过**环境变量**读取：

| 变量 | 默认值 | 说明 |
|----------|---------|------|
| `CURRENCY` | `BTC` | 币种（`BTC` 或 `ETH`） |
| `HTTP_PROXY` / `HTTPS_PROXY` | （无） | 代理地址，如 `http://127.0.0.1:7890` |

可内联设置或提前导出：

```bash
CURRENCY=ETH uv run python -m deribit_fetcher.future
```

其余调优参数位于 [`src/deribit_fetcher/config.py`](./src/deribit_fetcher/config.py)，需修改源码来调整：

| 参数 | 默认值 | 说明 |
|----------|---------|------|
| `CHUNK_SIZE` | `10000` | 每次 API 请求获取的成交数（Deribit 上限 10000） |
| `MAX_RPS` | `20` | 每秒请求数限制 |
| `MAX_WORKERS` | `40` | 最大并发 HTTP 连接数 |

数据默认存放在 `./data/<CURRENCY>`。可对任意命令（`future`、`option`、`gen_parquet.py`、`validate_data.py`）传 `--base-dir PATH` 来改变位置，例如 `uv run python -m deribit_fetcher.future --base-dir /mnt/disk/deribit`。

## 使用方法

### 1. 下载期货成交

```bash
# 下载 BTC 期货（默认）
uv run python -m deribit_fetcher.future

# 下载 ETH 期货，自定义设置
CURRENCY=ETH uv run python -m deribit_fetcher.future
```

### 2. 下载期权成交

```bash
uv run python -m deribit_fetcher.option
```

### 3. 导出为 Parquet

Parquet 生成器将所有 JSONL 文件合并为单个压缩 Parquet 文件，支持去重。JSONL 源文件不会被删除，只会额外生成 `.parquet` 文件，因此需要同时容纳原始 JSONL 和（小得多的）Parquet 输出的磁盘空间。Parquet + zstd 通常比源 JSONL 小数倍——运行 [`scripts/benchmark.py`](#基准测试benchmarks) 可测出你数据上的确切压缩比。

```bash
# 将所有 BTC 期货 JSONL 合并为单个 Parquet
uv run python scripts/gen_parquet.py --type future

# 将所有 BTC 期权 JSONL 合并
uv run python scripts/gen_parquet.py --type option

# 使用 lz4 压缩（更快，文件略大约 10-15%）
uv run python scripts/gen_parquet.py --type future --fast

# 跳过去重（速度更快，但可能包含重复行）
uv run python scripts/gen_parquet.py --type future --no-dedup

# 调整小文件线程池大小（默认使用所有 CPU 核心）
uv run python scripts/gen_parquet.py --type future --workers 8
```

生成器采用两阶段策略：
- **小文件**（`< --large-threshold-mb`，默认 100 MB，典型期权）：用线程池（`--workers`）并行读取，一个文件一个 worker，逐文件去重。
- **大文件**（`>= --large-threshold-mb`，典型永续合约）：在主线程中按固定大小分批流式读取（`--stream-batch-size` 行，默认 200000），用 `mmap` 做零拷贝换行切分。跨批去重使用按 instrument 隔离的精确位图（每个序号位置 1 bit），因此可以正确处理 API 降序响应以及并发追加造成的乱序 chunk，同时避免为每笔成交保留一个 Python 对象。

全部参数：

| 参数 | 默认值 | 说明 |
| ---- | ------- | ----------- |
| `--type` | （必填） | `future` 或 `option` |
| `--workers` | CPU 核心数 | 小文件阶段的线程池 worker 数 |
| `--fast` | 关 | 用 lz4 替代 zstd（更快，文件大约 10-15%） |
| `--no-dedup` | 关 | 跳过 `(instrument_name, trade_seq)` 去重 |
| `--large-threshold-mb` | `100` | 达到或超过此大小的文件走流式路径 |
| `--stream-batch-size` | `200000` | 大文件每个流式批次的行数 |

### 4. 数据校验

流式 Parquet 校验 — 按交易对检测 `trade_seq` 间隙并打印间隙分布直方图，使用流式安全聚合，全程不将完整文件加载到内存（避免 90 GB `future.parquet` 的 OOM 问题）。

```bash
# 校验期货和期权的 Parquet 文件
uv run python scripts/validate_data.py

# 仅校验特定类型
uv run python scripts/validate_data.py --type future
```

### 输出目录结构

```
data/
└── {CURRENCY}/
    ├── future/
    │   ├── BTC-27MAR26.jsonl     # 每个交易对一个文件
    │   └── ...
    ├── option/
    │   ├── BTC-27MAR26-70000-C.jsonl
    │   └── ...
    ├── future.db                  # 进度检查点（SQLite）
    ├── option.db
    ├── future.parquet             # 由 gen_parquet.py 生成
    └── option.parquet
```

## 项目结构

```
src/deribit_fetcher/
├── __init__.py          # 包版本号
├── client.py            # Deribit API 客户端（限流、重试）
├── config.py            # 配置（dataclass + 环境变量）
├── engine.py            # 通用异步生产者-消费者引擎
├── future.py            # 期货数据爬虫（入口）
├── option.py            # 期权数据爬虫（入口）
├── progress.py          # SQLite 检查点数据库
├── storage.py           # JSONL 文件写入器
└── log.py               # 日志配置（兼容 tqdm）

scripts/
├── gen_parquet.py       # JSONL → Parquet 转换（去重、流式）
├── validate_data.py     # 下载后数据完整性校验（间隙检测）
└── benchmark.py         # 可复现的吞吐 / 内存 / 压缩比基准测试
```

## 工作原理

### 期货抓取策略

1. 通过 `get_instruments` 获取所有期货交易对
2. 获取每个交易对的最新 `trade_seq`
3. 将 seq 范围 [1, last_seq] 按 `CHUNK_SIZE` 切分为固定大小的块
4. 使用生产者 - 消费者模式并发抓取所有块
5. 每个块完成后写入 JSONL，并在 SQLite 中记录进度
6. 全部完成后，标记块和交易对元数据为已完成（重启时跳过）

### 期权抓取策略

1. 获取所有期权交易对
2. 对每个未完成的期权，从 `last_no + 1` 开始（断点续传偏移量）
3. 通过 `on_success` 回调顺序抓取块，每次完成后将下一个范围入队
4. 写入 JSONL，使用 `MAX(last_no, ?)` 更新数据库进度，防止回退
5. 当没有更多成交时标记为完成（仅限已过期交易对）

### 断点续传

- **SQLite 检查点数据库** 记录哪些块已完成
- 重启时自动跳过已完成的块/交易对
- 期权进度使用 `MAX(last_no, ?)` 保护，防止崩溃恢复时进度回退

有关 API 行为的详细信息（如 `has_more` 语义、块边界重叠等），请参阅 [docs/api-reference.zh.md](./docs/api-reference.zh.md)。

## 基准测试（Benchmarks）

这里的性能数字都可复现——自己跑一遍：

```bash
# 合成数据快速冒烟测试（几秒）
uv run python scripts/benchmark.py --quick

# 用你真实下载的数据测（最可信）
uv run python scripts/benchmark.py --data-dir data/BTC/option
uv run python scripts/benchmark.py --data-dir data/BTC/future --large-threshold-mb 100
```

它会测量输入 rows/s 与 MB/s、线程池扩展性、去重开销、zstd vs lz4、流式批大小、JSONL→Parquet 压缩比，以及峰值内存 RSS（每个 case 在独立子进程中运行）。结果写入 `benchmark_results/BENCHMARK.md`。

_把下表占位符替换为你机器上的实测数字（见 `benchmark_results/BENCHMARK.md`）：_

| Case | Rows/s | MB/s in | 压缩比× | 峰值 RSS (MB) |
|------|-------:|--------:|--------:|--------------:|
| 小文件, workers=1, zstd | — | — | — | — |
| 小文件, workers=N, zstd | — | — | — | — |
| 关闭去重 | — | — | — | — |
| lz4 (`--fast`) | — | — | — | — |
| 大文件, 流式 | — | — | — | — |

## 文档

设计与开发文档在 [docs/](./docs/):[概览](./docs/overview.md)、[架构](./docs/architecture.md)、[设计取舍](./docs/design-decisions.md)、[数据模型](./docs/data-model.md)、[Deribit API 说明](./docs/deribit-api.md)、[运维](./docs/operations.md)、[开发](./docs/development.md)。

## 数据说明

- **块边界重叠**：Deribit 偶尔会在块边界返回 1 条重叠的成交。这可以容忍 — 重复数据可在 Parquet 转换阶段按 `(instrument_name, trade_seq)` 去重。
- **无成交交易对**：部分早期过期的交易对没有任何成交，自动跳过。
- **成交结构**：期货和期权的成交共享相同的字段。Parquet 生成器使用完整的 18 字段 union schema 以捕获真实 API 响应中出现的所有字段，包括罕见的 `liquidation`、`block_trade_id`、`block_rfq_id`、`combo_id` 等，缺失字段自动填充为 null。

## 致谢

部分工具代码、测试与文档在 [Claude](https://www.anthropic.com/claude)（Anthropic）的协助下完成。
