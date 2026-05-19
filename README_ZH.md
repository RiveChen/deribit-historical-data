# Deribit 历史数据爬虫

> 异步爬虫，从 [Deribit History API v2](https://docs.deribit.com/#public-get_last_trades_by_instrument) 下载全部历史成交数据，支持**期货（Future）**和**期权（Option）**。

## 功能特点

- **全量历史下载** — 基于 `trade_seq` 分块，下载每一笔成交，而不仅仅是近期数据
- **异步** — 最高 20 RPS（受 API 限制），通过 `asyncio` 可配置并发数
- **断点续传** — SQLite 检查点数据库记录进度，部分下载可从中断处恢复
- **优雅关闭** — 处理 `SIGINT`/`SIGTERM` 信号，保留已收集的全部数据
- **JSONL 输出** — 原始数据保存为换行符分隔的 JSON，每行一笔成交
- **Parquet 导出** — 工具脚本可将所有 JSONL 文件合并为单个压缩 Parquet 文件（支持去重）
- **数据校验** — 流式 Parquet 校验（间隙检测、重复估算、Schema 分析），无需将完整文件加载到内存
- **支持多种币种和品种** — 支持 BTC 和 ETH，期货和期权

## 环境要求

- Python 3.12+
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

所有配置通过环境变量管理：

| 变量 | 默认值 | 说明 |
|----------|---------|------|
| `CURRENCY` | `BTC` | 币种（`BTC` 或 `ETH`） |
| `CHUNK_SIZE` | `10000` | 每次 API 请求获取的成交数（Deribit 上限 10000） |
| `MAX_RPS` | `20` | 每秒请求数限制 |
| `MAX_WORKERS` | `40` | 最大并发 HTTP 连接数 |
| `HTTP_PROXY` / `HTTPS_PROXY` | （无） | 代理地址，如 `http://127.0.0.1:7890` |

可内联设置或提前导出：

```bash
CURRENCY=ETH MAX_RPS=10 uv run python -m deribit_fetcher.future
```

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

Parquet 生成器将所有 JSONL 文件合并为单个压缩 Parquet 文件，支持去重。JSONL 源文件不会被删除，只会额外生成 `.parquet` 文件，因此需要同时容纳原始 JSONL 和生成的 Parquet 的磁盘空间（例如，截至 2026 年 5 月数据量：期权约 10 GB，期货约 90 GB）。

```bash
# 将所有 BTC 期货 JSONL 合并为单个 Parquet
uv run python scripts/gen_parquet.py --type future

# 将所有 BTC 期权 JSONL 合并
uv run python scripts/gen_parquet.py --type option

# 使用 lz4 压缩（更快，文件略大约 10-15%）
uv run python scripts/gen_parquet.py --type future --fast

# 并行块处理（默认使用所有 CPU 核心）
uv run python scripts/gen_parquet.py --type future --stream-workers 8

# 跳过去重（速度更快，但可能包含重复行）
uv run python scripts/gen_parquet.py --type future --no-dedup
```

生成器采用两阶段策略：
- **小文件**（<100 MB，典型期权）：使用线程池并行处理
- **大文件**（>=100 MB，典型永续合约）：按 `\n` 对齐的字节块切割，使用进程池并行处理（`--stream-workers`），通过饱和磁盘队列深度达到接近 SSD 极限的读取速度。单线程回退模式（`--stream-workers 1`）使用 mmap 零拷贝分批读取。

性能调优建议：
- 对于单个大文件：`--stream-workers <N>` 默认使用所有 CPU 核心
- 可调整块大小：`--block-bytes 268435456`（默认 256 MB，越小粒度越细）
- 用空间换速度：`--fast`（lz4 替代 zstd，文件大约 10-15%）

### 4. 数据校验

流式 Parquet 校验 — 使用流式安全聚合检测间隙和重复，无需将完整文件加载到内存（避免 90 GB future.parquet 的 OOM 问题）。

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
├── gen_parquet.py       # JSONL → Parquet 转换
├── validate_data.py     # 下载后数据完整性校验
└── test_real_api.py     # Deribit API 行为测试工具
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

有关 API 行为的详细信息（如 `has_more` 语义、块边界重叠等），请参阅 [api-reference.md](api-reference.md)。

## 数据说明

- **块边界重叠**：Deribit 偶尔会在块边界返回 1 条重叠的成交。这可以容忍 — 重复数据可在 Parquet 转换阶段按 `(instrument_name, trade_seq)` 去重。
- **无成交交易对**：部分早期过期的交易对没有任何成交，自动跳过。
- **成交结构**：期货和期权的成交共享相同的字段（经真实 API 测试确认的 17 字段 union schema）。Parquet 生成器使用完整的 schema 以捕获所有字段，包括罕见的 `liquidation`、`block_trade_id`、`block_rfq_id`、`combo_id` 等，缺失字段自动填充为 null。
