# 历史数据爬虫项目 (deribit-historical-data) 重构意见

经过对代码库（特别是 `future.py`, `option.py`, `client.py`, `progress.py`, `config.py`）的阅读分析，提出以下五个维度的重构意见，目的是提升代码复用性、可维护性和健壮性。

## 1. 统一核心爬虫引擎（提炼共性）
**现状**：
`future.py` 和 `option.py` 的主干流程高度重复。包括生成 `Task Queue`, `Storage Queue`、生产者（`fetch_producer` / `fetch_option_worker`）、消费者（`write_consumer`）的代码骨架几乎一模一样。

**重构建议**：
- **提取 `BaseEngine` 或通用的 `run_fetcher` 函数**。将并发队列管理、`tqdm` 进度条管理、优雅退出（Graceful Shutdown / Poison Pill）等通用逻辑提取出来。
- **依赖反转 / 回调注入**：将“如何抓取数据”（API Endpoint，如何计算 next_seq）和“如何更新 DB 进度”作为接口或回调传给通用的爬虫引擎。
- 这样后续增加 Spot (现货) 或是 Perpetual (永续) 的抓取，只需少量胶水代码即可。

## 2. 存储层抽象（Storage/Sink Layer）
**现状**：
文件写入逻辑（打开文件、orjson 序列化、使用 `ab` 模式追加 `\n`）直接硬编码在业务消费者的 `_flush_to_files` 和 `_flush_option_batch` 函数内部。并且使用 `run_in_executor` 阻塞处理。

**重构建议**：
- 新增 `storage.py` 或 `sink.py`。
- 定义一个 `JSONLinesSink(base_dir: Path)` 的独立类，专门负责将内存 Buffer 落盘。
- 将来如果业务扩展，比如一次性写入 Parquet（正如前面某个修改需求那样）、存入 MinIO/S3，只需替换 Sink 实现，与爬虫抓取逻辑完全解耦。

## 3. 数据库和状态管理的拆分
**现状**：
`ProgressDB` (`progress.py`) 是一个“大上帝对象”，包含了 `future_meta`, `future_chunk`, `option_meta` 等所有表结构定义和混合的所有 SQL CRUD 方法。

**重构建议**：
- 拆分 Repo 模式。建立一个负责管理底层连接（WAL + 连接池）的 `DatabaseClient` 基础类。
- 创建 `FutureProgressRepo(DatabaseClient)` 和 `OptionProgressRepo(DatabaseClient)` 两个单独的管理类分别封装对应业务的 SQL。
- 对于 `Option` 中 `last_no` 状态和 `Future` 中的 `chunk` 进度更新模型，甚至可以考虑是否能在模型层面做一定程度的对齐，方便后期统一进度面板的查询。

## 4. 退出控制与异步上下文管理
**现状**：
- 队列获取：大量使用了 `asyncio.wait_for(queue.get(), timeout=1.0)` 进行轮询，这种方式会导致空闲时协程被无谓地频繁唤醒。
- 资源关闭：通过显式的 `await deribit_client.close()` 以及 `await progress_db.close()`。如果在代码中间抛出未捕获异常，可能无法进入最后 `finally` 清理阶段。

**重构建议**：
- 避免轮询：使用纯粹的 `queue.get()` 阻塞。当收到 `SIGINT` 时，给所有的 Worker 发送 `Cancel` 信号（或者标准的 Poison Pill），依靠捕获 `asyncio.CancelledError` 来中断循环，CPU 效率更高。
- 上下文管理器：让 `DeribitClient` 和 `ProgressDB` 支持 `__aenter__` 和 `__aexit__` 协议，采用 `async with DeribitClient() as client:` 的写法保证自动安全关闭。

## 5. 配置与日志的梳理
**现状**：
`config.py` 内使用 `@dataclass` 配合 `__post_init__` 来加载代理配置创建目录，并将 `logging` 的初始化一并打包。这使得每次 `settings = Config()` 被实例化时都有潜在的磁盘和全局日志重置等副作用（Side Effects）。

**重构建议**：
- **引入 `pydantic-settings`**：替代原生 DataClass，不仅有更强大的验证机制，并且可以直接从 `.env` 和环境自动读取和转换 `PROXY` 配置。
- **解耦日志配置**：将 `setup_logging()` 从 `Config.__post_init__` 中移出。单独封装一个 `init_logger()` 在项目入口（`main()`）的最开始仅仅调用一次，这样职责更加清晰，也不会干扰单元测试。
