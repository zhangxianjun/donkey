# Project Structure

这份文档用于记录当前仓库的目录结构和各目录职责。

维护约定：

- 以后这个仓库只要出现目录结构新增、删除、重命名，或关键文件落位发生变化，都要同步更新本文件。
- `__pycache__`、`.DS_Store`、临时缓存等运行时产物，不作为主结构的一部分维护。
- `data/raw` 和 `data/normalized` 会持续增长；下面既描述目录模式，也记录当前已经存在的样例文件。

## 1. 当前主结构

```text
donkey/
├── .dockerignore
├── Dockerfile
├── README.md
├── PROJECT_STRUCTURE.md
├── requirements.txt
├── config/
│   ├── factors/
│   │   └── daily_core_v1.yaml
│   └── strategies/
│       ├── atr_trailing_v1.yaml
│       └── vol_breakout_v1.yaml
├── docs/
│   ├── ARCHITECTURE.md
│   ├── EXAMPLES.md
│   ├── HOW_TO_USE.md
│   └── screenshots/
│       └── README.md
├── data/
│   ├── raw/
│   │   └── binance/
│   │       └── spot/
│   │           ├── <run_id>.manifest.json
│   │           └── <symbol>/
│   │               └── <interval>/
│   │                   ├── <run_id>.jsonl
│   │                   ├── <run_id>.meta.json
│   │                   └── _checkpoint.json
│   └── normalized/
│       └── <data_version>/
│           ├── market_ohlcv_<interval>.jsonl
│           ├── market_ohlcv_<interval>.parquet
│           └── normalize_manifest.json
├── db/
│   ├── .gitkeep
│   ├── experiments.duckdb
│   └── quant.duckdb
├── logs/
├── scripts/
│   ├── build_docker_image.sh
│   ├── push_docker_image.sh
│   └── run_docker_image.sh
├── sql/
│   └── init_v1.sql
├── src/
│   ├── admin/
│   │   ├── __init__.py
│   │   ├── pairs_dashboard.py
│   │   └── static/
│   │       └── pairs_dashboard.html
│   ├── ingestion/
│   │   ├── __init__.py
│   │   └── binance_ohlcv.py
│   ├── normalize/
│   │   ├── __init__.py
│   │   └── market_ohlcv.py
│   ├── strategies/
│   │   ├── builtin/
│   │   │   ├── __init__.py
│   │   │   └── atr.py
│   │   ├── __init__.py
│   │   ├── core.py
│   │   ├── loader.py
│   │   ├── run.py
│   │   └── simple_yaml.py
│   ├── warehouse/
│   │   ├── __init__.py
│   │   └── load_duckdb.py
│   └── __init__.py
└── tests/
    ├── test_binance_ohlcv.py
    ├── test_load_duckdb.py
    ├── test_market_ohlcv_normalize.py
    ├── test_pairs_dashboard.py
    └── test_strategy_loader.py
```

## 2. 各目录职责

`README.md`

- 项目首页说明。
- 当前用于表达产品定位、平台亮点、快速开始、文档导航和路线图。

`requirements.txt`

- 最小 Python 依赖入口。
- 当前包含 `duckdb` 和 `pyarrow`。

`.dockerignore`

- Docker 构建上下文排除规则。
- 当前会忽略本地数据、DuckDB 文件、日志、测试和 git 元信息。

`Dockerfile`

- 项目容器镜像定义。
- 当前默认安装 `duckdb`，并把 `src/`、`config/`、`sql/` 打进镜像。

`PROJECT_STRUCTURE.md`

- 当前文件；用于维护仓库目录结构说明。

`docs/`

- 文档目录。
- `docs/HOW_TO_USE.md` 提供使用教程。
- `docs/ARCHITECTURE.md` 说明平台架构和分层。
- `docs/EXAMPLES.md` 提供常见命令和接入示例。
- `docs/screenshots/` 预留项目截图资源。

`config/`

- 放配置类文件。
- `config/strategies/` 存放策略 YAML。
- `config/strategies/atr_trailing_v1.yaml` 是当前默认的 ATR 热加载示例策略。
- `config/factors/` 存放因子字段清单和元数据 YAML。

`data/raw/`

- 原始采集层。
- 保留交易所返回的原始 K 线记录，不做标准化修改。
- 当前抓取器按 `exchange / market_type / symbol / interval / run_id` 落盘。
- 如果某个下载任务中途中断，symbol/interval 目录里会临时留下 `_checkpoint.json`，用于续跑；成功完成后会自动删除。

`data/normalized/`

- 标准化后的行情层。
- 当前由 `src/normalize/market_ohlcv.py` 生成。
- 统一输出 `market_ohlcv` 标准字段，并按 interval 分文件保存。
- 当前支持 `jsonl`，安装 `pyarrow` 后也支持 `parquet`。

`db/`

- DuckDB 数据库文件目录。
- 当前已经落地 `quant.duckdb` 和 `experiments.duckdb`。

`logs/`

- 运行日志目录。
- 当前用于保存抓取器和后续容器启动日志。

`scripts/`

- 仓库级运维脚本。
- 当前包含 Docker 镜像构建、推送、运行脚本。

`sql/`

- 数据库初始化和建表 SQL。
- 当前已有 `init_v1.sql`。

`src/`

- 业务代码主目录。
- `src/admin/` 提供本地管理面板和后台下载任务入口。
- `src/admin/static/` 存放管理面板静态页面。
- `src/ingestion/` 负责采集 Binance 原始 K 线。
- `src/normalize/` 负责把 raw 数据统一成 `market_ohlcv`。
- `src/strategies/` 负责策略模块加载、热重载和信号生成。
- `src/warehouse/` 负责把 normalized 数据装入 DuckDB。

`tests/`

- 单元测试目录。
- 当前覆盖 ingestion、normalize、warehouse、admin 和 strategy loader。

## 3. 当前已存在的关键文件

### 根目录

- `.dockerignore`
- `Dockerfile`
- `README.md`
- `PROJECT_STRUCTURE.md`
- `requirements.txt`

### 文档

- `docs/ARCHITECTURE.md`
- `docs/EXAMPLES.md`
- `docs/HOW_TO_USE.md`
- `docs/screenshots/README.md`

### 配置

- `config/factors/daily_core_v1.yaml`
- `config/strategies/atr_trailing_v1.yaml`
- `config/strategies/vol_breakout_v1.yaml`

### SQL / DB

- `sql/init_v1.sql`
- `db/.gitkeep`
- `db/quant.duckdb`
- `db/experiments.duckdb`

### 脚本

- `scripts/build_docker_image.sh`
- `scripts/push_docker_image.sh`
- `scripts/run_docker_image.sh`

### 源码

- `src/__init__.py`
- `src/admin/__init__.py`
- `src/admin/pairs_dashboard.py`
- `src/admin/static/pairs_dashboard.html`
- `src/ingestion/__init__.py`
- `src/ingestion/binance_ohlcv.py`
- `src/normalize/__init__.py`
- `src/normalize/market_ohlcv.py`
- `src/strategies/__init__.py`
- `src/strategies/core.py`
- `src/strategies/loader.py`
- `src/strategies/run.py`
- `src/strategies/simple_yaml.py`
- `src/strategies/builtin/__init__.py`
- `src/strategies/builtin/atr.py`
- `src/warehouse/__init__.py`
- `src/warehouse/load_duckdb.py`

### 测试

- `tests/test_binance_ohlcv.py`
- `tests/test_market_ohlcv_normalize.py`
- `tests/test_load_duckdb.py`
- `tests/test_pairs_dashboard.py`
- `tests/test_strategy_loader.py`

## 4. 当前数据目录中的样例文件

### Raw 样例

`data/raw/binance/spot/`

- `20260318T013533Z.manifest.json`
- `20260318T013624Z.manifest.json`
- `20260318T013803Z.manifest.json`

`data/raw/binance/spot/BTCUSDT/1h/`

- `20260318T013533Z.jsonl`
- `20260318T013533Z.meta.json`

`data/raw/binance/spot/ETHUSDT/5m/`

- `20260318T013624Z.jsonl`
- `20260318T013624Z.meta.json`
- `20260318T013712Z.jsonl`
- `20260318T013803Z.jsonl`
- `20260318T013803Z.meta.json`

`data/raw/binance/spot/0GTRY/1d/`

- `20260318T020429Z.jsonl`
- `20260318T020429Z.meta.json`

`data/raw/binance/spot/0GUSDC/1d/`

- `20260318T020429Z.jsonl`
- `20260318T020429Z.meta.json`

### Normalized 样例

`data/normalized/v1/`

- `market_ohlcv_1h.jsonl`
- `market_ohlcv_5m.jsonl`
- `normalize_manifest.json`

### DuckDB 样例

`db/`

- `quant.duckdb`
- `experiments.duckdb`

## 5. 当前代码流转关系

第一步：采集

- `src/ingestion/binance_ohlcv.py`
- 输出到 `data/raw/binance/spot/...`

第二步：标准化

- `src/normalize/market_ohlcv.py`
- 读取 `data/raw/binance/spot/...`
- 输出到 `data/normalized/<data_version>/...`

第三步：入库

- `src/warehouse/load_duckdb.py`
- 读取 `data/normalized/<data_version>/...`
- 写入 `db/quant.duckdb` 的 `market_ohlcv`

第四步：策略信号生成

- `src/strategies/run.py`
- 读取 `config/strategies/*.yaml` 和 normalized 数据
- 输出到 `data/signals/...`

第五步：后台查看与产物聚合

- `src/admin/pairs_dashboard.py`
- 聚合展示数据源、本地数据、策略信息和回测产物状态

第六步：后续继续增强

- 因子计算
- 正式 backtest runner
- experiment tracking
