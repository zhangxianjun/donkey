# donkey

`donkey` 是一个面向量化研究与运营的本地控制台，用来把多源现货行情、下载任务、策略配置和回测产物放到同一个工作区里管理。

它当前更偏向研究基础设施，而不是生产级实盘系统：重点解决“数据怎么采、怎么落、怎么标准化、怎么查看、怎么把策略与产物串起来”的问题。对于个人研究者、小团队内网工具、或者准备继续往因子库/回测引擎/实验管理扩展的项目，这个仓库已经是一套能继续长出来的骨架。

## 项目定位

- 面向本地量化研究工作流，而不是交易所托管服务
- 面向现货市场数据管理、策略配置管理和回测产物展示
- 优先保证目录结构清晰、产物可追溯、流程可复现
- 后台采用 Python 标准库 HTTP Server，依赖轻，适合本地启动和二次改造

## 当前能力

- 多数据源交易对浏览：支持 Binance、OKX、Bybit、Hyperliquid 现货交易对浏览
- 本地研究清单维护：将交易对加入本地交易列表，作为后续研究对象
- Binance K 线采集：支持按 symbol / interval 下载 raw 数据
- 断点续传与失败重试：下载任务保留 checkpoint、manifest 和失败记录
- 标准化数据层：将 raw `jsonl` 统一为 `market_ohlcv`
- 数据版本管理：标准化数据按 `data_version` 组织
- DuckDB 装载：将 normalized 数据导入 `db/quant.duckdb`
- 策略配置与热加载：支持 YAML 配置 + Python 模块策略
- 信号生成：从 normalized 数据读取并生成策略信号文件
- 策略/回测产物展示：自动读取 `summary / trades / equity` 是否存在及核心指标
- 本地管理后台：统一查看数据源、本地数据、策略、回测记录、系统设置
- 货币图标管理：支持本地图标目录与 API 上传

## 架构概览

```text
交易所 API
  -> data/raw/<source>/spot/<symbol>/<interval>/
  -> data/normalized/<data_version>/market_ohlcv_<interval>.jsonl|parquet
  -> db/quant.duckdb
  -> config/strategies/*.yaml
  -> data/signals/*
  -> data/backtests/*
  -> src/admin/pairs_dashboard.py
```

核心模块：

- `src/ingestion/binance_ohlcv.py`
  负责 Binance 现货 K 线采集、重试、checkpoint 和 manifest。
- `src/normalize/market_ohlcv.py`
  负责把 raw 数据统一成标准 `market_ohlcv` 结构。
- `src/warehouse/load_duckdb.py`
  负责把 normalized 数据按版本装载进 DuckDB。
- `src/strategies/run.py`
  负责加载策略配置与 Python 模块，生成信号产物。
- `src/admin/pairs_dashboard.py`
  负责本地管理后台和相关 API。

更完整的目录说明见 [PROJECT_STRUCTURE.md](./PROJECT_STRUCTURE.md)。

## 适合谁用

- 想做自己的量化研究工作台，而不是只写一次性脚本的人
- 需要把“下载数据、整理数据、配策略、看回测产物”串成一条线的人
- 需要一个容易改、容易加功能、容易本地部署的 Python 项目骨架的人

## 不是什么

- 不是成熟的生产级交易执行系统
- 不是完整的一站式回测平台
- 不是已经接好所有交易所下载链路的多源数据平台

目前多源支持主要体现在“交易对浏览和本地管理”层面；实际 raw K 线下载链路目前已落地的是 Binance。

## 运行环境

- Python `3.12+` 推荐
- macOS / Linux 均可，本地开发最方便
- 可选 Docker 运行
- `pyarrow` 用于 Parquet 读写；没有它也可以走 `jsonl`

## 快速开始

### 1. 安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

### 2. 启动管理后台

```bash
python3 -m src.admin.pairs_dashboard --host 127.0.0.1 --port 8866
```

浏览器打开：

```text
http://127.0.0.1:8866
```

### 3. 下载 Binance 原始 K 线

```bash
python3 -m src.ingestion.binance_ohlcv \
  --symbols BTCUSDT ETHUSDT \
  --intervals 1d 4h \
  --start-date 2024-01-01 \
  --end-date 2025-01-01
```

如果想批量发现 Binance 现货交易对再抓取：

```bash
python3 -m src.ingestion.binance_ohlcv \
  --all-spot-symbols \
  --quote-assets USDT \
  --intervals 1d \
  --max-symbols 50 \
  --start-from-listing
```

### 4. 标准化 raw 数据

```bash
python3 -m src.normalize.market_ohlcv \
  --input-root data/raw/binance/spot \
  --output-root data/normalized \
  --data-version v1 \
  --output-format parquet
```

如果本地没有 `pyarrow`，把 `--output-format` 改成 `jsonl` 即可。

### 5. 装载到 DuckDB

```bash
python3 -m src.warehouse.load_duckdb \
  --normalized-root data/normalized \
  --data-version v1 \
  --db-path db/quant.duckdb
```

### 6. 生成策略信号

```bash
python3 -m src.strategies.run \
  --strategy config/strategies/atr_trailing_v1.yaml \
  --input data/normalized/v1/market_ohlcv_1d.parquet \
  --symbols BTCUSDT ETHUSDT
```

支持 `--watch` 模式，在策略 YAML 或策略模块变化时自动重跑：

```bash
python3 -m src.strategies.run \
  --strategy config/strategies/atr_trailing_v1.yaml \
  --input data/normalized/v1/market_ohlcv_1d.parquet \
  --watch
```

## 一个典型工作流

1. 在后台查看远端交易对并加入本地研究清单
2. 下载 Binance raw K 线到 `data/raw/binance/spot`
3. 标准化为 `market_ohlcv`
4. 选择性装载到 DuckDB
5. 用 YAML + Python 模块组织策略
6. 生成信号产物、写入 `data/signals`
7. 将回测摘要和产物落到 `data/backtests`
8. 在管理后台查看策略和回测记录

## 页面导览

你提供的截图对应当前版本已经实现的这些页面：

- 首页
  研究控制台总览，展示接入数据源数量、本地交易对、本地数据数量、下载任务数、策略数、回测记录数，以及当前工作区关键信息。
- 数据源
  浏览 Binance / OKX / Bybit / HL 的现货交易对，支持搜索、按报价资产过滤、查看状态，并把交易对加入本地研究清单。
- 本地数据
  查看 `data/raw/<source>/spot` 下已存在的本地数据，按 source / symbol / interval 聚合展示，同时显示任务状态与更新时间。
- 策略展示
  自动扫描 `config/strategies/*.yaml`，展示策略描述、Universe、回测区间和产物路径。
- 回测记录
  读取 `summary / trades / equity` 的存在情况，并汇总状态与基础指标。
- 系统设置
  展示工作区路径、数据库路径、默认报价资产、日志位置、图标资源等运行信息。

如果你准备把截图正式放进仓库，建议存到 `docs/screenshots/`，例如：

- `docs/screenshots/overview.png`
- `docs/screenshots/source-pairs.png`
- `docs/screenshots/local-data.png`
- `docs/screenshots/strategies.png`
- `docs/screenshots/backtests.png`

这样后面就可以直接在 README 里内嵌图片。

## 当前内置示例

- `config/strategies/atr_trailing_v1.yaml`
  ATR 突破 + trailing stop，支持模块热加载。
- `config/strategies/vol_breakout_v1.yaml`
  20-bar 突破 + 成交量确认 + MA50 趋势过滤。
- `config/factors/daily_core_v1.yaml`
  日级价格/成交量因子字段定义样例。

## 数据与产物约定

### Raw 层

```text
data/raw/binance/spot/<symbol>/<interval>/<run_id>.jsonl
data/raw/binance/spot/<symbol>/<interval>/<run_id>.meta.json
data/raw/binance/spot/<symbol>/<interval>/_checkpoint.json
data/raw/binance/spot/<run_id>.manifest.json
```

### Normalized 层

```text
data/normalized/<data_version>/market_ohlcv_<interval>.jsonl
data/normalized/<data_version>/market_ohlcv_<interval>.parquet
```

### 策略与回测产物

```text
data/signals/<strategy_id>/*
data/backtests/<strategy_id>/summary.json
data/backtests/<strategy_id>/trades.parquet
data/backtests/<strategy_id>/portfolio_equity.parquet
```

## Docker

构建镜像：

```bash
./scripts/build_docker_image.sh
```

如果你希望镜像同时带上 Parquet 依赖：

```bash
PIP_PACKAGES="duckdb pyarrow" ./scripts/build_docker_image.sh
```

运行交互式容器：

```bash
./scripts/run_docker_image.sh
```

容器内启动后台：

```bash
./scripts/run_docker_image.sh python3 -m src.admin.pairs_dashboard --host 0.0.0.0 --port 8866
```

## 测试

```bash
python3 -m unittest discover -s tests
```

`duckdb` 未安装时，相关集成测试会自动跳过。

## 当前限制

- 多源交易对浏览已支持 Binance / OKX / Bybit / HL，但 raw 下载链路当前主要接入 Binance
- README 展示用截图还没有作为仓库静态资源提交
- 因子计算、完整回测引擎、实验追踪表虽然已有结构预留，但还不是完整产品形态
- 后台目前定位为本地单机控制台，不是多用户协作后台

## 适合继续补的方向

- 增加 OKX / Bybit / Hyperliquid 的原始数据采集器
- 增加标准化后的因子计算层与因子产物落盘
- 增加统一回测执行入口，而不只展示回测产物
- 增加实验管理与参数对比
- 增加 CI、格式检查、发布说明和版本号策略
- 补齐仓库内嵌截图和示例数据集

## License

Apache License 2.0，见 [LICENSE](./LICENSE)。
