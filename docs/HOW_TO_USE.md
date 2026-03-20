# How To Use

这份文档用来说明，如何把 `donkey` 当成一套加密货币回测平台来使用。

当前版本最适合的使用方式是：

1. 维护研究交易对 Universe
2. 下载和整理行情数据
3. 生成标准化数据和 DuckDB 仓库
4. 配置策略并生成信号
5. 写入或接入回测产物
6. 在后台统一查看

## 1. 环境准备

推荐环境：

- Python 3.12+
- macOS / Linux
- 本地安装 `duckdb` 和 `pyarrow`

安装依赖：

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

## 2. 启动平台后台

```bash
./.venv/bin/python -m src.admin.pairs_dashboard --host 127.0.0.1 --port 8866
```

打开浏览器：

```text
http://127.0.0.1:8866
```

后台页面的用途如下：

- 首页：总览研究工作区状态
- 数据源：查看远端交易对并加入本地研究清单
- 本地数据：查看已经下载的数据
- 策略展示：查看策略配置
- 回测记录：查看产物状态和摘要指标
- 系统设置：查看路径和运行参数

## 3. 维护研究 Universe

推荐先在“数据源”页面浏览交易对，再把感兴趣的交易对加入本地研究清单。

当前支持浏览的远端数据源：

- Binance
- OKX
- Bybit
- Hyperliquid

注意：

- 当前多源支持主要覆盖交易对发现与浏览
- 实际 raw K 线下载器当前已实现的是 Binance

## 4. 下载原始行情

最简单的方式，是直接用 Binance 下载器。

### 按指定交易对下载

```bash
python3 -m src.ingestion.binance_ohlcv \
  --symbols BTCUSDT ETHUSDT \
  --intervals 1d 4h \
  --start-date 2024-01-01 \
  --end-date 2025-01-01
```

### 自动发现 Binance 现货交易对再下载

```bash
python3 -m src.ingestion.binance_ohlcv \
  --all-spot-symbols \
  --quote-assets USDT \
  --intervals 1d \
  --max-symbols 50 \
  --start-from-listing
```

下载完成后，数据会落到：

```text
data/raw/binance/spot/<symbol>/<interval>/<run_id>.jsonl
```

同时还会生成：

- `<run_id>.meta.json`
- `<run_id>.manifest.json`
- `_checkpoint.json`（未完成任务时）

这些文件的价值在于：

- 可追踪这批数据从哪里来
- 下载中断时可续跑
- 后续回测时可以定位原始数据来源

## 5. 标准化行情数据

raw 层只做原始保留，不直接拿来做研究。研究和回测应该基于统一结构的 `market_ohlcv`。

执行标准化：

```bash
python3 -m src.normalize.market_ohlcv \
  --input-root data/raw/binance/spot \
  --output-root data/normalized \
  --data-version v1 \
  --output-format parquet
```

如果本地没有 `pyarrow`，可以使用：

```bash
python3 -m src.normalize.market_ohlcv \
  --input-root data/raw/binance/spot \
  --output-root data/normalized \
  --data-version v1 \
  --output-format jsonl
```

输出文件路径：

```text
data/normalized/v1/market_ohlcv_<interval>.jsonl
data/normalized/v1/market_ohlcv_<interval>.parquet
```

为什么这里要有 `data_version`：

- 同一个策略需要可追溯到所使用的数据版本
- 数据修复、补历史、换字段后可以保留旧版本

## 6. 装载到 DuckDB

将标准化数据装载进 DuckDB，方便后续查询、联表和研究。

```bash
python3 -m src.warehouse.load_duckdb \
  --normalized-root data/normalized \
  --data-version v1 \
  --db-path db/quant.duckdb
```

当前默认数据库：

- `db/quant.duckdb`
- `db/experiments.duckdb`

## 7. 配置策略

当前策略采用：

- YAML 负责元数据、Universe、风控参数、产物路径
- Python 模块负责策略逻辑

内置示例：

- `config/strategies/atr_trailing_v1.yaml`
- `config/strategies/vol_breakout_v1.yaml`

其中 `atr_trailing_v1` 支持热加载策略模块，适合开发期快速调试。

## 8. 生成策略信号

使用标准化数据和策略配置生成信号：

```bash
python3 -m src.strategies.run \
  --strategy config/strategies/atr_trailing_v1.yaml \
  --input data/normalized/v1/market_ohlcv_1d.parquet \
  --symbols BTCUSDT ETHUSDT
```

持续监听配置和模块变化：

```bash
python3 -m src.strategies.run \
  --strategy config/strategies/atr_trailing_v1.yaml \
  --input data/normalized/v1/market_ohlcv_1d.parquet \
  --watch
```

输出默认会落到策略 YAML `artifacts.signal_path` 指定的位置，或默认 `data/signals/` 路径。

## 9. 接入回测产物

当前平台已经具备回测产物展示能力。最简单的接入方式是：

1. 在策略 YAML 中配置产物路径
2. 让你的回测器把产物写入这些路径
3. 在后台“回测记录”页面查看状态和摘要

例如 `vol_breakout_v1.yaml` 中已经配置：

```text
data/backtests/vol_breakout_v1/summary.json
data/backtests/vol_breakout_v1/trades.parquet
data/backtests/vol_breakout_v1/portfolio_equity.parquet
```

平台当前会重点读取 `summary.json` 中的这些字段：

- `total_return`
- `cagr`
- `max_drawdown`
- `sharpe`
- `win_rate`
- `trade_count`

一个最小的 `summary.json` 示例：

```json
{
  "total_return": 0.34,
  "cagr": 0.12,
  "max_drawdown": -0.18,
  "sharpe": 1.28,
  "win_rate": 0.47,
  "trade_count": 42
}
```

## 10. 在后台查看结果

当你把产物写到 YAML 中声明的路径后：

- “策略展示”页面会看到策略元信息和产物路径
- “回测记录”页面会识别产物是否存在
- 如果 `summary.json` 存在，会尝试展示关键指标

## 11. 推荐工作流

推荐把 `donkey` 当成下面这条流水线来使用：

1. 先在后台筛选和维护研究交易对
2. 下载 raw 数据
3. 标准化并做版本管理
4. 装载 DuckDB
5. 配置策略
6. 生成信号
7. 运行你自己的回测器
8. 把产物写回平台约定目录
9. 在后台统一查看

## 12. 常见问题

### `pyarrow` 不可用怎么办

可以继续使用 `jsonl`，平台不会被阻塞，只是 Parquet 读写不可用。

### 平台是否已经内置完整回测执行器

当前更准确的说法是：平台已经具备回测工作区、策略组织和产物展示能力，正式 backtest runner 适合继续在现有结构上补。

### 多数据源是否都能下载 raw 行情

当前多源支持主要覆盖交易对发现与展示，已落地的 raw 下载器当前是 Binance。
