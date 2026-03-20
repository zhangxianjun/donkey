# Examples

这份文档给出几个最常见的使用示例，帮助你快速把 `donkey` 当成一套加密货币回测平台来使用。

## 示例 1：跑通 ATR 示例策略

这个示例适合第一次体验整条链路。

### 步骤 1：下载日线数据

```bash
python3 -m src.ingestion.binance_ohlcv \
  --symbols BTCUSDT ETHUSDT \
  --intervals 1d \
  --start-date 2024-01-01 \
  --end-date 2025-01-01
```

### 步骤 2：标准化

```bash
python3 -m src.normalize.market_ohlcv \
  --input-root data/raw/binance/spot \
  --output-root data/normalized \
  --data-version v1 \
  --output-format parquet
```

### 步骤 3：生成信号

```bash
python3 -m src.strategies.run \
  --strategy config/strategies/atr_trailing_v1.yaml \
  --input data/normalized/v1/market_ohlcv_1d.parquet \
  --symbols BTCUSDT ETHUSDT
```

这个示例的特点：

- 使用内置 ATR 示例策略
- 策略逻辑放在 Python 模块里
- 策略配置放在 YAML 中
- 适合观察热加载和 signal output 产物

## 示例 2：使用成交量突破策略

这个示例适合做更偏“研究策略配置”的演示。

### 生成信号

```bash
python3 -m src.strategies.run \
  --strategy config/strategies/vol_breakout_v1.yaml \
  --input data/normalized/v1/market_ohlcv_1d.parquet \
  --symbols BTCUSDT ETHUSDT
```

这条策略的配置重点在：

- breakout window
- volume ratio filter
- MA50 trend filter
- 已声明的 backtest artifact paths

适合用来展示“配置驱动”的平台风格。

## 示例 3：批量发现 Binance 现货交易对

如果你想先搭一个更大的研究 Universe：

```bash
python3 -m src.ingestion.binance_ohlcv \
  --all-spot-symbols \
  --quote-assets USDT \
  --intervals 1d \
  --max-symbols 100 \
  --start-from-listing
```

适合场景：

- 想做更大的 Universe 回测
- 想先把日线基础数据铺起来
- 想对不同 symbol 做初筛

## 示例 4：接入你自己的回测结果

当前平台已经可以展示回测产物，因此最实用的玩法之一是：

1. 用 `donkey` 管理数据和策略
2. 用你自己的回测器跑策略
3. 把产物写回平台目录

例如在策略 YAML 中配置：

```text
data/backtests/vol_breakout_v1/summary.json
data/backtests/vol_breakout_v1/trades.parquet
data/backtests/vol_breakout_v1/portfolio_equity.parquet
```

然后把你的回测器输出对齐到这里，后台“回测记录”页面就能识别。

一个最小摘要文件可以是：

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

## 示例 5：本地开发时持续热加载策略

如果你在频繁改策略模块，推荐直接用 `--watch`：

```bash
python3 -m src.strategies.run \
  --strategy config/strategies/atr_trailing_v1.yaml \
  --input data/normalized/v1/market_ohlcv_1d.parquet \
  --watch
```

这样你每次修改：

- YAML 策略配置
- Python 策略模块
- 输入数据文件

都会触发重新加载，适合本地研究期快速迭代。

## 示例 6：平台的最小目录结果

跑完一条链路后，你通常会看到：

```text
data/
  raw/
    binance/spot/BTCUSDT/1d/
  normalized/
    v1/market_ohlcv_1d.parquet
  signals/
    atr_trailing_v1/
  backtests/
    vol_breakout_v1/
db/
  quant.duckdb
```

这也是 `donkey` 区别于实验脚本的关键点之一：

- 目录结构稳定
- 数据层次明确
- 回测产物有固定落点
- 后台可直接消费这些目录
