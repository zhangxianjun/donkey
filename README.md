好，我们就基于你现在这个版本，升级成一套 更像“正式量化研究系统” 的结构。
目标不是一下子做很重，而是：

先保留你现在能落地的简单架构

再补上真正决定长期可用性的部分

让它从“能跑”升级到“能持续研究、复现、迭代”

我建议升级成这 10 层。

一、升级后的总架构
数据源
  ↓
采集层 ingestion
  ↓
标准化层 normalize
  ↓
数据仓库 DuckDB / Parquet
  ↓
因子层 factor library
  ↓
信号层 signal engine
  ↓
回测层 backtest engine
  ↓
组合层 portfolio engine
  ↓
评估层 evaluation
  ↓
实验管理层 experiment tracking
  ↓
实盘执行层 live trading

和你原来的区别在于，多了 4 个关键能力：

标准化层

因子库

评估层

实验管理层

真正长期好不好用，主要看这四个。

二、升级重点 1：数据层拆成 3 层

你原来是：

数据 → DuckDB

升级后建议变成：

raw → normalized → warehouse
1. raw 原始层

只保存原始下载结果，不修改。

例如：

data/raw/binance/spot/BTCUSDT/1d/2026-03-17.parquet

保留目的：

可追溯

可重放

数据清洗出错时能回滚

2. normalized 标准化层

统一字段、时间、精度、symbol 命名。

统一成：

ts
symbol
exchange
interval
open
high
low
close
volume
quote_volume
trade_count

例如：

data/normalized/market_ohlcv_1d.parquet
data/normalized/market_ohlcv_4h.parquet
3. warehouse 仓库层

让 DuckDB 面向 normalized 层建视图或表。

例如：

market_ohlcv

funding_rate

open_interest

这样以后换交易所，因子和回测都不需要改。

三、升级重点 2：加入“数据版本”概念

这是研究系统非常重要但常被忽略的一步。

因为以后你会碰到这些情况：

重新补历史数据

修正某些缺失 K 线

换数据源

增加新的字段

所以建议每批数据都带版本号。

例如：

dataset_version = ohlcv_v1
dataset_version = ohlcv_v2

在 DuckDB 里可以加字段：

data_version

或者在目录里体现：

data/normalized/v1/market_ohlcv_1d.parquet
data/normalized/v2/market_ohlcv_1d.parquet

这样做的意义是：

同一个策略的回测结果，必须能追溯到使用了哪一版数据。

四、升级重点 3：把“因子”变成因子库

你原来是“算几个指标”。
升级后建议做成 factor library。

也就是每个因子都应该：

有名字

有定义

有输入

有输出

可单独复用

例子
因子 1：20 日动量
factor_name = momentum_20d
definition = close / close.shift(20) - 1
因子 2：成交量放大倍数
factor_name = volume_ratio_20d
definition = volume / volume_ma20
因子 3：量价突破
factor_name = breakout_volume_confirmed
definition = close > rolling_high_20 AND volume_ratio_20d > 2
因子库目录建议
src/factors/
  momentum.py
  volume.py
  breakout.py
  trend.py
因子元数据建议

每个因子建议都记录：

名称

公式

参数

依赖字段

输出字段

适用频率

例如：

name: volume_ratio
params: window=20
input: volume
output: volume_ratio_20

这样以后组合策略时不会乱。

五、升级重点 4：信号层和因子层彻底解耦

这个非常关键。

因子层只是给你“事实”：

动量是多少

成交量倍数是多少

是否站上均线

信号层才负责“决策”：

什么时候开仓

什么时候平仓

仓位目标是多少

正确关系
feature → rule → signal
示例

因子：

momentum_20d = 0.12
volume_ratio_20d = 2.4
close_above_ma50 = 1

信号规则：

if momentum_20d > 0.08
and volume_ratio_20d > 2
and close_above_ma50 == 1
then entry = 1

这样做的好处是：

同一组因子可以生成多个策略

修改规则不需要重算全部因子

回测更灵活

六、升级重点 5：回测层拆成“引擎”和“策略配置”

很多人把策略规则直接写死在回测代码里，后面很难维护。

建议拆成两部分：

1. 回测引擎

负责通用能力：

撮合逻辑

手续费

滑点

资金曲线

持仓变化

交易记录

2. 策略配置

负责参数：

初始资金

调仓频率

手续费率

开仓条件

平仓条件

仓位大小

例如：

strategy_name: vol_breakout
symbol: BTCUSDT
interval: 1d
entry:
  breakout_window: 20
  vol_ratio_min: 2.0
  require_ma50: true
exit:
  ma_exit: 20
fees:
  taker: 0.001
slippage: 0.0005

这样一个策略只是一个配置文件，不需要改底层引擎。

七、升级重点 6：加入组合层，不只回测单标的

虽然你现在主要是 BTC 和 ETH，但建议一开始就保留组合接口。

组合层负责什么

多标的权重分配

仓位归一化

风险预算

再平衡

最简单先做 3 种方式
1. 等权
BTC 50%
ETH 50%
2. 波动率反比

谁波动更大，权重更小。

3. 信号驱动

哪个信号强，哪个权重大。

输出
timestamp
symbol
target_weight

这样你的回测引擎既能吃单标的 signal，也能吃组合层输出。

八、升级重点 7：加入评估层，不只看收益

很多人回测完只看资金曲线，这是远远不够的。

评估层建议至少固定输出这几类指标。

收益类

累计收益

年化收益

月度收益

年度收益

风险类

最大回撤

年化波动率

Calmar

Sharpe

交易类

胜率

盈亏比

平均持仓天数

平均单笔收益

交易次数

稳定性类

分年份表现

分市场阶段表现

BTC / ETH 是否一致有效

输出建议
reports/
  vol_breakout_v1/
    summary.json
    equity.parquet
    trades.parquet
    equity.png
    drawdown.png
    yearly_returns.csv

这里的重点不是“画图”，而是：

每次实验都必须有统一格式的结果输出。

九、升级重点 8：加入实验管理层

这是从“个人脚本”升级到“研究系统”的分水岭。

因为你后面一定会不断试：

参数不同

因子不同

不同周期

不同手续费

不同入场规则

所以每次运行都要留下记录。

最低配做法

用一个 experiments.duckdb 或 experiments.csv 记录：

experiment_id

strategy_name

params

data_version

code_version

start_date

end_date

annual_return

max_drawdown

sharpe

notes

例如：

exp_20260317_001
vol_breakout
{"window":20,"vol_ratio":2.0}
ohlcv_v2
git:abc123
2019-01-01
2025-12-31
0.24
-0.18
1.21
更好的做法

后面可以升级到：

MLflow

Weights & Biases

自己做 DuckDB 元数据表

但一开始用 DuckDB 就够了。

十、升级重点 9：加入 walk-forward 和 out-of-sample

这是防过拟合最关键的一步。

不要只做全历史回测。
建议分成：

训练区间

验证区间

测试区间

例如：

2018-2022 训练
2023 验证
2024-2025 测试

或者做 walk-forward：

训练 2 年 → 测试 6 个月
滚动向前

这样你才知道策略是不是“只对过去有效”。

所以回测层建议支持两种模式：

full backtest

walk-forward backtest

十一、升级重点 10：实盘层加“仿真盘”

不要从回测直接跳真仓。
中间一定要加一层：

paper trading

流程变成：

回测 → 仿真盘 → 小资金实盘
仿真盘做什么

读取实时行情

按策略生成信号

模拟下单

记录理论成交

输出持仓和收益

这样能验证：

策略逻辑有没有未来函数

实际数据延迟会不会影响

信号和回测是否一致

十二、推荐升级后的项目结构
quant-system/
├── config/
│   ├── data_sources.yaml
│   ├── strategies/
│   │   ├── vol_breakout.yaml
│   │   └── trend_follow.yaml
│   └── portfolio/
│       └── btc_eth_equal.yaml
│
├── data/
│   ├── raw/
│   ├── normalized/
│   ├── warehouse/
│   └── cache/
│
├── db/
│   ├── quant.duckdb
│   └── experiments.duckdb
│
├── src/
│   ├── ingestion/
│   ├── normalize/
│   ├── warehouse/
│   ├── factors/
│   ├── signals/
│   ├── backtest/
│   ├── portfolio/
│   ├── evaluation/
│   ├── execution/
│   └── utils/
│
├── reports/
├── notebooks/
└── scripts/
十三、你现在最适合的“分阶段升级路线”

不要一次全做完，按 4 个阶段来最稳。

第一阶段：研究 MVP

先完成：

raw / normalized / DuckDB

基础因子

单策略回测

报表输出

目标：
先把一个 BTC/ETH 日线量价策略跑通。

第二阶段：研究系统化

加入：

因子库

信号配置

统一回测输出

实验记录

目标：
可以稳定迭代多个策略版本。

第三阶段：组合与稳健性

加入：

BTC/ETH 组合

walk-forward

out-of-sample

参数扫描

目标：
判断策略是否有稳健性。

第四阶段：仿真与实盘

加入：

仿真盘

风控

交易所执行

持仓管理

目标：
从研究走向可部署。

十四、基于你当前场景的最小升级建议

你现在主要做：

BTC / ETH

天级 / 几周级

量价因子

研究优先

所以我建议你下一步只做这 6 个核心模块：

ingestion：抓 Binance BTC/ETH 日线

normalize：统一成标准 OHLCV

warehouse：DuckDB 建 market_ohlcv 表

factors：先做 8 个基础量价因子

backtest：单标的 + 组合回测

experiments：记录每次参数和结果

这已经足够形成一套很像样的研究系统。

十五、一句话总结

这次升级的本质是把原来的：

“能跑的量化脚本”

升级成：

“可复现、可扩展、可比较、可走向实盘的研究系统”

下一步最合适的是，我直接继续帮你把这套升级版具体化成：

“目录结构 + DuckDB 表设计 + 因子表字段 + 实验表字段”。



好，我们把它具体化成一套可直接开工的结构。
我按 4 部分展开：

目录结构

DuckDB 表设计

因子/信号/回测输出结构

模块之间的数据流

目标是：你可以照着这个版本，直接开始搭第一版。

一、项目目录结构

先定一个根目录，比如：

/data/quant

完整建议结构：

/data/quant
├── config
│   ├── data_sources.yaml
│   ├── symbols.yaml
│   ├── strategies
│   │   ├── vol_breakout_v1.yaml
│   │   └── trend_volume_v1.yaml
│   └── portfolio
│       └── btc_eth_equal.yaml
│
├── data
│   ├── raw
│   │   └── binance
│   │       ├── BTCUSDT
│   │       │   ├── 1d
│   │       │   └── 4h
│   │       └── ETHUSDT
│   │           ├── 1d
│   │           └── 4h
│   │
│   ├── normalized
│   │   ├── v1
│   │   │   ├── market_ohlcv_1d.parquet
│   │   │   └── market_ohlcv_4h.parquet
│   │   └── v2
│   │
│   ├── features
│   │   ├── v1
│   │   │   ├── BTCUSDT_1d_features.parquet
│   │   │   └── ETHUSDT_1d_features.parquet
│   │
│   ├── signals
│   │   └── vol_breakout_v1
│   │       ├── BTCUSDT_1d_signal.parquet
│   │       └── ETHUSDT_1d_signal.parquet
│   │
│   ├── backtests
│   │   └── vol_breakout_v1
│   │       ├── btc_equity.parquet
│   │       ├── eth_equity.parquet
│   │       ├── portfolio_equity.parquet
│   │       ├── trades.parquet
│   │       └── summary.json
│   │
│   └── cache
│
├── db
│   ├── quant.duckdb
│   └── experiments.duckdb
│
├── reports
│   └── vol_breakout_v1
│       ├── equity.png
│       ├── drawdown.png
│       ├── yearly_returns.csv
│       └── summary.md
│
├── notebooks
│   ├── 01_data_check.ipynb
│   ├── 02_factor_research.ipynb
│   └── 03_strategy_review.ipynb
│
├── src
│   ├── ingestion
│   │   └── binance_ohlcv.py
│   ├── normalize
│   │   └── market_ohlcv.py
│   ├── warehouse
│   │   └── load_duckdb.py
│   ├── factors
│   │   ├── momentum.py
│   │   ├── volume.py
│   │   ├── trend.py
│   │   └── breakout.py
│   ├── signals
│   │   └── vol_breakout.py
│   ├── backtest
│   │   ├── engine.py
│   │   ├── broker.py
│   │   └── metrics.py
│   ├── portfolio
│   │   └── weighting.py
│   ├── evaluation
│   │   └── report.py
│   ├── experiments
│   │   └── tracker.py
│   └── utils
│       ├── dates.py
│       └── io.py
│
└── scripts
    ├── run_ingestion.sh
    ├── run_features.sh
    ├── run_backtest.sh
    └── run_pipeline.sh
二、DuckDB 数据库设计

建议两个数据库：

quant.duckdb：主数据仓库

experiments.duckdb：实验记录

1. quant.duckdb 里的核心表
表 1：market_ohlcv

这是最核心的行情表。

字段建议：

CREATE TABLE market_ohlcv (
    ts TIMESTAMP,
    symbol VARCHAR,
    exchange VARCHAR,
    market_type VARCHAR,
    interval VARCHAR,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    quote_volume DOUBLE,
    trade_count BIGINT,
    source_file VARCHAR,
    data_version VARCHAR,
    created_at TIMESTAMP
);

字段说明：

ts：K 线起始时间

symbol：BTCUSDT / ETHUSDT

exchange：binance

market_type：spot / perp

interval：1d / 4h

source_file：来自哪个 parquet

data_version：v1 / v2

表 2：factor_values

如果你希望把因子也统一管理到 DuckDB，可以建这个表。

CREATE TABLE factor_values (
    ts TIMESTAMP,
    symbol VARCHAR,
    interval VARCHAR,
    factor_set VARCHAR,
    factor_name VARCHAR,
    factor_value DOUBLE,
    data_version VARCHAR,
    created_at TIMESTAMP
);

这个表适合“长表”模式。
但第一阶段，我更建议因子先存 parquet，查询更直观。

所以第一版可以不建，或者只留着以后扩展。

表 3：strategy_signals
CREATE TABLE strategy_signals (
    ts TIMESTAMP,
    strategy_name VARCHAR,
    strategy_version VARCHAR,
    symbol VARCHAR,
    interval VARCHAR,
    signal_long_entry INTEGER,
    signal_long_exit INTEGER,
    position_target DOUBLE,
    data_version VARCHAR,
    created_at TIMESTAMP
);

说明：

signal_long_entry：1 表示开多

signal_long_exit：1 表示平仓

position_target：目标仓位，比如 0 / 0.5 / 1

表 4：backtest_trades
CREATE TABLE backtest_trades (
    trade_id VARCHAR,
    experiment_id VARCHAR,
    strategy_name VARCHAR,
    symbol VARCHAR,
    entry_ts TIMESTAMP,
    exit_ts TIMESTAMP,
    entry_price DOUBLE,
    exit_price DOUBLE,
    qty DOUBLE,
    gross_pnl DOUBLE,
    net_pnl DOUBLE,
    fees DOUBLE,
    holding_bars BIGINT,
    holding_days DOUBLE,
    created_at TIMESTAMP
);
表 5：backtest_equity
CREATE TABLE backtest_equity (
    ts TIMESTAMP,
    experiment_id VARCHAR,
    strategy_name VARCHAR,
    portfolio_name VARCHAR,
    symbol VARCHAR,
    equity DOUBLE,
    cash DOUBLE,
    position_value DOUBLE,
    daily_return DOUBLE,
    drawdown DOUBLE,
    created_at TIMESTAMP
);

这里 symbol 可以是：

单标的：BTCUSDT

组合层：PORTFOLIO

2. experiments.duckdb 里的核心表
表 1：experiments

这是最重要的实验登记表。

CREATE TABLE experiments (
    experiment_id VARCHAR,
    strategy_name VARCHAR,
    strategy_version VARCHAR,
    portfolio_name VARCHAR,
    data_version VARCHAR,
    symbols VARCHAR,
    interval VARCHAR,
    start_date DATE,
    end_date DATE,
    params_json VARCHAR,
    code_version VARCHAR,
    status VARCHAR,
    annual_return DOUBLE,
    max_drawdown DOUBLE,
    sharpe DOUBLE,
    calmar DOUBLE,
    win_rate DOUBLE,
    trade_count BIGINT,
    notes VARCHAR,
    created_at TIMESTAMP
);

这里的 params_json 例如：

{"breakout_window":20,"vol_ratio_min":2.0,"ma_filter":50}
表 2：artifacts

用来登记输出文件位置。

CREATE TABLE artifacts (
    experiment_id VARCHAR,
    artifact_type VARCHAR,
    artifact_path VARCHAR,
    created_at TIMESTAMP
);

例如：

equity_curve

trades

summary_json

drawdown_png

三、Parquet 文件结构设计

第一版建议大量用 parquet 文件，而不是所有东西都塞进 DuckDB。

1. normalized 行情 parquet

文件：

data/normalized/v1/market_ohlcv_1d.parquet

字段：

ts
symbol
exchange
market_type
interval
open
high
low
close
volume
quote_volume
trade_count
data_version
2. 因子 parquet

文件：

data/features/v1/BTCUSDT_1d_features.parquet

建议用“宽表”，因为研究最方便。

字段示例：

ts
symbol
close
volume
ret_1d
ret_5d
ret_20d
ma20
ma50
ma100
close_above_ma50
rolling_high_20
breakout_20d
vol_ma20
vol_ratio_20
obv
atr14
volatility_20
data_version
factor_set

这里：

breakout_20d：0/1

close_above_ma50：0/1

factor_set：例如 daily_core_v1

3. 信号 parquet

文件：

data/signals/vol_breakout_v1/BTCUSDT_1d_signal.parquet

字段建议：

ts
symbol
strategy_name
strategy_version
close
signal_long_entry
signal_long_exit
position_target
entry_reason
exit_reason
data_version

例如：

entry_reason = breakout_20d_and_vol_ratio

exit_reason = close_below_ma20

4. 回测成交 parquet

文件：

data/backtests/vol_breakout_v1/trades.parquet

字段：

trade_id
experiment_id
symbol
entry_ts
exit_ts
entry_price
exit_price
qty
gross_pnl
net_pnl
fees
holding_days
entry_reason
exit_reason
5. 净值 parquet

文件：

data/backtests/vol_breakout_v1/portfolio_equity.parquet

字段：

ts
experiment_id
equity
cash
position_value
daily_return
cum_return
drawdown
btc_weight
eth_weight
四、配置文件具体长什么样
1. 数据源配置 config/data_sources.yaml
binance:
  exchange: binance
  market_type: spot
  base_url: "https://api.binance.com"
  symbols:
    - BTCUSDT
    - ETHUSDT
  intervals:
    - 1d
    - 4h
  start_date: "2018-01-01"
2. 策略配置 config/strategies/vol_breakout_v1.yaml
strategy_name: vol_breakout
strategy_version: v1

universe:
  symbols:
    - BTCUSDT
    - ETHUSDT
  interval: 1d

data:
  exchange: binance
  data_version: v1

entry:
  breakout_window: 20
  vol_ratio_min: 2.0
  require_close_above_ma50: true

exit:
  close_below_ma20: true

execution:
  order_type: next_bar_open
  slippage_bps: 5
  fee_bps: 10

risk:
  max_position_per_symbol: 0.5
  max_total_exposure: 1.0
3. 组合配置 config/portfolio/btc_eth_equal.yaml
portfolio_name: btc_eth_equal

symbols:
  - BTCUSDT
  - ETHUSDT

weighting:
  method: equal

rebalance:
  frequency: daily
五、因子库要具体到什么程度

建议你先固定一组 daily_core_v1 因子。

第一版 10 个基础因子就够了
收益类

ret_1d

ret_5d

ret_20d

趋势类

ma20

ma50

close_above_ma50

突破类

rolling_high_20

breakout_20d

成交量类

vol_ma20

vol_ratio_20

如果你愿意再多一点：

obv

atr14

volatility_20

六、模块输入输出定义

这部分最重要，决定你后面不会乱。

1. ingestion 模块

输入：交易所 API
输出：data/raw/.../*.parquet

2. normalize 模块

输入：raw parquet
输出：data/normalized/v1/*.parquet

3. warehouse 模块

输入：normalized parquet
输出：写入 quant.duckdb.market_ohlcv

4. factors 模块

输入：normalized parquet 或 DuckDB 查询结果
输出：data/features/v1/*.parquet

5. signals 模块

输入：feature parquet + strategy yaml
输出：data/signals/<strategy>/*.parquet

6. backtest 模块

输入：signal parquet + price data + strategy yaml
输出：

trades parquet

equity parquet

summary json

7. portfolio 模块

输入：多标的 signal/equity
输出：portfolio equity parquet

8. experiments 模块

输入：本次运行参数 + 回测结果摘要
输出：写入 experiments.duckdb.experiments

七、第一版建议的脚本入口

建议只保留 4 个命令入口，先别搞复杂。

1. 拉数据
python -m src.ingestion.binance_ohlcv
2. 规范化
python -m src.normalize.market_ohlcv
3. 生成因子
python -m src.factors.daily_core
4. 跑回测
python -m src.backtest.run --strategy config/strategies/vol_breakout_v1.yaml

然后再包一层：

bash scripts/run_pipeline.sh
八、实验表里建议至少记录这些字段

你以后会非常感谢现在把它定好。

最小字段集：

experiment_id

strategy_name

strategy_version

data_version

symbols

interval

start_date

end_date

params_json

annual_return

max_drawdown

sharpe

trade_count

created_at

九、最小 MVP 版本应该长什么样

你第一阶段不用把所有表都建全。
真正建议先落地的是这 6 样：

必做

market_ohlcv 表

experiments 表

normalized parquet

feature parquet

signal parquet

equity/trades parquet

可以后做

factor_values 长表

artifacts 表

walk-forward 表

live execution 表

十、你现在最适合先做的 3 个策略文件

建议不要一下研究太多。

先做这 3 个：

1. vol_breakout_v1

20 日突破

成交量放大

MA50 趋势过滤

2. trend_pullback_v1

MA50 上方

回调到 MA20 附近

放量反弹入场

3. dual_momentum_v1

BTC、ETH 比较 20 日动量

强者持有

弱者空仓或低仓位

十一、推荐的第一版开发顺序

按这个顺序最稳：

第一步

建目录和配置文件

第二步

打通 raw -> normalized -> DuckDB

第三步

做 daily_core_v1 因子文件

第四步

做 vol_breakout_v1 信号

第五步

做单标的回测

第六步

做 BTC + ETH 等权组合

第七步

把结果登记到 experiments 表

十二、一句话落地版

把这套系统先具体化成：

1 张行情表 + 1 张实验表 + 1 套宽表因子文件 + 1 套信号文件 + 1 套回测输出文件

这就是最稳的第一版。

下一步最适合的是：我直接继续帮你写出
“第一版 DuckDB 建表 SQL + 第一版 strategy YAML + 第一版因子字段清单”。

十三、第一版已落地文件

已经补到仓库里的第一版文件：

sql/init_v1.sql

用途：
初始化 quant.duckdb 主库表，并同时创建 experiments.duckdb 里的实验表和产物表。

执行方式：

duckdb db/quant.duckdb < sql/init_v1.sql

config/strategies/vol_breakout_v1.yaml

用途：
第一版 20 日突破 + 放量确认 + MA50 趋势过滤策略配置。

config/factors/daily_core_v1.yaml

用途：
第一版日线核心因子字段清单，包含字段名、公式、依赖字段、预热窗口和策略依赖关系。

十四、第一版 Binance K 线抓取器

已经补到仓库：

src/ingestion/binance_ohlcv.py

当前能力：

抓 Binance spot K 线

支持区间：
5m、1h、4h、8h、1d、1w

支持多 symbol、多 interval 批量抓取

支持自动发现当前 Binance spot 全量币对

支持按 `quoteAsset` 过滤自动发现结果，例如只抓 `USDT`

支持从每个 symbol/interval 的最早可用 K 线开始抓取

自动按 Binance 单次最多 1000 根 K 线做翻页

对瞬时网络错误、429、5xx 自动重试

支持 checkpoint / 自动续跑，进程中断后再次执行同样请求会从上次进度继续

输出到 raw 层目录：

data/raw/binance/spot/<symbol>/<interval>/<run_id>.jsonl

并同时写：

<run_id>.meta.json

如果任务中断，运行中的 symbol/interval 目录里会暂时存在：

_checkpoint.json

以及本次批量抓取 manifest：

data/raw/binance/spot/<run_id>.manifest.json

说明：
第一版 raw 层先落 jsonl，而不是 parquet。这样可以零额外依赖先把采集跑通，并完整保留交易所原始返回；后续 normalize 再把 raw jsonl 转成 normalized parquet。

运行示例：

python3 -m src.ingestion.binance_ohlcv \
  --symbols BTCUSDT ETHUSDT \
  --intervals 5m 1h 4h 8h 1d 1w \
  --start-date 2018-01-01 \
  --end-date 2026-03-18

自动发现当前 spot 币对并从最早可用 K 线开始抓：

python3 -m src.ingestion.binance_ohlcv \
  --all-spot-symbols \
  --intervals 5m 1h 4h 8h 1d 1w \
  --start-from-listing \
  --end-date 2026-03-18

如果你只想先小样本回填：

python3 -m src.ingestion.binance_ohlcv \
  --all-spot-symbols \
  --symbol-statuses TRADING \
  --quote-assets USDT \
  --max-symbols 20 \
  --intervals 1d \
  --start-from-listing \
  --end-date 2026-03-18

时间参数规则：

start-date 是包含起点

end-date 如果写日期，例如 2026-03-18，表示“抓到这一天结束”，程序内部会转成 2026-03-19 00:00:00 UTC 的 exclusive end

如果开启 `--start-from-listing` 且不显式传 `--start-date`，程序会先探测该 symbol/interval 的最早可用 K 线，再从那个时间开始抓

如果任务中断，再次执行相同请求时，默认会自动读取 `_checkpoint.json` 续跑；如果你想忽略断点重来，可以加：

--no-resume-incomplete

十五、第一版 Normalize

已经补到仓库：

src/normalize/market_ohlcv.py

当前能力：

读取 raw 层 `data/raw/binance/spot/<symbol>/<interval>/*.jsonl`

统一成 `market_ohlcv` 标准字段：

ts
symbol
exchange
market_type
interval
open
high
low
close
volume
quote_volume
trade_count
source_file
data_version
created_at

按 `(exchange, market_type, symbol, interval, ts)` 去重

如果同一根 K 线被重复抓取，保留 `fetched_at` 更新的那条

按 interval 输出到：

data/normalized/<data_version>/market_ohlcv_<interval>.jsonl

并写 normalize manifest：

data/normalized/<data_version>/normalize_manifest.json

输出格式说明：

默认 `--output-format auto`

如果安装了 `pyarrow`，会直接输出 parquet

如果没有安装 `pyarrow`，会自动退回 jsonl

如果你想强制 parquet：

python3 -m pip install pyarrow

运行示例：

python3 -m src.normalize.market_ohlcv \
  --input-root data/raw/binance/spot \
  --output-root data/normalized \
  --data-version v1 \
  --output-format auto

十六、第一版 Warehouse

已经补到仓库：

src/warehouse/load_duckdb.py

当前能力：

读取 `data/normalized/<data_version>/market_ohlcv_<interval>.jsonl`

如果安装了 `pyarrow`，同样也支持读取 `.parquet`

自动执行 `sql/init_v1.sql` 初始化 `db/quant.duckdb` 和 `db/experiments.duckdb`

把 normalized `market_ohlcv` 批量写入 DuckDB 的 `market_ohlcv` 表

装载规则：

按 interval 文件逐个装载

每次装载某个 interval 时，先删除 `market_ohlcv` 里相同 `data_version + interval` 的旧数据，再整批写入

这样重复执行是幂等的，不会因为重复运行堆出重复记录

运行依赖：

需要 DuckDB Python 包

建议放在虚拟环境里安装：

python3 -m venv .venv
source .venv/bin/activate
pip install duckdb

运行示例：

python3 -m src.warehouse.load_duckdb \
  --data-version v1 \
  --normalized-root data/normalized \
  --db-path db/quant.duckdb \
  --init-sql sql/init_v1.sql

当前样例验证结果：

`v1` 已成功装入 `db/quant.duckdb`

当前 `market_ohlcv` 行数：

- `1h` = 48
- `5m` = 12
- 合计 `60`
