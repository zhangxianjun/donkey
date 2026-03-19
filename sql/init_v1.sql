-- Bootstrap the first-pass research warehouse.
-- Run from the repository root:
--   duckdb db/quant.duckdb < sql/init_v1.sql

CREATE TABLE IF NOT EXISTS market_ohlcv (
    ts TIMESTAMP NOT NULL,
    symbol VARCHAR NOT NULL,
    exchange VARCHAR NOT NULL,
    market_type VARCHAR NOT NULL,
    interval VARCHAR NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL,
    quote_volume DOUBLE,
    trade_count BIGINT,
    source_file VARCHAR,
    data_version VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Optional long-format factor storage. The first MVP can still write
-- factor wide tables to Parquet and only load selected slices here.
CREATE TABLE IF NOT EXISTS factor_values (
    ts TIMESTAMP NOT NULL,
    symbol VARCHAR NOT NULL,
    interval VARCHAR NOT NULL,
    factor_set VARCHAR NOT NULL,
    factor_name VARCHAR NOT NULL,
    factor_value DOUBLE,
    data_version VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS strategy_signals (
    ts TIMESTAMP NOT NULL,
    strategy_name VARCHAR NOT NULL,
    strategy_version VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    interval VARCHAR NOT NULL,
    signal_long_entry INTEGER NOT NULL DEFAULT 0,
    signal_long_exit INTEGER NOT NULL DEFAULT 0,
    position_target DOUBLE NOT NULL DEFAULT 0,
    entry_reason VARCHAR,
    exit_reason VARCHAR,
    data_version VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS backtest_trades (
    trade_id VARCHAR NOT NULL,
    experiment_id VARCHAR NOT NULL,
    strategy_name VARCHAR NOT NULL,
    strategy_version VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    entry_ts TIMESTAMP NOT NULL,
    exit_ts TIMESTAMP,
    entry_price DOUBLE NOT NULL,
    exit_price DOUBLE,
    qty DOUBLE NOT NULL,
    gross_pnl DOUBLE,
    net_pnl DOUBLE,
    fees DOUBLE,
    holding_bars BIGINT,
    holding_days DOUBLE,
    entry_reason VARCHAR,
    exit_reason VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS backtest_equity (
    ts TIMESTAMP NOT NULL,
    experiment_id VARCHAR NOT NULL,
    strategy_name VARCHAR NOT NULL,
    strategy_version VARCHAR NOT NULL,
    portfolio_name VARCHAR NOT NULL,
    symbol VARCHAR NOT NULL,
    equity DOUBLE NOT NULL,
    cash DOUBLE NOT NULL,
    position_value DOUBLE NOT NULL,
    daily_return DOUBLE,
    cum_return DOUBLE,
    drawdown DOUBLE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ATTACH 'db/experiments.duckdb' AS exp;

CREATE TABLE IF NOT EXISTS exp.experiments (
    experiment_id VARCHAR NOT NULL,
    strategy_name VARCHAR NOT NULL,
    strategy_version VARCHAR NOT NULL,
    portfolio_name VARCHAR,
    data_version VARCHAR NOT NULL,
    factor_set VARCHAR,
    symbols VARCHAR NOT NULL,
    interval VARCHAR NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    params_json VARCHAR NOT NULL,
    code_version VARCHAR,
    status VARCHAR NOT NULL,
    annual_return DOUBLE,
    max_drawdown DOUBLE,
    sharpe DOUBLE,
    calmar DOUBLE,
    win_rate DOUBLE,
    trade_count BIGINT,
    notes VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS exp.artifacts (
    experiment_id VARCHAR NOT NULL,
    artifact_type VARCHAR NOT NULL,
    artifact_path VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

DETACH exp;
