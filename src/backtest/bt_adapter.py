from __future__ import annotations

import importlib
from dataclasses import dataclass
from datetime import UTC
from typing import Any

from src.strategies.core import StrategyMetadata

from .core import (
    BacktestResult,
    BacktestSettings,
    EquityPoint,
    TargetWeightPlan,
    TradeRecord,
    build_summary_metrics,
    iso_to_datetime,
)


@dataclass
class BtOpenPosition:
    symbol: str
    entry_ts: str
    entry_price: float
    quantity: float
    entry_fee_paid: float
    entry_reason: str | None


def import_bt_modules() -> tuple[Any, Any]:
    try:
        bt = importlib.import_module("bt")
        pandas = importlib.import_module("pandas")
    except ImportError as exc:
        raise RuntimeError(
            "Engine `bt` requires optional dependencies `bt` and `pandas`. "
            "Install them with `python3 -m pip install -r requirements-bt.txt`."
        ) from exc
    return bt, pandas


def build_bt_frames(plan: TargetWeightPlan, pandas: Any) -> tuple[Any, Any]:
    index = pandas.to_datetime(list(plan.weights_by_ts.keys()), utc=True)
    open_rows: list[dict[str, float | None]] = []
    weight_rows: list[dict[str, float]] = []

    for ts in plan.weights_by_ts:
        open_rows.append(
            {
                symbol: plan.tables.open_prices.get(ts, {}).get(symbol)
                for symbol in plan.tables.symbols
            }
        )
        weight_rows.append(
            {
                symbol: plan.weights_by_ts[ts].get(symbol, 0.0)
                for symbol in plan.tables.symbols
            }
        )

    price_frame = pandas.DataFrame(open_rows, index=index, columns=plan.tables.symbols)
    weight_frame = pandas.DataFrame(weight_rows, index=index, columns=plan.tables.symbols)
    return price_frame, weight_frame


def extract_result_equity_curve(
    result: Any,
    *,
    metadata: StrategyMetadata,
) -> list[EquityPoint]:
    price_series = result.prices
    if getattr(price_series, "ndim", 1) == 2:
        if metadata.strategy_id in price_series.columns:
            price_series = price_series[metadata.strategy_id]
        else:
            price_series = price_series.iloc[:, 0]

    equity_curve: list[EquityPoint] = []
    for timestamp, total_equity in price_series.items():
        ts = pandas_timestamp_to_iso(timestamp)
        equity_curve.append(
            EquityPoint(
                ts=ts,
                strategy_name=metadata.strategy_name,
                strategy_version=metadata.strategy_version,
                total_equity=float(total_equity),
                cash=0.0,
                market_value=float(total_equity),
                gross_exposure=1.0 if float(total_equity) > 0 else 0.0,
            )
        )
    return equity_curve


def pandas_timestamp_to_iso(value: Any) -> str:
    as_datetime = value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
    if as_datetime.tzinfo is None:
        as_datetime = as_datetime.replace(tzinfo=UTC)
    return as_datetime.astimezone(UTC).isoformat().replace("+00:00", "Z")


def build_trades_from_transactions(
    transactions: Any,
    *,
    metadata: StrategyMetadata,
    settings: BacktestSettings,
    plan: TargetWeightPlan,
) -> list[TradeRecord]:
    if transactions is None or len(transactions) == 0:
        return []

    normalized_rows: list[tuple[str, str, float, float]] = []
    for (timestamp, symbol), row in transactions.iterrows():
        quantity = float(row["quantity"])
        price = float(row["price"])
        normalized_rows.append((pandas_timestamp_to_iso(timestamp), str(symbol), quantity, price))
    normalized_rows.sort(key=lambda item: (item[1], item[0]))

    open_positions: dict[str, BtOpenPosition] = {}
    trades: list[TradeRecord] = []
    fee_rate = settings.fee_rate + settings.slippage_rate

    for ts, symbol, quantity, price in normalized_rows:
        fee_paid = abs(quantity) * price * fee_rate
        if quantity > 0:
            open_positions[symbol] = BtOpenPosition(
                symbol=symbol,
                entry_ts=ts,
                entry_price=price,
                quantity=quantity,
                entry_fee_paid=fee_paid,
                entry_reason=plan.entry_reasons_by_ts.get(ts, {}).get(symbol),
            )
            continue

        position = open_positions.get(symbol)
        if position is None:
            continue

        exit_qty = abs(quantity)
        gross_pnl = (price - position.entry_price) * min(position.quantity, exit_qty)
        net_pnl = gross_pnl - position.entry_fee_paid - fee_paid
        entry_index = plan.tables.bar_index_by_symbol.get(symbol, {}).get(position.entry_ts, 0)
        exit_index = plan.tables.bar_index_by_symbol.get(symbol, {}).get(ts, entry_index)
        trades.append(
            TradeRecord(
                trade_id=f"{metadata.strategy_id}_{symbol}_{len(trades) + 1}",
                strategy_name=metadata.strategy_name,
                strategy_version=metadata.strategy_version,
                symbol=symbol,
                entry_ts=position.entry_ts,
                exit_ts=ts,
                entry_price=position.entry_price,
                exit_price=price,
                quantity=min(position.quantity, exit_qty),
                gross_pnl=gross_pnl,
                net_pnl=net_pnl,
                return_pct=(price / position.entry_price - 1.0),
                fees_paid=position.entry_fee_paid + fee_paid,
                bars_held=exit_index - entry_index,
                entry_reason=position.entry_reason,
                exit_reason=plan.exit_reasons_by_ts.get(ts, {}).get(symbol),
            )
        )
        del open_positions[symbol]

    return trades


def run_bt_backtest(
    plan: TargetWeightPlan,
    *,
    metadata: StrategyMetadata,
    settings: BacktestSettings,
) -> BacktestResult:
    bt, pandas = import_bt_modules()
    price_frame, weight_frame = build_bt_frames(plan, pandas)
    strategy_name = metadata.strategy_id

    strategy = bt.Strategy(
        strategy_name,
        [
            bt.algos.WeighTarget(weight_frame),
            bt.algos.Rebalance(),
        ],
    )
    commission_rate = settings.fee_rate + settings.slippage_rate
    backtest = bt.Backtest(
        strategy,
        price_frame,
        initial_capital=settings.initial_capital,
        commissions=lambda quantity, price: abs(quantity) * price * commission_rate,
        integer_positions=False,
    )
    result = bt.run(backtest)
    transactions = result.get_transactions(strategy_name)
    trades = build_trades_from_transactions(
        transactions,
        metadata=metadata,
        settings=settings,
        plan=plan,
    )
    equity_curve = extract_result_equity_curve(result, metadata=metadata)
    summary = build_summary_metrics(
        equity_curve,
        trades,
        metadata=metadata,
        settings=settings,
        engine="bt",
    )
    return BacktestResult(summary=summary, trades=trades, equity_curve=equity_curve)
