from __future__ import annotations

from dataclasses import dataclass

from src.strategies.core import StrategyMetadata

from .core import (
    BacktestResult,
    BacktestSettings,
    EquityPoint,
    TargetWeightPlan,
    TradeRecord,
    build_summary_metrics,
)


@dataclass
class OpenPosition:
    symbol: str
    entry_ts: str
    entry_price: float
    quantity: float
    entry_fee_paid: float
    entry_reason: str | None
    entry_bar_index: int


def run_native_backtest(
    plan: TargetWeightPlan,
    *,
    metadata: StrategyMetadata,
    settings: BacktestSettings,
) -> BacktestResult:
    cash = settings.initial_capital
    open_positions: dict[str, OpenPosition] = {}
    equity_curve: list[EquityPoint] = []
    trades: list[TradeRecord] = []

    for ts in plan.tables.timestamps:
        target_weights = plan.weights_by_ts.get(ts)
        if target_weights is None:
            continue

        open_prices = plan.tables.open_prices.get(ts, {})
        close_prices = plan.tables.close_prices.get(ts, {})
        equity_before = cash + sum(
            position.quantity * open_prices[position.symbol]
            for position in open_positions.values()
            if position.symbol in open_prices
        )

        for symbol in plan.tables.symbols:
            if symbol not in open_prices:
                continue

            position = open_positions.get(symbol)
            current_qty = position.quantity if position is not None else 0.0
            target_weight = target_weights.get(symbol, 0.0)
            desired_value = equity_before * target_weight
            target_qty = desired_value / open_prices[symbol] if target_weight > 0 else 0.0
            if abs(target_qty - current_qty) < 1e-12:
                continue

            if target_qty > current_qty:
                fill_price = open_prices[symbol] * (1.0 + settings.slippage_rate)
                buy_qty = target_qty - current_qty
                gross_cost = buy_qty * fill_price
                fee_paid = gross_cost * settings.fee_rate
                affordable_qty = buy_qty
                if settings.allow_partial_cash and gross_cost + fee_paid > cash and fill_price > 0:
                    affordable_qty = cash / (fill_price * (1.0 + settings.fee_rate))
                    gross_cost = affordable_qty * fill_price
                    fee_paid = gross_cost * settings.fee_rate
                elif not settings.allow_partial_cash and gross_cost + fee_paid > cash:
                    continue
                if affordable_qty <= 0:
                    continue

                cash -= gross_cost + fee_paid
                if position is None:
                    open_positions[symbol] = OpenPosition(
                        symbol=symbol,
                        entry_ts=ts,
                        entry_price=fill_price,
                        quantity=affordable_qty,
                        entry_fee_paid=fee_paid,
                        entry_reason=plan.entry_reasons_by_ts.get(ts, {}).get(symbol),
                        entry_bar_index=plan.tables.bar_index_by_symbol[symbol][ts],
                    )
                else:
                    combined_qty = position.quantity + affordable_qty
                    weighted_entry_price = (
                        (position.entry_price * position.quantity) + (fill_price * affordable_qty)
                    ) / combined_qty
                    position.entry_price = weighted_entry_price
                    position.quantity = combined_qty
                    position.entry_fee_paid += fee_paid
            else:
                if position is None:
                    continue
                sell_qty = current_qty - target_qty
                fill_price = open_prices[symbol] * (1.0 - settings.slippage_rate)
                gross_proceeds = sell_qty * fill_price
                fee_paid = gross_proceeds * settings.fee_rate
                cash += gross_proceeds - fee_paid
                remaining_qty = position.quantity - sell_qty
                proportion = sell_qty / position.quantity if position.quantity > 0 else 1.0
                allocated_entry_fee = position.entry_fee_paid * proportion
                position.entry_fee_paid -= allocated_entry_fee

                if remaining_qty <= 1e-12:
                    gross_pnl = (fill_price - position.entry_price) * sell_qty
                    net_pnl = gross_pnl - allocated_entry_fee - fee_paid
                    bars_held = (
                        plan.tables.bar_index_by_symbol[symbol][ts] - position.entry_bar_index
                    )
                    trades.append(
                        TradeRecord(
                            trade_id=f"{metadata.strategy_id}_{symbol}_{len(trades) + 1}",
                            strategy_name=metadata.strategy_name,
                            strategy_version=metadata.strategy_version,
                            symbol=symbol,
                            entry_ts=position.entry_ts,
                            exit_ts=ts,
                            entry_price=position.entry_price,
                            exit_price=fill_price,
                            quantity=sell_qty,
                            gross_pnl=gross_pnl,
                            net_pnl=net_pnl,
                            return_pct=(fill_price / position.entry_price - 1.0),
                            fees_paid=allocated_entry_fee + fee_paid,
                            bars_held=bars_held,
                            entry_reason=position.entry_reason,
                            exit_reason=plan.exit_reasons_by_ts.get(ts, {}).get(symbol),
                        )
                    )
                    del open_positions[symbol]
                else:
                    position.quantity = remaining_qty

        market_value = sum(
            position.quantity * close_prices[position.symbol]
            for position in open_positions.values()
            if position.symbol in close_prices
        )
        total_equity = cash + market_value
        equity_curve.append(
            EquityPoint(
                ts=ts,
                strategy_name=metadata.strategy_name,
                strategy_version=metadata.strategy_version,
                total_equity=total_equity,
                cash=cash,
                market_value=market_value,
                gross_exposure=(market_value / total_equity) if total_equity > 0 else 0.0,
            )
        )

    if equity_curve and open_positions:
        last_ts = equity_curve[-1].ts
        last_close_prices = plan.tables.close_prices[last_ts]
        for symbol in sorted(list(open_positions)):
            position = open_positions.pop(symbol)
            if symbol not in last_close_prices:
                continue
            fill_price = last_close_prices[symbol] * (1.0 - settings.slippage_rate)
            gross_proceeds = position.quantity * fill_price
            fee_paid = gross_proceeds * settings.fee_rate
            cash += gross_proceeds - fee_paid
            gross_pnl = (fill_price - position.entry_price) * position.quantity
            net_pnl = gross_pnl - position.entry_fee_paid - fee_paid
            bars_held = plan.tables.bar_index_by_symbol[symbol][last_ts] - position.entry_bar_index
            trades.append(
                TradeRecord(
                    trade_id=f"{metadata.strategy_id}_{symbol}_{len(trades) + 1}",
                    strategy_name=metadata.strategy_name,
                    strategy_version=metadata.strategy_version,
                    symbol=symbol,
                    entry_ts=position.entry_ts,
                    exit_ts=last_ts,
                    entry_price=position.entry_price,
                    exit_price=fill_price,
                    quantity=position.quantity,
                    gross_pnl=gross_pnl,
                    net_pnl=net_pnl,
                    return_pct=(fill_price / position.entry_price - 1.0),
                    fees_paid=position.entry_fee_paid + fee_paid,
                    bars_held=bars_held,
                    entry_reason=position.entry_reason,
                    exit_reason="forced_liquidation_at_backtest_end",
                )
            )

        equity_curve[-1] = EquityPoint(
            ts=last_ts,
            strategy_name=metadata.strategy_name,
            strategy_version=metadata.strategy_version,
            total_equity=cash,
            cash=cash,
            market_value=0.0,
            gross_exposure=0.0,
        )

    summary = build_summary_metrics(
        equity_curve,
        trades,
        metadata=metadata,
        settings=settings,
        engine="native",
    )
    return BacktestResult(summary=summary, trades=trades, equity_curve=equity_curve)
