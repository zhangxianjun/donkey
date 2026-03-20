from __future__ import annotations

import importlib
from dataclasses import dataclass
from datetime import UTC, datetime
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
class BacktraderOpenPosition:
    symbol: str
    entry_ts: str
    entry_price: float
    quantity: float
    entry_fee_paid: float
    entry_reason: str | None
    entry_bar_index: int


def import_backtrader_modules() -> tuple[Any, Any]:
    try:
        backtrader = importlib.import_module("backtrader")
        pandas = importlib.import_module("pandas")
    except ImportError as exc:
        raise RuntimeError(
            "Engine `backtrader` requires optional dependencies `backtrader` and `pandas`. "
            "Install them with `python3 -m pip install -r requirements-backtrader.txt`."
        ) from exc
    return backtrader, pandas


def iso_to_backtrader_datetime(value: str) -> datetime:
    return iso_to_datetime(value).astimezone(UTC).replace(tzinfo=None)


def normalize_iso_timestamp(value: str) -> str:
    return iso_to_datetime(value).isoformat(timespec="seconds").replace("+00:00", "Z")


def backtrader_datetime_to_iso(value: Any) -> str:
    as_datetime = value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
    if as_datetime.tzinfo is None:
        as_datetime = as_datetime.replace(tzinfo=UTC)
    return as_datetime.astimezone(UTC).isoformat().replace("+00:00", "Z")


def build_backtrader_feeds(plan: TargetWeightPlan, *, pandas: Any, backtrader: Any) -> list[Any]:
    feeds: list[Any] = []
    for symbol in plan.tables.symbols:
        rows: list[dict[str, Any]] = []
        for ts in plan.tables.timestamps:
            open_price = plan.tables.open_prices.get(ts, {}).get(symbol)
            close_price = plan.tables.close_prices.get(ts, {}).get(symbol)
            if open_price is None or close_price is None:
                continue
            rows.append(
                {
                    "datetime": iso_to_backtrader_datetime(ts),
                    "open": float(open_price),
                    "high": float(max(open_price, close_price)),
                    "low": float(min(open_price, close_price)),
                    "close": float(close_price),
                    "volume": 0.0,
                    "openinterest": 0.0,
                }
            )

        if not rows:
            continue

        frame = pandas.DataFrame(rows).set_index("datetime")
        feeds.append(backtrader.feeds.PandasData(dataname=frame, name=symbol))
    return feeds


def build_fractional_commission_info(backtrader: Any, settings: BacktestSettings) -> Any:
    class FractionalCommissionInfo(backtrader.CommInfoBase):
        params = (
            ("commission", settings.fee_rate),
            ("stocklike", True),
            ("commtype", backtrader.CommInfoBase.COMM_PERC),
            ("percabs", True),
        )

        def getsize(self, price: float, cash: float) -> float:
            if price <= 0:
                return 0.0
            return self.p.leverage * (cash / price)

    return FractionalCommissionInfo()


def build_backtrader_strategy(backtrader: Any) -> type[Any]:
    class PlanDrivenStrategy(backtrader.Strategy):
        params = (
            ("plan", None),
            ("metadata", None),
            ("settings", None),
        )

        def __init__(self) -> None:
            self.plan: TargetWeightPlan = self.p.plan
            self.metadata: StrategyMetadata = self.p.metadata
            self.settings: BacktestSettings = self.p.settings
            self.data_by_symbol = {data._name: data for data in self.datas}
            self.in_range_timestamps = {
                normalize_iso_timestamp(ts) for ts in self.plan.weights_by_ts.keys()
            }
            self.entry_reasons_by_ts = {
                normalize_iso_timestamp(ts): value
                for ts, value in self.plan.entry_reasons_by_ts.items()
            }
            self.exit_reasons_by_ts = {
                normalize_iso_timestamp(ts): value
                for ts, value in self.plan.exit_reasons_by_ts.items()
            }
            self.bar_index_by_symbol = {
                symbol: {
                    normalize_iso_timestamp(ts): index for ts, index in values.items()
                }
                for symbol, values in self.plan.tables.bar_index_by_symbol.items()
            }
            self.target_weights_by_trigger_dt: dict[datetime, dict[str, float]] = {}
            for index, trigger_ts in enumerate(self.plan.tables.timestamps[:-1]):
                execution_ts = self.plan.tables.timestamps[index + 1]
                target_weights = self.plan.weights_by_ts.get(execution_ts)
                if target_weights is None:
                    continue
                self.target_weights_by_trigger_dt[iso_to_backtrader_datetime(trigger_ts)] = {
                    symbol: float(weight) for symbol, weight in target_weights.items()
                }
            self.open_positions: dict[str, BacktraderOpenPosition] = {}
            self.trade_records: list[TradeRecord] = []
            self.equity_curve: list[EquityPoint] = []
            self.current_targets = {symbol: 0.0 for symbol in self.data_by_symbol}

        def _current_weight(self, data: Any) -> float:
            broker_value = float(self.broker.getvalue())
            if broker_value <= 0:
                return 0.0
            position = self.getposition(data)
            if abs(position.size) <= 1e-12:
                return 0.0
            return float(position.size) * float(data.close[0]) / broker_value

        def next_open(self) -> None:
            current_dt = self.datetime.datetime(0)
            target_weights = self.target_weights_by_trigger_dt.get(current_dt)
            if target_weights is None:
                return

            for symbol, data in self.data_by_symbol.items():
                target_weight = float(target_weights.get(symbol, 0.0))
                current_target = float(self.current_targets.get(symbol, 0.0))
                if abs(target_weight - current_target) < 1e-12:
                    continue
                self.order_target_percent(data=data, target=target_weight)
                self.current_targets[symbol] = target_weight

        def notify_order(self, order: Any) -> None:
            if order.status in (order.Submitted, order.Accepted):
                return

            symbol = order.data._name
            if order.status in (order.Canceled, order.Margin, order.Rejected):
                self.current_targets[symbol] = self._current_weight(order.data)
                return

            if order.status != order.Completed:
                return

            ts = backtrader_datetime_to_iso(self.datetime.datetime(0))
            current_position = self.getposition(order.data)
            executed_size = abs(float(order.executed.size))
            if executed_size <= 1e-12:
                return

            if order.isbuy():
                if symbol in self.open_positions or current_position.size <= 1e-12:
                    return
                self.open_positions[symbol] = BacktraderOpenPosition(
                    symbol=symbol,
                    entry_ts=ts,
                    entry_price=float(order.executed.price),
                    quantity=float(current_position.size),
                    entry_fee_paid=float(order.executed.comm),
                    entry_reason=self.entry_reasons_by_ts.get(ts, {}).get(symbol),
                    entry_bar_index=self.bar_index_by_symbol.get(symbol, {}).get(ts, 0),
                )
                return

            position = self.open_positions.get(symbol)
            if position is None:
                return
            if current_position.size > 1e-12:
                position.quantity = float(current_position.size)
                return

            gross_pnl = (float(order.executed.price) - position.entry_price) * executed_size
            net_pnl = gross_pnl - position.entry_fee_paid - float(order.executed.comm)
            exit_index = self.bar_index_by_symbol.get(symbol, {}).get(
                ts,
                position.entry_bar_index,
            )
            self.trade_records.append(
                TradeRecord(
                    trade_id=f"{self.metadata.strategy_id}_{symbol}_{len(self.trade_records) + 1}",
                    strategy_name=self.metadata.strategy_name,
                    strategy_version=self.metadata.strategy_version,
                    symbol=symbol,
                    entry_ts=position.entry_ts,
                    exit_ts=ts,
                    entry_price=position.entry_price,
                    exit_price=float(order.executed.price),
                    quantity=executed_size,
                    gross_pnl=gross_pnl,
                    net_pnl=net_pnl,
                    return_pct=(float(order.executed.price) / position.entry_price - 1.0),
                    fees_paid=position.entry_fee_paid + float(order.executed.comm),
                    bars_held=exit_index - position.entry_bar_index,
                    entry_reason=position.entry_reason,
                    exit_reason=self.exit_reasons_by_ts.get(ts, {}).get(symbol),
                )
            )
            del self.open_positions[symbol]

        def next(self) -> None:
            ts = backtrader_datetime_to_iso(self.datetime.datetime(0))
            cash = float(self.broker.getcash())
            market_value = 0.0
            for data in self.datas:
                position = self.getposition(data)
                if abs(position.size) <= 1e-12:
                    continue
                market_value += float(position.size) * float(data.close[0])

            total_equity = float(self.broker.getvalue())
            self.equity_curve.append(
                EquityPoint(
                    ts=ts,
                    strategy_name=self.metadata.strategy_name,
                    strategy_version=self.metadata.strategy_version,
                    total_equity=total_equity,
                    cash=cash,
                    market_value=market_value,
                    gross_exposure=(market_value / total_equity) if total_equity > 0 else 0.0,
                )
            )

    return PlanDrivenStrategy


def run_backtrader_backtest(
    plan: TargetWeightPlan,
    *,
    metadata: StrategyMetadata,
    settings: BacktestSettings,
) -> BacktestResult:
    backtrader, pandas = import_backtrader_modules()
    feeds = build_backtrader_feeds(plan, pandas=pandas, backtrader=backtrader)
    if not feeds:
        raise ValueError("No market data rows available for backtrader engine.")

    strategy_class = build_backtrader_strategy(backtrader)
    cerebro = backtrader.Cerebro(cheat_on_open=True, stdstats=False)
    cerebro.broker.setcash(settings.initial_capital)
    cerebro.broker.addcommissioninfo(build_fractional_commission_info(backtrader, settings))
    if settings.slippage_rate > 0:
        cerebro.broker.set_slippage_perc(
            settings.slippage_rate,
            slip_open=True,
            slip_limit=True,
            slip_match=True,
            slip_out=False,
        )

    for feed in feeds:
        cerebro.adddata(feed)

    cerebro.addstrategy(
        strategy_class,
        plan=plan,
        metadata=metadata,
        settings=settings,
    )
    strategies = cerebro.run()
    if not strategies:
        raise RuntimeError("backtrader returned no strategy instances.")

    strategy = strategies[0]
    equity_curve = [
        point for point in strategy.equity_curve if point.ts in strategy.in_range_timestamps
    ]
    if not equity_curve:
        equity_curve = list(strategy.equity_curve)
    summary = build_summary_metrics(
        equity_curve,
        strategy.trade_records,
        metadata=metadata,
        settings=settings,
        engine="backtrader",
    )
    return BacktestResult(
        summary=summary,
        trades=list(strategy.trade_records),
        equity_curve=equity_curve,
    )
