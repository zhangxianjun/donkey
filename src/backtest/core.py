from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any

from src.strategies.core import MarketBar, SignalRecord, StrategyMetadata

DEFAULT_INITIAL_CAPITAL = 100000.0
DEFAULT_ORDER_TYPE = "next_bar_open"
DEFAULT_ENGINE = "native"
SUPPORTED_BACKTEST_ENGINES = ("native", "bt", "backtrader")
SUPPORTED_POSITION_SIZING = ("equal_weight_active", "signal_target_weight")


@dataclass(frozen=True)
class BacktestSettings:
    initial_capital: float
    fee_bps: float
    slippage_bps: float
    order_type: str
    position_sizing: str
    max_position_per_symbol: float
    max_total_exposure: float
    max_active_positions: int
    allow_partial_cash: bool
    engine: str
    start_date: str | None
    end_date: str | None
    benchmark_symbol: str | None
    signal_path: str | None
    trades_path: str | None
    equity_path: str | None
    summary_path: str | None

    @property
    def fee_rate(self) -> float:
        return self.fee_bps / 10000.0

    @property
    def slippage_rate(self) -> float:
        return self.slippage_bps / 10000.0

    @property
    def fixed_active_weight(self) -> float:
        slot_cap = self.max_total_exposure / float(self.max_active_positions)
        return min(self.max_position_per_symbol, slot_cap)


@dataclass(frozen=True)
class TradeRecord:
    trade_id: str
    strategy_name: str
    strategy_version: str
    symbol: str
    entry_ts: str
    exit_ts: str
    entry_price: float
    exit_price: float
    quantity: float
    gross_pnl: float
    net_pnl: float
    return_pct: float
    fees_paid: float
    bars_held: int
    entry_reason: str | None = None
    exit_reason: str | None = None

    def to_output_record(self) -> dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "strategy_name": self.strategy_name,
            "strategy_version": self.strategy_version,
            "symbol": self.symbol,
            "entry_ts": self.entry_ts,
            "exit_ts": self.exit_ts,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "quantity": self.quantity,
            "gross_pnl": self.gross_pnl,
            "net_pnl": self.net_pnl,
            "return_pct": self.return_pct,
            "fees_paid": self.fees_paid,
            "bars_held": self.bars_held,
            "entry_reason": self.entry_reason,
            "exit_reason": self.exit_reason,
        }


@dataclass(frozen=True)
class EquityPoint:
    ts: str
    strategy_name: str
    strategy_version: str
    total_equity: float
    cash: float
    market_value: float
    gross_exposure: float

    def to_output_record(self) -> dict[str, Any]:
        return {
            "ts": self.ts,
            "strategy_name": self.strategy_name,
            "strategy_version": self.strategy_version,
            "total_equity": self.total_equity,
            "cash": self.cash,
            "market_value": self.market_value,
            "gross_exposure": self.gross_exposure,
        }


@dataclass(frozen=True)
class BacktestResult:
    summary: dict[str, Any]
    trades: list[TradeRecord]
    equity_curve: list[EquityPoint]


@dataclass(frozen=True)
class MarketTables:
    timestamps: list[str]
    symbols: list[str]
    open_prices: dict[str, dict[str, float]]
    close_prices: dict[str, dict[str, float]]
    bar_index_by_symbol: dict[str, dict[str, int]]


@dataclass(frozen=True)
class TargetWeightPlan:
    tables: MarketTables
    weights_by_ts: dict[str, dict[str, float]]
    rebalance_weights_by_ts: dict[str, dict[str, float]]
    entry_reasons_by_ts: dict[str, dict[str, str | None]]
    exit_reasons_by_ts: dict[str, dict[str, str | None]]


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def iso_to_datetime(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_boundary(value: str | None, *, is_end: bool) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        if parsed.hour == 0 and parsed.minute == 0 and parsed.second == 0 and parsed.microsecond == 0:
            parsed_time = time.max if is_end else time.min
            parsed = datetime.combine(parsed.date(), parsed_time, tzinfo=UTC)
        else:
            parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def resolve_workspace_path(repo_root: Path, raw_path: str | None) -> Path | None:
    if raw_path is None or raw_path.strip() == "":
        return None
    candidate = Path(raw_path).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (repo_root / candidate).resolve()


def resolve_backtest_engine(requested: str | None) -> str:
    if requested is None:
        return DEFAULT_ENGINE
    normalized = requested.strip().lower()
    if normalized not in SUPPORTED_BACKTEST_ENGINES:
        raise ValueError(
            f"Unsupported backtest engine {requested!r}. "
            f"Expected one of: {', '.join(SUPPORTED_BACKTEST_ENGINES)}."
        )
    return normalized


def load_backtest_settings(
    config: dict[str, Any],
    *,
    engine_override: str | None = None,
) -> BacktestSettings:
    execution = config.get("execution", {}) if isinstance(config.get("execution"), dict) else {}
    risk = config.get("risk", {}) if isinstance(config.get("risk"), dict) else {}
    backtest = config.get("backtest", {}) if isinstance(config.get("backtest"), dict) else {}
    artifacts = config.get("artifacts", {}) if isinstance(config.get("artifacts"), dict) else {}

    max_active_positions = int(risk.get("max_active_positions", 1))
    if max_active_positions <= 0:
        raise ValueError("risk.max_active_positions must be > 0")

    settings = BacktestSettings(
        initial_capital=float(backtest.get("initial_capital", DEFAULT_INITIAL_CAPITAL)),
        fee_bps=float(execution.get("fee_bps", 0.0)),
        slippage_bps=float(execution.get("slippage_bps", 0.0)),
        order_type=str(execution.get("order_type", DEFAULT_ORDER_TYPE)),
        position_sizing=str(risk.get("position_sizing", "equal_weight_active")),
        max_position_per_symbol=float(risk.get("max_position_per_symbol", 1.0)),
        max_total_exposure=float(risk.get("max_total_exposure", 1.0)),
        max_active_positions=max_active_positions,
        allow_partial_cash=bool(risk.get("allow_partial_cash", True)),
        engine=resolve_backtest_engine(engine_override or backtest.get("engine")),
        start_date=str(backtest.get("start_date")) if backtest.get("start_date") is not None else None,
        end_date=str(backtest.get("end_date")) if backtest.get("end_date") is not None else None,
        benchmark_symbol=(
            str(backtest.get("benchmark_symbol")) if backtest.get("benchmark_symbol") is not None else None
        ),
        signal_path=str(artifacts.get("signal_path")) if artifacts.get("signal_path") is not None else None,
        trades_path=str(artifacts.get("trades_path")) if artifacts.get("trades_path") is not None else None,
        equity_path=str(artifacts.get("equity_path")) if artifacts.get("equity_path") is not None else None,
        summary_path=str(artifacts.get("summary_path")) if artifacts.get("summary_path") is not None else None,
    )
    validate_backtest_settings(settings)
    return settings


def validate_backtest_settings(settings: BacktestSettings) -> None:
    if settings.initial_capital <= 0:
        raise ValueError("backtest.initial_capital must be > 0.")
    if settings.max_position_per_symbol <= 0:
        raise ValueError("risk.max_position_per_symbol must be > 0.")
    if settings.max_total_exposure <= 0:
        raise ValueError("risk.max_total_exposure must be > 0.")
    if settings.max_position_per_symbol > settings.max_total_exposure:
        raise ValueError("risk.max_position_per_symbol must be <= risk.max_total_exposure.")
    if settings.position_sizing not in SUPPORTED_POSITION_SIZING:
        raise ValueError(
            "Supported risk.position_sizing values are: "
            + ", ".join(SUPPORTED_POSITION_SIZING)
            + "."
        )
    if settings.order_type != DEFAULT_ORDER_TYPE:
        raise ValueError("Only execution.order_type=next_bar_open is currently supported.")


def filter_ts_in_backtest_range(ts: str, settings: BacktestSettings) -> bool:
    dt = iso_to_datetime(ts)
    start_dt = parse_boundary(settings.start_date, is_end=False)
    end_dt = parse_boundary(settings.end_date, is_end=True)
    if start_dt is not None and dt < start_dt:
        return False
    if end_dt is not None and dt > end_dt:
        return False
    return True


def build_market_tables(bars: list[MarketBar]) -> MarketTables:
    open_prices: dict[str, dict[str, float]] = {}
    close_prices: dict[str, dict[str, float]] = {}
    symbols: set[str] = set()
    bar_index_by_symbol: dict[str, dict[str, int]] = {}
    symbol_rows: dict[str, list[MarketBar]] = {}

    for bar in sorted(bars, key=lambda item: (item.ts, item.symbol)):
        symbols.add(bar.symbol)
        open_prices.setdefault(bar.ts, {})[bar.symbol] = bar.open
        close_prices.setdefault(bar.ts, {})[bar.symbol] = bar.close
        symbol_rows.setdefault(bar.symbol, []).append(bar)

    for symbol, rows in symbol_rows.items():
        bar_index_by_symbol[symbol] = {
            row.ts: index for index, row in enumerate(sorted(rows, key=lambda item: item.ts))
        }

    timestamps = sorted(open_prices)
    return MarketTables(
        timestamps=timestamps,
        symbols=sorted(symbols),
        open_prices=open_prices,
        close_prices=close_prices,
        bar_index_by_symbol=bar_index_by_symbol,
    )


def build_target_weight_plan(
    bars: list[MarketBar],
    signals: list[SignalRecord],
    settings: BacktestSettings,
) -> TargetWeightPlan:
    tables = build_market_tables(bars)
    signals_by_symbol: dict[str, list[SignalRecord]] = {}
    bars_by_symbol: dict[str, list[MarketBar]] = {}
    for bar in sorted(bars, key=lambda item: (item.symbol, item.ts)):
        bars_by_symbol.setdefault(bar.symbol, []).append(bar)
    for signal in sorted(signals, key=lambda item: (item.symbol, item.ts)):
        signals_by_symbol.setdefault(signal.symbol, []).append(signal)

    validate_signal_alignment(
        bars_by_symbol=bars_by_symbol,
        signals_by_symbol=signals_by_symbol,
    )

    if settings.position_sizing == "signal_target_weight":
        return build_signal_target_weight_plan(
            tables=tables,
            bars_by_symbol=bars_by_symbol,
            signals_by_symbol=signals_by_symbol,
            settings=settings,
        )

    return build_equal_weight_target_plan(
        tables=tables,
        bars_by_symbol=bars_by_symbol,
        signals_by_symbol=signals_by_symbol,
        settings=settings,
    )


def validate_signal_alignment(
    *,
    bars_by_symbol: dict[str, list[MarketBar]],
    signals_by_symbol: dict[str, list[SignalRecord]],
) -> None:
    for symbol, symbol_signals in signals_by_symbol.items():
        symbol_bars = bars_by_symbol.get(symbol, [])
        if len(symbol_bars) != len(symbol_signals):
            raise ValueError(
                f"Signal count does not match bar count for {symbol}: "
                f"{len(symbol_signals)} vs {len(symbol_bars)}."
            )


def build_equal_weight_target_plan(
    *,
    tables: MarketTables,
    bars_by_symbol: dict[str, list[MarketBar]],
    signals_by_symbol: dict[str, list[SignalRecord]],
    settings: BacktestSettings,
) -> TargetWeightPlan:
    entry_schedule: dict[str, list[str]] = {}
    exit_schedule: dict[str, list[str]] = {}
    entry_reasons: dict[str, dict[str, str | None]] = {}
    exit_reasons: dict[str, dict[str, str | None]] = {}

    for symbol, symbol_signals in signals_by_symbol.items():
        symbol_bars = bars_by_symbol.get(symbol, [])
        for index, signal in enumerate(symbol_signals[:-1]):
            execution_ts = symbol_bars[index + 1].ts
            if not filter_ts_in_backtest_range(execution_ts, settings):
                continue
            if signal.signal_long_exit:
                exit_schedule.setdefault(execution_ts, []).append(symbol)
                exit_reasons.setdefault(execution_ts, {})[symbol] = signal.exit_reason
            if signal.signal_long_entry:
                entry_schedule.setdefault(execution_ts, []).append(symbol)
                entry_reasons.setdefault(execution_ts, {})[symbol] = signal.entry_reason

    active_symbols: list[str] = []
    weights_by_ts: dict[str, dict[str, float]] = {}
    fixed_weight = settings.fixed_active_weight

    for ts in tables.timestamps:
        if not filter_ts_in_backtest_range(ts, settings):
            continue

        for symbol in sorted(exit_schedule.get(ts, [])):
            if symbol in active_symbols:
                active_symbols.remove(symbol)

        slots_left = settings.max_active_positions - len(active_symbols)
        for symbol in sorted(entry_schedule.get(ts, [])):
            if symbol in active_symbols:
                continue
            if slots_left <= 0:
                break
            active_symbols.append(symbol)
            slots_left -= 1

        weights_by_ts[ts] = {
            symbol: (fixed_weight if symbol in active_symbols else 0.0)
            for symbol in tables.symbols
        }

    return TargetWeightPlan(
        tables=tables,
        weights_by_ts=weights_by_ts,
        rebalance_weights_by_ts=build_rebalance_weight_snapshots(
            weights_by_ts=weights_by_ts,
            symbols=tables.symbols,
        ),
        entry_reasons_by_ts=entry_reasons,
        exit_reasons_by_ts=exit_reasons,
    )


def build_signal_target_weight_plan(
    *,
    tables: MarketTables,
    bars_by_symbol: dict[str, list[MarketBar]],
    signals_by_symbol: dict[str, list[SignalRecord]],
    settings: BacktestSettings,
) -> TargetWeightPlan:
    scheduled_weights: dict[str, dict[str, float]] = {}
    entry_reasons: dict[str, dict[str, str | None]] = {}
    exit_reasons: dict[str, dict[str, str | None]] = {}
    previous_targets = {symbol: 0.0 for symbol in tables.symbols}

    for symbol, symbol_signals in signals_by_symbol.items():
        symbol_bars = bars_by_symbol.get(symbol, [])
        previous_target = 0.0
        for index, signal in enumerate(symbol_signals[:-1]):
            execution_ts = symbol_bars[index + 1].ts
            if signal.target_weight is None:
                raise ValueError(
                    "risk.position_sizing=signal_target_weight requires every signal "
                    "record to include target_weight."
                )
            target_weight = clamp_target_weight(float(signal.target_weight), settings=settings)
            if not filter_ts_in_backtest_range(execution_ts, settings):
                previous_target = target_weight
                continue

            scheduled_weights.setdefault(execution_ts, {})[symbol] = target_weight
            if target_weight > previous_target + 1e-12:
                entry_reasons.setdefault(execution_ts, {})[symbol] = signal.entry_reason
            elif target_weight + 1e-12 < previous_target:
                exit_reasons.setdefault(execution_ts, {})[symbol] = signal.exit_reason
            previous_target = target_weight

    weights_by_ts: dict[str, dict[str, float]] = {}

    for ts in tables.timestamps:
        if not filter_ts_in_backtest_range(ts, settings):
            continue

        for symbol, target_weight in scheduled_weights.get(ts, {}).items():
            previous_targets[symbol] = target_weight

        normalized_weights = normalize_signal_target_weights(
            previous_targets,
            settings=settings,
        )
        weights_by_ts[ts] = normalized_weights
        previous_targets = dict(normalized_weights)

    return TargetWeightPlan(
        tables=tables,
        weights_by_ts=weights_by_ts,
        rebalance_weights_by_ts=build_rebalance_weight_snapshots(
            weights_by_ts=weights_by_ts,
            symbols=tables.symbols,
        ),
        entry_reasons_by_ts=entry_reasons,
        exit_reasons_by_ts=exit_reasons,
    )


def clamp_target_weight(value: float, *, settings: BacktestSettings) -> float:
    return min(max(value, 0.0), settings.max_position_per_symbol)


def normalize_signal_target_weights(
    targets: dict[str, float],
    *,
    settings: BacktestSettings,
) -> dict[str, float]:
    clipped = {
        symbol: clamp_target_weight(weight, settings=settings)
        for symbol, weight in targets.items()
    }
    active = [
        (symbol, weight)
        for symbol, weight in clipped.items()
        if weight > 1e-12
    ]
    if len(active) > settings.max_active_positions:
        allowed = {
            symbol
            for symbol, _ in sorted(active, key=lambda item: (-item[1], item[0]))[
                : settings.max_active_positions
            ]
        }
        clipped = {
            symbol: (weight if symbol in allowed else 0.0)
            for symbol, weight in clipped.items()
        }

    total_weight = sum(clipped.values())
    if total_weight > settings.max_total_exposure and total_weight > 0:
        scale = settings.max_total_exposure / total_weight
        clipped = {
            symbol: weight * scale
            for symbol, weight in clipped.items()
        }

    return clipped


def build_rebalance_weight_snapshots(
    *,
    weights_by_ts: dict[str, dict[str, float]],
    symbols: list[str],
) -> dict[str, dict[str, float]]:
    snapshots: dict[str, dict[str, float]] = {}
    previous = {symbol: 0.0 for symbol in symbols}

    for ts in sorted(weights_by_ts):
        current = {
            symbol: float(weights_by_ts[ts].get(symbol, 0.0))
            for symbol in symbols
        }
        if not weights_changed(previous, current, symbols=symbols):
            continue
        snapshots[ts] = current
        previous = current

    return snapshots


def weights_changed(
    previous: dict[str, float],
    current: dict[str, float],
    *,
    symbols: list[str],
) -> bool:
    for symbol in symbols:
        if abs(float(previous.get(symbol, 0.0)) - float(current.get(symbol, 0.0))) > 1e-12:
            return True
    return False


def build_summary_metrics(
    equity_curve: list[EquityPoint],
    trades: list[TradeRecord],
    *,
    metadata: StrategyMetadata,
    settings: BacktestSettings,
    engine: str,
) -> dict[str, Any]:
    if not equity_curve:
        final_equity = settings.initial_capital
        total_return = 0.0
        cagr = 0.0
        max_drawdown = 0.0
        sharpe = 0.0
    else:
        equity_values = [point.total_equity for point in equity_curve]
        final_equity = equity_values[-1]
        total_return = final_equity / settings.initial_capital - 1.0
        cagr = compute_cagr(
            start_ts=equity_curve[0].ts,
            end_ts=equity_curve[-1].ts,
            start_equity=settings.initial_capital,
            end_equity=final_equity,
        )
        max_drawdown = compute_max_drawdown(equity_values)
        sharpe = compute_sharpe(
            equity_values,
            interval=infer_interval_from_equity(equity_curve),
        )

    wins = sum(1 for trade in trades if trade.net_pnl > 0)
    losses = sum(1 for trade in trades if trade.net_pnl < 0)
    gross_profit = sum(max(trade.net_pnl, 0.0) for trade in trades)
    gross_loss = abs(sum(min(trade.net_pnl, 0.0) for trade in trades))
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else None
    win_rate = wins / len(trades) if trades else 0.0

    return {
        "generated_at": utc_now_iso(),
        "engine": engine,
        "strategy_name": metadata.strategy_name,
        "strategy_version": metadata.strategy_version,
        "initial_capital": settings.initial_capital,
        "final_equity": final_equity,
        "total_return": total_return,
        "cagr": cagr,
        "max_drawdown": max_drawdown,
        "sharpe": sharpe,
        "win_rate": win_rate,
        "trade_count": len(trades),
        "winning_trade_count": wins,
        "losing_trade_count": losses,
        "profit_factor": profit_factor,
        "benchmark_symbol": settings.benchmark_symbol,
        "start_date": settings.start_date,
        "end_date": settings.end_date,
    }


def compute_cagr(
    *,
    start_ts: str,
    end_ts: str,
    start_equity: float,
    end_equity: float,
) -> float:
    if start_equity <= 0 or end_equity <= 0:
        return -1.0
    start_dt = iso_to_datetime(start_ts)
    end_dt = iso_to_datetime(end_ts)
    years = max((end_dt - start_dt).total_seconds() / (365.0 * 24.0 * 3600.0), 1.0 / 365.0)
    return (end_equity / start_equity) ** (1.0 / years) - 1.0


def compute_max_drawdown(equity_values: list[float]) -> float:
    peak = equity_values[0]
    max_drawdown = 0.0
    for value in equity_values:
        peak = max(peak, value)
        drawdown = value / peak - 1.0 if peak > 0 else 0.0
        max_drawdown = min(max_drawdown, drawdown)
    return max_drawdown


def infer_interval_from_equity(equity_curve: list[EquityPoint]) -> str:
    if len(equity_curve) < 2:
        return "1d"
    delta = iso_to_datetime(equity_curve[1].ts) - iso_to_datetime(equity_curve[0].ts)
    minutes = int(delta.total_seconds() // 60)
    if minutes <= 5:
        return "5m"
    if minutes <= 15:
        return "15m"
    if minutes <= 60:
        return "1h"
    if minutes <= 240:
        return "4h"
    return "1d"


def periods_per_year(interval: str) -> int:
    mapping = {
        "1m": 60 * 24 * 365,
        "5m": 12 * 24 * 365,
        "15m": 4 * 24 * 365,
        "1h": 24 * 365,
        "4h": 6 * 365,
        "1d": 365,
    }
    return mapping.get(interval, 365)


def compute_sharpe(equity_values: list[float], *, interval: str) -> float:
    if len(equity_values) < 2:
        return 0.0
    returns: list[float] = []
    for previous, current in zip(equity_values[:-1], equity_values[1:]):
        if previous <= 0:
            continue
        returns.append(current / previous - 1.0)
    if len(returns) < 2:
        return 0.0
    mean = sum(returns) / len(returns)
    variance = sum((item - mean) ** 2 for item in returns) / (len(returns) - 1)
    std = variance ** 0.5
    if std == 0:
        return 0.0
    return (mean / std) * (periods_per_year(interval) ** 0.5)


def ensure_pyarrow_available() -> None:
    try:
        import pyarrow  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Parquet output requires pyarrow. Install with `python3 -m pip install pyarrow`."
        ) from exc


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")


def write_parquet(path: Path, records: list[dict[str, Any]]) -> None:
    ensure_pyarrow_available()

    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records)
    pq.write_table(table, path)


def write_records(path: Path, records: list[dict[str, Any]]) -> None:
    if path.suffix == ".jsonl":
        write_jsonl(path, records)
        return
    if path.suffix == ".parquet":
        write_parquet(path, records)
        return
    raise ValueError(f"Unsupported artifact suffix: {path.suffix}")


def write_summary_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
