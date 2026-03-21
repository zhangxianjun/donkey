from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.strategies.core import MarketBar, SignalRecord, StrategyMetadata

DEFAULT_ATR_WINDOW = 14
DEFAULT_BREAKOUT_WINDOW = 20
DEFAULT_ATR_MULTIPLIER = 3.0


@dataclass(frozen=True)
class ATRParameters:
    atr_window: int
    breakout_window: int
    atr_multiplier: float
    warmup_bars: int


def parse_positive_int(value: Any, *, field_name: str, default: int) -> int:
    if value is None:
        return default
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return parsed


def parse_positive_float(value: Any, *, field_name: str, default: float) -> float:
    if value is None:
        return default
    parsed = float(value)
    if parsed <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return parsed


class ATRStrategy:
    def __init__(
        self,
        config: dict[str, Any],
        metadata: StrategyMetadata | None = None,
    ) -> None:
        signal = config.get("signal", {}) if isinstance(config.get("signal"), dict) else {}
        self.metadata = metadata or StrategyMetadata(
            strategy_name=str(config.get("strategy_name", "atr_strategy")),
            strategy_version=str(config.get("strategy_version", "v1")),
            description=str(config.get("description", "")),
            config_path="",
            module_path=__file__,
        )
        atr_window = parse_positive_int(
            signal.get("atr_window"),
            field_name="signal.atr_window",
            default=DEFAULT_ATR_WINDOW,
        )
        breakout_window = parse_positive_int(
            signal.get("breakout_window"),
            field_name="signal.breakout_window",
            default=DEFAULT_BREAKOUT_WINDOW,
        )
        warmup_default = max(atr_window, breakout_window + 1)
        self.params = ATRParameters(
            atr_window=atr_window,
            breakout_window=breakout_window,
            atr_multiplier=parse_positive_float(
                signal.get("atr_multiplier"),
                field_name="signal.atr_multiplier",
                default=DEFAULT_ATR_MULTIPLIER,
            ),
            warmup_bars=parse_positive_int(
                signal.get("warmup_bars"),
                field_name="signal.warmup_bars",
                default=warmup_default,
            ),
        )

    def generate_signals(self, bars: list[MarketBar]) -> list[SignalRecord]:
        grouped: dict[str, list[MarketBar]] = {}
        for bar in sorted(bars, key=lambda item: (item.symbol, item.ts)):
            grouped.setdefault(bar.symbol, []).append(bar)

        signals: list[SignalRecord] = []
        for symbol in sorted(grouped):
            signals.extend(self._generate_symbol_signals(grouped[symbol]))
        return signals

    def _generate_symbol_signals(self, bars: list[MarketBar]) -> list[SignalRecord]:
        true_ranges = compute_true_ranges(bars)
        atr_values = rolling_average(true_ranges, window=self.params.atr_window)
        signals: list[SignalRecord] = []
        in_position = False
        trailing_stop: float | None = None

        for index, bar in enumerate(bars):
            atr = atr_values[index]
            entry_signal = 0
            exit_signal = 0
            entry_reason: str | None = None
            exit_reason: str | None = None

            if in_position and trailing_stop is not None and bar.close < trailing_stop:
                in_position = False
                exit_signal = 1
                exit_reason = "close_below_atr_trailing_stop"
                trailing_stop = None

            if (
                not in_position
                and index + 1 >= self.params.warmup_bars
                and atr is not None
                and breakout_reference_high(bars, index=index, lookback=self.params.breakout_window)
                is not None
            ):
                breakout_high = breakout_reference_high(
                    bars,
                    index=index,
                    lookback=self.params.breakout_window,
                )
                assert breakout_high is not None
                if bar.close > breakout_high:
                    in_position = True
                    entry_signal = 1
                    entry_reason = "close_breakout_above_prev_high"
                    trailing_stop = bar.close - self.params.atr_multiplier * atr

            if in_position and atr is not None:
                candidate_stop = bar.close - self.params.atr_multiplier * atr
                trailing_stop = (
                    candidate_stop if trailing_stop is None else max(trailing_stop, candidate_stop)
                )

            signals.append(
                SignalRecord(
                    ts=bar.ts,
                    symbol=bar.symbol,
                    strategy_name=self.metadata.strategy_name,
                    strategy_version=self.metadata.strategy_version,
                    interval=bar.interval,
                    signal_long_entry=entry_signal,
                    signal_long_exit=exit_signal,
                    position=1 if in_position else 0,
                    close=bar.close,
                    target_weight=1.0 if in_position else 0.0,
                    atr=atr,
                    atr_stop=trailing_stop if in_position else None,
                    entry_reason=entry_reason,
                    exit_reason=exit_reason,
                    exchange=bar.exchange,
                    market_type=bar.market_type,
                )
            )

        return signals


def compute_true_ranges(bars: list[MarketBar]) -> list[float]:
    true_ranges: list[float] = []
    previous_close: float | None = None
    for bar in bars:
        if previous_close is None:
            true_range = bar.high - bar.low
        else:
            true_range = max(
                bar.high - bar.low,
                abs(bar.high - previous_close),
                abs(bar.low - previous_close),
            )
        true_ranges.append(true_range)
        previous_close = bar.close
    return true_ranges


def rolling_average(values: list[float], *, window: int) -> list[float | None]:
    averages: list[float | None] = [None] * len(values)
    rolling_sum = 0.0
    for index, value in enumerate(values):
        rolling_sum += value
        if index >= window:
            rolling_sum -= values[index - window]
        if index + 1 >= window:
            averages[index] = rolling_sum / window
    return averages


def breakout_reference_high(
    bars: list[MarketBar],
    *,
    index: int,
    lookback: int,
) -> float | None:
    if index < lookback:
        return None
    return max(bar.high for bar in bars[index - lookback : index])


def build_strategy(
    config: dict[str, Any],
    metadata: StrategyMetadata | None = None,
) -> ATRStrategy:
    return ATRStrategy(config=config, metadata=metadata)
