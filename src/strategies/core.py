from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class StrategyMetadata:
    strategy_name: str
    strategy_version: str
    description: str
    config_path: str
    module_path: str

    @property
    def strategy_id(self) -> str:
        return f"{self.strategy_name}_{self.strategy_version}"


@dataclass(frozen=True)
class MarketBar:
    ts: str
    symbol: str
    interval: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    exchange: str | None = None
    market_type: str | None = None

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "MarketBar":
        required_fields = ("ts", "symbol", "interval", "open", "high", "low", "close", "volume")
        for field_name in required_fields:
            if field_name not in record:
                raise ValueError(f"Missing {field_name!r} in market bar record.")

        return cls(
            ts=str(record["ts"]),
            symbol=str(record["symbol"]),
            interval=str(record["interval"]),
            open=float(record["open"]),
            high=float(record["high"]),
            low=float(record["low"]),
            close=float(record["close"]),
            volume=float(record["volume"]),
            exchange=str(record["exchange"]) if record.get("exchange") is not None else None,
            market_type=(
                str(record["market_type"]) if record.get("market_type") is not None else None
            ),
        )


@dataclass(frozen=True)
class SignalRecord:
    ts: str
    symbol: str
    strategy_name: str
    strategy_version: str
    interval: str
    signal_long_entry: int
    signal_long_exit: int
    position: int
    close: float
    atr: float | None = None
    atr_stop: float | None = None
    entry_reason: str | None = None
    exit_reason: str | None = None
    exchange: str | None = None
    market_type: str | None = None

    def to_output_record(self) -> dict[str, Any]:
        return {
            "ts": self.ts,
            "symbol": self.symbol,
            "strategy_name": self.strategy_name,
            "strategy_version": self.strategy_version,
            "interval": self.interval,
            "signal_long_entry": self.signal_long_entry,
            "signal_long_exit": self.signal_long_exit,
            "position": self.position,
            "close": self.close,
            "atr": self.atr,
            "atr_stop": self.atr_stop,
            "entry_reason": self.entry_reason,
            "exit_reason": self.exit_reason,
            "exchange": self.exchange,
            "market_type": self.market_type,
        }


class Strategy(Protocol):
    def generate_signals(self, bars: list[MarketBar]) -> list[SignalRecord]:
        ...
