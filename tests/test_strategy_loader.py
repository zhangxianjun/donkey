from __future__ import annotations

import tempfile
import textwrap
import time
import unittest
from pathlib import Path

from src.strategies.core import MarketBar
from src.strategies.loader import ReloadableStrategyLoader


class StrategyLoaderTests(unittest.TestCase):
    @staticmethod
    def make_bars() -> list[MarketBar]:
        closes = [
            100.0,
            101.0,
            102.0,
            103.0,
            104.0,
            105.0,
            106.0,
            107.0,
            108.0,
            109.0,
            110.0,
            111.0,
            112.0,
            113.0,
            114.0,
            115.0,
            116.0,
            117.0,
            118.0,
            130.0,
            126.0,
            122.0,
            118.0,
            112.0,
            105.0,
        ]
        bars: list[MarketBar] = []
        for index, close in enumerate(closes, start=1):
            bars.append(
                MarketBar(
                    ts=f"2026-03-{index:02d}T00:00:00Z",
                    symbol="BTCUSDT",
                    interval="1d",
                    open=close - 1.0,
                    high=close + 1.0,
                    low=close - 2.0,
                    close=close,
                    volume=1000.0 + index,
                    exchange="binance",
                    market_type="spot",
                )
            )
        return bars

    def test_default_atr_strategy_generates_entry_and_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)
            config_path = workspace / "atr.yaml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    strategy_name: atr_demo
                    strategy_version: v1
                    signal:
                      atr_window: 3
                      breakout_window: 5
                      atr_multiplier: 1.5
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            loader = ReloadableStrategyLoader(config_path, repo_root=workspace)
            strategy = loader.get_strategy()
            signals = strategy.generate_signals(self.make_bars())

        self.assertEqual(len(signals), 25)
        self.assertEqual(sum(item.signal_long_entry for item in signals), 1)
        self.assertEqual(sum(item.signal_long_exit for item in signals), 1)
        self.assertTrue(any(item.position == 1 for item in signals))

    def test_loader_reloads_python_module_after_file_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)
            module_path = workspace / "custom_strategy.py"
            config_path = workspace / "custom.yaml"

            config_path.write_text(
                textwrap.dedent(
                    f"""
                    strategy_name: custom
                    strategy_version: v1
                    module:
                      path: {module_path}
                      factory_name: build_strategy
                      reload_on_change: true
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            self.write_custom_strategy(module_path, entry_reason="v1")
            loader = ReloadableStrategyLoader(config_path, repo_root=workspace)
            strategy_v1 = loader.get_strategy()
            signals_v1 = strategy_v1.generate_signals(self.make_bars()[:1])

            time.sleep(0.02)
            self.write_custom_strategy(module_path, entry_reason="v2")
            reloaded = loader.refresh()
            strategy_v2 = loader.get_strategy()
            signals_v2 = strategy_v2.generate_signals(self.make_bars()[:1])

        self.assertTrue(reloaded)
        self.assertEqual(signals_v1[0].entry_reason, "v1")
        self.assertEqual(signals_v2[0].entry_reason, "v2")

    @staticmethod
    def write_custom_strategy(module_path: Path, *, entry_reason: str) -> None:
        module_path.write_text(
            textwrap.dedent(
                f"""
                from src.strategies.core import SignalRecord


                class DemoStrategy:
                    def __init__(self, config, metadata):
                        self.metadata = metadata

                    def generate_signals(self, bars):
                        return [
                            SignalRecord(
                                ts=bars[0].ts,
                                symbol=bars[0].symbol,
                                strategy_name=self.metadata.strategy_name,
                                strategy_version=self.metadata.strategy_version,
                                interval=bars[0].interval,
                                signal_long_entry=1,
                                signal_long_exit=0,
                                position=1,
                                close=bars[0].close,
                                entry_reason="{entry_reason}",
                            )
                        ]


                def build_strategy(config, metadata):
                    return DemoStrategy(config, metadata)
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
