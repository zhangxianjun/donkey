from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import textwrap
import unittest
from pathlib import Path

from src.backtest.backtrader_adapter import run_backtrader_backtest
from src.backtest.bt_adapter import run_bt_backtest
from src.backtest.core import build_target_weight_plan, load_backtest_settings
from src.backtest.run import execute_backtest, main as backtest_main
from src.strategies.loader import ReloadableStrategyLoader
from src.strategies.run import read_market_bars

HAS_BT = importlib.util.find_spec("bt") is not None
HAS_BACKTRADER = importlib.util.find_spec("backtrader") is not None
HAS_PANDAS = importlib.util.find_spec("pandas") is not None


class BacktestRunnerTests(unittest.TestCase):
    def test_native_backtest_runner_writes_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)
            input_path = workspace / "market.jsonl"
            module_path = workspace / "demo_strategy.py"
            signal_path = workspace / "signals.jsonl"
            trades_path = workspace / "trades.jsonl"
            equity_path = workspace / "equity.jsonl"
            summary_path = workspace / "summary.json"
            config_path = workspace / "demo_strategy.yaml"

            self.write_market_data(input_path)
            self.write_strategy_module(module_path)
            self.write_strategy_config(
                config_path,
                module_path=module_path,
                signal_path=signal_path,
                trades_path=trades_path,
                equity_path=equity_path,
                summary_path=summary_path,
            )

            exit_code = backtest_main(
                [
                    "--strategy",
                    str(config_path),
                    "--input",
                    str(input_path),
                ]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue(signal_path.exists())
            self.assertTrue(trades_path.exists())
            self.assertTrue(equity_path.exists())
            self.assertTrue(summary_path.exists())

            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            trades = [
                json.loads(line)
                for line in trades_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            equity = [
                json.loads(line)
                for line in equity_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

            self.assertEqual(summary["engine"], "native")
            self.assertEqual(summary["trade_count"], 1)
            self.assertGreater(summary["total_return"], 0.0)
            self.assertEqual(len(trades), 1)
            self.assertEqual(trades[0]["entry_reason"], "test_entry")
            self.assertEqual(trades[0]["exit_reason"], "test_exit")
            self.assertGreater(len(equity), 0)

    def test_bt_engine_dependency_or_execution_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)
            input_path = workspace / "market.jsonl"
            module_path = workspace / "demo_strategy.py"
            config_path = workspace / "demo_strategy.yaml"

            self.write_market_data(input_path)
            self.write_strategy_module(module_path)
            self.write_strategy_config(
                config_path,
                module_path=module_path,
                signal_path=workspace / "signals.jsonl",
                trades_path=workspace / "trades.jsonl",
                equity_path=workspace / "equity.jsonl",
                summary_path=workspace / "summary.json",
            )

            loader = ReloadableStrategyLoader(config_path)
            bars = read_market_bars(input_path, symbols=None)
            strategy = loader.get_strategy()
            signals = strategy.generate_signals(bars)
            settings = load_backtest_settings(loader.definition.config, engine_override="bt")
            plan = build_target_weight_plan(bars, signals, settings)

            if not (HAS_BT and HAS_PANDAS):
                with self.assertRaises(RuntimeError):
                    run_bt_backtest(
                        plan,
                        metadata=loader.definition.metadata,
                        settings=settings,
                    )
                return

            previous_xdg_cache_home = os.environ.get("XDG_CACHE_HOME")
            previous_mplconfigdir = os.environ.get("MPLCONFIGDIR")
            cache_root = workspace / ".cache"
            matplotlib_root = cache_root / "matplotlib"
            cache_root.mkdir(parents=True)
            matplotlib_root.mkdir(parents=True)
            os.environ["XDG_CACHE_HOME"] = str(cache_root)
            os.environ["MPLCONFIGDIR"] = str(matplotlib_root)
            try:
                result = run_bt_backtest(
                    plan,
                    metadata=loader.definition.metadata,
                    settings=settings,
                )
            finally:
                if previous_xdg_cache_home is None:
                    os.environ.pop("XDG_CACHE_HOME", None)
                else:
                    os.environ["XDG_CACHE_HOME"] = previous_xdg_cache_home
                if previous_mplconfigdir is None:
                    os.environ.pop("MPLCONFIGDIR", None)
                else:
                    os.environ["MPLCONFIGDIR"] = previous_mplconfigdir

            self.assertEqual(result.summary["engine"], "bt")
            self.assertEqual(result.summary["trade_count"], 1)
            self.assertEqual(len(result.trades), 1)
            self.assertGreater(len(result.equity_curve), 0)

    def test_backtrader_engine_dependency_or_execution_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)
            input_path = workspace / "market.jsonl"
            module_path = workspace / "demo_strategy.py"
            config_path = workspace / "demo_strategy.yaml"

            self.write_market_data(input_path)
            self.write_strategy_module(module_path)
            self.write_strategy_config(
                config_path,
                module_path=module_path,
                signal_path=workspace / "signals.jsonl",
                trades_path=workspace / "trades.jsonl",
                equity_path=workspace / "equity.jsonl",
                summary_path=workspace / "summary.json",
            )

            loader = ReloadableStrategyLoader(config_path)
            bars = read_market_bars(input_path, symbols=None)
            strategy = loader.get_strategy()
            signals = strategy.generate_signals(bars)
            settings = load_backtest_settings(loader.definition.config, engine_override="backtrader")
            plan = build_target_weight_plan(bars, signals, settings)

            if not (HAS_BACKTRADER and HAS_PANDAS):
                with self.assertRaises(RuntimeError):
                    run_backtrader_backtest(
                        plan,
                        metadata=loader.definition.metadata,
                        settings=settings,
                    )
                return

            result = run_backtrader_backtest(
                plan,
                metadata=loader.definition.metadata,
                settings=settings,
            )

            self.assertEqual(result.summary["engine"], "backtrader")
            self.assertEqual(result.summary["trade_count"], 1)
            self.assertEqual(len(result.trades), 1)
            self.assertGreater(len(result.equity_curve), 0)

    def test_signal_target_weight_plan_supports_staged_weights(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)
            input_path = workspace / "market.jsonl"
            module_path = workspace / "weighted_strategy.py"
            config_path = workspace / "weighted_strategy.yaml"
            signal_path = workspace / "weighted_signals.jsonl"
            trades_path = workspace / "weighted_trades.jsonl"
            equity_path = workspace / "weighted_equity.jsonl"
            summary_path = workspace / "weighted_summary.json"

            self.write_staged_market_data(input_path)
            self.write_weighted_strategy_module(module_path)
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    strategy_name: weighted_demo
                    strategy_version: v1
                    module:
                      path: {module_path}
                      factory_name: build_strategy
                      reload_on_change: true
                    universe:
                      symbols:
                        - BTCUSDT
                      interval: 1d
                    execution:
                      order_type: next_bar_open
                      slippage_bps: 0
                      fee_bps: 0
                    risk:
                      position_sizing: signal_target_weight
                      max_position_per_symbol: 1.0
                      max_total_exposure: 1.0
                      max_active_positions: 1
                      allow_partial_cash: true
                    backtest:
                      engine: native
                      initial_capital: 1000
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            loader = ReloadableStrategyLoader(config_path)
            bars = read_market_bars(input_path, symbols=None)
            signals = loader.get_strategy().generate_signals(bars)
            settings = load_backtest_settings(loader.definition.config)
            plan = build_target_weight_plan(bars, signals, settings)
            execution = execute_backtest(
                strategy_path=config_path,
                input_path=input_path,
                skip_signal_write=False,
                repo_root=workspace,
                signal_path_override=str(signal_path),
                trades_path_override=str(trades_path),
                equity_path_override=str(equity_path),
                summary_path_override=str(summary_path),
            )

            self.assertAlmostEqual(plan.weights_by_ts["2026-03-02T00:00:00Z"]["BTCUSDT"], 0.2)
            self.assertAlmostEqual(plan.weights_by_ts["2026-03-03T00:00:00Z"]["BTCUSDT"], 0.5)
            self.assertAlmostEqual(plan.weights_by_ts["2026-03-04T00:00:00Z"]["BTCUSDT"], 0.2)
            self.assertAlmostEqual(plan.weights_by_ts["2026-03-05T00:00:00Z"]["BTCUSDT"], 0.0)
            self.assertEqual(execution.result.summary["trade_count"], 2)
            self.assertTrue(signal_path.exists())
            self.assertTrue(trades_path.exists())
            self.assertTrue(equity_path.exists())
            self.assertTrue(summary_path.exists())

    def test_signal_target_weight_only_rebalances_on_weight_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)
            input_path = workspace / "market.jsonl"
            module_path = workspace / "stable_weight_strategy.py"
            config_path = workspace / "stable_weight_strategy.yaml"

            self.write_staged_market_data(input_path)
            module_path.write_text(
                textwrap.dedent(
                    """
                    from src.strategies.core import SignalRecord


                    class StableWeightStrategy:
                        def __init__(self, config, metadata):
                            self.metadata = metadata

                        def generate_signals(self, bars):
                            targets = [0.2, 0.2, 0.2, 0.0, 0.0]
                            previous_target = 0.0
                            signals = []
                            for index, bar in enumerate(bars):
                                target_weight = targets[index]
                                signals.append(
                                    SignalRecord(
                                        ts=bar.ts,
                                        symbol=bar.symbol,
                                        strategy_name=self.metadata.strategy_name,
                                        strategy_version=self.metadata.strategy_version,
                                        interval=bar.interval,
                                        signal_long_entry=1 if target_weight > previous_target else 0,
                                        signal_long_exit=1 if target_weight < previous_target else 0,
                                        position=1 if target_weight > 0 else 0,
                                        close=bar.close,
                                        target_weight=target_weight,
                                        entry_reason=(
                                            "stable_entry" if target_weight > previous_target else None
                                        ),
                                        exit_reason=(
                                            "stable_exit" if target_weight < previous_target else None
                                        ),
                                    )
                                )
                                previous_target = target_weight
                            return signals


                    def build_strategy(config, metadata):
                        return StableWeightStrategy(config, metadata)
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )
            config_path.write_text(
                textwrap.dedent(
                    f"""
                    strategy_name: stable_weight_demo
                    strategy_version: v1
                    module:
                      path: {module_path}
                      factory_name: build_strategy
                      reload_on_change: true
                    universe:
                      symbols:
                        - BTCUSDT
                      interval: 1d
                    execution:
                      order_type: next_bar_open
                      slippage_bps: 0
                      fee_bps: 0
                    risk:
                      position_sizing: signal_target_weight
                      max_position_per_symbol: 1.0
                      max_total_exposure: 1.0
                      max_active_positions: 1
                      allow_partial_cash: true
                    backtest:
                      engine: native
                      initial_capital: 1000
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            loader = ReloadableStrategyLoader(config_path)
            bars = read_market_bars(input_path, symbols=None)
            signals = loader.get_strategy().generate_signals(bars)
            settings = load_backtest_settings(loader.definition.config)
            plan = build_target_weight_plan(bars, signals, settings)
            execution = execute_backtest(
                strategy_path=config_path,
                input_path=input_path,
                skip_signal_write=True,
                repo_root=workspace,
            )

        self.assertEqual(sorted(plan.rebalance_weights_by_ts), ["2026-03-02T00:00:00Z", "2026-03-05T00:00:00Z"])
        self.assertEqual(execution.result.summary["trade_count"], 1)
        self.assertEqual(len(execution.result.trades), 1)

    @staticmethod
    def write_market_data(path: Path) -> None:
        rows = [
            {
                "ts": "2026-03-01T00:00:00Z",
                "symbol": "BTCUSDT",
                "interval": "1d",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1000.0,
                "exchange": "binance",
                "market_type": "spot",
            },
            {
                "ts": "2026-03-02T00:00:00Z",
                "symbol": "BTCUSDT",
                "interval": "1d",
                "open": 110.0,
                "high": 112.0,
                "low": 109.0,
                "close": 111.0,
                "volume": 1000.0,
                "exchange": "binance",
                "market_type": "spot",
            },
            {
                "ts": "2026-03-03T00:00:00Z",
                "symbol": "BTCUSDT",
                "interval": "1d",
                "open": 120.0,
                "high": 122.0,
                "low": 119.0,
                "close": 121.0,
                "volume": 1000.0,
                "exchange": "binance",
                "market_type": "spot",
            },
            {
                "ts": "2026-03-04T00:00:00Z",
                "symbol": "BTCUSDT",
                "interval": "1d",
                "open": 130.0,
                "high": 131.0,
                "low": 128.0,
                "close": 129.0,
                "volume": 1000.0,
                "exchange": "binance",
                "market_type": "spot",
            },
        ]
        path.write_text(
            "".join(json.dumps(row, ensure_ascii=True) + "\n" for row in rows),
            encoding="utf-8",
        )

    @staticmethod
    def write_strategy_module(path: Path) -> None:
        path.write_text(
            textwrap.dedent(
                """
                from src.strategies.core import SignalRecord


                class DemoStrategy:
                    def __init__(self, config, metadata):
                        self.metadata = metadata

                    def generate_signals(self, bars):
                        signals = []
                        for index, bar in enumerate(bars):
                            signals.append(
                                SignalRecord(
                                    ts=bar.ts,
                                    symbol=bar.symbol,
                                    strategy_name=self.metadata.strategy_name,
                                    strategy_version=self.metadata.strategy_version,
                                    interval=bar.interval,
                                    signal_long_entry=1 if index == 0 else 0,
                                    signal_long_exit=1 if index == 2 else 0,
                                    position=1 if index < 3 else 0,
                                    close=bar.close,
                                    target_weight=1.0 if index < 3 else 0.0,
                                    entry_reason="test_entry" if index == 0 else None,
                                    exit_reason="test_exit" if index == 2 else None,
                                    exchange=bar.exchange,
                                    market_type=bar.market_type,
                                )
                            )
                        return signals


                def build_strategy(config, metadata):
                    return DemoStrategy(config, metadata)
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def write_staged_market_data(path: Path) -> None:
        rows = [
            {
                "ts": "2026-03-01T00:00:00Z",
                "symbol": "BTCUSDT",
                "interval": "1d",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 1000.0,
            },
            {
                "ts": "2026-03-02T00:00:00Z",
                "symbol": "BTCUSDT",
                "interval": "1d",
                "open": 110.0,
                "high": 111.0,
                "low": 109.0,
                "close": 110.0,
                "volume": 1000.0,
            },
            {
                "ts": "2026-03-03T00:00:00Z",
                "symbol": "BTCUSDT",
                "interval": "1d",
                "open": 120.0,
                "high": 121.0,
                "low": 119.0,
                "close": 120.0,
                "volume": 1000.0,
            },
            {
                "ts": "2026-03-04T00:00:00Z",
                "symbol": "BTCUSDT",
                "interval": "1d",
                "open": 130.0,
                "high": 131.0,
                "low": 129.0,
                "close": 130.0,
                "volume": 1000.0,
            },
            {
                "ts": "2026-03-05T00:00:00Z",
                "symbol": "BTCUSDT",
                "interval": "1d",
                "open": 140.0,
                "high": 141.0,
                "low": 139.0,
                "close": 140.0,
                "volume": 1000.0,
            },
        ]
        path.write_text(
            "".join(json.dumps(row, ensure_ascii=True) + "\n" for row in rows),
            encoding="utf-8",
        )

    @staticmethod
    def write_weighted_strategy_module(path: Path) -> None:
        path.write_text(
            textwrap.dedent(
                """
                from src.strategies.core import SignalRecord


                class WeightedDemoStrategy:
                    def __init__(self, config, metadata):
                        self.metadata = metadata

                    def generate_signals(self, bars):
                        targets = [0.2, 0.5, 0.2, 0.0, 0.0]
                        previous_target = 0.0
                        signals = []
                        for index, bar in enumerate(bars):
                            target_weight = targets[index]
                            signals.append(
                                SignalRecord(
                                    ts=bar.ts,
                                    symbol=bar.symbol,
                                    strategy_name=self.metadata.strategy_name,
                                    strategy_version=self.metadata.strategy_version,
                                    interval=bar.interval,
                                    signal_long_entry=1 if target_weight > previous_target else 0,
                                    signal_long_exit=1 if target_weight < previous_target else 0,
                                    position=1 if target_weight > 0 else 0,
                                    close=bar.close,
                                    target_weight=target_weight,
                                    entry_reason=(
                                        "weighted_entry" if target_weight > previous_target else None
                                    ),
                                    exit_reason=(
                                        "weighted_exit" if target_weight < previous_target else None
                                    ),
                                )
                            )
                            previous_target = target_weight
                        return signals


                def build_strategy(config, metadata):
                    return WeightedDemoStrategy(config, metadata)
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def write_strategy_config(
        path: Path,
        *,
        module_path: Path,
        signal_path: Path,
        trades_path: Path,
        equity_path: Path,
        summary_path: Path,
    ) -> None:
        path.write_text(
            textwrap.dedent(
                f"""
                strategy_name: demo_backtest
                strategy_version: v1
                module:
                  path: {module_path}
                  factory_name: build_strategy
                  reload_on_change: true
                universe:
                  symbols:
                    - BTCUSDT
                  interval: 1d
                execution:
                  order_type: next_bar_open
                  slippage_bps: 0
                  fee_bps: 0
                risk:
                  position_sizing: equal_weight_active
                  max_position_per_symbol: 1.0
                  max_total_exposure: 1.0
                  max_active_positions: 1
                  allow_partial_cash: true
                backtest:
                  engine: native
                  initial_capital: 1000.0
                  start_date: "2026-03-01"
                  end_date: "2026-03-04"
                artifacts:
                  signal_path: {signal_path}
                  trades_path: {trades_path}
                  equity_path: {equity_path}
                  summary_path: {summary_path}
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main()
