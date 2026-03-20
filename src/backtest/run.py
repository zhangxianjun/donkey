from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.strategies.loader import ReloadableStrategyLoader
from src.strategies.run import (
    default_output_template,
    read_market_bars,
    resolve_output_format,
    write_signal_outputs,
)

from .bt_adapter import run_bt_backtest
from .core import (
    BacktestResult,
    SUPPORTED_BACKTEST_ENGINES,
    build_target_weight_plan,
    load_backtest_settings,
    resolve_workspace_path,
    write_records,
    write_summary_json,
)
from .native import run_native_backtest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a strategy backtest from normalized market data."
    )
    parser.add_argument(
        "--strategy",
        required=True,
        help="Strategy yaml path.",
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Normalized market data file (.jsonl or .parquet).",
    )
    parser.add_argument(
        "--engine",
        choices=("auto",) + SUPPORTED_BACKTEST_ENGINES,
        default="auto",
        help="Backtest engine. auto follows strategy config and falls back to native.",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Optional symbol filter.",
    )
    parser.add_argument(
        "--skip-signal-write",
        action="store_true",
        help="Do not write signal artifacts before running the backtest.",
    )
    return parser.parse_args(argv)


def write_signal_artifacts(
    *,
    loader: ReloadableStrategyLoader,
    bars: list,
    signals: list,
    settings,
) -> None:
    if settings.signal_path is None:
        return
    output_template = default_output_template(
        loader=loader,
        input_path=Path("market_data.jsonl"),
        output_format=resolve_output_format("auto", output_template=settings.signal_path),
    )
    write_signal_outputs(
        signals,
        output_template=output_template,
        output_format=resolve_output_format("auto", output_template=output_template),
        repo_root=loader.repo_root,
    )


def write_backtest_artifacts(
    result: BacktestResult,
    *,
    loader: ReloadableStrategyLoader,
    settings,
) -> None:
    repo_root = loader.repo_root
    trades_path = resolve_workspace_path(
        repo_root,
        settings.trades_path or f"data/backtests/{loader.definition.metadata.strategy_id}/trades.jsonl",
    )
    equity_path = resolve_workspace_path(
        repo_root,
        settings.equity_path or f"data/backtests/{loader.definition.metadata.strategy_id}/portfolio_equity.jsonl",
    )
    summary_path = resolve_workspace_path(
        repo_root,
        settings.summary_path or f"data/backtests/{loader.definition.metadata.strategy_id}/summary.json",
    )
    assert trades_path is not None
    assert equity_path is not None
    assert summary_path is not None

    write_records(trades_path, [trade.to_output_record() for trade in result.trades])
    write_records(equity_path, [point.to_output_record() for point in result.equity_curve])
    write_summary_json(summary_path, result.summary)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = Path.cwd().resolve()
    loader = ReloadableStrategyLoader(Path(args.strategy), repo_root=repo_root)

    try:
        loader.refresh()
        settings = load_backtest_settings(
            loader.definition.config,
            engine_override=None if args.engine == "auto" else args.engine,
        )
        input_path = Path(args.input).expanduser()
        if not input_path.is_absolute():
            input_path = (repo_root / input_path).resolve()

        symbol_filter = set(args.symbols) if args.symbols else None
        bars = read_market_bars(input_path, symbols=symbol_filter)
        strategy = loader.get_strategy()
        signals = strategy.generate_signals(bars)
        if not args.skip_signal_write:
            write_signal_artifacts(
                loader=loader,
                bars=bars,
                signals=signals,
                settings=settings,
            )

        plan = build_target_weight_plan(bars, signals, settings)
        if settings.engine == "native":
            result = run_native_backtest(
                plan,
                metadata=loader.definition.metadata,
                settings=settings,
            )
        else:
            result = run_bt_backtest(
                plan,
                metadata=loader.definition.metadata,
                settings=settings,
            )

        write_backtest_artifacts(
            result,
            loader=loader,
            settings=settings,
        )
        print(
            f"[backtest] engine={settings.engine} strategy={loader.definition.metadata.strategy_id} "
            f"trades={len(result.trades)} total_return={result.summary['total_return']:.6f}",
            file=sys.stderr,
        )
        return 0
    except Exception as exc:
        print(f"[backtest] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
