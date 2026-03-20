from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, replace
from pathlib import Path

from src.strategies.loader import ReloadableStrategyLoader
from src.strategies.run import (
    default_output_template,
    read_market_bars,
    resolve_output_format,
    write_signal_outputs,
)

from .backtrader_adapter import run_backtrader_backtest
from .bt_adapter import run_bt_backtest
from .core import (
    BacktestResult,
    BacktestSettings,
    SUPPORTED_BACKTEST_ENGINES,
    build_target_weight_plan,
    load_backtest_settings,
    resolve_workspace_path,
    write_records,
    write_summary_json,
)
from .native import run_native_backtest


@dataclass(frozen=True)
class BacktestArtifactPaths:
    signal_paths: tuple[Path, ...]
    trades_path: Path
    equity_path: Path
    summary_path: Path


@dataclass(frozen=True)
class BacktestExecution:
    engine: str
    loader: ReloadableStrategyLoader
    settings: BacktestSettings
    input_path: Path
    result: BacktestResult
    artifact_paths: BacktestArtifactPaths


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
    signals: list,
    settings,
    input_path: Path,
    signal_template_override: str | None = None,
) -> list[Path]:
    output_template = signal_template_override or settings.signal_path
    if output_template is None:
        output_template = default_output_template(
            loader=loader,
            input_path=input_path,
            output_format=resolve_output_format("auto", output_template=None),
        )
    output_format = resolve_output_format("auto", output_template=output_template)
    return write_signal_outputs(
        signals,
        output_template=output_template,
        output_format=output_format,
        repo_root=loader.repo_root,
    )


def resolve_backtest_artifact_paths(
    *,
    loader: ReloadableStrategyLoader,
    settings: BacktestSettings,
) -> BacktestArtifactPaths:
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
    return BacktestArtifactPaths(
        signal_paths=tuple(),
        trades_path=trades_path,
        equity_path=equity_path,
        summary_path=summary_path,
    )


def write_backtest_artifacts(
    result: BacktestResult,
    *,
    artifact_paths: BacktestArtifactPaths,
) -> None:
    write_records(artifact_paths.trades_path, [trade.to_output_record() for trade in result.trades])
    write_records(artifact_paths.equity_path, [point.to_output_record() for point in result.equity_curve])
    write_summary_json(artifact_paths.summary_path, result.summary)


def apply_artifact_overrides(
    settings: BacktestSettings,
    *,
    signal_path: str | None = None,
    trades_path: str | None = None,
    equity_path: str | None = None,
    summary_path: str | None = None,
) -> BacktestSettings:
    return replace(
        settings,
        signal_path=signal_path if signal_path is not None else settings.signal_path,
        trades_path=trades_path if trades_path is not None else settings.trades_path,
        equity_path=equity_path if equity_path is not None else settings.equity_path,
        summary_path=summary_path if summary_path is not None else settings.summary_path,
    )


def run_backtest_engine(
    *,
    plan,
    loader: ReloadableStrategyLoader,
    settings: BacktestSettings,
) -> BacktestResult:
    if settings.engine == "native":
        return run_native_backtest(
            plan,
            metadata=loader.definition.metadata,
            settings=settings,
        )
    if settings.engine == "bt":
        return run_bt_backtest(
            plan,
            metadata=loader.definition.metadata,
            settings=settings,
        )
    if settings.engine == "backtrader":
        return run_backtrader_backtest(
            plan,
            metadata=loader.definition.metadata,
            settings=settings,
        )
    raise ValueError(f"Unsupported backtest engine: {settings.engine}")


def execute_backtest(
    *,
    strategy_path: str | Path,
    input_path: str | Path,
    engine: str = "auto",
    symbols: list[str] | None = None,
    skip_signal_write: bool = False,
    repo_root: Path | None = None,
    signal_path_override: str | None = None,
    trades_path_override: str | None = None,
    equity_path_override: str | None = None,
    summary_path_override: str | None = None,
) -> BacktestExecution:
    resolved_repo_root = (repo_root or Path.cwd()).resolve()
    loader = ReloadableStrategyLoader(Path(strategy_path), repo_root=resolved_repo_root)
    loader.refresh()

    settings = load_backtest_settings(
        loader.definition.config,
        engine_override=None if engine == "auto" else engine,
    )
    settings = apply_artifact_overrides(
        settings,
        signal_path=signal_path_override,
        trades_path=trades_path_override,
        equity_path=equity_path_override,
        summary_path=summary_path_override,
    )

    resolved_input_path = Path(input_path).expanduser()
    if not resolved_input_path.is_absolute():
        resolved_input_path = (resolved_repo_root / resolved_input_path).resolve()

    symbol_filter = set(symbols) if symbols else None
    bars = read_market_bars(resolved_input_path, symbols=symbol_filter)
    strategy = loader.get_strategy()
    signals = strategy.generate_signals(bars)

    signal_paths: list[Path] = []
    if not skip_signal_write:
        signal_paths = write_signal_artifacts(
            loader=loader,
            signals=signals,
            settings=settings,
            input_path=resolved_input_path,
            signal_template_override=settings.signal_path,
        )

    plan = build_target_weight_plan(bars, signals, settings)
    result = run_backtest_engine(
        plan=plan,
        loader=loader,
        settings=settings,
    )

    artifact_paths = resolve_backtest_artifact_paths(
        loader=loader,
        settings=settings,
    )
    artifact_paths = BacktestArtifactPaths(
        signal_paths=tuple(signal_paths),
        trades_path=artifact_paths.trades_path,
        equity_path=artifact_paths.equity_path,
        summary_path=artifact_paths.summary_path,
    )
    write_backtest_artifacts(result, artifact_paths=artifact_paths)
    return BacktestExecution(
        engine=settings.engine,
        loader=loader,
        settings=settings,
        input_path=resolved_input_path,
        result=result,
        artifact_paths=artifact_paths,
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        execution = execute_backtest(
            strategy_path=args.strategy,
            input_path=args.input,
            engine=args.engine,
            symbols=args.symbols,
            skip_signal_write=args.skip_signal_write,
        )
        print(
            f"[backtest] engine={execution.engine} "
            f"strategy={execution.loader.definition.metadata.strategy_id} "
            f"trades={len(execution.result.trades)} "
            f"total_return={execution.result.summary['total_return']:.6f}",
            file=sys.stderr,
        )
        return 0
    except Exception as exc:
        print(f"[backtest] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
