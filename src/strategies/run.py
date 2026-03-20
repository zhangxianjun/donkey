from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .core import MarketBar, SignalRecord
from .loader import ReloadableStrategyLoader

DEFAULT_OUTPUT_ROOT = Path("data/signals")
SUPPORTED_OUTPUT_FORMATS = ("auto", "jsonl", "parquet")
SUPPORTED_INPUT_SUFFIXES = {".jsonl", ".parquet"}
DEFAULT_POLL_SECONDS = 1.0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate strategy signals from normalized market data."
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
        "--output",
        default=None,
        help="Optional output path. Supports {symbol} placeholder.",
    )
    parser.add_argument(
        "--output-format",
        default="auto",
        choices=SUPPORTED_OUTPUT_FORMATS,
        help="auto follows output suffix when present, otherwise picks parquet if pyarrow exists.",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Optional symbol filter.",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Watch strategy/config/input file changes and rerun automatically.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=DEFAULT_POLL_SECONDS,
        help="Polling interval used together with --watch.",
    )
    return parser.parse_args(argv)


def iso_to_datetime(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def pyarrow_available() -> bool:
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        return False
    return True


def ensure_pyarrow_available() -> None:
    if not pyarrow_available():
        raise RuntimeError(
            "Parquet input/output requires pyarrow. Install with `python3 -m pip install pyarrow`."
        )


def read_market_bars(path: Path, *, symbols: set[str] | None) -> list[MarketBar]:
    if not path.exists():
        raise FileNotFoundError(f"Input data file not found: {path}")
    if path.suffix not in SUPPORTED_INPUT_SUFFIXES:
        raise ValueError(f"Unsupported input suffix: {path.suffix}")

    if path.suffix == ".jsonl":
        return read_market_bars_from_jsonl(path, symbols=symbols)
    return read_market_bars_from_parquet(path, symbols=symbols)


def read_market_bars_from_jsonl(path: Path, *, symbols: set[str] | None) -> list[MarketBar]:
    bars: list[MarketBar] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in {path}:{line_no}: {exc}") from exc
            bar = MarketBar.from_record(record)
            if symbols is not None and bar.symbol not in symbols:
                continue
            bars.append(bar)
    return sorted(bars, key=lambda item: (item.symbol, item.ts))


def read_market_bars_from_parquet(path: Path, *, symbols: set[str] | None) -> list[MarketBar]:
    ensure_pyarrow_available()

    import pyarrow.parquet as pq

    table = pq.read_table(path)
    bars = [MarketBar.from_record(record) for record in table.to_pylist()]
    if symbols is not None:
        bars = [bar for bar in bars if bar.symbol in symbols]
    return sorted(bars, key=lambda item: (item.symbol, item.ts))


def write_signal_jsonl(path: Path, records: list[SignalRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record.to_output_record(), ensure_ascii=True) + "\n")


def write_signal_parquet(path: Path, records: list[SignalRecord]) -> None:
    ensure_pyarrow_available()

    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [
            ("ts", pa.timestamp("us", tz="UTC")),
            ("symbol", pa.string()),
            ("strategy_name", pa.string()),
            ("strategy_version", pa.string()),
            ("interval", pa.string()),
            ("signal_long_entry", pa.int8()),
            ("signal_long_exit", pa.int8()),
            ("position", pa.int8()),
            ("close", pa.float64()),
            ("atr", pa.float64()),
            ("atr_stop", pa.float64()),
            ("entry_reason", pa.string()),
            ("exit_reason", pa.string()),
            ("exchange", pa.string()),
            ("market_type", pa.string()),
        ]
    )
    columns = {
        "ts": [iso_to_datetime(record.ts) for record in records],
        "symbol": [record.symbol for record in records],
        "strategy_name": [record.strategy_name for record in records],
        "strategy_version": [record.strategy_version for record in records],
        "interval": [record.interval for record in records],
        "signal_long_entry": [record.signal_long_entry for record in records],
        "signal_long_exit": [record.signal_long_exit for record in records],
        "position": [record.position for record in records],
        "close": [record.close for record in records],
        "atr": [record.atr for record in records],
        "atr_stop": [record.atr_stop for record in records],
        "entry_reason": [record.entry_reason for record in records],
        "exit_reason": [record.exit_reason for record in records],
        "exchange": [record.exchange for record in records],
        "market_type": [record.market_type for record in records],
    }
    table = pa.table(columns, schema=schema)
    pq.write_table(table, path)


def detect_path_format(output_template: str | None) -> str | None:
    if output_template is None:
        return None
    probe_path = Path(output_template.replace("{symbol}", "sample"))
    if probe_path.suffix == ".jsonl":
        return "jsonl"
    if probe_path.suffix == ".parquet":
        return "parquet"
    return None


def resolve_output_format(requested: str, *, output_template: str | None) -> str:
    path_format = detect_path_format(output_template)
    if requested == "jsonl":
        return "jsonl"
    if requested == "parquet":
        ensure_pyarrow_available()
        return "parquet"
    if path_format == "parquet":
        ensure_pyarrow_available()
        return "parquet"
    if path_format == "jsonl":
        return "jsonl"
    if pyarrow_available():
        return "parquet"
    return "jsonl"


def default_output_template(
    *,
    loader: ReloadableStrategyLoader,
    input_path: Path,
    output_format: str,
) -> str:
    artifacts = (
        loader.definition.config.get("artifacts", {})
        if isinstance(loader.definition.config.get("artifacts"), dict)
        else {}
    )
    if artifacts.get("signal_path") is not None:
        return str(artifacts["signal_path"])

    suffix = ".parquet" if output_format == "parquet" else ".jsonl"
    return str(
        DEFAULT_OUTPUT_ROOT
        / loader.definition.metadata.strategy_id
        / f"{input_path.stem}_signal{suffix}"
    )


def resolve_output_path(template: str, *, symbol: str | None, repo_root: Path) -> Path:
    rendered = template.format(symbol=symbol) if symbol is not None else template
    candidate = Path(rendered).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (repo_root / candidate).resolve()


def split_signal_records(records: list[SignalRecord], *, output_template: str) -> dict[str | None, list[SignalRecord]]:
    if "{symbol}" not in output_template:
        return {None: records}

    grouped: dict[str | None, list[SignalRecord]] = {}
    for record in records:
        grouped.setdefault(record.symbol, []).append(record)
    return grouped


def write_signal_outputs(
    records: list[SignalRecord],
    *,
    output_template: str,
    output_format: str,
    repo_root: Path,
) -> list[Path]:
    written_paths: list[Path] = []
    for symbol, group in split_signal_records(records, output_template=output_template).items():
        path = resolve_output_path(output_template, symbol=symbol, repo_root=repo_root)
        if output_format == "parquet":
            write_signal_parquet(path, group)
        else:
            write_signal_jsonl(path, group)
        written_paths.append(path)
    return written_paths


def generate_signals_once(
    *,
    loader: ReloadableStrategyLoader,
    input_path: Path,
    output_template: str | None,
    requested_output_format: str,
    symbols: set[str] | None,
) -> tuple[list[SignalRecord], list[Path], str]:
    strategy = loader.get_strategy()
    bars = read_market_bars(input_path, symbols=symbols)
    if not bars:
        raise ValueError(f"No market bars found in {input_path}")

    resolved_template = output_template or default_output_template(
        loader=loader,
        input_path=input_path,
        output_format=resolve_output_format(requested_output_format, output_template=output_template),
    )
    output_format = resolve_output_format(requested_output_format, output_template=resolved_template)
    signals = strategy.generate_signals(bars)
    written_paths = write_signal_outputs(
        signals,
        output_template=resolved_template,
        output_format=output_format,
        repo_root=loader.repo_root,
    )
    return signals, written_paths, output_format


def validate_args(args: argparse.Namespace) -> None:
    if args.output_format not in SUPPORTED_OUTPUT_FORMATS:
        raise ValueError(
            f"--output-format must be one of {', '.join(SUPPORTED_OUTPUT_FORMATS)}."
        )
    if args.poll_seconds <= 0:
        raise ValueError("--poll-seconds must be > 0.")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        validate_args(args)
    except ValueError as exc:
        print(f"argument error: {exc}", file=sys.stderr)
        return 2

    repo_root = Path.cwd().resolve()
    loader = ReloadableStrategyLoader(Path(args.strategy), repo_root=repo_root)
    input_path = Path(args.input).expanduser()
    if not input_path.is_absolute():
        input_path = (repo_root / input_path).resolve()

    symbol_filter = set(args.symbols) if args.symbols else None
    last_input_mtime_ns: int | None = None
    run_count = 0

    try:
        while True:
            changed = loader.refresh()
            if input_path.exists():
                input_mtime_ns = input_path.stat().st_mtime_ns
            else:
                if args.watch:
                    input_mtime_ns = None
                    time.sleep(args.poll_seconds)
                    continue
                print(f"[strategy] input data file not found: {input_path}", file=sys.stderr)
                return 1

            should_run = run_count == 0 or changed or input_mtime_ns != last_input_mtime_ns
            if should_run:
                signals, written_paths, output_format = generate_signals_once(
                    loader=loader,
                    input_path=input_path,
                    output_template=args.output,
                    requested_output_format=args.output_format,
                    symbols=symbol_filter,
                )
                path_text = ", ".join(str(path) for path in written_paths)
                print(
                    f"[strategy] generated={len(signals)} format={output_format} "
                    f"strategy={loader.definition.metadata.strategy_id} "
                    f"loaded_at={utc_now_iso()} outputs={path_text}",
                    file=sys.stderr,
                )
                last_input_mtime_ns = input_mtime_ns
                run_count += 1

            if not args.watch:
                return 0

            time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        print(f"[strategy] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
