from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_INPUT_ROOT = Path("data/raw/binance/spot")
DEFAULT_OUTPUT_ROOT = Path("data/normalized")
DEFAULT_DATA_VERSION = "v1"
SUPPORTED_OUTPUT_FORMATS = ("auto", "jsonl", "parquet")


@dataclass(frozen=True)
class IntervalOutput:
    interval: str
    output_path: str
    row_count: int
    source_file_count: int
    duplicate_rows_removed: int


@dataclass(frozen=True)
class NormalizedBar:
    exchange: str
    market_type: str
    symbol: str
    interval: str
    ts: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float | None
    trade_count: int | None
    source_file: str
    data_version: str
    created_at: str
    fetched_at: str

    def dedupe_key(self) -> tuple[str, str, str, str, str]:
        return (self.exchange, self.market_type, self.symbol, self.interval, self.ts)

    def to_output_record(self) -> dict[str, Any]:
        return {
            "ts": self.ts,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "market_type": self.market_type,
            "interval": self.interval,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "quote_volume": self.quote_volume,
            "trade_count": self.trade_count,
            "source_file": self.source_file,
            "data_version": self.data_version,
            "created_at": self.created_at,
        }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize Binance raw kline jsonl files into market_ohlcv datasets."
    )
    parser.add_argument(
        "--input-root",
        default=str(DEFAULT_INPUT_ROOT),
        help="Raw input root produced by ingestion.",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Normalized output root.",
    )
    parser.add_argument(
        "--data-version",
        default=DEFAULT_DATA_VERSION,
        help="Dataset version name, for example v1.",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Optional symbol filter.",
    )
    parser.add_argument(
        "--intervals",
        nargs="*",
        default=None,
        help="Optional interval filter.",
    )
    parser.add_argument(
        "--output-format",
        default="auto",
        choices=SUPPORTED_OUTPUT_FORMATS,
        help="auto picks parquet when pyarrow is available, otherwise jsonl.",
    )
    return parser.parse_args(argv)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def iso_to_datetime(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def discover_raw_files(
    input_root: Path,
    *,
    symbols: set[str] | None,
    intervals: set[str] | None,
) -> list[Path]:
    if not input_root.exists():
        return []

    discovered: list[Path] = []
    for path in sorted(input_root.rglob("*.jsonl")):
        relative_parts = path.relative_to(input_root).parts
        if len(relative_parts) != 3:
            continue

        symbol, interval, _filename = relative_parts
        if symbols is not None and symbol not in symbols:
            continue
        if intervals is not None and interval not in intervals:
            continue

        discovered.append(path)

    return discovered


def parse_float(value: Any, *, field_name: str, source_file: str, line_no: int) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid float for {field_name} in {source_file}:{line_no}: {value!r}"
        ) from exc


def parse_optional_float(
    value: Any,
    *,
    field_name: str,
    source_file: str,
    line_no: int,
) -> float | None:
    if value is None:
        return None
    return parse_float(value, field_name=field_name, source_file=source_file, line_no=line_no)


def parse_optional_int(
    value: Any,
    *,
    field_name: str,
    source_file: str,
    line_no: int,
) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Invalid int for {field_name} in {source_file}:{line_no}: {value!r}"
        ) from exc


def require_field(
    raw: dict[str, Any],
    field_name: str,
    *,
    source_file: str,
    line_no: int,
) -> Any:
    if field_name not in raw:
        raise ValueError(f"Missing {field_name!r} in {source_file}:{line_no}")
    return raw[field_name]


def normalize_raw_record(
    raw: dict[str, Any],
    *,
    source_file: str,
    line_no: int,
    data_version: str,
    created_at: str,
) -> NormalizedBar:
    ts = raw.get("open_time_iso")
    if ts is None:
        open_time = require_field(
            raw,
            "open_time",
            source_file=source_file,
            line_no=line_no,
        )
        ts = (
            datetime.fromtimestamp(int(open_time) / 1000, tz=UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

    fetched_at = raw.get("fetched_at", created_at)

    return NormalizedBar(
        exchange=str(
            require_field(raw, "exchange", source_file=source_file, line_no=line_no)
        ),
        market_type=str(
            require_field(raw, "market_type", source_file=source_file, line_no=line_no)
        ),
        symbol=str(require_field(raw, "symbol", source_file=source_file, line_no=line_no)),
        interval=str(
            require_field(raw, "interval", source_file=source_file, line_no=line_no)
        ),
        ts=str(ts),
        open=parse_float(
            require_field(raw, "open", source_file=source_file, line_no=line_no),
            field_name="open",
            source_file=source_file,
            line_no=line_no,
        ),
        high=parse_float(
            require_field(raw, "high", source_file=source_file, line_no=line_no),
            field_name="high",
            source_file=source_file,
            line_no=line_no,
        ),
        low=parse_float(
            require_field(raw, "low", source_file=source_file, line_no=line_no),
            field_name="low",
            source_file=source_file,
            line_no=line_no,
        ),
        close=parse_float(
            require_field(raw, "close", source_file=source_file, line_no=line_no),
            field_name="close",
            source_file=source_file,
            line_no=line_no,
        ),
        volume=parse_float(
            require_field(raw, "volume", source_file=source_file, line_no=line_no),
            field_name="volume",
            source_file=source_file,
            line_no=line_no,
        ),
        quote_volume=parse_optional_float(
            raw.get("quote_asset_volume", raw.get("quote_volume")),
            field_name="quote_volume",
            source_file=source_file,
            line_no=line_no,
        ),
        trade_count=parse_optional_int(
            raw.get("number_of_trades", raw.get("trade_count")),
            field_name="trade_count",
            source_file=source_file,
            line_no=line_no,
        ),
        source_file=source_file,
        data_version=data_version,
        created_at=created_at,
        fetched_at=str(fetched_at),
    )


def load_and_dedupe_records(
    raw_files: list[Path],
    *,
    data_version: str,
    created_at: str,
    repo_root: Path,
) -> tuple[dict[str, dict[tuple[str, str, str, str, str], NormalizedBar]], dict[str, int]]:
    by_interval: dict[str, dict[tuple[str, str, str, str, str], NormalizedBar]] = {}
    duplicate_counter: dict[str, int] = {}
    repo_root_resolved = repo_root.resolve()

    for path in raw_files:
        resolved_path = path.resolve()
        try:
            relative_source_file = str(resolved_path.relative_to(repo_root_resolved))
        except ValueError:
            relative_source_file = str(path)
        with path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                if not line.strip():
                    continue

                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Invalid JSON in {relative_source_file}:{line_no}: {exc}"
                    ) from exc

                normalized = normalize_raw_record(
                    raw,
                    source_file=relative_source_file,
                    line_no=line_no,
                    data_version=data_version,
                    created_at=created_at,
                )

                interval_store = by_interval.setdefault(normalized.interval, {})
                key = normalized.dedupe_key()
                existing = interval_store.get(key)
                if existing is not None:
                    duplicate_counter[normalized.interval] = (
                        duplicate_counter.get(normalized.interval, 0) + 1
                    )
                    if normalized.fetched_at <= existing.fetched_at:
                        continue

                interval_store[key] = normalized

    return by_interval, duplicate_counter


def sort_records(records: list[NormalizedBar]) -> list[NormalizedBar]:
    return sorted(records, key=lambda row: (row.symbol, row.ts))


def resolve_output_format(requested: str) -> str:
    if requested == "jsonl":
        return "jsonl"
    if requested == "parquet":
        ensure_pyarrow_available()
        return "parquet"

    if pyarrow_available():
        return "parquet"
    return "jsonl"


def pyarrow_available() -> bool:
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        return False
    return True


def ensure_pyarrow_available() -> None:
    if not pyarrow_available():
        raise RuntimeError(
            "Parquet output requires pyarrow. Install with `python3 -m pip install pyarrow`."
        )


def write_jsonl(path: Path, records: list[NormalizedBar]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record.to_output_record(), ensure_ascii=True) + "\n")


def write_parquet(path: Path, records: list[NormalizedBar]) -> None:
    ensure_pyarrow_available()

    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema(
        [
            ("ts", pa.timestamp("us", tz="UTC")),
            ("symbol", pa.string()),
            ("exchange", pa.string()),
            ("market_type", pa.string()),
            ("interval", pa.string()),
            ("open", pa.float64()),
            ("high", pa.float64()),
            ("low", pa.float64()),
            ("close", pa.float64()),
            ("volume", pa.float64()),
            ("quote_volume", pa.float64()),
            ("trade_count", pa.int64()),
            ("source_file", pa.string()),
            ("data_version", pa.string()),
            ("created_at", pa.timestamp("us", tz="UTC")),
        ]
    )

    columns = {
        "ts": [iso_to_datetime(record.ts) for record in records],
        "symbol": [record.symbol for record in records],
        "exchange": [record.exchange for record in records],
        "market_type": [record.market_type for record in records],
        "interval": [record.interval for record in records],
        "open": [record.open for record in records],
        "high": [record.high for record in records],
        "low": [record.low for record in records],
        "close": [record.close for record in records],
        "volume": [record.volume for record in records],
        "quote_volume": [record.quote_volume for record in records],
        "trade_count": [record.trade_count for record in records],
        "source_file": [record.source_file for record in records],
        "data_version": [record.data_version for record in records],
        "created_at": [iso_to_datetime(record.created_at) for record in records],
    }

    table = pa.table(columns, schema=schema)
    pq.write_table(table, path)


def write_interval_output(
    output_root: Path,
    *,
    data_version: str,
    interval: str,
    records: list[NormalizedBar],
    output_format: str,
) -> Path:
    output_dir = output_root / data_version
    output_dir.mkdir(parents=True, exist_ok=True)

    suffix = ".parquet" if output_format == "parquet" else ".jsonl"
    output_path = output_dir / f"market_ohlcv_{interval}{suffix}"

    if output_format == "parquet":
        write_parquet(output_path, records)
    else:
        write_jsonl(output_path, records)

    return output_path


def validate_args(args: argparse.Namespace) -> None:
    if args.output_format not in SUPPORTED_OUTPUT_FORMATS:
        raise ValueError(
            f"--output-format must be one of {', '.join(SUPPORTED_OUTPUT_FORMATS)}."
        )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        validate_args(args)
    except ValueError as exc:
        print(f"argument error: {exc}", file=sys.stderr)
        return 2

    repo_root = Path.cwd()
    input_root = Path(args.input_root)
    output_root = Path(args.output_root)
    created_at = utc_now_iso()
    symbols = set(args.symbols) if args.symbols else None
    intervals = set(args.intervals) if args.intervals else None

    raw_files = discover_raw_files(input_root, symbols=symbols, intervals=intervals)
    if not raw_files:
        print(f"[normalize] no raw files found under {input_root}", file=sys.stderr)
        return 1

    resolved_output_format = resolve_output_format(args.output_format)
    print(
        f"[normalize] files={len(raw_files)} output_format={resolved_output_format}",
        file=sys.stderr,
    )

    by_interval, duplicate_counter = load_and_dedupe_records(
        raw_files,
        data_version=args.data_version,
        created_at=created_at,
        repo_root=repo_root,
    )

    summaries: list[IntervalOutput] = []
    raw_file_count_by_interval: dict[str, int] = {}
    for path in raw_files:
        interval = path.parent.name
        raw_file_count_by_interval[interval] = raw_file_count_by_interval.get(interval, 0) + 1

    for interval in sorted(by_interval):
        records = sort_records(list(by_interval[interval].values()))
        output_path = write_interval_output(
            output_root,
            data_version=args.data_version,
            interval=interval,
            records=records,
            output_format=resolved_output_format,
        )
        summary = IntervalOutput(
            interval=interval,
            output_path=str(output_path),
            row_count=len(records),
            source_file_count=raw_file_count_by_interval.get(interval, 0),
            duplicate_rows_removed=duplicate_counter.get(interval, 0),
        )
        summaries.append(summary)
        print(
            f"[normalized] interval={interval} rows={summary.row_count} "
            f"duplicates_removed={summary.duplicate_rows_removed} file={summary.output_path}",
            file=sys.stderr,
        )

    manifest_path = output_root / args.data_version / "normalize_manifest.json"
    manifest = {
        "data_version": args.data_version,
        "input_root": str(input_root),
        "output_root": str(output_root),
        "output_format": resolved_output_format,
        "created_at": created_at,
        "filters": {
            "symbols": sorted(symbols) if symbols is not None else None,
            "intervals": sorted(intervals) if intervals is not None else None,
        },
        "raw_file_count": len(raw_files),
        "interval_outputs": [asdict(summary) for summary in summaries],
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"[manifest] {manifest_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
