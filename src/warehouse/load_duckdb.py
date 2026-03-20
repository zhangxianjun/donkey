from __future__ import annotations

import argparse
import importlib
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_NORMALIZED_ROOT = Path("data/normalized")
DEFAULT_DB_PATH = Path("db/quant.duckdb")
DEFAULT_INIT_SQL_PATH = Path("sql/init_v1.sql")
SUPPORTED_FILE_SUFFIXES = {".jsonl": "jsonl", ".parquet": "parquet"}
NORMALIZED_FILE_RE = re.compile(r"^market_ohlcv_(?P<interval>.+)\.(?P<ext>jsonl|parquet)$")


@dataclass(frozen=True)
class NormalizedFile:
    interval: str
    file_format: str
    path: Path


@dataclass(frozen=True)
class LoadSummary:
    interval: str
    file_format: str
    source_path: str
    inserted_rows: int
    replaced_rows: int


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load normalized market_ohlcv files into DuckDB."
    )
    parser.add_argument(
        "--normalized-root",
        default=str(DEFAULT_NORMALIZED_ROOT),
        help="Root directory that contains normalized dataset versions.",
    )
    parser.add_argument(
        "--data-version",
        required=True,
        help="Normalized data version to load, for example v1.",
    )
    parser.add_argument(
        "--intervals",
        nargs="*",
        default=None,
        help="Optional interval filter.",
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Destination DuckDB database path.",
    )
    parser.add_argument(
        "--init-sql",
        default=str(DEFAULT_INIT_SQL_PATH),
        help="Schema bootstrap SQL file.",
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


def discover_normalized_files(
    normalized_root: Path,
    *,
    data_version: str,
    intervals: set[str] | None,
) -> list[NormalizedFile]:
    version_dir = normalized_root / data_version
    if not version_dir.exists():
        return []

    discovered: list[NormalizedFile] = []
    for path in sorted(version_dir.iterdir()):
        if not path.is_file():
            continue

        matched = NORMALIZED_FILE_RE.match(path.name)
        if not matched:
            continue

        interval = matched.group("interval")
        ext = "." + matched.group("ext")
        if ext not in SUPPORTED_FILE_SUFFIXES:
            continue
        if intervals is not None and interval not in intervals:
            continue

        discovered.append(
            NormalizedFile(
                interval=interval,
                file_format=SUPPORTED_FILE_SUFFIXES[ext],
                path=path,
            )
        )

    return discovered


def local_site_packages(repo_root: Path) -> list[Path]:
    venv_root = repo_root / ".venv"
    if not venv_root.exists():
        return []

    candidates = sorted((venv_root / "lib").glob("python*/site-packages"))
    windows_candidate = venv_root / "Lib" / "site-packages"
    if windows_candidate.exists():
        candidates.append(windows_candidate)
    return [path for path in candidates if path.exists()]


def import_duckdb(*, repo_root: Path | None = None) -> Any:
    try:
        return importlib.import_module("duckdb")
    except ImportError as exc:
        if repo_root is not None:
            for site_packages in local_site_packages(repo_root):
                site_packages_str = str(site_packages.resolve())
                if site_packages_str not in sys.path:
                    sys.path.insert(0, site_packages_str)
                try:
                    return importlib.import_module("duckdb")
                except ImportError:
                    continue

        raise RuntimeError(
            "DuckDB Python package is required for warehouse loading. "
            "Create a project virtualenv and install dependencies with "
            "`python3 -m venv .venv && ./.venv/bin/python -m pip install -r requirements.txt`."
        ) from exc


def render_init_sql(init_sql_path: Path, *, repo_root: Path) -> str:
    sql_text = init_sql_path.read_text(encoding="utf-8")
    experiments_path = (repo_root / "db" / "experiments.duckdb").resolve().as_posix()
    return sql_text.replace(
        "ATTACH 'db/experiments.duckdb' AS exp;",
        f"ATTACH '{experiments_path}' AS exp;",
    )


def initialize_database(connection: Any, *, init_sql_path: Path, repo_root: Path) -> None:
    bootstrap_sql = render_init_sql(init_sql_path, repo_root=repo_root)
    connection.execute(bootstrap_sql)


def count_existing_rows(connection: Any, *, data_version: str, interval: str) -> int:
    result = connection.execute(
        """
        SELECT COUNT(*)
        FROM market_ohlcv
        WHERE data_version = ? AND interval = ?
        """,
        [data_version, interval],
    ).fetchone()
    return int(result[0]) if result is not None else 0


def delete_existing_rows(connection: Any, *, data_version: str, interval: str) -> None:
    connection.execute(
        "DELETE FROM market_ohlcv WHERE data_version = ? AND interval = ?",
        [data_version, interval],
    )


def read_normalized_jsonl(path: Path) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            if not line.strip():
                continue

            try:
                raw = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in {path}:{line_no}: {exc}") from exc

            rows.append(normalized_record_to_row(raw, source_path=path, line_no=line_no))

    return rows


def normalized_record_to_row(
    record: dict[str, Any],
    *,
    source_path: Path,
    line_no: int,
) -> tuple[Any, ...]:
    required_fields = (
        "ts",
        "symbol",
        "exchange",
        "market_type",
        "interval",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source_file",
        "data_version",
        "created_at",
    )
    for field_name in required_fields:
        if field_name not in record:
            raise ValueError(f"Missing {field_name!r} in {source_path}:{line_no}")

    return (
        iso_to_datetime(str(record["ts"])),
        str(record["symbol"]),
        str(record["exchange"]),
        str(record["market_type"]),
        str(record["interval"]),
        float(record["open"]),
        float(record["high"]),
        float(record["low"]),
        float(record["close"]),
        float(record["volume"]),
        None if record.get("quote_volume") is None else float(record["quote_volume"]),
        None if record.get("trade_count") is None else int(record["trade_count"]),
        str(record["source_file"]),
        str(record["data_version"]),
        iso_to_datetime(str(record["created_at"])),
    )


def insert_jsonl_rows(connection: Any, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return

    connection.executemany(
        """
        INSERT INTO market_ohlcv (
            ts,
            symbol,
            exchange,
            market_type,
            interval,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count,
            source_file,
            data_version,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def insert_parquet_rows(connection: Any, path: Path) -> int:
    inserted_rows = connection.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [str(path)],
    ).fetchone()[0]
    connection.execute(
        """
        INSERT INTO market_ohlcv (
            ts,
            symbol,
            exchange,
            market_type,
            interval,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count,
            source_file,
            data_version,
            created_at
        )
        SELECT
            ts,
            symbol,
            exchange,
            market_type,
            interval,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count,
            source_file,
            data_version,
            created_at
        FROM read_parquet(?)
        """,
        [str(path)],
    )
    return int(inserted_rows)


def load_normalized_file(
    connection: Any,
    normalized_file: NormalizedFile,
    *,
    data_version: str,
    repo_root: Path,
) -> LoadSummary:
    relative_source_path = make_relative_path(normalized_file.path, repo_root=repo_root)
    replaced_rows = count_existing_rows(
        connection,
        data_version=data_version,
        interval=normalized_file.interval,
    )

    connection.begin()
    try:
        delete_existing_rows(
            connection,
            data_version=data_version,
            interval=normalized_file.interval,
        )
        if normalized_file.file_format == "jsonl":
            rows = read_normalized_jsonl(normalized_file.path)
            insert_jsonl_rows(connection, rows)
            inserted_rows = len(rows)
        else:
            inserted_rows = insert_parquet_rows(connection, normalized_file.path)
        connection.commit()
    except Exception:
        connection.rollback()
        raise

    return LoadSummary(
        interval=normalized_file.interval,
        file_format=normalized_file.file_format,
        source_path=relative_source_path,
        inserted_rows=inserted_rows,
        replaced_rows=replaced_rows,
    )


def make_relative_path(path: Path, *, repo_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except ValueError:
        return str(path)


def validate_args(args: argparse.Namespace) -> None:
    if not args.data_version.strip():
        raise ValueError("--data-version must not be empty.")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        validate_args(args)
    except ValueError as exc:
        print(f"argument error: {exc}", file=sys.stderr)
        return 2

    repo_root = Path.cwd()
    normalized_root = Path(args.normalized_root)
    db_path = Path(args.db_path)
    init_sql_path = Path(args.init_sql)
    intervals = set(args.intervals) if args.intervals else None

    normalized_files = discover_normalized_files(
        normalized_root,
        data_version=args.data_version,
        intervals=intervals,
    )
    if not normalized_files:
        print(
            f"[warehouse] no normalized files found for data_version={args.data_version}",
            file=sys.stderr,
        )
        return 1

    try:
        duckdb = import_duckdb(repo_root=repo_root)
    except RuntimeError as exc:
        print(f"[warehouse] {exc}", file=sys.stderr)
        return 1

    db_path.parent.mkdir(parents=True, exist_ok=True)
    if not init_sql_path.exists():
        print(f"[warehouse] init sql not found: {init_sql_path}", file=sys.stderr)
        return 1

    connection = duckdb.connect(str(db_path))
    try:
        initialize_database(connection, init_sql_path=init_sql_path, repo_root=repo_root)
        summaries: list[LoadSummary] = []

        for normalized_file in normalized_files:
            summary = load_normalized_file(
                connection,
                normalized_file,
                data_version=args.data_version,
                repo_root=repo_root,
            )
            summaries.append(summary)
            print(
                f"[loaded] interval={summary.interval} format={summary.file_format} "
                f"inserted={summary.inserted_rows} replaced={summary.replaced_rows} "
                f"source={summary.source_path}",
                file=sys.stderr,
            )

        final_count = connection.execute(
            "SELECT COUNT(*) FROM market_ohlcv WHERE data_version = ?",
            [args.data_version],
        ).fetchone()[0]

        manifest = {
            "loaded_at": utc_now_iso(),
            "db_path": str(db_path),
            "data_version": args.data_version,
            "interval_filters": sorted(intervals) if intervals is not None else None,
            "loaded_files": [asdict(summary) for summary in summaries],
            "market_ohlcv_rows_for_data_version": int(final_count),
        }
        print(json.dumps(manifest, ensure_ascii=True, indent=2))
        return 0
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
