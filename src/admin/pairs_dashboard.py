from __future__ import annotations

import argparse
import base64
import copy
import json
import mimetypes
import os
import platform
import re
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from functools import lru_cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlencode, urlparse
from urllib.request import Request, urlopen

from src.backtest.core import SUPPORTED_BACKTEST_ENGINES
from src.backtest.reporting import build_backtest_report
from src.backtest.run import execute_backtest
from src.ingestion.binance_ohlcv import (
    DEFAULT_EXCHANGE_INFO_BASE_URL,
    DEFAULT_INTERVALS,
    DEFAULT_KLINE_FALLBACK_BASE_URLS,
    DEFAULT_MANUAL_START_DATE,
    DEFAULT_MARKET_DATA_BASE_URL,
    EXCHANGE_INFO_PATH,
    MAX_LIMIT,
    build_candidate_urls,
    build_requests,
    download_klines,
    failure_from_exception,
    fetch_json_payload,
    make_run_id,
    utc_now_iso,
    validate_args,
)
from src.normalize import market_ohlcv as normalized_market_ohlcv
from src.warehouse import load_duckdb as warehouse_load_duckdb

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8866
DEFAULT_RAW_ROOT = Path("data/raw/binance/spot")
DEFAULT_NORMALIZED_ROOT = Path("data/normalized")
DEFAULT_STRATEGIES_ROOT = Path("config/strategies")
EXTRA_STRATEGY_ROOTS_ENV = "DONKEY_EXTRA_STRATEGY_ROOTS"
DEFAULT_STRATEGY_ROOT_STORE = Path("data/admin/strategy_roots.json")
DEFAULT_BACKTESTS_ROOT = Path("data/backtests")
DEFAULT_LOGS_ROOT = Path("logs")
DEFAULT_QUANT_DB_PATH = Path("db/quant.duckdb")
DEFAULT_EXPERIMENTS_DB_PATH = Path("db/experiments.duckdb")
DEFAULT_LOCAL_TRADING_STORE = Path("data/admin/local_trading_pairs.json")
DEFAULT_PAIR_PREFERENCES_STORE = Path("data/admin/pair_preferences.json")
DEFAULT_CURRENCY_ICON_ROOT = Path("data/admin/currency_icons")
DEFAULT_QUOTE_ASSET = "USDT"
DEFAULT_RETRY_JITTER_SECONDS = 0.5
DEFAULT_SLEEP_SECONDS = 0.1
DEFAULT_TRADEABLE_ONLY = True
CURRENCY_ICON_CATALOG_NAME = "catalog.json"
DEFAULT_CURRENCY_ICON_ASSET = "DEFAULT"
MAX_CURRENCY_ICON_BYTES = 2 * 1024 * 1024
CHARTING_LIBRARY_STATIC_ROOT = Path(__file__).resolve().parent / "static" / "charting_library"
CHARTING_LIBRARY_BUNDLE_PATH = CHARTING_LIBRARY_STATIC_ROOT / "charting_library.js"
CHARTING_LIBRARY_SAMEORIGIN_PATH = CHARTING_LIBRARY_STATIC_ROOT / "sameorigin.html"
CHARTING_LIBRARY_HOSTED_BASE_URL = "https://charting-library.tradingview-widget.com/charting_library/"
CHARTING_LIBRARY_FETCH_TIMEOUT_SECONDS = 30.0
CHARTING_LIBRARY_FETCH_LOCK = threading.Lock()
STRATEGY_ROOT_STORE_LOCK = threading.Lock()
DEFAULT_NORMALIZE_OUTPUT_FORMAT = "auto"
SUPPORTED_NORMALIZE_SOURCES = {"binance"}
ALLOWED_CURRENCY_ICON_EXTENSIONS = {
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".webp": "image/webp",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
}
ASSET_INVALID_CHAR_PATTERN = re.compile(r"[\\/\x00-\x1f]")
STRATEGY_CLONE_FILENAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]+\.ya?ml$")
STRATEGY_FIELD_PATH_PATTERN = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$")
SOURCE_ORDER = ("binance", "okx", "bybit", "hl")
SOURCE_LABELS = {
    "binance": "币安",
    "okx": "欧意",
    "bybit": "Bybit",
    "hl": "HL",
}
INDEX_HTML_PATH = Path(__file__).resolve().parent / "static" / "pairs_dashboard.html"
ICON_SVG_PATH = Path(__file__).resolve().parent / "static" / "donkey-icon.svg"


@dataclass(frozen=True)
class SourcePair:
    source: str
    source_label: str
    symbol: str
    display_symbol: str
    base_asset: str
    quote_asset: str
    status: str
    tradeable: bool
    source_kind: str
    created_at: str | None = None
    note: str | None = None


@dataclass(frozen=True)
class LocalPair:
    source: str
    source_label: str
    symbol: str
    display_symbol: str
    root: str
    intervals: list[str]
    interval_count: int
    data_file_count: int
    metadata_file_count: int
    checkpoint_count: int
    last_updated: str | None


@dataclass(frozen=True)
class StrategyEntry:
    strategy_id: str
    strategy_name: str
    strategy_version: str
    display_name: str | None
    description: str
    strategy_root: str
    location_kind: str
    exchange: str | None
    market_type: str | None
    interval: str | None
    data_version: str | None
    configured_engine: str | None
    symbols: list[str]
    symbol_count: int
    backtest_start: str | None
    backtest_end: str | None
    input_path: str | None
    input_exists: bool
    strategy_path: str
    signal_path: str | None
    trades_path: str | None
    equity_path: str | None
    summary_path: str | None
    summary_exists: bool
    updated_at: str | None


@dataclass(frozen=True)
class BacktestRecord:
    record_id: str
    run_id: str | None
    strategy_id: str
    strategy_name: str
    display_name: str | None
    strategy_version: str
    engine: str | None
    status: str
    input_path: str | None
    strategy_path: str | None
    manifest_path: str | None
    report_available: bool
    summary_path: str | None
    summary_exists: bool
    trades_path: str | None
    trades_exists: bool
    equity_path: str | None
    equity_exists: bool
    created_at: str | None
    started_at: str | None
    finished_at: str | None
    updated_at: str | None
    metrics: dict[str, Any] | None
    error: str | None


@dataclass(frozen=True)
class DashboardConfig:
    workspace_root: Path
    raw_root: Path
    normalized_root: Path
    strategies_root: Path
    extra_strategy_roots: tuple[Path, ...]
    strategy_root_store_path: Path
    backtests_root: Path
    logs_root: Path
    quant_db_path: Path
    experiments_db_path: Path
    local_trading_store_path: Path
    pair_preferences_store_path: Path
    currency_icon_root: Path
    exchange_info_base_url: str
    market_data_base_url: str
    fallback_base_urls: tuple[str, ...]
    timeout_seconds: float
    max_retries: int
    retry_delay_seconds: float
    retry_jitter_seconds: float
    limit: int
    sleep_seconds: float
    default_quote_asset: str
    default_tradeable_only: bool


@dataclass(frozen=True)
class KlineDownloadRequest:
    symbol: str
    intervals: list[str]
    start_date: str | None
    end_date: str
    start_from_listing: bool


@dataclass
class DownloadJob:
    job_id: str
    symbol: str
    intervals: list[str]
    start_date: str | None
    end_date: str
    start_from_listing: bool
    status: str
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    run_id: str | None = None
    manifest_path: str | None = None
    summaries: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


@dataclass(frozen=True)
class NormalizeRequest:
    source: str
    symbols: list[str]
    intervals: list[str]
    data_version: str
    output_format: str


@dataclass
class NormalizeJob:
    job_id: str
    source: str
    symbols: list[str]
    intervals: list[str]
    data_version: str
    output_format: str
    status: str
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    raw_root: str | None = None
    output_root: str | None = None
    manifest_path: str | None = None
    raw_file_count: int = 0
    interval_outputs: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None


@dataclass(frozen=True)
class DuckDBLoadRequest:
    data_version: str
    intervals: list[str] | None


@dataclass
class DuckDBLoadJob:
    job_id: str
    data_version: str
    intervals: list[str] | None
    status: str
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    db_path: str | None = None
    loaded_files: list[dict[str, Any]] = field(default_factory=list)
    market_ohlcv_rows_for_data_version: int | None = None
    error: str | None = None


@dataclass(frozen=True)
class BacktestRunRequest:
    strategy_path: str
    engine: str | None
    skip_signal_write: bool


@dataclass(frozen=True)
class StrategyCloneRequest:
    source_strategy_path: str
    target_filename: str
    strategy_name: str
    strategy_version: str
    display_name: str | None
    description: str | None
    updates: dict[str, Any]


@dataclass(frozen=True)
class StrategyRootRequest:
    root_path: str


@dataclass
class BacktestJob:
    job_id: str
    run_id: str
    strategy_id: str
    strategy_name: str
    display_name: str | None
    strategy_version: str
    engine: str
    interval: str | None
    data_version: str | None
    input_path: str
    skip_signal_write: bool
    status: str
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    strategy_path: str | None = None
    manifest_path: str | None = None
    signal_paths: list[str] = field(default_factory=list)
    trades_path: str | None = None
    equity_path: str | None = None
    summary_path: str | None = None
    metrics: dict[str, Any] | None = None
    error: str | None = None


@dataclass(frozen=True)
class ManualTradingPairRequest:
    source: str
    symbol: str
    quote_asset: str
    note: str | None


@dataclass(frozen=True)
class PairPreferenceEntry:
    kind: str
    key: str
    hidden: bool
    pinned: bool
    updated_at: str


@dataclass(frozen=True)
class PairPreferenceUpdateRequest:
    kind: str
    key: str
    hidden: bool | None
    pinned: bool | None


class DownloadJobConflictError(RuntimeError):
    """Raised when a conflicting background download job already exists."""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a local admin dashboard for market data and backtest management."
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help="Bind host.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Bind port.")
    parser.add_argument(
        "--raw-root",
        default=str(DEFAULT_RAW_ROOT),
        help="Local raw root, default: data/raw/binance/spot",
    )
    parser.add_argument(
        "--normalized-root",
        default=str(DEFAULT_NORMALIZED_ROOT),
        help="Normalized data root.",
    )
    parser.add_argument(
        "--strategies-root",
        default=str(DEFAULT_STRATEGIES_ROOT),
        help="Strategy config root.",
    )
    parser.add_argument(
        "--extra-strategy-root",
        action="append",
        default=None,
        help=(
            "Additional strategy config root. Can be repeated. "
            f"Also supports {EXTRA_STRATEGY_ROOTS_ENV} with os.pathsep-separated paths."
        ),
    )
    parser.add_argument(
        "--strategy-root-store",
        default=str(DEFAULT_STRATEGY_ROOT_STORE),
        help="Persistent JSON storage for UI-added external strategy roots.",
    )
    parser.add_argument(
        "--backtests-root",
        default=str(DEFAULT_BACKTESTS_ROOT),
        help="Backtest artifact root.",
    )
    parser.add_argument(
        "--logs-root",
        default=str(DEFAULT_LOGS_ROOT),
        help="Log directory root.",
    )
    parser.add_argument(
        "--local-trading-store",
        default=str(DEFAULT_LOCAL_TRADING_STORE),
        help="JSON storage path for manually added local trading pairs.",
    )
    parser.add_argument(
        "--pair-preferences-store",
        default=str(DEFAULT_PAIR_PREFERENCES_STORE),
        help="JSON storage path for pinned/hidden pair preferences.",
    )
    parser.add_argument(
        "--currency-icon-root",
        default=str(DEFAULT_CURRENCY_ICON_ROOT),
        help="Directory path for currency icon assets.",
    )
    parser.add_argument(
        "--exchange-info-base-url",
        default=DEFAULT_EXCHANGE_INFO_BASE_URL,
        help="Binance base URL used for exchangeInfo.",
    )
    parser.add_argument(
        "--market-data-base-url",
        default=DEFAULT_MARKET_DATA_BASE_URL,
        help="Binance market data base URL used for klines.",
    )
    parser.add_argument(
        "--fallback-base-urls",
        nargs="*",
        default=list(DEFAULT_KLINE_FALLBACK_BASE_URLS),
        help="Optional fallback Binance base URLs used for klines.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="HTTP timeout per request.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Retry count for remote source requests.",
    )
    parser.add_argument(
        "--retry-delay-seconds",
        type=float,
        default=1.0,
        help="Base delay before retry.",
    )
    parser.add_argument(
        "--retry-jitter-seconds",
        type=float,
        default=DEFAULT_RETRY_JITTER_SECONDS,
        help="Additional random jitter added to retry backoff.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=MAX_LIMIT,
        help="Page size per request. Binance spot klines max is 1000.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=DEFAULT_SLEEP_SECONDS,
        help="Sleep between pages to reduce burstiness.",
    )
    parser.add_argument(
        "--default-quote-asset",
        default=DEFAULT_QUOTE_ASSET,
        help="Default quote asset filter for remote pairs.",
    )
    parser.add_argument(
        "--default-tradeable-only",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_TRADEABLE_ONLY,
        help="Default tradeable filter for remote pairs.",
    )
    return parser.parse_args(argv)


def dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def parse_multi_value_query(values: list[str] | None) -> set[str] | None:
    if not values:
        return None

    parsed = {
        item.strip().upper()
        for raw_value in values
        for item in raw_value.split(",")
        if item.strip()
    }
    return parsed or None


def resolve_quote_asset_filter(
    values: list[str] | None,
    *,
    default_quote_asset: str,
) -> set[str] | None:
    parsed = parse_multi_value_query(values)
    if parsed is None:
        return {default_quote_asset.upper()} if default_quote_asset.strip() else None
    if "ALL" in parsed:
        return None
    return parsed


def parse_bool_query_value(value: str | None, *, default: bool) -> bool:
    if value is None or value.strip() == "":
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError("Boolean query must be true or false.")


def parse_bool_value(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return parse_bool_query_value(value, default=default)
    raise ValueError("Boolean field must be true or false.")


def parse_intervals_value(value: Any) -> list[str]:
    if value is None:
        return list(DEFAULT_INTERVALS)

    raw_items: list[str]
    if isinstance(value, str):
        raw_items = value.split(",")
    elif isinstance(value, list):
        raw_items = []
        for item in value:
            raw_items.extend(str(item).split(","))
    else:
        raise ValueError("intervals must be a comma-separated string or an array.")

    intervals = dedupe_preserve_order([item.strip().lower() for item in raw_items if item.strip()])
    if not intervals:
        raise ValueError("At least one interval is required.")
    return intervals


def isoformat_utc_from_timestamp(timestamp_seconds: float) -> str:
    return datetime.fromtimestamp(timestamp_seconds, tz=UTC).isoformat().replace("+00:00", "Z")


def normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace("/", "").replace("-", "").replace("_", "")


def load_index_html() -> bytes:
    return INDEX_HTML_PATH.read_bytes()


@lru_cache(maxsize=1)
def cached_index_html() -> bytes:
    return load_index_html()


def load_icon_svg() -> bytes:
    return ICON_SVG_PATH.read_bytes()


@lru_cache(maxsize=1)
def cached_icon_svg() -> bytes:
    return load_icon_svg()


def normalize_asset_code(asset: str) -> str:
    normalized = asset.strip().upper()
    if (
        not normalized
        or len(normalized) > 64
        or normalized in {".", ".."}
        or any(char.isspace() for char in normalized)
        or ASSET_INVALID_CHAR_PATTERN.search(normalized)
    ):
        raise ValueError("asset contains unsupported characters.")
    return normalized


def ensure_currency_icon_root(config: DashboardConfig) -> Path:
    config.currency_icon_root.mkdir(parents=True, exist_ok=True)
    return config.currency_icon_root


def currency_icon_catalog_path(config: DashboardConfig) -> Path:
    return config.currency_icon_root / CURRENCY_ICON_CATALOG_NAME


def currency_icon_default_path(config: DashboardConfig) -> Path:
    return config.currency_icon_root / f"{DEFAULT_CURRENCY_ICON_ASSET}.svg"


def normalize_mime_type(mime_type: Any) -> str | None:
    if mime_type is None:
        return None
    normalized = str(mime_type).strip().lower()
    if not normalized:
        return None
    return normalized.partition(";")[0]


def sniff_currency_icon_extension(payload: bytes) -> str | None:
    if payload.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    if payload.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    if len(payload) >= 12 and payload[:4] == b"RIFF" and payload[8:12] == b"WEBP":
        return ".webp"

    stripped = payload.lstrip()
    if stripped.startswith(b"\xef\xbb\xbf"):
        stripped = stripped[3:]
    if stripped.startswith(b"<") and b"<svg" in stripped[:512].lower():
        return ".svg"
    return None


def validate_svg_payload(payload: bytes) -> None:
    lowered = payload.decode("utf-8", errors="ignore").lower()
    if "<script" in lowered or "javascript:" in lowered or "onload=" in lowered:
        raise ValueError("svg content contains disallowed script payload.")


def guess_currency_icon_extension(
    *,
    filename: str | None,
    mime_type: str | None,
    payload: bytes,
) -> str:
    sniffed_extension = sniff_currency_icon_extension(payload)
    if sniffed_extension is not None:
        return sniffed_extension

    if filename:
        filename_extension = Path(filename).suffix.lower()
        if filename_extension in ALLOWED_CURRENCY_ICON_EXTENSIONS:
            return filename_extension

    normalized_mime_type = normalize_mime_type(mime_type)
    if normalized_mime_type is not None:
        for extension, allowed_mime_type in ALLOWED_CURRENCY_ICON_EXTENSIONS.items():
            if normalized_mime_type == allowed_mime_type:
                return extension

    raise ValueError("Unsupported icon format. Use SVG, PNG, WEBP, JPG or JPEG.")


def find_currency_icon_path(config: DashboardConfig, asset: str) -> Path | None:
    normalized_asset = normalize_asset_code(asset)
    root = ensure_currency_icon_root(config)
    for extension in ALLOWED_CURRENCY_ICON_EXTENSIONS:
        candidate = root / f"{normalized_asset}{extension}"
        if candidate.is_file():
            return candidate
    return None


def load_currency_icon_catalog(config: DashboardConfig) -> dict[str, Any]:
    path = currency_icon_catalog_path(config)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def iter_currency_icon_assets(config: DashboardConfig) -> tuple[list[str], set[str]]:
    root = ensure_currency_icon_root(config)
    catalog = load_currency_icon_catalog(config)

    catalog_assets: set[str] = set()
    raw_assets = catalog.get("assets", [])
    if isinstance(raw_assets, list):
        for raw_asset in raw_assets:
            try:
                catalog_assets.add(normalize_asset_code(str(raw_asset)))
            except ValueError:
                continue

    file_assets: set[str] = set()
    for path in sorted(root.iterdir(), key=lambda item: item.name):
        if (
            not path.is_file()
            or path.name.startswith(".")
            or path.name == CURRENCY_ICON_CATALOG_NAME
            or path.suffix.lower() not in ALLOWED_CURRENCY_ICON_EXTENSIONS
        ):
            continue
        asset = path.stem.upper()
        if asset == DEFAULT_CURRENCY_ICON_ASSET:
            continue
        try:
            file_assets.add(normalize_asset_code(asset))
        except ValueError:
            continue

    return sorted(catalog_assets | file_assets), catalog_assets


def describe_currency_icon_entry(
    config: DashboardConfig,
    *,
    asset: str,
    catalog_assets: set[str],
) -> dict[str, Any]:
    icon_path = find_currency_icon_path(config, asset)
    mime_type = None
    updated_at = None
    if icon_path is not None:
        mime_type = ALLOWED_CURRENCY_ICON_EXTENSIONS.get(icon_path.suffix.lower()) or mimetypes.guess_type(
            str(icon_path)
        )[0]
        updated_at = isoformat_utc_from_timestamp(icon_path.stat().st_mtime)

    return {
        "asset": asset,
        "exists": icon_path is not None,
        "cataloged": asset in catalog_assets,
        "extension": icon_path.suffix.lower().lstrip(".") if icon_path is not None else None,
        "mime_type": mime_type,
        "path": str(icon_path) if icon_path is not None else None,
        "icon_url": f"/currency-icons/{asset}",
        "updated_at": updated_at,
    }


def list_currency_icons(config: DashboardConfig) -> dict[str, Any]:
    root = ensure_currency_icon_root(config)
    catalog = load_currency_icon_catalog(config)
    assets, catalog_assets = iter_currency_icon_assets(config)
    entries = [
        describe_currency_icon_entry(config, asset=asset, catalog_assets=catalog_assets) for asset in assets
    ]
    available_count = sum(1 for entry in entries if entry["exists"])
    missing_assets = [entry["asset"] for entry in entries if not entry["exists"] and entry["cataloged"]]

    return {
        "root": str(root),
        "catalog_path": (
            str(currency_icon_catalog_path(config)) if currency_icon_catalog_path(config).is_file() else None
        ),
        "generated_at": catalog.get("generated_at"),
        "source_counts": catalog.get("source_counts", {}),
        "count": len(entries),
        "catalog_count": len(catalog_assets),
        "available_count": available_count,
        "missing_count": len(missing_assets),
        "missing_assets": missing_assets,
        "entries": entries,
        "fetched_at": utc_now_iso(),
    }


def parse_currency_icon_upload_payload(payload: Any) -> tuple[str, str | None, str | None, bytes]:
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object.")

    asset = normalize_asset_code(str(payload.get("asset", "")))
    filename = str(payload.get("filename", "")).strip() or None
    mime_type = normalize_mime_type(payload.get("mime_type"))
    content_base64 = str(payload.get("content_base64", "")).strip()
    if not content_base64:
        raise ValueError("content_base64 is required.")
    if content_base64.startswith("data:") and "," in content_base64:
        content_base64 = content_base64.split(",", 1)[1]

    try:
        content = base64.b64decode(content_base64, validate=True)
    except ValueError as exc:
        raise ValueError("content_base64 is not valid base64.") from exc

    if not content:
        raise ValueError("icon content is empty.")
    if len(content) > MAX_CURRENCY_ICON_BYTES:
        raise ValueError("icon file is too large. Max size is 2 MB.")

    extension = guess_currency_icon_extension(filename=filename, mime_type=mime_type, payload=content)
    if extension == ".svg":
        validate_svg_payload(content)
    return asset, filename, extension, content


def save_currency_icon(config: DashboardConfig, payload: Any) -> dict[str, Any]:
    asset, _, extension, content = parse_currency_icon_upload_payload(payload)
    root = ensure_currency_icon_root(config)
    for allowed_extension in ALLOWED_CURRENCY_ICON_EXTENSIONS:
        existing_path = root / f"{asset}{allowed_extension}"
        if existing_path.is_file():
            existing_path.unlink()

    icon_path = root / f"{asset}{extension}"
    icon_path.write_bytes(content)
    _, catalog_assets = iter_currency_icon_assets(config)
    return describe_currency_icon_entry(config, asset=asset, catalog_assets=catalog_assets)


def build_http_request(url: str, *, method: str, body: bytes | None = None) -> Request:
    headers = {
        "Accept": "application/json",
        "Connection": "close",
        "User-Agent": "donkey-admin/0.1",
    }
    if body is not None:
        headers["Content-Type"] = "application/json"
    return Request(url, data=body, headers=headers, method=method)


def fetch_json_request(
    request: Request,
    *,
    timeout_seconds: float,
    max_retries: int,
    retry_delay_seconds: float,
    retry_jitter_seconds: float,
    request_label: str,
) -> Any:
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except (URLError, TimeoutError, OSError, json.JSONDecodeError, HTTPError) as exc:
            last_error = exc
            if attempt >= max_retries:
                break
            time.sleep(retry_delay_seconds + retry_jitter_seconds * max(0, attempt))
    raise RuntimeError(f"{request_label} request failed: {last_error}")


def fetch_okx_pairs(
    *,
    timeout_seconds: float,
    max_retries: int,
    retry_delay_seconds: float,
    retry_jitter_seconds: float,
    allowed_quote_assets: set[str] | None,
    tradeable_only: bool,
) -> list[SourcePair]:
    url = "https://www.okx.com/api/v5/public/instruments?instType=SPOT"
    payload = fetch_json_request(
        build_http_request(url, method="GET"),
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_jitter_seconds=retry_jitter_seconds,
        request_label="okx instruments",
    )
    if not isinstance(payload, dict) or str(payload.get("code")) != "0":
        raise RuntimeError(f"Unexpected OKX response: {payload}")

    pairs: list[SourcePair] = []
    for item in payload.get("data", []):
        base_asset = str(item.get("baseCcy", "")).upper()
        quote_asset = str(item.get("quoteCcy", "")).upper()
        status = str(item.get("state", "")).upper()
        tradeable = status == "LIVE"
        if allowed_quote_assets is not None and quote_asset not in allowed_quote_assets:
            continue
        if tradeable_only and not tradeable:
            continue
        display_symbol = str(item.get("instId", "")).upper()
        symbol = display_symbol.replace("-", "")
        if not symbol:
            continue
        pairs.append(
            SourcePair(
                source="okx",
                source_label=SOURCE_LABELS["okx"],
                symbol=symbol,
                display_symbol=display_symbol,
                base_asset=base_asset,
                quote_asset=quote_asset,
                status=status or "UNKNOWN",
                tradeable=tradeable,
                source_kind="remote",
            )
        )
    return sorted(pairs, key=lambda pair: pair.symbol)


def fetch_bybit_pairs(
    *,
    timeout_seconds: float,
    max_retries: int,
    retry_delay_seconds: float,
    retry_jitter_seconds: float,
    allowed_quote_assets: set[str] | None,
    tradeable_only: bool,
) -> list[SourcePair]:
    cursor: str | None = None
    pairs: list[SourcePair] = []
    seen_cursors: set[str] = set()

    while True:
        params = {"category": "spot", "limit": "1000"}
        if cursor:
            params["cursor"] = cursor
        url = "https://api.bybit.com/v5/market/instruments-info?" + urlencode(params)
        payload = fetch_json_request(
            build_http_request(url, method="GET"),
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            retry_jitter_seconds=retry_jitter_seconds,
            request_label="bybit instruments",
        )
        if not isinstance(payload, dict) or int(payload.get("retCode", -1)) != 0:
            raise RuntimeError(f"Unexpected Bybit response: {payload}")

        result = payload.get("result", {})
        if not isinstance(result, dict):
            raise RuntimeError(f"Unexpected Bybit result: {payload}")

        for item in result.get("list", []):
            base_asset = str(item.get("baseCoin", "")).upper()
            quote_asset = str(item.get("quoteCoin", "")).upper()
            status = str(item.get("status", "")).upper()
            tradeable = status == "TRADING"
            if allowed_quote_assets is not None and quote_asset not in allowed_quote_assets:
                continue
            if tradeable_only and not tradeable:
                continue
            symbol = str(item.get("symbol", "")).upper()
            if not symbol:
                continue
            pairs.append(
                SourcePair(
                    source="bybit",
                    source_label=SOURCE_LABELS["bybit"],
                    symbol=symbol,
                    display_symbol=symbol,
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                    status=status or "UNKNOWN",
                    tradeable=tradeable,
                    source_kind="remote",
                )
            )

        next_cursor = str(result.get("nextPageCursor", "")).strip()
        if not next_cursor or next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    return sorted(pairs, key=lambda pair: pair.symbol)


def fetch_hl_pairs(
    *,
    timeout_seconds: float,
    max_retries: int,
    retry_delay_seconds: float,
    retry_jitter_seconds: float,
    allowed_quote_assets: set[str] | None,
    tradeable_only: bool,
) -> list[SourcePair]:
    payload = fetch_json_request(
        build_http_request(
            "https://api.hyperliquid.xyz/info",
            method="POST",
            body=json.dumps({"type": "spotMeta"}).encode("utf-8"),
        ),
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_jitter_seconds=retry_jitter_seconds,
        request_label="hl spotMeta",
    )
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected HL response: {payload}")

    tokens_raw = payload.get("tokens")
    universe_raw = payload.get("universe")
    if not isinstance(tokens_raw, list) or not isinstance(universe_raw, list):
        raise RuntimeError(f"Unexpected HL response shape: {payload}")

    token_names = {int(item.get("index")): str(item.get("name", "")).upper() for item in tokens_raw}
    pairs: list[SourcePair] = []
    for item in universe_raw:
        token_indexes = item.get("tokens", [])
        if not isinstance(token_indexes, list) or len(token_indexes) != 2:
            continue
        base_asset = token_names.get(int(token_indexes[0]), "")
        quote_asset = token_names.get(int(token_indexes[1]), "")
        tradeable = bool(item.get("isCanonical"))
        if allowed_quote_assets is not None and quote_asset not in allowed_quote_assets:
            continue
        if tradeable_only and not tradeable:
            continue
        display_symbol = str(item.get("name", "")).upper()
        if not display_symbol or display_symbol.startswith("@"):
            continue
        symbol = display_symbol.replace("/", "").replace("-", "")
        pairs.append(
            SourcePair(
                source="hl",
                source_label=SOURCE_LABELS["hl"],
                symbol=symbol,
                display_symbol=display_symbol,
                base_asset=base_asset,
                quote_asset=quote_asset,
                status="TRADING" if tradeable else "INACTIVE",
                tradeable=tradeable,
                source_kind="remote",
            )
        )
    return sorted(pairs, key=lambda pair: pair.symbol)


def fetch_binance_pairs(
    *,
    base_url: str,
    timeout_seconds: float,
    max_retries: int,
    retry_delay_seconds: float,
    allowed_statuses: set[str] | None = None,
    allowed_quote_assets: set[str] | None = None,
) -> list[SourcePair]:
    params = {
        "permissions": "SPOT",
        "showPermissionSets": "false",
    }
    payload = fetch_json_payload(
        build_candidate_urls(base_url, EXCHANGE_INFO_PATH, params),
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_jitter_seconds=0.0,
        request_label="admin exchangeInfo",
    )

    if not isinstance(payload, dict) or "symbols" not in payload:
        raise RuntimeError(f"Unexpected Binance response: {payload}")

    pairs: list[SourcePair] = []
    for raw_symbol in payload["symbols"]:
        symbol = str(raw_symbol.get("symbol", "")).upper()
        status = str(raw_symbol.get("status", "")).upper()
        base_asset = str(raw_symbol.get("baseAsset", "")).upper()
        quote_asset = str(raw_symbol.get("quoteAsset", "")).upper()
        tradeable = status == "TRADING"
        if not symbol:
            continue
        if allowed_statuses is not None and status not in allowed_statuses:
            continue
        if allowed_quote_assets is not None and quote_asset not in allowed_quote_assets:
            continue
        pairs.append(
            SourcePair(
                source="binance",
                source_label=SOURCE_LABELS["binance"],
                symbol=symbol,
                display_symbol=symbol,
                base_asset=base_asset,
                quote_asset=quote_asset,
                status=status,
                tradeable=tradeable,
                source_kind="remote",
            )
        )
    return sorted(pairs, key=lambda pair: pair.symbol)


def fetch_source_pairs(
    *,
    source: str,
    config: DashboardConfig,
    allowed_quote_assets: set[str] | None,
    tradeable_only: bool,
) -> list[SourcePair]:
    normalized_source = source.strip().lower()
    if normalized_source == "binance":
        allowed_statuses = {"TRADING"} if tradeable_only else None
        return fetch_binance_pairs(
            base_url=config.exchange_info_base_url,
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_retries,
            retry_delay_seconds=config.retry_delay_seconds,
            allowed_statuses=allowed_statuses,
            allowed_quote_assets=allowed_quote_assets,
        )
    if normalized_source == "okx":
        return fetch_okx_pairs(
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_retries,
            retry_delay_seconds=config.retry_delay_seconds,
            retry_jitter_seconds=config.retry_jitter_seconds,
            allowed_quote_assets=allowed_quote_assets,
            tradeable_only=tradeable_only,
        )
    if normalized_source == "bybit":
        return fetch_bybit_pairs(
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_retries,
            retry_delay_seconds=config.retry_delay_seconds,
            retry_jitter_seconds=config.retry_jitter_seconds,
            allowed_quote_assets=allowed_quote_assets,
            tradeable_only=tradeable_only,
        )
    if normalized_source == "hl":
        return fetch_hl_pairs(
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_retries,
            retry_delay_seconds=config.retry_delay_seconds,
            retry_jitter_seconds=config.retry_jitter_seconds,
            allowed_quote_assets=allowed_quote_assets,
            tradeable_only=tradeable_only,
        )
    raise ValueError(f"Unsupported source: {source}")


def resolve_source_raw_root(config: DashboardConfig, source: str) -> Path:
    normalized_source = source.strip().lower()
    if normalized_source not in SOURCE_LABELS:
        raise ValueError(f"Unsupported source: {source}")
    if normalized_source == "binance":
        return config.raw_root
    return (config.workspace_root / "data" / "raw" / normalized_source / "spot").resolve()


def discover_local_pairs(raw_root: Path, *, source: str = "binance") -> list[LocalPair]:
    normalized_source = source.strip().lower()
    if normalized_source not in SOURCE_LABELS:
        raise ValueError(f"Unsupported source: {source}")
    if not raw_root.exists() or not raw_root.is_dir():
        return []

    pairs: list[LocalPair] = []
    for symbol_dir in sorted(raw_root.iterdir(), key=lambda path: path.name):
        if not symbol_dir.is_dir() or symbol_dir.name.startswith("."):
            continue

        intervals: list[str] = []
        data_file_count = 0
        metadata_file_count = 0
        checkpoint_count = 0
        last_updated: float | None = None

        for interval_dir in sorted(symbol_dir.iterdir(), key=lambda path: path.name):
            if not interval_dir.is_dir() or interval_dir.name.startswith("."):
                continue

            interval_has_files = False
            for file_path in sorted(interval_dir.iterdir(), key=lambda path: path.name):
                if not file_path.is_file():
                    continue
                interval_has_files = True
                file_stat = file_path.stat()
                if last_updated is None or file_stat.st_mtime > last_updated:
                    last_updated = file_stat.st_mtime
                if file_path.name.endswith(".jsonl"):
                    data_file_count += 1
                elif file_path.name.endswith(".meta.json"):
                    metadata_file_count += 1
                elif file_path.name == "_checkpoint.json":
                    checkpoint_count += 1

            if interval_has_files:
                intervals.append(interval_dir.name)

        if not intervals:
            continue

        display_symbol = symbol_dir.name.upper()
        pairs.append(
            LocalPair(
                source=normalized_source,
                source_label=SOURCE_LABELS[normalized_source],
                symbol=normalize_symbol(display_symbol),
                display_symbol=display_symbol,
                root=str(raw_root),
                intervals=intervals,
                interval_count=len(intervals),
                data_file_count=data_file_count,
                metadata_file_count=metadata_file_count,
                checkpoint_count=checkpoint_count,
                last_updated=(
                    isoformat_utc_from_timestamp(last_updated) if last_updated is not None else None
                ),
            )
        )

    return sorted(pairs, key=lambda pair: (pair.source, pair.symbol))


def discover_local_pairs_catalog(
    config: DashboardConfig,
    *,
    source: str | None = None,
) -> tuple[list[LocalPair], dict[str, dict[str, Any]]]:
    sources = [source] if source is not None else list(SOURCE_ORDER)
    pairs: list[LocalPair] = []
    roots: dict[str, dict[str, Any]] = {}
    for source_name in sources:
        raw_root = resolve_source_raw_root(config, source_name)
        source_pairs = discover_local_pairs(raw_root, source=source_name)
        roots[source_name] = {
            "source": source_name,
            "source_label": SOURCE_LABELS[source_name],
            "path": str(raw_root),
            "exists": raw_root.exists(),
            "count": len(source_pairs),
        }
        pairs.extend(source_pairs)
    return sorted(pairs, key=lambda pair: (pair.source, pair.symbol)), roots


def parse_json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def resolve_charting_library_asset_path(
    relative_path: str,
    *,
    root: Path = CHARTING_LIBRARY_STATIC_ROOT,
) -> Path | None:
    normalized = relative_path.strip().lstrip("/")
    if not normalized:
        return None

    root_path = root.resolve()
    candidate = (root_path / normalized).resolve()
    try:
        candidate.relative_to(root_path)
    except ValueError:
        return None
    return candidate


def charting_library_asset_exists(path: Path) -> bool:
    try:
        return path.is_file() and path.stat().st_size > 0
    except OSError:
        return False


def charting_library_asset_url(relative_path: str) -> str:
    normalized = relative_path.strip().lstrip("/")
    return f"{CHARTING_LIBRARY_HOSTED_BASE_URL}{quote(normalized, safe='/._-')}"


def fetch_charting_library_asset(
    relative_path: str,
    *,
    root: Path = CHARTING_LIBRARY_STATIC_ROOT,
    timeout_seconds: float = CHARTING_LIBRARY_FETCH_TIMEOUT_SECONDS,
) -> Path:
    target_path = resolve_charting_library_asset_path(relative_path, root=root)
    if target_path is None:
        raise ValueError("Invalid Charting Library asset path.")

    if charting_library_asset_exists(target_path):
        return target_path

    request = Request(
        charting_library_asset_url(relative_path),
        headers={"User-Agent": "donkey-admin/1.0"},
    )

    with CHARTING_LIBRARY_FETCH_LOCK:
        if charting_library_asset_exists(target_path):
            return target_path

        target_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = target_path.with_name(f".{target_path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                payload = response.read()
            temp_path.write_bytes(payload)
            temp_path.replace(target_path)
        except Exception:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except OSError:
                pass
            raise

    return target_path


def charting_library_status() -> dict[str, Any]:
    return {
        "root": str(CHARTING_LIBRARY_STATIC_ROOT),
        "bundle_path": str(CHARTING_LIBRARY_BUNDLE_PATH),
        "sameorigin_path": str(CHARTING_LIBRARY_SAMEORIGIN_PATH),
        "bundle_exists": charting_library_asset_exists(CHARTING_LIBRARY_BUNDLE_PATH),
        "sameorigin_exists": charting_library_asset_exists(CHARTING_LIBRARY_SAMEORIGIN_PATH),
        "auto_cache_enabled": True,
        "fetched_at": utc_now_iso(),
    }


def discover_normalized_datasets(config: DashboardConfig) -> dict[str, Any]:
    versions: list[dict[str, Any]] = []
    files: list[dict[str, Any]] = []
    if not config.normalized_root.exists():
        return {
            "normalized_root": str(config.normalized_root),
            "count": 0,
            "version_count": 0,
            "versions": versions,
            "files": files,
            "charting_library": charting_library_status(),
            "fetched_at": utc_now_iso(),
        }

    for version_dir in sorted(config.normalized_root.iterdir(), key=lambda path: path.name):
        if not version_dir.is_dir() or version_dir.name.startswith("."):
            continue

        data_version = version_dir.name
        manifest_path = version_dir / "normalize_manifest.json"
        manifest = parse_json_file(manifest_path)
        manifest_outputs: dict[str, dict[str, Any]] = {}
        if isinstance(manifest, dict):
            for item in manifest.get("interval_outputs", []):
                if not isinstance(item, dict):
                    continue
                interval = str(item.get("interval", "")).strip()
                if interval:
                    manifest_outputs[interval] = item

        normalized_files = warehouse_load_duckdb.discover_normalized_files(
            config.normalized_root,
            data_version=data_version,
            intervals=None,
        )
        if not normalized_files and not manifest_path.exists():
            continue

        version_intervals: list[str] = []
        version_formats: list[str] = []
        version_updated_at: float | None = None
        version_row_count = 0

        for normalized_file in normalized_files:
            file_stat = normalized_file.path.stat()
            if version_updated_at is None or file_stat.st_mtime > version_updated_at:
                version_updated_at = file_stat.st_mtime

            manifest_output = manifest_outputs.get(normalized_file.interval, {})
            row_count = manifest_output.get("row_count")
            if isinstance(row_count, int):
                version_row_count += row_count

            files.append(
                {
                    "data_version": data_version,
                    "interval": normalized_file.interval,
                    "file_format": normalized_file.file_format,
                    "path": str(normalized_file.path),
                    "size_bytes": int(file_stat.st_size),
                    "updated_at": isoformat_utc_from_timestamp(file_stat.st_mtime),
                    "row_count": row_count if isinstance(row_count, int) else None,
                    "source_file_count": (
                        int(manifest_output.get("source_file_count"))
                        if isinstance(manifest_output.get("source_file_count"), int)
                        else None
                    ),
                    "duplicate_rows_removed": (
                        int(manifest_output.get("duplicate_rows_removed"))
                        if isinstance(manifest_output.get("duplicate_rows_removed"), int)
                        else None
                    ),
                }
            )
            version_intervals.append(normalized_file.interval)
            version_formats.append(normalized_file.file_format)

        if version_updated_at is None and manifest_path.exists():
            version_updated_at = manifest_path.stat().st_mtime

        versions.append(
            {
                "data_version": data_version,
                "path": str(version_dir),
                "file_count": len(normalized_files),
                "intervals": dedupe_preserve_order(version_intervals),
                "formats": dedupe_preserve_order(version_formats),
                "row_count": version_row_count or None,
                "manifest_path": str(manifest_path) if manifest_path.exists() else None,
                "manifest_created_at": (
                    str(manifest.get("created_at")) if isinstance(manifest, dict) else None
                ),
                "raw_file_count": (
                    int(manifest.get("raw_file_count"))
                    if isinstance(manifest, dict) and isinstance(manifest.get("raw_file_count"), int)
                    else None
                ),
                "output_format": (
                    str(manifest.get("output_format"))
                    if isinstance(manifest, dict) and manifest.get("output_format") is not None
                    else None
                ),
                "updated_at": (
                    isoformat_utc_from_timestamp(version_updated_at)
                    if version_updated_at is not None
                    else None
                ),
            }
        )

    return {
        "normalized_root": str(config.normalized_root),
        "count": len(files),
        "version_count": len(versions),
        "versions": versions,
        "files": files,
        "charting_library": charting_library_status(),
        "fetched_at": utc_now_iso(),
    }


def parse_symbols_value(value: Any) -> list[str]:
    if value is None or value == "" or value == []:
        return []

    raw_items: list[str]
    if isinstance(value, str):
        raw_items = value.split(",")
    elif isinstance(value, list):
        raw_items = []
        for item in value:
            raw_items.extend(str(item).split(","))
    else:
        raise ValueError("symbols must be a comma-separated string or an array.")

    return dedupe_preserve_order(
        [normalize_symbol(item) for item in raw_items if str(item).strip()]
    )


def parse_normalize_request_payload(payload: Any) -> NormalizeRequest:
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object.")

    source = str(payload.get("source", "binance")).strip().lower() or "binance"
    if source not in SUPPORTED_NORMALIZE_SOURCES:
        raise ValueError("normalize currently supports: binance.")

    symbol_value = payload.get("symbol")
    symbols = (
        [normalize_symbol(str(symbol_value))]
        if symbol_value not in {None, ""}
        else parse_symbols_value(payload.get("symbols"))
    )

    data_version = str(payload.get("data_version", "")).strip()
    if not data_version:
        raise ValueError("data_version is required.")

    output_format = str(payload.get("output_format", DEFAULT_NORMALIZE_OUTPUT_FORMAT)).strip().lower()
    if output_format not in normalized_market_ohlcv.SUPPORTED_OUTPUT_FORMATS:
        raise ValueError(
            "output_format must be one of: "
            + ", ".join(normalized_market_ohlcv.SUPPORTED_OUTPUT_FORMATS)
            + "."
        )

    return NormalizeRequest(
        source=source,
        symbols=symbols,
        intervals=parse_intervals_value(payload.get("intervals")),
        data_version=data_version,
        output_format=output_format,
    )


def parse_duckdb_load_request_payload(payload: Any) -> DuckDBLoadRequest:
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object.")

    data_version = str(payload.get("data_version", "")).strip()
    if not data_version:
        raise ValueError("data_version is required.")

    intervals_value = payload.get("intervals")
    intervals = None
    if intervals_value is not None and intervals_value != "" and intervals_value != []:
        intervals = parse_intervals_value(intervals_value)

    return DuckDBLoadRequest(data_version=data_version, intervals=intervals)


def timestamp_seconds_to_datetime(value: Any) -> datetime:
    try:
        numeric = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("timestamp filter must be an integer unix timestamp.") from exc
    return datetime.fromtimestamp(numeric, tz=UTC)


def parse_positive_int(value: Any, *, field_name: str, default: int, max_value: int) -> int:
    if value in {None, ""}:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer.") from exc
    if parsed <= 0:
        raise ValueError(f"{field_name} must be positive.")
    return min(parsed, max_value)


def duckdb_table_exists(connection: Any, *, table_name: str) -> bool:
    result = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(result and int(result[0]) > 0)


def duckdb_connection(config: DashboardConfig, *, read_only: bool = True) -> Any:
    duckdb = warehouse_load_duckdb.import_duckdb(repo_root=config.workspace_root)
    if read_only and not config.quant_db_path.exists():
        raise FileNotFoundError(f"DuckDB file does not exist: {config.quant_db_path}")
    return duckdb.connect(str(config.quant_db_path), read_only=read_only)


def query_duckdb_overview(config: DashboardConfig) -> dict[str, Any]:
    overview: dict[str, Any] = {
        "db_path": str(config.quant_db_path),
        "db_exists": config.quant_db_path.exists(),
        "duckdb_available": False,
        "table_exists": False,
        "total_rows": 0,
        "versions": [],
        "symbol_rows": [],
        "symbols": [],
        "intervals": [],
        "charting_library": charting_library_status(),
        "fetched_at": utc_now_iso(),
    }

    try:
        connection = duckdb_connection(config, read_only=True)
    except FileNotFoundError:
        return overview
    except RuntimeError as exc:
        overview["error"] = str(exc)
        return overview

    overview["duckdb_available"] = True
    try:
        if not duckdb_table_exists(connection, table_name="market_ohlcv"):
            return overview

        overview["table_exists"] = True
        total_rows = connection.execute("SELECT COUNT(*) FROM market_ohlcv").fetchone()
        overview["total_rows"] = int(total_rows[0]) if total_rows is not None else 0

        version_rows = connection.execute(
            """
            SELECT
                data_version,
                interval,
                COUNT(*) AS row_count,
                COUNT(DISTINCT symbol) AS symbol_count,
                MIN(ts) AS first_ts,
                MAX(ts) AS last_ts
            FROM market_ohlcv
            GROUP BY data_version, interval
            ORDER BY data_version, interval
            """
        ).fetchall()
        overview["versions"] = [
            {
                "data_version": str(row[0]),
                "interval": str(row[1]),
                "row_count": int(row[2]),
                "symbol_count": int(row[3]),
                "first_ts": (
                    row[4].replace(tzinfo=UTC).isoformat().replace("+00:00", "Z")
                    if row[4] is not None and getattr(row[4], "tzinfo", None) is None
                    else row[4].astimezone(UTC).isoformat().replace("+00:00", "Z")
                    if row[4] is not None
                    else None
                ),
                "last_ts": (
                    row[5].replace(tzinfo=UTC).isoformat().replace("+00:00", "Z")
                    if row[5] is not None and getattr(row[5], "tzinfo", None) is None
                    else row[5].astimezone(UTC).isoformat().replace("+00:00", "Z")
                    if row[5] is not None
                    else None
                ),
            }
            for row in version_rows
        ]

        distinct_symbol_rows = connection.execute(
            """
            SELECT DISTINCT symbol
            FROM market_ohlcv
            ORDER BY symbol
            """
        ).fetchall()
        interval_rows = connection.execute(
            """
            SELECT DISTINCT interval
            FROM market_ohlcv
            ORDER BY interval
            """
        ).fetchall()
        symbol_detail_rows = connection.execute(
            """
            SELECT
                data_version,
                interval,
                symbol,
                MIN(exchange) AS exchange,
                MIN(market_type) AS market_type,
                COUNT(*) AS row_count,
                MIN(ts) AS first_ts,
                MAX(ts) AS last_ts
            FROM market_ohlcv
            GROUP BY data_version, interval, symbol
            ORDER BY data_version, interval, symbol
            """
        ).fetchall()
        overview["symbols"] = [str(row[0]) for row in distinct_symbol_rows]
        overview["intervals"] = [str(row[0]) for row in interval_rows]
        overview["symbol_rows"] = [
            {
                "data_version": str(row[0]),
                "interval": str(row[1]),
                "symbol": str(row[2]),
                "exchange": str(row[3]),
                "market_type": str(row[4]),
                "row_count": int(row[5]),
                "first_ts": (
                    row[6].replace(tzinfo=UTC).isoformat().replace("+00:00", "Z")
                    if row[6] is not None and getattr(row[6], "tzinfo", None) is None
                    else row[6].astimezone(UTC).isoformat().replace("+00:00", "Z")
                    if row[6] is not None
                    else None
                ),
                "last_ts": (
                    row[7].replace(tzinfo=UTC).isoformat().replace("+00:00", "Z")
                    if row[7] is not None and getattr(row[7], "tzinfo", None) is None
                    else row[7].astimezone(UTC).isoformat().replace("+00:00", "Z")
                    if row[7] is not None
                    else None
                ),
            }
            for row in symbol_detail_rows
        ]
        return overview
    finally:
        connection.close()


def query_duckdb_symbol_catalog(
    config: DashboardConfig,
    *,
    data_version: str | None,
    interval: str | None,
) -> dict[str, Any]:
    try:
        connection = duckdb_connection(config, read_only=True)
    except (FileNotFoundError, RuntimeError) as exc:
        return {
            "count": 0,
            "symbols": [],
            "data_version": data_version,
            "interval": interval,
            "error": str(exc),
            "fetched_at": utc_now_iso(),
        }

    try:
        if not duckdb_table_exists(connection, table_name="market_ohlcv"):
            return {
                "count": 0,
                "symbols": [],
                "data_version": data_version,
                "interval": interval,
                "fetched_at": utc_now_iso(),
            }

        conditions: list[str] = []
        params: list[Any] = []
        if data_version:
            conditions.append("data_version = ?")
            params.append(data_version)
        if interval:
            conditions.append("interval = ?")
            params.append(interval)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = connection.execute(
            f"""
            SELECT
                symbol,
                MIN(exchange) AS exchange,
                MIN(market_type) AS market_type,
                COUNT(*) AS row_count,
                MIN(ts) AS first_ts,
                MAX(ts) AS last_ts
            FROM market_ohlcv
            {where_clause}
            GROUP BY symbol
            ORDER BY symbol
            """,
            params,
        ).fetchall()

        symbols = []
        for row in rows:
            first_ts = row[4]
            last_ts = row[5]
            if first_ts is not None and getattr(first_ts, "tzinfo", None) is None:
                first_ts = first_ts.replace(tzinfo=UTC)
            if last_ts is not None and getattr(last_ts, "tzinfo", None) is None:
                last_ts = last_ts.replace(tzinfo=UTC)
            symbols.append(
                {
                    "symbol": str(row[0]),
                    "exchange": str(row[1]),
                    "market_type": str(row[2]),
                    "row_count": int(row[3]),
                    "first_ts": (
                        first_ts.astimezone(UTC).isoformat().replace("+00:00", "Z")
                        if first_ts is not None
                        else None
                    ),
                    "last_ts": (
                        last_ts.astimezone(UTC).isoformat().replace("+00:00", "Z")
                        if last_ts is not None
                        else None
                    ),
                }
            )

        return {
            "count": len(symbols),
            "symbols": symbols,
            "data_version": data_version,
            "interval": interval,
            "fetched_at": utc_now_iso(),
        }
    finally:
        connection.close()


def query_market_bars(
    config: DashboardConfig,
    *,
    symbol: str,
    interval: str,
    data_version: str,
    from_ts: int | None,
    to_ts: int | None,
    limit: int,
) -> dict[str, Any]:
    connection = duckdb_connection(config, read_only=True)
    try:
        if not duckdb_table_exists(connection, table_name="market_ohlcv"):
            raise ValueError("market_ohlcv table does not exist.")

        conditions = [
            "symbol = ?",
            "interval = ?",
            "data_version = ?",
        ]
        params: list[Any] = [symbol, interval, data_version]
        if from_ts is not None:
            conditions.append("ts >= ?")
            params.append(timestamp_seconds_to_datetime(from_ts))
        if to_ts is not None:
            conditions.append("ts <= ?")
            params.append(timestamp_seconds_to_datetime(to_ts))

        rows = connection.execute(
            f"""
            SELECT ts, open, high, low, close, volume
            FROM market_ohlcv
            WHERE {' AND '.join(conditions)}
            ORDER BY ts
            LIMIT ?
            """,
            params + [limit],
        ).fetchall()

        bars: list[dict[str, Any]] = []
        for row in rows:
            ts_value = row[0]
            if getattr(ts_value, "tzinfo", None) is None:
                ts_value = ts_value.replace(tzinfo=UTC)
            bars.append(
                {
                    "time": int(ts_value.timestamp() * 1000),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                }
            )

        return {
            "symbol": symbol,
            "interval": interval,
            "data_version": data_version,
            "count": len(bars),
            "bars": bars,
            "fetched_at": utc_now_iso(),
        }
    finally:
        connection.close()


def next_relevant_line(lines: list[str], start_index: int) -> tuple[str | None, int | None]:
    for line in lines[start_index:]:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        return stripped, len(line) - len(line.lstrip(" "))
    return None, None


def parse_yaml_scalar(value: str) -> Any:
    normalized = value.strip()
    if normalized in {"null", "Null", "NULL", "~"}:
        return None
    if normalized in {"true", "True", "TRUE"}:
        return True
    if normalized in {"false", "False", "FALSE"}:
        return False
    if len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in {"'", '"'}:
        return normalized[1:-1]
    try:
        return int(normalized)
    except ValueError:
        pass
    try:
        return float(normalized)
    except ValueError:
        pass
    return normalized


def parse_simple_yaml_file(path: Path) -> dict[str, Any]:
    lines = path.read_text(encoding="utf-8").splitlines()
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any] | list[Any]]] = [(-1, root)]

    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(line) - len(line.lstrip(" "))
        while len(stack) > 1 and indent <= stack[-1][0]:
            stack.pop()
        container = stack[-1][1]

        if stripped.startswith("- "):
            if not isinstance(container, list):
                raise ValueError(f"Unexpected list item in {path}: {line}")
            container.append(parse_yaml_scalar(stripped[2:]))
            continue

        if ":" not in stripped:
            continue

        key, _, raw_value = stripped.partition(":")
        key = key.strip()
        value = raw_value.strip()
        if not isinstance(container, dict):
            raise ValueError(f"Unexpected mapping item in {path}: {line}")

        if value == "":
            next_line, next_indent = next_relevant_line(lines, index + 1)
            if next_line is not None and next_indent is not None and next_indent > indent:
                child: dict[str, Any] | list[Any]
                child = [] if next_line.startswith("- ") else {}
            else:
                child = {}
            container[key] = child
            stack.append((indent, child))
            continue

        container[key] = parse_yaml_scalar(value)

    return root


def yaml_scalar_to_text(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    text = str(value)
    if text == "":
        return '""'
    if text.strip() != text:
        return json.dumps(text, ensure_ascii=False)
    if any(char in text for char in {":", "#", "\n", "\r", "\t"}):
        return json.dumps(text, ensure_ascii=False)
    if text.lower() in {"null", "true", "false", "~"}:
        return json.dumps(text, ensure_ascii=False)
    try:
        int(text)
        return json.dumps(text, ensure_ascii=False)
    except ValueError:
        pass
    try:
        float(text)
        return json.dumps(text, ensure_ascii=False)
    except ValueError:
        pass
    return text


def is_yaml_scalar_value(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def dump_simple_yaml_lines(value: Any, *, indent: int = 0) -> list[str]:
    prefix = " " * indent
    if isinstance(value, dict):
        lines: list[str] = []
        for key, item in value.items():
            if isinstance(item, dict):
                lines.append(f"{prefix}{key}:")
                lines.extend(dump_simple_yaml_lines(item, indent=indent + 2))
            elif isinstance(item, list):
                lines.append(f"{prefix}{key}:")
                if not item:
                    continue
                for entry in item:
                    if not is_yaml_scalar_value(entry):
                        raise ValueError("Only lists of scalar values are supported when saving strategy configs.")
                    lines.append(f"{prefix}  - {yaml_scalar_to_text(entry)}")
            else:
                lines.append(f"{prefix}{key}: {yaml_scalar_to_text(item)}")
        return lines
    raise ValueError("Top-level YAML payload must be a mapping.")


def dump_simple_yaml(value: dict[str, Any]) -> str:
    return "\n".join(dump_simple_yaml_lines(value)) + "\n"


def format_editor_label(value: str) -> str:
    return value.replace("_", " ").strip().title()


def infer_editor_field_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "number"
    if isinstance(value, list):
        return "list_string"
    return "string"


def is_supported_editor_list(value: Any) -> bool:
    return isinstance(value, list) and all(is_yaml_scalar_value(item) for item in value)


def flatten_editor_fields(
    value: dict[str, Any],
    *,
    prefix: str,
) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for key, item in value.items():
        field_path = f"{prefix}.{key}" if prefix else key
        if isinstance(item, dict):
            fields.extend(flatten_editor_fields(item, prefix=field_path))
            continue
        if not is_yaml_scalar_value(item) and not is_supported_editor_list(item):
            continue
        fields.append(
            {
                "path": field_path,
                "key": key,
                "label": format_editor_label(key),
                "type": infer_editor_field_type(item),
                "value": item,
            }
        )
    return fields


def editor_section_title(name: str) -> str:
    mapping = {
        "universe": "Universe",
        "data": "Data",
        "signal": "Signal",
        "execution": "Execution",
        "risk": "Risk",
        "backtest": "Backtest",
    }
    return mapping.get(name, format_editor_label(name))


def sanitize_strategy_filename_component(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return sanitized.strip("._-") or "strategy"


def clone_version_candidates(base_version: str) -> list[str]:
    normalized = base_version.strip() or "v1"
    candidates = [f"{normalized}_cal"]
    candidates.extend(f"{normalized}_cal{index}" for index in range(2, 20))
    return candidates


def build_strategy_clone_defaults(config: DashboardConfig, entry: StrategyEntry) -> dict[str, Any]:
    existing_ids = {item.strategy_id for item in discover_strategy_entries(config)}
    selected_version = f"{entry.strategy_version}_cal"
    selected_filename = (
        f"{sanitize_strategy_filename_component(entry.strategy_name)}_"
        f"{sanitize_strategy_filename_component(selected_version)}.yaml"
    )
    for candidate_version in clone_version_candidates(entry.strategy_version):
        candidate_id = strategy_identifier(entry.strategy_name, candidate_version)
        candidate_filename = (
            f"{sanitize_strategy_filename_component(entry.strategy_name)}_"
            f"{sanitize_strategy_filename_component(candidate_version)}.yaml"
        )
        if candidate_id not in existing_ids and not (Path(entry.strategy_root).resolve() / candidate_filename).exists():
            selected_version = candidate_version
            selected_filename = candidate_filename
            break

    source_display_name = entry.display_name or entry.strategy_name
    clone_display_name = source_display_name if source_display_name.endswith("调参版") else f"{source_display_name} 调参版"
    return {
        "strategy_name": entry.strategy_name,
        "strategy_version": selected_version,
        "display_name": clone_display_name,
        "description": entry.description,
        "target_filename": selected_filename,
        "target_root": entry.strategy_root,
        "engine": entry.configured_engine or "native",
    }


def build_strategy_config_payload(config: DashboardConfig, strategy_path: str) -> dict[str, Any]:
    entry = find_strategy_entry_by_path(config, strategy_path)
    if entry is None:
        raise ValueError(f"Strategy not found: {strategy_path}")

    parsed = parse_simple_yaml_file(Path(entry.strategy_path))
    editable_sections: list[dict[str, Any]] = []
    section_order = ["universe", "data", "signal", "execution", "risk", "backtest"]
    included: set[str] = set()
    for section_name in section_order:
        section_value = parsed.get(section_name)
        if not isinstance(section_value, dict):
            continue
        fields = flatten_editor_fields(section_value, prefix=section_name)
        if not fields:
            continue
        editable_sections.append(
            {
                "name": section_name,
                "title": editor_section_title(section_name),
                "fields": fields,
            }
        )
        included.add(section_name)

    for section_name, section_value in parsed.items():
        if section_name in included or section_name in {"module", "artifacts"}:
            continue
        if not isinstance(section_value, dict):
            continue
        fields = flatten_editor_fields(section_value, prefix=section_name)
        if not fields:
            continue
        editable_sections.append(
            {
                "name": str(section_name),
                "title": editor_section_title(str(section_name)),
                "fields": fields,
            }
        )

    artifacts = parsed.get("artifacts", {}) if isinstance(parsed.get("artifacts"), dict) else {}
    module = parsed.get("module", {}) if isinstance(parsed.get("module"), dict) else {}
    return {
        "strategy": asdict(entry),
        "clone_defaults": build_strategy_clone_defaults(config, entry),
        "editable_sections": editable_sections,
        "readonly": {
            "module_path": module.get("path"),
            "factory_name": module.get("factory_name"),
            "signal_path": artifacts.get("signal_path"),
            "trades_path": artifacts.get("trades_path"),
            "equity_path": artifacts.get("equity_path"),
            "summary_path": artifacts.get("summary_path"),
        },
        "fetched_at": utc_now_iso(),
    }


def set_dotted_value(container: dict[str, Any], path: str, value: Any) -> None:
    keys = [item for item in path.split(".") if item]
    if not keys:
        raise ValueError("Field path must not be empty.")
    current = container
    for key in keys[:-1]:
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]
    current[keys[-1]] = value


def build_artifact_ext(path_value: Any, default_ext: str) -> str:
    if path_value is None:
        return default_ext
    suffix = Path(str(path_value)).suffix
    return suffix or default_ext


def normalize_clone_artifact_paths(
    strategy_config: dict[str, Any],
    *,
    strategy_id: str,
    interval: str | None,
    source_artifacts: dict[str, Any],
) -> None:
    artifacts = strategy_config.setdefault("artifacts", {})
    signal_ext = build_artifact_ext(source_artifacts.get("signal_path"), ".jsonl")
    trades_ext = build_artifact_ext(source_artifacts.get("trades_path"), ".jsonl")
    equity_ext = build_artifact_ext(source_artifacts.get("equity_path"), ".jsonl")
    summary_ext = build_artifact_ext(source_artifacts.get("summary_path"), ".json")
    interval_token = interval or "bars"
    artifacts["signal_path"] = f"data/signals/{strategy_id}/{{symbol}}_{interval_token}_signal{signal_ext}"
    artifacts["trades_path"] = f"data/backtests/{strategy_id}/trades{trades_ext}"
    artifacts["equity_path"] = f"data/backtests/{strategy_id}/portfolio_equity{equity_ext}"
    artifacts["summary_path"] = f"data/backtests/{strategy_id}/summary{summary_ext}"


def parse_strategy_clone_payload(payload: Any) -> StrategyCloneRequest:
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object.")

    source_strategy_path = str(payload.get("source_strategy_path", "")).strip()
    if not source_strategy_path:
        raise ValueError("source_strategy_path is required.")

    target_filename = str(payload.get("target_filename", "")).strip()
    if not target_filename:
        raise ValueError("target_filename is required.")
    if not STRATEGY_CLONE_FILENAME_PATTERN.fullmatch(target_filename):
        raise ValueError("target_filename must be a simple .yaml filename.")

    strategy_name = str(payload.get("strategy_name", "")).strip()
    if not strategy_name:
        raise ValueError("strategy_name is required.")

    strategy_version = str(payload.get("strategy_version", "")).strip()
    if not strategy_version:
        raise ValueError("strategy_version is required.")

    display_name = payload.get("display_name")
    normalized_display_name = str(display_name).strip() if display_name is not None else None
    if normalized_display_name == "":
        normalized_display_name = None

    description = payload.get("description")
    normalized_description = str(description).strip() if description is not None else None
    if normalized_description == "":
        normalized_description = None

    raw_updates = payload.get("updates", {})
    if not isinstance(raw_updates, dict):
        raise ValueError("updates must be an object.")
    updates: dict[str, Any] = {}
    for raw_path, value in raw_updates.items():
        path = str(raw_path).strip()
        if not path or not STRATEGY_FIELD_PATH_PATTERN.fullmatch(path):
            raise ValueError(f"Invalid update field path: {raw_path!r}")
        if isinstance(value, dict):
            raise ValueError(f"Unsupported nested update value for {path!r}.")
        if isinstance(value, list) and not all(is_yaml_scalar_value(item) for item in value):
            raise ValueError(f"Unsupported list value for {path!r}.")
        if not isinstance(value, list) and not is_yaml_scalar_value(value):
            raise ValueError(f"Unsupported value for {path!r}.")
        updates[path] = value

    return StrategyCloneRequest(
        source_strategy_path=source_strategy_path,
        target_filename=target_filename,
        strategy_name=strategy_name,
        strategy_version=strategy_version,
        display_name=normalized_display_name,
        description=normalized_description,
        updates=updates,
    )


def parse_strategy_root_payload(payload: Any) -> StrategyRootRequest:
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object.")
    root_path = str(payload.get("root_path", "")).strip()
    if not root_path:
        raise ValueError("root_path is required.")
    return StrategyRootRequest(root_path=root_path)


def add_persisted_strategy_root(config: DashboardConfig, request: StrategyRootRequest) -> dict[str, Any]:
    resolved_root = resolve_strategy_root_path(config, request.root_path)
    persisted_roots = list(load_persisted_strategy_roots(config))
    created = resolved_root not in persisted_roots
    if created:
        persisted_roots.append(resolved_root)
        save_persisted_strategy_roots(config, persisted_roots)

    discovered_entries = [
        entry
        for entry in discover_strategy_entries(config)
        if Path(entry.strategy_root).resolve() == resolved_root
    ]
    configured_roots = {path.resolve() for path in config.extra_strategy_roots}
    persisted_root_set = {path.resolve() for path in load_persisted_strategy_roots(config)}
    return {
        "root_path": str(resolved_root),
        "created": created,
        "configured_via_cli": resolved_root in configured_roots,
        "persisted": resolved_root in persisted_root_set,
        "strategy_count": len(discovered_entries),
        "strategies": [asdict(entry) for entry in discovered_entries],
        "saved_at": utc_now_iso(),
    }


def clone_strategy_config(config: DashboardConfig, request: StrategyCloneRequest) -> dict[str, Any]:
    source_entry = find_strategy_entry_by_path(config, request.source_strategy_path)
    if source_entry is None:
        raise ValueError(f"Strategy not found: {request.source_strategy_path}")

    source_path = Path(source_entry.strategy_path).resolve()
    source_root = Path(source_entry.strategy_root).resolve()
    target_path = (source_root / request.target_filename).resolve()
    try:
        target_path.relative_to(source_root)
    except ValueError as exc:
        raise ValueError("target_filename resolves outside the strategy root.") from exc
    if target_path.exists():
        raise ValueError(f"Target file already exists: {target_path}")

    existing_entries = discover_strategy_entries(config)
    new_strategy_id = strategy_identifier(request.strategy_name, request.strategy_version)
    for entry in existing_entries:
        if entry.strategy_id == new_strategy_id:
            raise ValueError(
                f"strategy_name + strategy_version already exists: {entry.strategy_name} / {entry.strategy_version}"
            )

    source_config = parse_simple_yaml_file(source_path)
    cloned_config = copy.deepcopy(source_config)
    cloned_config["strategy_name"] = request.strategy_name
    cloned_config["strategy_version"] = request.strategy_version
    if request.display_name is not None:
        cloned_config["display_name"] = request.display_name
    elif "display_name" in cloned_config:
        cloned_config.pop("display_name", None)
    if request.description is not None:
        cloned_config["description"] = request.description

    for field_path, value in request.updates.items():
        set_dotted_value(cloned_config, field_path, value)

    universe = cloned_config.get("universe", {}) if isinstance(cloned_config.get("universe"), dict) else {}
    source_artifacts = source_config.get("artifacts", {}) if isinstance(source_config.get("artifacts"), dict) else {}
    normalize_clone_artifact_paths(
        cloned_config,
        strategy_id=new_strategy_id,
        interval=str(universe.get("interval")) if universe.get("interval") is not None else None,
        source_artifacts=source_artifacts,
    )

    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(dump_simple_yaml(cloned_config), encoding="utf-8")
    target_entry = find_strategy_entry_by_path(config, str(target_path))
    return {
        "strategy_path": str(target_path),
        "strategy_id": new_strategy_id,
        "strategy_root": str(source_root),
        "strategy": asdict(target_entry) if target_entry is not None else None,
        "saved_at": utc_now_iso(),
    }


def resolve_workspace_path(config: DashboardConfig, raw_path: str | None) -> Path | None:
    if raw_path is None or raw_path.strip() == "":
        return None
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return candidate
    return (config.workspace_root / candidate).resolve()


def strategy_identifier(strategy_name: str, strategy_version: str) -> str:
    return f"{strategy_name.strip()}_{strategy_version.strip()}"


def resolve_strategy_root_path(config: DashboardConfig, raw_path: str) -> Path:
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = config.workspace_root / candidate
    resolved = candidate.resolve()
    if not resolved.exists():
        raise ValueError(f"Strategy root does not exist: {resolved}")
    if not resolved.is_dir():
        raise ValueError(f"Strategy root must be a directory: {resolved}")
    if resolved == config.strategies_root.resolve():
        raise ValueError("Default workspace strategy root is already enabled.")
    return resolved


def load_persisted_strategy_roots(config: DashboardConfig) -> tuple[Path, ...]:
    path = config.strategy_root_store_path
    if not path.is_file():
        return tuple()

    with STRATEGY_ROOT_STORE_LOCK:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return tuple()

    raw_roots = payload.get("roots", []) if isinstance(payload, dict) else []
    if not isinstance(raw_roots, list):
        return tuple()

    roots: list[Path] = []
    seen: set[Path] = set()
    for raw_value in raw_roots:
        if not isinstance(raw_value, str) or not raw_value.strip():
            continue
        candidate = Path(raw_value).expanduser().resolve()
        if candidate == config.strategies_root.resolve() or candidate in seen:
            continue
        seen.add(candidate)
        roots.append(candidate)
    return tuple(roots)


def save_persisted_strategy_roots(config: DashboardConfig, roots: list[Path]) -> None:
    deduped: list[str] = []
    seen: set[Path] = set()
    for raw_path in roots:
        resolved = raw_path.resolve()
        if resolved == config.strategies_root.resolve() or resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(str(resolved))

    payload = {
        "roots": deduped,
        "updated_at": utc_now_iso(),
    }
    target_path = config.strategy_root_store_path
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_name(f"{target_path.name}.tmp")
    body = json.dumps(payload, ensure_ascii=True, indent=2) + "\n"
    with STRATEGY_ROOT_STORE_LOCK:
        temp_path.write_text(body, encoding="utf-8")
        temp_path.replace(target_path)


def iter_extra_strategy_roots(config: DashboardConfig) -> list[Path]:
    roots: list[Path] = []
    seen: set[Path] = set()
    for candidate in (*config.extra_strategy_roots, *load_persisted_strategy_roots(config)):
        resolved = candidate.resolve()
        if resolved == config.strategies_root.resolve() or resolved in seen:
            continue
        seen.add(resolved)
        roots.append(resolved)
    return roots


def iter_strategy_roots(config: DashboardConfig) -> list[Path]:
    roots: list[Path] = []
    seen: set[Path] = set()
    for candidate in (config.strategies_root, *iter_extra_strategy_roots(config)):
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        roots.append(resolved)
    return roots


def iter_strategy_config_paths(root: Path) -> list[Path]:
    paths: list[Path] = []
    seen: set[Path] = set()
    for pattern in ("**/*.yaml", "**/*.yml"):
        for path in sorted(root.glob(pattern)):
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            paths.append(path)
    return paths


def strategy_location_kind(config: DashboardConfig, path: Path) -> str:
    try:
        path.resolve().relative_to(config.workspace_root)
    except ValueError:
        return "external"
    return "workspace"


def resolve_normalized_input_path(
    config: DashboardConfig,
    *,
    data_version: str | None,
    interval: str | None,
) -> Path | None:
    if data_version is None or interval is None:
        return None
    base_path = config.normalized_root / data_version / f"market_ohlcv_{interval}"
    for suffix in (".jsonl", ".parquet"):
        candidate = base_path.with_suffix(suffix)
        if candidate.exists():
            return candidate.resolve()
    return base_path.with_suffix(".jsonl").resolve()


def discover_strategy_entries(config: DashboardConfig) -> list[StrategyEntry]:
    entries: list[StrategyEntry] = []
    seen_paths: set[Path] = set()
    for root in iter_strategy_roots(config):
        if not root.exists():
            continue
        for path in iter_strategy_config_paths(root):
            resolved_path = path.resolve()
            if resolved_path in seen_paths:
                continue
            seen_paths.add(resolved_path)

            try:
                parsed = parse_simple_yaml_file(path)
            except Exception:
                parsed = {}

            universe = parsed.get("universe", {}) if isinstance(parsed.get("universe"), dict) else {}
            data_config = parsed.get("data", {}) if isinstance(parsed.get("data"), dict) else {}
            backtest = parsed.get("backtest", {}) if isinstance(parsed.get("backtest"), dict) else {}
            artifacts = parsed.get("artifacts", {}) if isinstance(parsed.get("artifacts"), dict) else {}
            symbols = universe.get("symbols", []) if isinstance(universe.get("symbols"), list) else []
            data_version = (
                str(data_config.get("data_version")) if data_config.get("data_version") is not None else None
            )
            interval = str(universe.get("interval")) if universe.get("interval") is not None else None
            input_path = resolve_normalized_input_path(
                config,
                data_version=data_version,
                interval=interval,
            )
            summary_path = resolve_workspace_path(config, str(artifacts.get("summary_path", "")) or None)
            updated_at = None
            if summary_path is not None and summary_path.exists():
                updated_at = isoformat_utc_from_timestamp(summary_path.stat().st_mtime)
            else:
                updated_at = isoformat_utc_from_timestamp(path.stat().st_mtime)

            entries.append(
                StrategyEntry(
                    strategy_id=strategy_identifier(
                        str(parsed.get("strategy_name", path.stem)),
                        str(parsed.get("strategy_version", "unknown")),
                    ),
                    strategy_name=str(parsed.get("strategy_name", path.stem)),
                    strategy_version=str(parsed.get("strategy_version", "unknown")),
                    display_name=(
                        str(parsed.get("display_name")) if parsed.get("display_name") is not None else None
                    ),
                    description=str(parsed.get("description", "")),
                    strategy_root=str(root),
                    location_kind=strategy_location_kind(config, resolved_path),
                    exchange=str(universe.get("exchange")) if universe.get("exchange") is not None else None,
                    market_type=(
                        str(universe.get("market_type"))
                        if universe.get("market_type") is not None
                        else None
                    ),
                    interval=interval,
                    data_version=data_version,
                    configured_engine=(
                        str(backtest.get("engine")) if backtest.get("engine") is not None else None
                    ),
                    symbols=[str(item) for item in symbols],
                    symbol_count=len(symbols),
                    backtest_start=(
                        str(backtest.get("start_date")) if backtest.get("start_date") is not None else None
                    ),
                    backtest_end=(
                        str(backtest.get("end_date")) if backtest.get("end_date") is not None else None
                    ),
                    input_path=str(input_path) if input_path is not None else None,
                    input_exists=bool(input_path and input_path.exists()),
                    strategy_path=str(resolved_path),
                    signal_path=str(resolve_workspace_path(config, str(artifacts.get("signal_path", "")) or None))
                    if artifacts.get("signal_path") is not None
                    else None,
                    trades_path=str(resolve_workspace_path(config, str(artifacts.get("trades_path", "")) or None))
                    if artifacts.get("trades_path") is not None
                    else None,
                    equity_path=str(resolve_workspace_path(config, str(artifacts.get("equity_path", "")) or None))
                    if artifacts.get("equity_path") is not None
                    else None,
                    summary_path=str(summary_path) if summary_path is not None else None,
                    summary_exists=bool(summary_path and summary_path.exists()),
                    updated_at=updated_at,
                )
            )
    return entries


def load_summary_metrics(
    summary_path: Path | None,
    *,
    summary_payload: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    payload = summary_payload
    if payload is None:
        if summary_path is None or not summary_path.exists():
            return None
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
    if not isinstance(payload, dict):
        return None
    metrics: dict[str, Any] = {}
    for key in ("total_return", "cagr", "max_drawdown", "sharpe", "win_rate", "trade_count"):
        if key in payload:
            metrics[key] = payload[key]
    return metrics or payload


def build_fallback_backtest_record(entry: StrategyEntry) -> BacktestRecord:
    summary_path = Path(entry.summary_path) if entry.summary_path else None
    trades_path = Path(entry.trades_path) if entry.trades_path else None
    equity_path = Path(entry.equity_path) if entry.equity_path else None
    summary_exists = bool(summary_path and summary_path.exists())
    trades_exists = bool(trades_path and trades_path.exists())
    equity_exists = bool(equity_path and equity_path.exists())
    updated_candidates = [
        path.stat().st_mtime
        for path in (summary_path, trades_path, equity_path)
        if path is not None and path.exists()
    ]
    updated_at = isoformat_utc_from_timestamp(max(updated_candidates)) if updated_candidates else None
    status = "ready" if summary_exists else "pending"
    return BacktestRecord(
        record_id=f"strategy:{entry.strategy_id}",
        run_id=None,
        strategy_id=entry.strategy_id,
        strategy_name=entry.strategy_name,
        display_name=entry.display_name,
        strategy_version=entry.strategy_version,
        engine=entry.configured_engine,
        status=status,
        input_path=entry.input_path,
        strategy_path=entry.strategy_path,
        manifest_path=None,
        report_available=bool(summary_exists and equity_exists),
        summary_path=entry.summary_path,
        summary_exists=summary_exists,
        trades_path=entry.trades_path,
        trades_exists=trades_exists,
        equity_path=entry.equity_path,
        equity_exists=equity_exists,
        created_at=None,
        started_at=None,
        finished_at=None,
        updated_at=updated_at,
        metrics=load_summary_metrics(summary_path),
        error=None,
    )


def load_backtest_manifest(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def discover_manifest_backtest_records(config: DashboardConfig) -> dict[str, list[BacktestRecord]]:
    if not config.backtests_root.exists():
        return {}

    entries_by_id = {entry.strategy_id: entry for entry in discover_strategy_entries(config)}
    records_by_strategy: dict[str, list[BacktestRecord]] = {}
    for manifest_path in sorted(config.backtests_root.glob("*/runs/*/manifest.json")):
        manifest = load_backtest_manifest(manifest_path)
        if manifest is None:
            continue

        strategy_id = str(manifest.get("strategy_id", "")).strip()
        strategy_name = str(manifest.get("strategy_name", "")).strip()
        strategy_version = str(manifest.get("strategy_version", "")).strip()
        if not strategy_id and strategy_name and strategy_version:
            strategy_id = strategy_identifier(strategy_name, strategy_version)
        if not strategy_id:
            continue

        entry = entries_by_id.get(strategy_id)
        summary_path = resolve_workspace_path(config, str(manifest.get("summary_path", "")) or None)
        trades_path = resolve_workspace_path(config, str(manifest.get("trades_path", "")) or None)
        equity_path = resolve_workspace_path(config, str(manifest.get("equity_path", "")) or None)
        summary_exists = bool(summary_path and summary_path.exists())
        trades_exists = bool(trades_path and trades_path.exists())
        equity_exists = bool(equity_path and equity_path.exists())
        updated_candidates = [
            candidate.stat().st_mtime
            for candidate in (manifest_path, summary_path, trades_path, equity_path)
            if candidate is not None and candidate.exists()
        ]
        updated_at = isoformat_utc_from_timestamp(max(updated_candidates)) if updated_candidates else None
        metrics = load_summary_metrics(
            summary_path,
            summary_payload=manifest.get("summary") if isinstance(manifest.get("summary"), dict) else None,
        )
        record = BacktestRecord(
            record_id=f"run:{manifest.get('run_id', manifest_path.parent.name)}",
            run_id=str(manifest.get("run_id")) if manifest.get("run_id") is not None else manifest_path.parent.name,
            strategy_id=strategy_id,
            strategy_name=strategy_name or strategy_id,
            display_name=(
                str(manifest.get("display_name"))
                if manifest.get("display_name") is not None
                else (entry.display_name if entry is not None else None)
            ),
            strategy_version=strategy_version or "unknown",
            engine=str(manifest.get("engine")) if manifest.get("engine") is not None else None,
            status=str(manifest.get("status", "unknown")),
            input_path=str(manifest.get("input_path")) if manifest.get("input_path") is not None else None,
            strategy_path=(
                str(resolve_workspace_path(config, str(manifest.get("strategy_path"))))
                if manifest.get("strategy_path") is not None
                else (entry.strategy_path if entry is not None else None)
            ),
            manifest_path=str(manifest_path.resolve()),
            report_available=bool(summary_exists and equity_exists),
            summary_path=str(summary_path) if summary_path is not None else None,
            summary_exists=summary_exists,
            trades_path=str(trades_path) if trades_path is not None else None,
            trades_exists=trades_exists,
            equity_path=str(equity_path) if equity_path is not None else None,
            equity_exists=equity_exists,
            created_at=str(manifest.get("created_at")) if manifest.get("created_at") is not None else None,
            started_at=str(manifest.get("started_at")) if manifest.get("started_at") is not None else None,
            finished_at=str(manifest.get("finished_at")) if manifest.get("finished_at") is not None else None,
            updated_at=updated_at,
            metrics=metrics,
            error=str(manifest.get("error")) if manifest.get("error") is not None else None,
        )
        records_by_strategy.setdefault(strategy_id, []).append(record)

    for strategy_id, records in records_by_strategy.items():
        records_by_strategy[strategy_id] = sorted(
            records,
            key=lambda item: (item.updated_at or "", item.record_id),
            reverse=True,
        )
    return records_by_strategy


def discover_backtest_records(config: DashboardConfig) -> list[BacktestRecord]:
    entries = discover_strategy_entries(config)
    manifest_records = discover_manifest_backtest_records(config)
    records: list[BacktestRecord] = []
    for entry in entries:
        if entry.strategy_id in manifest_records:
            records.extend(manifest_records[entry.strategy_id])
            continue
        records.append(build_fallback_backtest_record(entry))

    return sorted(
        records,
        key=lambda item: (item.updated_at or "", item.record_id),
        reverse=True,
    )


def find_strategy_entry_by_path(config: DashboardConfig, strategy_path: str) -> StrategyEntry | None:
    normalized = Path(strategy_path).expanduser()
    if not normalized.is_absolute():
        normalized = (config.workspace_root / normalized).resolve()
    else:
        normalized = normalized.resolve()

    for entry in discover_strategy_entries(config):
        if Path(entry.strategy_path).resolve() == normalized:
            return entry
    return None


def find_backtest_record_by_id(config: DashboardConfig, record_id: str) -> BacktestRecord | None:
    for record in discover_backtest_records(config):
        if record.record_id == record_id:
            return record
    return None


def build_backtest_report_payload(config: DashboardConfig, record_id: str) -> dict[str, Any]:
    record = find_backtest_record_by_id(config, record_id)
    if record is None:
        raise ValueError(f"Backtest record not found: {record_id}")
    return build_backtest_report(asdict(record))


def parse_backtest_run_payload(payload: Any) -> BacktestRunRequest:
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object.")

    strategy_path = str(payload.get("strategy_path", "")).strip()
    if not strategy_path:
        raise ValueError("strategy_path is required.")

    raw_engine = payload.get("engine")
    engine = str(raw_engine).strip().lower() if raw_engine is not None and str(raw_engine).strip() else None
    if engine is not None and engine not in SUPPORTED_BACKTEST_ENGINES:
        raise ValueError(
            f"Unsupported engine {engine!r}. Expected one of {', '.join(SUPPORTED_BACKTEST_ENGINES)}."
        )

    return BacktestRunRequest(
        strategy_path=strategy_path,
        engine=engine,
        skip_signal_write=parse_bool_value(payload.get("skip_signal_write"), default=False),
    )


def infer_base_asset(symbol: str, quote_asset: str) -> str:
    normalized_symbol = symbol.strip().upper()
    normalized_quote = quote_asset.strip().upper()
    for separator in ("-", "/", "_"):
        if separator in normalized_symbol:
            left, _, right = normalized_symbol.partition(separator)
            if right == normalized_quote and left:
                return left
    if normalized_symbol.endswith(normalized_quote) and len(normalized_symbol) > len(normalized_quote):
        return normalized_symbol[: -len(normalized_quote)]
    return normalized_symbol


def normalize_display_symbol(source: str, symbol: str, quote_asset: str) -> str:
    normalized_symbol = symbol.strip().upper()
    base_asset = infer_base_asset(normalized_symbol, quote_asset)
    normalized_quote = quote_asset.strip().upper()
    if source == "okx":
        return f"{base_asset}-{normalized_quote}"
    if source == "hl":
        return f"{base_asset}/{normalized_quote}"
    return normalized_symbol.replace("/", "").replace("-", "").replace("_", "")


def load_local_trading_pairs(config: DashboardConfig) -> list[SourcePair]:
    path = config.local_trading_store_path
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    items = payload.get("pairs", payload if isinstance(payload, list) else [])
    if not isinstance(items, list):
        return []

    pairs: list[SourcePair] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        source = str(item.get("source", "")).strip().lower()
        symbol = str(item.get("symbol", "")).strip().upper()
        quote_asset = str(item.get("quote_asset", DEFAULT_QUOTE_ASSET)).strip().upper()
        if source not in SOURCE_LABELS or not symbol:
            continue
        pairs.append(
            SourcePair(
                source=source,
                source_label=SOURCE_LABELS[source],
                symbol=normalize_symbol(symbol),
                display_symbol=str(item.get("display_symbol", normalize_display_symbol(source, symbol, quote_asset))),
                base_asset=str(item.get("base_asset", infer_base_asset(symbol, quote_asset))).strip().upper(),
                quote_asset=quote_asset,
                status="LOCAL",
                tradeable=True,
                source_kind="manual",
                created_at=str(item.get("created_at", "")) or None,
                note=str(item.get("note", "")) or None,
            )
        )
    return sorted(pairs, key=lambda pair: (pair.source, pair.symbol))


def save_local_trading_pairs(config: DashboardConfig, pairs: list[SourcePair]) -> None:
    config.local_trading_store_path.parent.mkdir(parents=True, exist_ok=True)
    serializable = {
        "pairs": [
            {
                "source": pair.source,
                "symbol": pair.symbol,
                "display_symbol": pair.display_symbol,
                "base_asset": pair.base_asset,
                "quote_asset": pair.quote_asset,
                "created_at": pair.created_at,
                "note": pair.note,
            }
            for pair in pairs
        ]
    }
    config.local_trading_store_path.write_text(
        json.dumps(serializable, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )


def filter_local_trading_pairs(
    pairs: list[SourcePair],
    *,
    source: str | None,
    allowed_quote_assets: set[str] | None,
) -> list[SourcePair]:
    filtered: list[SourcePair] = []
    normalized_source = source.strip().lower() if source else None
    for pair in pairs:
        if normalized_source is not None and pair.source != normalized_source:
            continue
        if allowed_quote_assets is not None and pair.quote_asset not in allowed_quote_assets:
            continue
        filtered.append(pair)
    return filtered


def parse_download_request_payload(payload: Any) -> KlineDownloadRequest:
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object.")

    symbol = str(payload.get("symbol", "")).strip().upper()
    if not symbol:
        raise ValueError("symbol is required.")

    start_date_raw = payload.get("start_date")
    start_date = None if start_date_raw in {None, ""} else str(start_date_raw).strip()
    end_date_raw = payload.get("end_date")
    if end_date_raw in {None, ""}:
        end_date = datetime.now(UTC).date().isoformat()
    else:
        end_date = str(end_date_raw).strip()

    return KlineDownloadRequest(
        symbol=symbol,
        intervals=parse_intervals_value(payload.get("intervals")),
        start_date=start_date,
        end_date=end_date,
        start_from_listing=parse_bool_value(payload.get("start_from_listing"), default=True),
    )


def parse_manual_trading_pair_payload(payload: Any) -> ManualTradingPairRequest:
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object.")

    source = str(payload.get("source", "")).strip().lower()
    if source not in SOURCE_LABELS:
        raise ValueError("source must be one of: binance, okx, bybit, hl.")

    symbol = str(payload.get("symbol", "")).strip().upper()
    if not symbol:
        raise ValueError("symbol is required.")

    quote_asset = str(payload.get("quote_asset", DEFAULT_QUOTE_ASSET)).strip().upper()
    if not quote_asset:
        raise ValueError("quote_asset is required.")

    note_value = payload.get("note")
    note = None if note_value in {None, ""} else str(note_value).strip()

    return ManualTradingPairRequest(
        source=source,
        symbol=symbol,
        quote_asset=quote_asset,
        note=note,
    )


PAIR_PREFERENCE_KIND_RULES: dict[str, tuple[str, ...]] = {
    "local": ("source", "symbol"),
    "normalized": ("source", "symbol"),
    "duckdb": ("data_version", "interval", "symbol"),
}


def build_pair_preference_key(kind: str, **parts: str) -> str:
    normalized_kind = kind.strip().lower()
    if normalized_kind not in PAIR_PREFERENCE_KIND_RULES:
        raise ValueError("Unsupported pair preference kind.")

    normalized_parts: list[str] = []
    for name in PAIR_PREFERENCE_KIND_RULES[normalized_kind]:
        raw_value = str(parts.get(name, "")).strip()
        if not raw_value:
            raise ValueError(f"{name} is required for {normalized_kind} preference.")
        normalized_parts.append(raw_value.upper() if name == "symbol" else raw_value.lower())
    return ":".join([normalized_kind, *normalized_parts])


def normalize_pair_preference_key(kind: str, raw_key: Any) -> str:
    normalized_kind = str(kind).strip().lower()
    if normalized_kind not in PAIR_PREFERENCE_KIND_RULES:
        raise ValueError("Unsupported pair preference kind.")

    key = str(raw_key or "").strip()
    if not key:
        raise ValueError("key is required.")

    parts = key.split(":")
    expected_fields = PAIR_PREFERENCE_KIND_RULES[normalized_kind]
    if len(parts) != len(expected_fields) + 1 or parts[0].strip().lower() != normalized_kind:
        raise ValueError(f"key must match {normalized_kind} preference format.")

    field_values = {field: parts[index + 1] for index, field in enumerate(expected_fields)}
    return build_pair_preference_key(normalized_kind, **field_values)


def load_pair_preferences(config: DashboardConfig) -> list[PairPreferenceEntry]:
    payload = parse_json_file(config.pair_preferences_store_path)
    items = payload.get("entries", []) if isinstance(payload, dict) else []
    entries: list[PairPreferenceEntry] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            kind = str(item.get("kind", "")).strip().lower()
            key = normalize_pair_preference_key(kind, item.get("key"))
        except ValueError:
            continue
        hidden = bool(item.get("hidden", False))
        pinned = bool(item.get("pinned", False))
        if not hidden and not pinned:
            continue
        entries.append(
            PairPreferenceEntry(
                kind=kind,
                key=key,
                hidden=hidden,
                pinned=pinned,
                updated_at=str(item.get("updated_at", "")) or utc_now_iso(),
            )
        )
    return sorted(entries, key=lambda entry: (entry.kind, entry.key))


def save_pair_preferences(config: DashboardConfig, entries: list[PairPreferenceEntry]) -> None:
    config.pair_preferences_store_path.parent.mkdir(parents=True, exist_ok=True)
    serializable = {
        "entries": [
            {
                "kind": entry.kind,
                "key": entry.key,
                "hidden": entry.hidden,
                "pinned": entry.pinned,
                "updated_at": entry.updated_at,
            }
            for entry in sorted(entries, key=lambda item: (item.kind, item.key))
        ]
    }
    config.pair_preferences_store_path.write_text(
        json.dumps(serializable, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )


def parse_pair_preference_payload(payload: Any) -> PairPreferenceUpdateRequest:
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object.")

    kind = str(payload.get("kind", "")).strip().lower()
    if kind not in PAIR_PREFERENCE_KIND_RULES:
        raise ValueError("kind must be one of: local, normalized, duckdb.")

    key = normalize_pair_preference_key(kind, payload.get("key"))
    hidden = payload.get("hidden")
    pinned = payload.get("pinned")
    if hidden is None and pinned is None:
        raise ValueError("At least one of hidden or pinned must be provided.")
    if hidden is not None and not isinstance(hidden, bool):
        raise ValueError("hidden must be a boolean.")
    if pinned is not None and not isinstance(pinned, bool):
        raise ValueError("pinned must be a boolean.")

    return PairPreferenceUpdateRequest(
        kind=kind,
        key=key,
        hidden=hidden if isinstance(hidden, bool) else None,
        pinned=pinned if isinstance(pinned, bool) else None,
    )


def update_pair_preference(
    config: DashboardConfig,
    request: PairPreferenceUpdateRequest,
) -> PairPreferenceEntry | None:
    entries = load_pair_preferences(config)
    entry_map = {(entry.kind, entry.key): entry for entry in entries}
    existing = entry_map.get((request.kind, request.key))
    hidden = request.hidden if request.hidden is not None else (existing.hidden if existing else False)
    pinned = request.pinned if request.pinned is not None else (existing.pinned if existing else False)

    if not hidden and not pinned:
        if existing is not None:
            entry_map.pop((request.kind, request.key), None)
            save_pair_preferences(config, list(entry_map.values()))
        return None

    updated = PairPreferenceEntry(
        kind=request.kind,
        key=request.key,
        hidden=hidden,
        pinned=pinned,
        updated_at=utc_now_iso(),
    )
    entry_map[(updated.kind, updated.key)] = updated
    save_pair_preferences(config, list(entry_map.values()))
    return updated


def add_local_trading_pair(
    config: DashboardConfig,
    request: ManualTradingPairRequest,
) -> SourcePair:
    pairs = load_local_trading_pairs(config)
    normalized_symbol = normalize_symbol(request.symbol)
    for pair in pairs:
        if pair.source == request.source and pair.symbol == normalized_symbol:
            raise ValueError(f"{request.source}:{normalized_symbol} already exists.")

    new_pair = SourcePair(
        source=request.source,
        source_label=SOURCE_LABELS[request.source],
        symbol=normalized_symbol,
        display_symbol=normalize_display_symbol(request.source, request.symbol, request.quote_asset),
        base_asset=infer_base_asset(request.symbol, request.quote_asset),
        quote_asset=request.quote_asset,
        status="LOCAL",
        tradeable=True,
        source_kind="manual",
        created_at=utc_now_iso(),
        note=request.note,
    )
    save_local_trading_pairs(config, pairs + [new_pair])
    return new_pair


def build_download_args(
    *,
    config: DashboardConfig,
    request: KlineDownloadRequest,
) -> argparse.Namespace:
    return argparse.Namespace(
        all_spot_symbols=False,
        symbols=[request.symbol],
        intervals=list(request.intervals),
        start_date=request.start_date,
        end_date=request.end_date,
        base_url=config.market_data_base_url,
        fallback_base_urls=list(config.fallback_base_urls),
        exchange_info_base_url=config.exchange_info_base_url,
        output_root=str(config.raw_root),
        limit=config.limit,
        timeout_seconds=config.timeout_seconds,
        sleep_seconds=config.sleep_seconds,
        max_retries=config.max_retries,
        retry_delay_seconds=config.retry_delay_seconds,
        retry_jitter_seconds=config.retry_jitter_seconds,
        start_from_listing=request.start_from_listing,
        symbol_statuses=None,
        quote_assets=None,
        max_symbols=None,
        resume_incomplete=True,
        continue_on_error=True,
    )


class DownloadJobRegistry:
    def __init__(self, config: DashboardConfig) -> None:
        self._config = config
        self._jobs: dict[str, DownloadJob] = {}
        self._lock = threading.Lock()

    def create_job(self, request: KlineDownloadRequest) -> DownloadJob:
        with self._lock:
            for existing in self._jobs.values():
                if existing.symbol == request.symbol and existing.status in {"queued", "running"}:
                    raise DownloadJobConflictError(
                        f"{request.symbol} already has an active download job."
                    )

            job = DownloadJob(
                job_id=uuid.uuid4().hex[:12],
                symbol=request.symbol,
                intervals=list(request.intervals),
                start_date=request.start_date,
                end_date=request.end_date,
                start_from_listing=request.start_from_listing,
                status="queued",
                created_at=utc_now_iso(),
            )
            self._jobs[job.job_id] = job

        thread = threading.Thread(target=self._run_job, args=(job.job_id,), daemon=True)
        thread.start()
        return self._clone_job(job)

    def list_jobs(self) -> list[DownloadJob]:
        with self._lock:
            jobs = sorted(
                self._jobs.values(),
                key=lambda item: (item.created_at, item.job_id),
                reverse=True,
            )
            return [self._clone_job(job) for job in jobs]

    @staticmethod
    def _clone_job(job: DownloadJob) -> DownloadJob:
        return DownloadJob(**asdict(job))

    def _write_manifest(
        self,
        *,
        job: DownloadJob,
        args: argparse.Namespace,
        summaries: list[dict[str, Any]],
        failures: list[dict[str, Any]],
    ) -> Path:
        manifest_path = self._config.raw_root / f"{job.run_id}.manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "exchange": "binance",
            "market_type": "spot",
            "run_id": job.run_id,
            "symbol_source": "manual",
            "requested_symbols": [job.symbol],
            "requested_intervals": list(job.intervals),
            "requested_start": job.start_date or DEFAULT_MANUAL_START_DATE,
            "requested_end_exclusive": job.end_date,
            "start_from_listing": job.start_from_listing,
            "fallback_base_urls": args.fallback_base_urls,
            "resume_incomplete": args.resume_incomplete,
            "continue_on_error": args.continue_on_error,
            "success_count": len(summaries),
            "failure_count": len(failures),
            "downloads": summaries,
            "failed_downloads": failures,
            "finished_at": utc_now_iso(),
        }
        manifest_path.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
        return manifest_path

    def _run_job(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.status = "running"
            job.started_at = utc_now_iso()
            job.run_id = f"{make_run_id()}_{job.job_id}"
            request = KlineDownloadRequest(
                symbol=job.symbol,
                intervals=list(job.intervals),
                start_date=job.start_date,
                end_date=job.end_date,
                start_from_listing=job.start_from_listing,
            )

        summary_dicts: list[dict[str, Any]] = []
        failure_dicts: list[dict[str, Any]] = []
        manifest_path: Path | None = None
        error_message: str | None = None

        try:
            args = build_download_args(config=self._config, request=request)
            validate_args(args)
            requests = build_requests(args, [request.symbol])

            for download_request in requests:
                print(
                    f"[admin-download] {download_request.symbol} {download_request.interval} "
                    f"run_id={job.run_id}",
                    file=sys.stderr,
                )
                try:
                    summary = download_klines(download_request, run_id=job.run_id or make_run_id())
                except Exception as exc:
                    failure = failure_from_exception(download_request, exc)
                    failure_dicts.append(asdict(failure))
                    print(
                        f"[admin-download-failed] {download_request.symbol} "
                        f"{download_request.interval} error={failure.error_type}:{failure.error_message}",
                        file=sys.stderr,
                    )
                    continue

                summary_dicts.append(asdict(summary))
                print(
                    f"[admin-download-saved] {summary.symbol} {summary.interval} "
                    f"rows={summary.row_count} file={summary.data_path}",
                    file=sys.stderr,
                )

            manifest_path = self._write_manifest(
                job=job,
                args=args,
                summaries=summary_dicts,
                failures=failure_dicts,
            )
            if failure_dicts:
                error_message = f"{len(failure_dicts)} interval downloads failed."
        except Exception as exc:
            error_message = str(exc)

        with self._lock:
            job = self._jobs[job_id]
            job.finished_at = utc_now_iso()
            job.manifest_path = str(manifest_path) if manifest_path is not None else None
            job.summaries = summary_dicts
            job.failures = failure_dicts
            job.error = error_message
            job.status = "succeeded" if error_message is None and not failure_dicts else "failed"


class NormalizeJobConflictError(RuntimeError):
    """Raised when a conflicting normalize job already exists."""


class NormalizeJobRegistry:
    def __init__(self, config: DashboardConfig) -> None:
        self._config = config
        self._jobs: dict[str, NormalizeJob] = {}
        self._lock = threading.Lock()

    def create_job(self, request: NormalizeRequest) -> NormalizeJob:
        with self._lock:
            for existing in self._jobs.values():
                if (
                    existing.data_version == request.data_version
                    and existing.status in {"queued", "running"}
                ):
                    raise NormalizeJobConflictError(
                        f"{request.data_version} already has an active normalize job."
                    )

            job = NormalizeJob(
                job_id=uuid.uuid4().hex[:12],
                source=request.source,
                symbols=list(request.symbols),
                intervals=list(request.intervals),
                data_version=request.data_version,
                output_format=request.output_format,
                status="queued",
                created_at=utc_now_iso(),
            )
            self._jobs[job.job_id] = job

        thread = threading.Thread(target=self._run_job, args=(job.job_id,), daemon=True)
        thread.start()
        return self._clone_job(job)

    def list_jobs(self) -> list[NormalizeJob]:
        with self._lock:
            jobs = sorted(
                self._jobs.values(),
                key=lambda item: (item.created_at, item.job_id),
                reverse=True,
            )
            return [self._clone_job(job) for job in jobs]

    @staticmethod
    def _clone_job(job: NormalizeJob) -> NormalizeJob:
        return NormalizeJob(**asdict(job))

    def _run_job(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.status = "running"
            job.started_at = utc_now_iso()
            request = NormalizeRequest(
                source=job.source,
                symbols=list(job.symbols),
                intervals=list(job.intervals),
                data_version=job.data_version,
                output_format=job.output_format,
            )

        manifest_path: Path | None = None
        interval_outputs: list[dict[str, Any]] = []
        raw_file_count = 0
        error_message: str | None = None

        try:
            input_root = resolve_source_raw_root(self._config, request.source)
            output_root = self._config.normalized_root
            job.raw_root = str(input_root)
            job.output_root = str(output_root)
            created_at = utc_now_iso()
            symbols = set(request.symbols) if request.symbols else None
            intervals = set(request.intervals) if request.intervals else None
            raw_files = normalized_market_ohlcv.discover_raw_files(
                input_root,
                symbols=symbols,
                intervals=intervals,
            )
            if not raw_files:
                raise RuntimeError(f"No raw files found under {input_root}.")

            raw_file_count = len(raw_files)
            resolved_output_format = normalized_market_ohlcv.resolve_output_format(request.output_format)
            by_interval, duplicate_counter = normalized_market_ohlcv.load_and_dedupe_records(
                raw_files,
                data_version=request.data_version,
                created_at=created_at,
                repo_root=self._config.workspace_root,
            )

            raw_file_count_by_interval: dict[str, int] = {}
            for path in raw_files:
                interval = path.parent.name
                raw_file_count_by_interval[interval] = raw_file_count_by_interval.get(interval, 0) + 1

            for interval in sorted(by_interval):
                records = normalized_market_ohlcv.sort_records(list(by_interval[interval].values()))
                output_path = normalized_market_ohlcv.write_interval_output(
                    output_root,
                    data_version=request.data_version,
                    interval=interval,
                    records=records,
                    output_format=resolved_output_format,
                )
                interval_outputs.append(
                    {
                        "interval": interval,
                        "output_path": str(output_path),
                        "row_count": len(records),
                        "source_file_count": raw_file_count_by_interval.get(interval, 0),
                        "duplicate_rows_removed": duplicate_counter.get(interval, 0),
                    }
                )

            manifest_path = output_root / request.data_version / "normalize_manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "data_version": request.data_version,
                        "input_root": str(input_root),
                        "output_root": str(output_root),
                        "output_format": resolved_output_format,
                        "created_at": created_at,
                        "filters": {
                            "source": request.source,
                            "symbols": sorted(symbols) if symbols is not None else None,
                            "intervals": sorted(intervals) if intervals is not None else None,
                        },
                        "raw_file_count": raw_file_count,
                        "interval_outputs": interval_outputs,
                    },
                    ensure_ascii=True,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
        except Exception as exc:
            error_message = str(exc)

        with self._lock:
            job = self._jobs[job_id]
            job.finished_at = utc_now_iso()
            job.manifest_path = str(manifest_path) if manifest_path is not None else None
            job.raw_file_count = raw_file_count
            job.interval_outputs = interval_outputs
            job.error = error_message
            job.status = "succeeded" if error_message is None else "failed"


class DuckDBLoadJobConflictError(RuntimeError):
    """Raised when a conflicting DuckDB load job already exists."""


class DuckDBLoadJobRegistry:
    def __init__(self, config: DashboardConfig) -> None:
        self._config = config
        self._jobs: dict[str, DuckDBLoadJob] = {}
        self._lock = threading.Lock()

    def create_job(self, request: DuckDBLoadRequest) -> DuckDBLoadJob:
        with self._lock:
            for existing in self._jobs.values():
                if (
                    existing.data_version == request.data_version
                    and existing.status in {"queued", "running"}
                ):
                    raise DuckDBLoadJobConflictError(
                        f"{request.data_version} already has an active DuckDB load job."
                    )

            job = DuckDBLoadJob(
                job_id=uuid.uuid4().hex[:12],
                data_version=request.data_version,
                intervals=list(request.intervals) if request.intervals is not None else None,
                status="queued",
                created_at=utc_now_iso(),
                db_path=str(self._config.quant_db_path),
            )
            self._jobs[job.job_id] = job

        thread = threading.Thread(target=self._run_job, args=(job.job_id,), daemon=True)
        thread.start()
        return self._clone_job(job)

    def list_jobs(self) -> list[DuckDBLoadJob]:
        with self._lock:
            jobs = sorted(
                self._jobs.values(),
                key=lambda item: (item.created_at, item.job_id),
                reverse=True,
            )
            return [self._clone_job(job) for job in jobs]

    @staticmethod
    def _clone_job(job: DuckDBLoadJob) -> DuckDBLoadJob:
        return DuckDBLoadJob(**asdict(job))

    def _run_job(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.status = "running"
            job.started_at = utc_now_iso()
            request = DuckDBLoadRequest(
                data_version=job.data_version,
                intervals=list(job.intervals) if job.intervals is not None else None,
            )

        loaded_files: list[dict[str, Any]] = []
        row_count: int | None = None
        error_message: str | None = None

        try:
            duckdb = warehouse_load_duckdb.import_duckdb(repo_root=self._config.workspace_root)
            self._config.quant_db_path.parent.mkdir(parents=True, exist_ok=True)
            init_sql_path = warehouse_load_duckdb.DEFAULT_INIT_SQL_PATH.expanduser().resolve()
            if not init_sql_path.exists():
                raise RuntimeError(f"Init SQL not found: {init_sql_path}")

            connection = duckdb.connect(str(self._config.quant_db_path))
            try:
                warehouse_load_duckdb.initialize_database(
                    connection,
                    init_sql_path=init_sql_path,
                    repo_root=self._config.workspace_root,
                )
                normalized_files = warehouse_load_duckdb.discover_normalized_files(
                    self._config.normalized_root,
                    data_version=request.data_version,
                    intervals=set(request.intervals) if request.intervals is not None else None,
                )
                if not normalized_files:
                    raise RuntimeError(
                        f"No normalized files found for data_version={request.data_version}."
                    )
                for normalized_file in normalized_files:
                    summary = warehouse_load_duckdb.load_normalized_file(
                        connection,
                        normalized_file,
                        data_version=request.data_version,
                        repo_root=self._config.workspace_root,
                    )
                    loaded_files.append(asdict(summary))

                result = connection.execute(
                    "SELECT COUNT(*) FROM market_ohlcv WHERE data_version = ?",
                    [request.data_version],
                ).fetchone()
                row_count = int(result[0]) if result is not None else 0
            finally:
                connection.close()
        except Exception as exc:
            error_message = str(exc)

        with self._lock:
            job = self._jobs[job_id]
            job.finished_at = utc_now_iso()
            job.loaded_files = loaded_files
            job.market_ohlcv_rows_for_data_version = row_count
            job.error = error_message
            job.status = "succeeded" if error_message is None else "failed"


class BacktestJobConflictError(RuntimeError):
    """Raised when a conflicting backtest job already exists."""


class BacktestJobRegistry:
    def __init__(self, config: DashboardConfig) -> None:
        self._config = config
        self._jobs: dict[str, BacktestJob] = {}
        self._lock = threading.Lock()

    def create_job(self, request: BacktestRunRequest) -> BacktestJob:
        strategy_entry = find_strategy_entry_by_path(self._config, request.strategy_path)
        if strategy_entry is None:
            raise ValueError(f"Strategy not found: {request.strategy_path}")
        if not strategy_entry.input_path or not strategy_entry.input_exists:
            raise ValueError(
                f"Normalized input not found for {strategy_entry.strategy_id}. "
                "Please prepare normalized data first."
            )

        resolved_engine = request.engine or strategy_entry.configured_engine or "native"
        if resolved_engine not in SUPPORTED_BACKTEST_ENGINES:
            raise ValueError(
                f"Unsupported engine {resolved_engine!r}. Expected one of "
                f"{', '.join(SUPPORTED_BACKTEST_ENGINES)}."
            )

        with self._lock:
            for existing in self._jobs.values():
                if (
                    existing.strategy_id == strategy_entry.strategy_id
                    and existing.engine == resolved_engine
                    and existing.status in {"queued", "running"}
                ):
                    raise BacktestJobConflictError(
                        f"{strategy_entry.strategy_id} already has an active {resolved_engine} backtest job."
                    )

            created_at = utc_now_iso()
            run_id = (
                created_at.replace("-", "")
                .replace(":", "")
                .replace("T", "_")
                .replace("Z", "")
                + f"_{uuid.uuid4().hex[:6]}"
            )
            job = BacktestJob(
                job_id=uuid.uuid4().hex[:12],
                run_id=run_id,
                strategy_id=strategy_entry.strategy_id,
                strategy_name=strategy_entry.strategy_name,
                display_name=strategy_entry.display_name,
                strategy_version=strategy_entry.strategy_version,
                engine=resolved_engine,
                interval=strategy_entry.interval,
                data_version=strategy_entry.data_version,
                input_path=str(strategy_entry.input_path),
                skip_signal_write=request.skip_signal_write,
                status="queued",
                created_at=created_at,
                strategy_path=strategy_entry.strategy_path,
            )
            self._jobs[job.job_id] = job

        thread = threading.Thread(target=self._run_job, args=(job.job_id,), daemon=True)
        thread.start()
        return self._clone_job(job)

    def list_jobs(self) -> list[BacktestJob]:
        with self._lock:
            jobs = sorted(
                self._jobs.values(),
                key=lambda item: (item.created_at, item.job_id),
                reverse=True,
            )
            return [self._clone_job(job) for job in jobs]

    @staticmethod
    def _clone_job(job: BacktestJob) -> BacktestJob:
        return BacktestJob(**asdict(job))

    def _path_for_manifest(self, path: Path | None) -> str | None:
        if path is None:
            return None
        try:
            return str(path.resolve().relative_to(self._config.workspace_root))
        except ValueError:
            return str(path.resolve())

    def _write_manifest(
        self,
        *,
        job: BacktestJob,
        manifest_path: Path,
        execution: Any | None,
        error_message: str | None,
    ) -> None:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        summary = execution.result.summary if execution is not None else None
        payload = {
            "job_id": job.job_id,
            "run_id": job.run_id,
            "strategy_id": job.strategy_id,
            "strategy_name": job.strategy_name,
            "display_name": job.display_name,
            "strategy_version": job.strategy_version,
            "engine": job.engine,
            "status": "succeeded" if error_message is None and execution is not None else "failed",
            "created_at": job.created_at,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
            "strategy_path": self._path_for_manifest(Path(job.strategy_path)) if job.strategy_path else None,
            "input_path": self._path_for_manifest(Path(job.input_path)),
            "skip_signal_write": job.skip_signal_write,
            "interval": job.interval,
            "data_version": job.data_version,
            "signal_paths": [self._path_for_manifest(path) for path in execution.artifact_paths.signal_paths]
            if execution is not None
            else [],
            "trades_path": self._path_for_manifest(execution.artifact_paths.trades_path)
            if execution is not None
            else None,
            "equity_path": self._path_for_manifest(execution.artifact_paths.equity_path)
            if execution is not None
            else None,
            "summary_path": self._path_for_manifest(execution.artifact_paths.summary_path)
            if execution is not None
            else None,
            "summary": summary,
            "error": error_message,
        }
        manifest_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")

    def _run_job(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.status = "running"
            job.started_at = utc_now_iso()

        execution = None
        manifest_path = self._config.backtests_root / job.strategy_id / "runs" / job.run_id / "manifest.json"
        error_message: str | None = None
        metrics: dict[str, Any] | None = None
        signal_paths: list[str] = []
        trades_path: str | None = None
        equity_path: str | None = None
        summary_path: str | None = None

        try:
            run_root = self._config.backtests_root / job.strategy_id / "runs" / job.run_id
            signal_root = self._config.workspace_root / "data" / "signals" / job.strategy_id / "runs" / job.run_id
            execution = execute_backtest(
                strategy_path=job.strategy_path or "",
                input_path=job.input_path,
                engine=job.engine,
                skip_signal_write=job.skip_signal_write,
                repo_root=self._config.workspace_root,
                signal_path_override=str(signal_root / "{symbol}_signal.jsonl"),
                trades_path_override=str(run_root / "trades.jsonl"),
                equity_path_override=str(run_root / "portfolio_equity.jsonl"),
                summary_path_override=str(run_root / "summary.json"),
            )
            metrics = load_summary_metrics(
                execution.artifact_paths.summary_path,
                summary_payload=execution.result.summary,
            )
            signal_paths = [str(path) for path in execution.artifact_paths.signal_paths]
            trades_path = str(execution.artifact_paths.trades_path)
            equity_path = str(execution.artifact_paths.equity_path)
            summary_path = str(execution.artifact_paths.summary_path)
        except Exception as exc:
            error_message = str(exc)

        with self._lock:
            job = self._jobs[job_id]
            job.finished_at = utc_now_iso()
            self._write_manifest(
                job=job,
                manifest_path=manifest_path,
                execution=execution,
                error_message=error_message,
            )
            job.manifest_path = str(manifest_path)
            job.signal_paths = signal_paths
            job.trades_path = trades_path
            job.equity_path = equity_path
            job.summary_path = summary_path
            job.metrics = metrics
            job.error = error_message
            job.status = "succeeded" if error_message is None else "failed"

class PairAdminHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], config: DashboardConfig) -> None:
        ensure_currency_icon_root(config)
        super().__init__(server_address, PairAdminHandler)
        self.config = config
        self.download_jobs = DownloadJobRegistry(config=config)
        self.normalize_jobs = NormalizeJobRegistry(config=config)
        self.duckdb_load_jobs = DuckDBLoadJobRegistry(config=config)
        self.backtest_jobs = BacktestJobRegistry(config=config)


class PairAdminHandler(BaseHTTPRequestHandler):
    server: PairAdminHTTPServer

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in {"/", "/chart"}:
            self.respond_html(cached_index_html())
            return
        if parsed.path in {"/icon.svg", "/favicon.svg", "/favicon.ico"}:
            self.respond_svg(cached_icon_svg(), cache_control="public, max-age=3600")
            return
        if parsed.path.startswith("/charting_library/"):
            self.handle_charting_library_file(parsed.path)
            return
        if parsed.path.startswith("/currency-icons/"):
            self.handle_currency_icon_file(parsed.path)
            return
        if parsed.path == "/api/source-pairs":
            self.handle_source_pairs(parsed.query)
            return
        if parsed.path == "/api/local-trading-pairs":
            self.handle_local_trading_pairs(parsed.query)
            return
        if parsed.path == "/api/pair-preferences":
            self.handle_pair_preferences()
            return
        if parsed.path == "/api/local-pairs":
            self.handle_local_pairs(parsed.query)
            return
        if parsed.path == "/api/strategies":
            self.handle_strategies()
            return
        if parsed.path == "/api/strategy-config":
            self.handle_strategy_config(parsed.query)
            return
        if parsed.path == "/api/backtest-records":
            self.handle_backtest_records()
            return
        if parsed.path == "/api/backtest-report":
            self.handle_backtest_report(parsed.query)
            return
        if parsed.path == "/api/system-settings":
            self.handle_system_settings()
            return
        if parsed.path == "/api/currency-icons":
            self.handle_currency_icons()
            return
        if parsed.path == "/api/download-jobs":
            self.handle_download_jobs()
            return
        if parsed.path == "/api/normalized-datasets":
            self.handle_normalized_datasets()
            return
        if parsed.path == "/api/normalize-jobs":
            self.handle_normalize_jobs()
            return
        if parsed.path == "/api/duckdb-status":
            self.handle_duckdb_status()
            return
        if parsed.path == "/api/duckdb-symbols":
            self.handle_duckdb_symbols(parsed.query)
            return
        if parsed.path == "/api/duckdb-load-jobs":
            self.handle_duckdb_load_jobs()
            return
        if parsed.path == "/api/backtest-jobs":
            self.handle_backtest_jobs()
            return
        if parsed.path == "/api/chart-bars":
            self.handle_chart_bars(parsed.query)
            return
        self.respond_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/download-klines":
            self.handle_download_klines()
            return
        if parsed.path == "/api/local-trading-pairs":
            self.handle_add_local_trading_pair()
            return
        if parsed.path == "/api/pair-preferences":
            self.handle_update_pair_preference()
            return
        if parsed.path == "/api/currency-icons":
            self.handle_upload_currency_icon()
            return
        if parsed.path == "/api/normalize":
            self.handle_normalize()
            return
        if parsed.path == "/api/load-duckdb":
            self.handle_load_duckdb()
            return
        if parsed.path == "/api/run-backtest":
            self.handle_run_backtest()
            return
        if parsed.path == "/api/strategy-roots":
            self.handle_add_strategy_root()
            return
        if parsed.path == "/api/clone-strategy-config":
            self.handle_clone_strategy_config()
            return
        self.respond_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def handle_source_pairs(self, query: str) -> None:
        params = parse_qs(query)
        source = params.get("source", ["binance"])[0].strip().lower() or "binance"
        allowed_quote_assets = resolve_quote_asset_filter(
            params.get("quote_asset"),
            default_quote_asset=self.server.config.default_quote_asset,
        )
        try:
            tradeable_only = parse_bool_query_value(
                params.get("tradeable_only", [None])[0],
                default=self.server.config.default_tradeable_only,
            )
            pairs = fetch_source_pairs(
                source=source,
                config=self.server.config,
                allowed_quote_assets=allowed_quote_assets,
                tradeable_only=tradeable_only,
            )
        except ValueError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:
            self.respond_json(
                {
                    "error": f"Failed to fetch source pairs for {source}: {exc}",
                    "source": source,
                },
                status=HTTPStatus.BAD_GATEWAY,
            )
            return

        self.respond_json(
            {
                "source": source,
                "source_label": SOURCE_LABELS.get(source, source.upper()),
                "count": len(pairs),
                "filters": {
                    "quote_asset": (
                        sorted(allowed_quote_assets) if allowed_quote_assets is not None else None
                    ),
                    "tradeable_only": tradeable_only,
                },
                "pairs": [asdict(pair) for pair in pairs],
                "fetched_at": utc_now_iso(),
            }
        )

    def handle_local_trading_pairs(self, query: str) -> None:
        params = parse_qs(query)
        source = params.get("source", [None])[0]
        allowed_quote_assets = resolve_quote_asset_filter(
            params.get("quote_asset"),
            default_quote_asset=self.server.config.default_quote_asset,
        )
        pairs = filter_local_trading_pairs(
            load_local_trading_pairs(self.server.config),
            source=source,
            allowed_quote_assets=allowed_quote_assets,
        )
        self.respond_json(
            {
                "count": len(pairs),
                "pairs": [asdict(pair) for pair in pairs],
                "fetched_at": utc_now_iso(),
            }
        )

    def handle_pair_preferences(self) -> None:
        entries = load_pair_preferences(self.server.config)
        self.respond_json(
            {
                "count": len(entries),
                "entries": [asdict(entry) for entry in entries],
                "fetched_at": utc_now_iso(),
            }
        )

    def handle_update_pair_preference(self) -> None:
        try:
            payload = self.read_json_body()
            request = parse_pair_preference_payload(payload)
            entry = update_pair_preference(self.server.config, request)
        except ValueError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self.respond_json(
            {
                "message": "Pair preference updated.",
                "entry": asdict(entry) if entry is not None else None,
            }
        )

    def handle_add_local_trading_pair(self) -> None:
        try:
            payload = self.read_json_body()
            request = parse_manual_trading_pair_payload(payload)
            pair = add_local_trading_pair(self.server.config, request)
        except ValueError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self.respond_json({"message": "Local trading pair added.", "pair": asdict(pair)}, status=HTTPStatus.CREATED)

    def handle_local_pairs(self, query: str) -> None:
        params = parse_qs(query)
        source_raw = params.get("source", [None])[0]
        source = source_raw.strip().lower() if source_raw else None
        if source is not None and source not in SOURCE_LABELS:
            self.respond_json({"error": f"Unsupported source: {source}"}, status=HTTPStatus.BAD_REQUEST)
            return

        pairs, roots = discover_local_pairs_catalog(self.server.config, source=source)
        payload: dict[str, Any] = {
            "source": source,
            "count": len(pairs),
            "count_by_source": {
                source_name: int(root_info["count"]) for source_name, root_info in roots.items()
            },
            "roots": roots,
            "pairs": [asdict(pair) for pair in pairs],
            "fetched_at": utc_now_iso(),
        }
        if source is not None:
            root_info = roots[source]
            payload["source_label"] = SOURCE_LABELS[source]
            payload["root"] = root_info["path"]
            payload["root_exists"] = root_info["exists"]
        else:
            payload["root"] = None
            payload["root_exists"] = None
        self.respond_json(payload)

    def handle_strategies(self) -> None:
        entries = discover_strategy_entries(self.server.config)
        self.respond_json(
            {
                "count": len(entries),
                "strategies": [asdict(entry) for entry in entries],
                "fetched_at": utc_now_iso(),
            }
        )

    def handle_strategy_config(self, query: str) -> None:
        params = parse_qs(query)
        strategy_path = params.get("strategy_path", [""])[0].strip()
        if not strategy_path:
            self.respond_json({"error": "strategy_path is required."}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            payload = build_strategy_config_payload(self.server.config, strategy_path)
        except ValueError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
            return
        except Exception as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self.respond_json(payload)

    def handle_add_strategy_root(self) -> None:
        try:
            payload = self.read_json_body()
            request = parse_strategy_root_payload(payload)
            result = add_persisted_strategy_root(self.server.config, request)
        except ValueError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self.respond_json(
            {
                "message": "Strategy root saved." if result["created"] else "Strategy root already saved.",
                **result,
            },
            status=HTTPStatus.CREATED if result["created"] else HTTPStatus.OK,
        )

    def handle_backtest_records(self) -> None:
        records = discover_backtest_records(self.server.config)
        self.respond_json(
            {
                "count": len(records),
                "records": [asdict(record) for record in records],
                "fetched_at": utc_now_iso(),
            }
        )

    def handle_backtest_report(self, query: str) -> None:
        params = parse_qs(query)
        record_id = params.get("record_id", [""])[0].strip()
        if not record_id:
            self.respond_json({"error": "record_id is required."}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            payload = build_backtest_report_payload(self.server.config, record_id)
        except ValueError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
            return
        self.respond_json(payload)

    def handle_system_settings(self) -> None:
        latest_log_path = None
        log_count = 0
        if self.server.config.logs_root.exists():
            log_files = sorted(self.server.config.logs_root.glob("*"), key=lambda path: path.stat().st_mtime)
            log_count = len(log_files)
            if log_files:
                latest_log_path = str(log_files[-1])
        icon_payload = list_currency_icons(self.server.config)
        persisted_strategy_roots = load_persisted_strategy_roots(self.server.config)
        merged_extra_strategy_roots = iter_extra_strategy_roots(self.server.config)

        self.respond_json(
            {
                "workspace_root": str(self.server.config.workspace_root),
                "raw_root": str(self.server.config.raw_root),
                "normalized_root": str(self.server.config.normalized_root),
                "strategies_root": str(self.server.config.strategies_root),
                "configured_extra_strategy_roots": [
                    str(path) for path in self.server.config.extra_strategy_roots
                ],
                "persisted_extra_strategy_roots": [str(path) for path in persisted_strategy_roots],
                "extra_strategy_roots": [str(path) for path in merged_extra_strategy_roots],
                "strategy_roots": [str(path) for path in iter_strategy_roots(self.server.config)],
                "strategy_root_store_path": str(self.server.config.strategy_root_store_path),
                "backtests_root": str(self.server.config.backtests_root),
                "logs_root": str(self.server.config.logs_root),
                "quant_db_path": str(self.server.config.quant_db_path),
                "experiments_db_path": str(self.server.config.experiments_db_path),
                "local_trading_store_path": str(self.server.config.local_trading_store_path),
                "pair_preferences_store_path": str(self.server.config.pair_preferences_store_path),
                "currency_icon_root": str(self.server.config.currency_icon_root),
                "currency_icon_catalog_path": icon_payload["catalog_path"],
                "currency_icon_count": icon_payload["available_count"],
                "currency_icon_missing_count": icon_payload["missing_count"],
                "exchange_info_base_url": self.server.config.exchange_info_base_url,
                "market_data_base_url": self.server.config.market_data_base_url,
                "fallback_base_urls": list(self.server.config.fallback_base_urls),
                "source_raw_roots": {
                    source: str(resolve_source_raw_root(self.server.config, source))
                    for source in SOURCE_ORDER
                },
                "default_quote_asset": self.server.config.default_quote_asset,
                "default_tradeable_only": self.server.config.default_tradeable_only,
                "python_version": platform.python_version(),
                "latest_log_path": latest_log_path,
                "log_count": log_count,
                "local_trading_pair_count": len(load_local_trading_pairs(self.server.config)),
                "pair_preference_count": len(load_pair_preferences(self.server.config)),
                "download_job_count": len(self.server.download_jobs.list_jobs()),
                "normalize_job_count": len(self.server.normalize_jobs.list_jobs()),
                "duckdb_load_job_count": len(self.server.duckdb_load_jobs.list_jobs()),
                "backtest_job_count": len(self.server.backtest_jobs.list_jobs()),
                "charting_library_root": str(CHARTING_LIBRARY_STATIC_ROOT),
                "charting_library_bundle_path": str(CHARTING_LIBRARY_BUNDLE_PATH),
                "charting_library_bundle_exists": charting_library_asset_exists(CHARTING_LIBRARY_BUNDLE_PATH),
                "fetched_at": utc_now_iso(),
            }
        )

    def handle_currency_icon_file(self, raw_path: str) -> None:
        asset_segment = raw_path.removeprefix("/currency-icons/").strip("/")
        if not asset_segment:
            self.respond_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
            return
        asset_name = Path(asset_segment).name
        asset = asset_name.split(".", 1)[0].upper()
        try:
            icon_path = find_currency_icon_path(self.server.config, asset)
        except ValueError:
            self.respond_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
            return

        if icon_path is None:
            icon_path = currency_icon_default_path(self.server.config)
            if not icon_path.is_file():
                self.respond_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
                return
        self.respond_file(icon_path, cache_control="public, max-age=3600")

    def handle_currency_icons(self) -> None:
        self.respond_json(list_currency_icons(self.server.config))

    def handle_upload_currency_icon(self) -> None:
        try:
            payload = self.read_json_body()
            entry = save_currency_icon(self.server.config, payload)
        except ValueError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self.respond_json({"message": "Currency icon saved.", "entry": entry}, status=HTTPStatus.CREATED)

    def handle_download_jobs(self) -> None:
        jobs = self.server.download_jobs.list_jobs()
        self.respond_json(
            {
                "count": len(jobs),
                "jobs": [asdict(job) for job in jobs],
                "fetched_at": utc_now_iso(),
            }
        )

    def handle_normalized_datasets(self) -> None:
        self.respond_json(discover_normalized_datasets(self.server.config))

    def handle_normalize_jobs(self) -> None:
        jobs = self.server.normalize_jobs.list_jobs()
        self.respond_json(
            {
                "count": len(jobs),
                "jobs": [asdict(job) for job in jobs],
                "fetched_at": utc_now_iso(),
            }
        )

    def handle_duckdb_status(self) -> None:
        self.respond_json(query_duckdb_overview(self.server.config))

    def handle_duckdb_symbols(self, query: str) -> None:
        params = parse_qs(query)
        data_version = params.get("data_version", [None])[0]
        interval = params.get("interval", [None])[0]
        payload = query_duckdb_symbol_catalog(
            self.server.config,
            data_version=str(data_version).strip() if data_version else None,
            interval=str(interval).strip() if interval else None,
        )
        self.respond_json(payload)

    def handle_duckdb_load_jobs(self) -> None:
        jobs = self.server.duckdb_load_jobs.list_jobs()
        self.respond_json(
            {
                "count": len(jobs),
                "jobs": [asdict(job) for job in jobs],
                "fetched_at": utc_now_iso(),
            }
        )

    def handle_backtest_jobs(self) -> None:
        jobs = self.server.backtest_jobs.list_jobs()
        self.respond_json(
            {
                "count": len(jobs),
                "jobs": [asdict(job) for job in jobs],
                "fetched_at": utc_now_iso(),
            }
        )

    def handle_chart_bars(self, query: str) -> None:
        params = parse_qs(query)
        symbol = str(params.get("symbol", [""])[0]).strip().upper()
        interval = str(params.get("interval", [""])[0]).strip().lower()
        data_version = str(params.get("data_version", [""])[0]).strip()
        if not symbol or not interval or not data_version:
            self.respond_json(
                {"error": "symbol, interval and data_version are required."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        try:
            from_ts = None
            if params.get("from"):
                from_ts = int(params["from"][0])
            to_ts = None
            if params.get("to"):
                to_ts = int(params["to"][0])
            limit = parse_positive_int(
                params.get("limit", [None])[0],
                field_name="limit",
                default=5000,
                max_value=50000,
            )
            payload = query_market_bars(
                self.server.config,
                symbol=symbol,
                interval=interval,
                data_version=data_version,
                from_ts=from_ts,
                to_ts=to_ts,
                limit=limit,
            )
        except (ValueError, RuntimeError, FileNotFoundError) as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self.respond_json(payload)

    def handle_download_klines(self) -> None:
        try:
            payload = self.read_json_body()
            request = parse_download_request_payload(payload)
            job = self.server.download_jobs.create_job(request)
        except ValueError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except DownloadJobConflictError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except Exception as exc:
            self.respond_json(
                {"error": f"Failed to create download job: {exc}"},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return
        self.respond_json({"message": "Download job created.", "job": asdict(job)}, status=HTTPStatus.ACCEPTED)

    def handle_normalize(self) -> None:
        try:
            payload = self.read_json_body()
            request = parse_normalize_request_payload(payload)
            job = self.server.normalize_jobs.create_job(request)
        except ValueError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except NormalizeJobConflictError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except Exception as exc:
            self.respond_json(
                {"error": f"Failed to create normalize job: {exc}"},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return
        self.respond_json({"message": "Normalize job created.", "job": asdict(job)}, status=HTTPStatus.ACCEPTED)

    def handle_load_duckdb(self) -> None:
        try:
            payload = self.read_json_body()
            request = parse_duckdb_load_request_payload(payload)
            job = self.server.duckdb_load_jobs.create_job(request)
        except ValueError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except DuckDBLoadJobConflictError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except Exception as exc:
            self.respond_json(
                {"error": f"Failed to create DuckDB load job: {exc}"},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return
        self.respond_json({"message": "DuckDB load job created.", "job": asdict(job)}, status=HTTPStatus.ACCEPTED)

    def handle_run_backtest(self) -> None:
        try:
            payload = self.read_json_body()
            request = parse_backtest_run_payload(payload)
            job = self.server.backtest_jobs.create_job(request)
        except ValueError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except BacktestJobConflictError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.CONFLICT)
            return
        except Exception as exc:
            self.respond_json(
                {"error": f"Failed to create backtest job: {exc}"},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return
        self.respond_json({"message": "Backtest job created.", "job": asdict(job)}, status=HTTPStatus.ACCEPTED)

    def handle_clone_strategy_config(self) -> None:
        try:
            payload = self.read_json_body()
            request = parse_strategy_clone_payload(payload)
            result = clone_strategy_config(self.server.config, request)
        except ValueError as exc:
            self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:
            self.respond_json(
                {"error": f"Failed to clone strategy config: {exc}"},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return
        self.respond_json(
            {
                "message": "Strategy config cloned.",
                **result,
            },
            status=HTTPStatus.CREATED,
        )

    def handle_charting_library_file(self, raw_path: str) -> None:
        relative_path = raw_path.removeprefix("/charting_library/").strip("/")
        requested_path = resolve_charting_library_asset_path(relative_path)
        if requested_path is None:
            self.respond_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
            return

        if not charting_library_asset_exists(requested_path):
            try:
                requested_path = fetch_charting_library_asset(relative_path)
            except ValueError:
                self.respond_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
                return
            except HTTPError as exc:
                self.respond_json(
                    {
                        "error": f"Failed to fetch Charting Library asset: upstream returned HTTP {exc.code}.",
                        "asset_path": relative_path,
                        "upstream_url": charting_library_asset_url(relative_path),
                    },
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return
            except (URLError, OSError, ValueError) as exc:
                self.respond_json(
                    {
                        "error": f"Failed to fetch Charting Library asset: {exc}",
                        "asset_path": relative_path,
                        "upstream_url": charting_library_asset_url(relative_path),
                    },
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return

        self.respond_file(requested_path, cache_control="public, max-age=3600")

    def read_json_body(self) -> Any:
        content_length_raw = self.headers.get("Content-Length")
        if content_length_raw is None:
            raise ValueError("Missing Content-Length header.")
        try:
            content_length = int(content_length_raw)
        except ValueError as exc:
            raise ValueError("Invalid Content-Length header.") from exc
        body = self.rfile.read(content_length)
        if not body:
            raise ValueError("Request body is empty.")
        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON body: {exc}") from exc

    def respond_html(self, payload: bytes, status: HTTPStatus = HTTPStatus.OK) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def respond_svg(
        self,
        payload: bytes,
        status: HTTPStatus = HTTPStatus.OK,
        *,
        cache_control: str = "public, max-age=3600",
    ) -> None:
        self.respond_bytes(
            payload,
            content_type="image/svg+xml",
            status=status,
            cache_control=cache_control,
        )

    def respond_bytes(
        self,
        payload: bytes,
        *,
        content_type: str,
        status: HTTPStatus = HTTPStatus.OK,
        cache_control: str = "public, max-age=3600",
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", cache_control)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(payload)

    def respond_file(
        self,
        path: Path,
        *,
        status: HTTPStatus = HTTPStatus.OK,
        cache_control: str = "public, max-age=3600",
    ) -> None:
        content_type = ALLOWED_CURRENCY_ICON_EXTENSIONS.get(path.suffix.lower()) or mimetypes.guess_type(
            str(path)
        )[0]
        self.respond_bytes(
            path.read_bytes(),
            content_type=content_type or "application/octet-stream",
            status=status,
            cache_control=cache_control,
        )

    def respond_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def build_config(args: argparse.Namespace) -> DashboardConfig:
    workspace_root = Path.cwd().resolve()
    extra_strategy_roots = resolve_extra_strategy_roots(args.extra_strategy_root)
    return DashboardConfig(
        workspace_root=workspace_root,
        raw_root=Path(args.raw_root).expanduser().resolve(),
        normalized_root=Path(args.normalized_root).expanduser().resolve(),
        strategies_root=Path(args.strategies_root).expanduser().resolve(),
        extra_strategy_roots=extra_strategy_roots,
        strategy_root_store_path=Path(args.strategy_root_store).expanduser().resolve(),
        backtests_root=Path(args.backtests_root).expanduser().resolve(),
        logs_root=Path(args.logs_root).expanduser().resolve(),
        quant_db_path=DEFAULT_QUANT_DB_PATH.expanduser().resolve(),
        experiments_db_path=DEFAULT_EXPERIMENTS_DB_PATH.expanduser().resolve(),
        local_trading_store_path=Path(args.local_trading_store).expanduser().resolve(),
        pair_preferences_store_path=Path(args.pair_preferences_store).expanduser().resolve(),
        currency_icon_root=Path(args.currency_icon_root).expanduser().resolve(),
        exchange_info_base_url=args.exchange_info_base_url,
        market_data_base_url=args.market_data_base_url,
        fallback_base_urls=tuple(args.fallback_base_urls or []),
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        retry_delay_seconds=args.retry_delay_seconds,
        retry_jitter_seconds=args.retry_jitter_seconds,
        limit=args.limit,
        sleep_seconds=args.sleep_seconds,
        default_quote_asset=args.default_quote_asset.strip().upper(),
        default_tradeable_only=args.default_tradeable_only,
    )


def resolve_extra_strategy_roots(raw_values: list[str] | None) -> tuple[Path, ...]:
    env_values = os.environ.get(EXTRA_STRATEGY_ROOTS_ENV, "")
    combined: list[str] = list(raw_values or [])
    if env_values.strip():
        combined.extend(item for item in env_values.split(os.pathsep) if item.strip())

    deduped: list[Path] = []
    seen: set[Path] = set()
    for raw_value in combined:
        candidate = Path(raw_value).expanduser().resolve()
        if candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return tuple(deduped)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = build_config(args)
    server = PairAdminHTTPServer((args.host, args.port), config=config)
    host, port = server.server_address
    print(f"[admin] serving http://{host}:{port}", file=sys.stderr)
    print(f"[admin] local raw root {config.raw_root}", file=sys.stderr)
    print(f"[admin] default quote asset {config.default_quote_asset}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("[admin] shutdown requested", file=sys.stderr)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
