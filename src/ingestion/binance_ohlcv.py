from __future__ import annotations

import argparse
import http.client
import json
import random
import socket
import ssl
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_MARKET_DATA_BASE_URL = "https://data-api.binance.vision"
DEFAULT_EXCHANGE_INFO_BASE_URL = "https://api.binance.com"
DEFAULT_KLINE_FALLBACK_BASE_URLS = (DEFAULT_EXCHANGE_INFO_BASE_URL,)
DEFAULT_MANUAL_START_DATE = "2018-01-01"
DEFAULT_INTERVALS = ("5m", "1h", "4h", "8h", "1d", "1w")
DEFAULT_SYMBOLS = ("BTCUSDT", "ETHUSDT")
DEFAULT_OUTPUT_ROOT = Path("data/raw/binance/spot")
CHECKPOINT_FILENAME = "_checkpoint.json"
MAX_LIMIT = 1000
KLINES_PATH = "/api/v3/klines"
EXCHANGE_INFO_PATH = "/api/v3/exchangeInfo"

INTERVAL_TO_MS = {
    "1s": 1_000,
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
}


@dataclass(frozen=True)
class DownloadRequest:
    symbol: str
    interval: str
    start_ms: int
    end_ms: int
    base_url: str
    fallback_base_urls: tuple[str, ...]
    output_root: Path
    limit: int
    timeout_seconds: float
    sleep_seconds: float
    max_retries: int
    retry_delay_seconds: float
    retry_jitter_seconds: float
    start_from_listing: bool
    resume_incomplete: bool


@dataclass
class DownloadSummary:
    symbol: str
    interval: str
    data_path: str
    metadata_path: str
    row_count: int
    page_count: int
    effective_start: str | None
    first_open_time: str | None
    last_open_time: str | None
    resumed_from_checkpoint: bool


@dataclass
class FailedDownload:
    symbol: str
    interval: str
    error_type: str
    error_message: str
    checkpoint_path: str
    checkpoint_exists: bool
    failed_at: str


@dataclass(frozen=True)
class ExchangeSymbol:
    symbol: str
    status: str
    base_asset: str
    quote_asset: str


@dataclass
class DownloadSession:
    run_id: str
    data_path: Path
    metadata_path: Path
    checkpoint_path: Path
    effective_start_ms: int
    current_start_ms: int
    row_count: int
    page_count: int
    first_open_ms: int | None
    last_open_ms: int | None
    run_started_at: str
    resumed_from_checkpoint: bool


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Binance spot kline data into the raw layer."
    )
    parser.add_argument(
        "--all-spot-symbols",
        action="store_true",
        help="Discover current Binance spot symbols from exchangeInfo and fetch all of them.",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=list(DEFAULT_SYMBOLS),
        help="Symbols to fetch, for example BTCUSDT ETHUSDT.",
    )
    parser.add_argument(
        "--intervals",
        nargs="+",
        default=list(DEFAULT_INTERVALS),
        help="Kline intervals to fetch.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help=(
            "Inclusive UTC start, supports YYYY-MM-DD or ISO-8601. "
            f"If omitted, defaults to {DEFAULT_MANUAL_START_DATE} unless --start-from-listing is used."
        ),
    )
    parser.add_argument(
        "--end-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Exclusive UTC end for datetimes; date-only values mean end-of-day.",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_MARKET_DATA_BASE_URL,
        help="Binance market-data base URL used for klines.",
    )
    parser.add_argument(
        "--fallback-base-urls",
        nargs="*",
        default=list(DEFAULT_KLINE_FALLBACK_BASE_URLS),
        help=(
            "Optional fallback Binance base URLs used for klines after transient failures. "
            "Example: https://api.binance.com https://api-gcp.binance.com"
        ),
    )
    parser.add_argument(
        "--exchange-info-base-url",
        default=DEFAULT_EXCHANGE_INFO_BASE_URL,
        help="Binance base URL used for exchangeInfo symbol discovery.",
    )
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Root directory for raw downloads.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=MAX_LIMIT,
        help="Page size per request. Binance spot klines max is 1000.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="HTTP timeout per request.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.1,
        help="Sleep between pages to reduce burstiness.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Retry count for transient network or 429/5xx errors.",
    )
    parser.add_argument(
        "--retry-delay-seconds",
        type=float,
        default=1.0,
        help="Base delay before retry; exponential backoff applies.",
    )
    parser.add_argument(
        "--retry-jitter-seconds",
        type=float,
        default=0.5,
        help="Additional random jitter added to retry backoff to reduce repeated edge failures.",
    )
    parser.add_argument(
        "--start-from-listing",
        action="store_true",
        help=(
            "Before downloading each symbol/interval, query the earliest available kline "
            "and start from that point."
        ),
    )
    parser.add_argument(
        "--symbol-statuses",
        nargs="*",
        default=None,
        help=(
            "Optional status filter when --all-spot-symbols is used. "
            "Examples: TRADING HALT BREAK."
        ),
    )
    parser.add_argument(
        "--quote-assets",
        nargs="*",
        default=None,
        help=(
            "Optional quoteAsset filter when --all-spot-symbols is used. "
            "Examples: USDT FDUSD USDC."
        ),
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="Optional cap after symbol discovery, useful for staged backfills.",
    )
    parser.add_argument(
        "--resume-incomplete",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume unfinished symbol/interval downloads from checkpoint files when available.",
    )
    parser.add_argument(
        "--continue-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Continue the batch when an individual symbol/interval download fails. "
            "The process still exits non-zero if any failures occurred."
        ),
    )
    return parser.parse_args(argv)


def interval_to_milliseconds(interval: str) -> int:
    try:
        return INTERVAL_TO_MS[interval]
    except KeyError as exc:
        supported = ", ".join(sorted(INTERVAL_TO_MS))
        raise ValueError(
            f"Unsupported interval {interval!r}. Supported intervals: {supported}"
        ) from exc


def parse_datetime_utc(value: str, *, end_value: bool) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    else:
        parsed = parsed.astimezone(UTC)

    if len(value.strip()) == 10 and end_value:
        parsed += timedelta(days=1)

    return parsed


def datetime_to_milliseconds(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def milliseconds_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    return (
        datetime.fromtimestamp(value / 1000, tz=UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def build_url(base_url: str, path: str, params: dict[str, Any]) -> str:
    return f"{base_url.rstrip('/')}{path}?{urlencode(params)}"


def dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def build_candidate_urls(
    primary_base_url: str,
    path: str,
    params: dict[str, Any],
    *,
    fallback_base_urls: tuple[str, ...] = (),
) -> list[str]:
    base_urls = dedupe_preserve_order([primary_base_url, *fallback_base_urls])
    return [build_url(base_url, path, params) for base_url in base_urls]


def endpoint_label(url: str) -> str:
    return url.split("/", 3)[2]


def retry_delay_with_backoff(
    *,
    base_delay_seconds: float,
    attempt: int,
    jitter_seconds: float,
) -> float:
    delay = base_delay_seconds * (2**attempt)
    if jitter_seconds > 0:
        delay += random.uniform(0.0, jitter_seconds)
    return delay


def transient_retry_delay(
    *,
    retry_delay_seconds: float,
    retry_jitter_seconds: float,
    attempt: int,
) -> float:
    return retry_delay_with_backoff(
        base_delay_seconds=retry_delay_seconds,
        attempt=attempt,
        jitter_seconds=retry_jitter_seconds,
    )


def log_retry(
    *,
    request_label: str,
    error_label: str,
    url: str,
    next_url: str,
    attempt: int,
    max_retries: int,
    delay: float,
) -> None:
    print(
        f"[retry] {request_label} {error_label} "
        f"endpoint={endpoint_label(url)} "
        f"next_endpoint={endpoint_label(next_url)} "
        f"attempt={attempt + 1}/{max_retries} sleep={delay:.1f}s",
        file=sys.stderr,
    )


def parse_json_response_body(body: bytes | str) -> Any:
    if isinstance(body, bytes):
        return json.loads(body.decode("utf-8"))
    return json.loads(body)


def fetch_json_payload(
    urls: list[str],
    *,
    timeout_seconds: float,
    max_retries: int,
    retry_delay_seconds: float,
    retry_jitter_seconds: float,
    request_label: str,
) -> Any:
    if not urls:
        raise ValueError("fetch_json_payload requires at least one URL.")

    for attempt in range(max_retries + 1):
        url = urls[attempt % len(urls)]
        http_request = Request(
            url,
            headers={
                "Accept": "application/json",
                "Connection": "close",
                "User-Agent": "donkey-ingestion/0.1",
            },
        )
        try:
            with urlopen(http_request, timeout=timeout_seconds) as response:
                body = response.read()
            return parse_json_response_body(body)
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            retryable = exc.code in {418, 429} or 500 <= exc.code < 600
            if retryable and attempt < max_retries:
                delay = retry_delay_with_retry_after(
                    exc,
                    base_delay_seconds=retry_delay_seconds,
                    attempt=attempt,
                    jitter_seconds=retry_jitter_seconds,
                )
                next_url = urls[(attempt + 1) % len(urls)]
                log_retry(
                    request_label=request_label,
                    error_label=f"http={exc.code}",
                    url=url,
                    next_url=next_url,
                    attempt=attempt,
                    max_retries=max_retries,
                    delay=delay,
                )
                time.sleep(delay)
                continue
            raise RuntimeError(f"Binance HTTP {exc.code} for {request_label}: {body}") from exc
        except json.JSONDecodeError as exc:
            if attempt < max_retries:
                delay = transient_retry_delay(
                    retry_delay_seconds=retry_delay_seconds,
                    retry_jitter_seconds=retry_jitter_seconds,
                    attempt=attempt,
                )
                next_url = urls[(attempt + 1) % len(urls)]
                log_retry(
                    request_label=request_label,
                    error_label=f"parse={type(exc).__name__}:{exc}",
                    url=url,
                    next_url=next_url,
                    attempt=attempt,
                    max_retries=max_retries,
                    delay=delay,
                )
                time.sleep(delay)
                continue
            raise RuntimeError(
                f"Invalid JSON response for {request_label} via {endpoint_label(url)}: {exc}"
            ) from exc
        except (
            URLError,
            TimeoutError,
            socket.timeout,
            ssl.SSLError,
            http.client.IncompleteRead,
            ConnectionResetError,
            OSError,
        ) as exc:
            if attempt < max_retries:
                delay = transient_retry_delay(
                    retry_delay_seconds=retry_delay_seconds,
                    retry_jitter_seconds=retry_jitter_seconds,
                    attempt=attempt,
                )
                next_url = urls[(attempt + 1) % len(urls)]
                reason = exc.reason if isinstance(exc, URLError) else exc
                log_retry(
                    request_label=request_label,
                    error_label=f"network={type(exc).__name__}:{reason}",
                    url=url,
                    next_url=next_url,
                    attempt=attempt,
                    max_retries=max_retries,
                    delay=delay,
                )
                time.sleep(delay)
                continue
            reason = exc.reason if isinstance(exc, URLError) else exc
            raise RuntimeError(
                f"Network error for {request_label} via {endpoint_label(url)}: {reason}"
            ) from exc


def retry_delay_with_retry_after(
    error: HTTPError,
    *,
    base_delay_seconds: float,
    attempt: int,
    jitter_seconds: float,
) -> float:
    delay = retry_delay_with_backoff(
        base_delay_seconds=base_delay_seconds,
        attempt=attempt,
        jitter_seconds=jitter_seconds,
    )
    retry_after = error.headers.get("Retry-After")
    if retry_after is None:
        return delay

    try:
        return max(delay, float(retry_after))
    except ValueError:
        return delay


def fetch_exchange_symbols(
    *,
    base_url: str,
    timeout_seconds: float,
    max_retries: int,
    retry_delay_seconds: float,
    allowed_statuses: set[str] | None,
    allowed_quote_assets: set[str] | None,
    max_symbols: int | None,
) -> list[str]:
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
        request_label="exchangeInfo",
    )

    if not isinstance(payload, dict) or "symbols" not in payload:
        raise RuntimeError(f"Unexpected exchangeInfo response: {payload}")

    discovered: list[ExchangeSymbol] = []
    for raw_symbol in payload["symbols"]:
        symbol = ExchangeSymbol(
            symbol=str(raw_symbol.get("symbol", "")),
            status=str(raw_symbol.get("status", "")),
            base_asset=str(raw_symbol.get("baseAsset", "")),
            quote_asset=str(raw_symbol.get("quoteAsset", "")),
        )
        if not symbol.symbol:
            continue
        if allowed_statuses is not None and symbol.status not in allowed_statuses:
            continue
        if allowed_quote_assets is not None and symbol.quote_asset not in allowed_quote_assets:
            continue
        discovered.append(symbol)

    symbols = sorted(item.symbol for item in discovered)
    if max_symbols is not None:
        symbols = symbols[:max_symbols]
    return symbols


def fetch_klines_page(request: DownloadRequest, start_ms: int) -> list[list[Any]]:
    params = {
        "symbol": request.symbol,
        "interval": request.interval,
        "startTime": start_ms,
        "endTime": request.end_ms - 1,
        "limit": request.limit,
    }
    payload = fetch_json_payload(
        build_candidate_urls(
            request.base_url,
            KLINES_PATH,
            params,
            fallback_base_urls=request.fallback_base_urls,
        ),
        timeout_seconds=request.timeout_seconds,
        max_retries=request.max_retries,
        retry_delay_seconds=request.retry_delay_seconds,
        retry_jitter_seconds=request.retry_jitter_seconds,
        request_label=f"{request.symbol} {request.interval}",
    )

    if not isinstance(payload, list):
        raise RuntimeError(
            f"Unexpected Binance response for {request.symbol} {request.interval}: {payload}"
        )

    return payload


def fetch_earliest_kline_open_time(request: DownloadRequest) -> int | None:
    rows = fetch_klines_page(request, 0)
    if not rows:
        return None
    return int(rows[0][0])


def checkpoint_path_for(output_dir: Path) -> Path:
    return output_dir / CHECKPOINT_FILENAME


def load_checkpoint(checkpoint_path: Path) -> dict[str, Any] | None:
    if not checkpoint_path.exists():
        return None
    try:
        return json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def checkpoint_matches_request(checkpoint: dict[str, Any], request: DownloadRequest) -> bool:
    if checkpoint.get("symbol") != request.symbol:
        return False
    if checkpoint.get("interval") != request.interval:
        return False
    if checkpoint.get("base_url") != request.base_url:
        return False
    if int(checkpoint.get("requested_start_ms", -1)) != request.start_ms:
        return False
    if int(checkpoint.get("requested_end_ms", -1)) != request.end_ms:
        return False
    if bool(checkpoint.get("start_from_listing")) != request.start_from_listing:
        return False

    data_path = checkpoint.get("data_path")
    if not data_path:
        return False
    return Path(str(data_path)).exists()


def write_checkpoint(request: DownloadRequest, session: DownloadSession) -> None:
    payload = {
        "status": "running",
        "symbol": request.symbol,
        "interval": request.interval,
        "base_url": request.base_url,
        "requested_start_ms": request.start_ms,
        "requested_end_ms": request.end_ms,
        "start_from_listing": request.start_from_listing,
        "run_id": session.run_id,
        "data_path": str(session.data_path),
        "metadata_path": str(session.metadata_path),
        "effective_start_ms": session.effective_start_ms,
        "next_start_ms": session.current_start_ms,
        "row_count": session.row_count,
        "page_count": session.page_count,
        "first_open_ms": session.first_open_ms,
        "last_open_ms": session.last_open_ms,
        "run_started_at": session.run_started_at,
        "updated_at": utc_now_iso(),
    }
    session.checkpoint_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )


def remove_checkpoint(checkpoint_path: Path) -> None:
    if checkpoint_path.exists():
        checkpoint_path.unlink()


def failure_from_exception(request: DownloadRequest, exc: Exception) -> FailedDownload:
    checkpoint_path = checkpoint_path_for(request.output_root / request.symbol / request.interval)
    return FailedDownload(
        symbol=request.symbol,
        interval=request.interval,
        error_type=type(exc).__name__,
        error_message=str(exc),
        checkpoint_path=str(checkpoint_path),
        checkpoint_exists=checkpoint_path.exists(),
        failed_at=utc_now_iso(),
    )


def restore_session_from_checkpoint(
    checkpoint: dict[str, Any],
    *,
    output_dir: Path,
) -> DownloadSession:
    run_id = str(checkpoint["run_id"])
    data_path = Path(str(checkpoint["data_path"]))
    metadata_path = Path(str(checkpoint["metadata_path"]))
    return DownloadSession(
        run_id=run_id,
        data_path=data_path,
        metadata_path=metadata_path,
        checkpoint_path=checkpoint_path_for(output_dir),
        effective_start_ms=int(checkpoint["effective_start_ms"]),
        current_start_ms=int(checkpoint["next_start_ms"]),
        row_count=int(checkpoint["row_count"]),
        page_count=int(checkpoint["page_count"]),
        first_open_ms=(
            None
            if checkpoint.get("first_open_ms") is None
            else int(checkpoint["first_open_ms"])
        ),
        last_open_ms=(
            None if checkpoint.get("last_open_ms") is None else int(checkpoint["last_open_ms"])
        ),
        run_started_at=str(checkpoint["run_started_at"]),
        resumed_from_checkpoint=True,
    )


def initialize_download_session(request: DownloadRequest, *, run_id: str) -> DownloadSession:
    output_dir = request.output_root / request.symbol / request.interval
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = checkpoint_path_for(output_dir)

    if request.resume_incomplete:
        checkpoint = load_checkpoint(checkpoint_path)
        if checkpoint is not None:
            if checkpoint_matches_request(checkpoint, request):
                session = restore_session_from_checkpoint(checkpoint, output_dir=output_dir)
                print(
                    f"[resume] {request.symbol} {request.interval} "
                    f"next_start={milliseconds_to_iso(session.current_start_ms)} "
                    f"file={session.data_path}",
                    file=sys.stderr,
                )
                return session
            print(
                f"[checkpoint] ignoring mismatched checkpoint for {request.symbol} {request.interval}",
                file=sys.stderr,
            )

    data_path = output_dir / f"{run_id}.jsonl"
    metadata_path = output_dir / f"{run_id}.meta.json"
    effective_start_ms = request.start_ms
    if request.start_from_listing:
        earliest_open_ms = fetch_earliest_kline_open_time(request)
        if earliest_open_ms is None:
            effective_start_ms = request.end_ms
        else:
            effective_start_ms = max(request.start_ms, earliest_open_ms)

    session = DownloadSession(
        run_id=run_id,
        data_path=data_path,
        metadata_path=metadata_path,
        checkpoint_path=checkpoint_path,
        effective_start_ms=effective_start_ms,
        current_start_ms=effective_start_ms,
        row_count=0,
        page_count=0,
        first_open_ms=None,
        last_open_ms=None,
        run_started_at=utc_now_iso(),
        resumed_from_checkpoint=False,
    )
    write_checkpoint(request, session)
    return session


def kline_row_to_record(
    row: list[Any],
    *,
    symbol: str,
    interval: str,
    fetched_at: str,
) -> dict[str, Any]:
    open_time = int(row[0])
    close_time = int(row[6])
    return {
        "exchange": "binance",
        "market_type": "spot",
        "symbol": symbol,
        "interval": interval,
        "open_time": open_time,
        "open_time_iso": milliseconds_to_iso(open_time),
        "open": row[1],
        "high": row[2],
        "low": row[3],
        "close": row[4],
        "volume": row[5],
        "close_time": close_time,
        "close_time_iso": milliseconds_to_iso(close_time),
        "quote_asset_volume": row[7],
        "number_of_trades": row[8],
        "taker_buy_base_asset_volume": row[9],
        "taker_buy_quote_asset_volume": row[10],
        "ignore": row[11],
        "fetched_at": fetched_at,
        "source_path": KLINES_PATH,
    }


def download_klines(request: DownloadRequest, *, run_id: str) -> DownloadSummary:
    session = initialize_download_session(request, run_id=run_id)
    open_mode = "a" if session.resumed_from_checkpoint else "w"

    with session.data_path.open(open_mode, encoding="utf-8") as handle:
        while session.current_start_ms < request.end_ms:
            rows = fetch_klines_page(request, session.current_start_ms)

            if not rows:
                break

            session.page_count += 1

            for row in rows:
                open_time = int(row[0])
                if open_time < session.current_start_ms or open_time >= request.end_ms:
                    continue
                if session.last_open_ms is not None and open_time <= session.last_open_ms:
                    continue

                if session.first_open_ms is None:
                    session.first_open_ms = open_time
                session.last_open_ms = open_time

                record = kline_row_to_record(
                    row,
                    symbol=request.symbol,
                    interval=request.interval,
                    fetched_at=session.run_started_at,
                )
                handle.write(json.dumps(record, ensure_ascii=True) + "\n")
                session.row_count += 1

            next_start_ms = int(rows[-1][0]) + 1
            if next_start_ms <= session.current_start_ms:
                raise RuntimeError(
                    f"Paginator stalled for {request.symbol} {request.interval} "
                    f"at {milliseconds_to_iso(session.current_start_ms)}"
                )

            session.current_start_ms = next_start_ms
            handle.flush()
            write_checkpoint(request, session)

            if request.sleep_seconds > 0:
                time.sleep(request.sleep_seconds)

    metadata = {
        "exchange": "binance",
        "market_type": "spot",
        "symbol": request.symbol,
        "interval": request.interval,
        "base_url": request.base_url,
        "source_path": KLINES_PATH,
        "requested_start": milliseconds_to_iso(request.start_ms),
        "effective_start": milliseconds_to_iso(session.effective_start_ms),
        "requested_end_exclusive": milliseconds_to_iso(request.end_ms),
        "limit": request.limit,
        "row_count": session.row_count,
        "page_count": session.page_count,
        "first_open_time": milliseconds_to_iso(session.first_open_ms),
        "last_open_time": milliseconds_to_iso(session.last_open_ms),
        "run_started_at": session.run_started_at,
        "run_finished_at": utc_now_iso(),
        "data_path": str(session.data_path),
        "resumed_from_checkpoint": session.resumed_from_checkpoint,
    }
    session.metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    remove_checkpoint(session.checkpoint_path)

    return DownloadSummary(
        symbol=request.symbol,
        interval=request.interval,
        data_path=str(session.data_path),
        metadata_path=str(session.metadata_path),
        row_count=session.row_count,
        page_count=session.page_count,
        effective_start=milliseconds_to_iso(session.effective_start_ms),
        first_open_time=milliseconds_to_iso(session.first_open_ms),
        last_open_time=milliseconds_to_iso(session.last_open_ms),
        resumed_from_checkpoint=session.resumed_from_checkpoint,
    )


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def make_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def validate_args(args: argparse.Namespace) -> None:
    if args.limit <= 0 or args.limit > MAX_LIMIT:
        raise ValueError(f"--limit must be between 1 and {MAX_LIMIT}.")
    if args.timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be positive.")
    if args.sleep_seconds < 0:
        raise ValueError("--sleep-seconds must be >= 0.")
    if args.max_retries < 0:
        raise ValueError("--max-retries must be >= 0.")
    if args.retry_delay_seconds < 0:
        raise ValueError("--retry-delay-seconds must be >= 0.")
    if args.retry_jitter_seconds < 0:
        raise ValueError("--retry-jitter-seconds must be >= 0.")
    if args.max_symbols is not None and args.max_symbols <= 0:
        raise ValueError("--max-symbols must be > 0.")

    unknown_intervals = [value for value in args.intervals if value not in INTERVAL_TO_MS]
    if unknown_intervals:
        raise ValueError(
            "Unsupported intervals: " + ", ".join(sorted(unknown_intervals))
        )

    if args.start_date is None:
        start_value = "1970-01-01" if args.start_from_listing else DEFAULT_MANUAL_START_DATE
    else:
        start_value = args.start_date

    start_dt = parse_datetime_utc(start_value, end_value=False)
    end_dt = parse_datetime_utc(args.end_date, end_value=True)
    if start_dt >= end_dt:
        raise ValueError("--start-date must be earlier than --end-date.")


def resolve_start_ms(args: argparse.Namespace) -> int:
    if args.start_date is None:
        value = "1970-01-01" if args.start_from_listing else DEFAULT_MANUAL_START_DATE
    else:
        value = args.start_date
    return datetime_to_milliseconds(parse_datetime_utc(value, end_value=False))


def build_requests(args: argparse.Namespace, symbols: list[str]) -> list[DownloadRequest]:
    start_ms = resolve_start_ms(args)
    end_ms = datetime_to_milliseconds(parse_datetime_utc(args.end_date, end_value=True))
    output_root = Path(args.output_root)

    requests: list[DownloadRequest] = []
    for symbol in symbols:
        for interval in args.intervals:
            requests.append(
                DownloadRequest(
                    symbol=symbol,
                    interval=interval,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    base_url=args.base_url,
                    fallback_base_urls=tuple(args.fallback_base_urls or []),
                    output_root=output_root,
                    limit=args.limit,
                    timeout_seconds=args.timeout_seconds,
                    sleep_seconds=args.sleep_seconds,
                    max_retries=args.max_retries,
                    retry_delay_seconds=args.retry_delay_seconds,
                    retry_jitter_seconds=args.retry_jitter_seconds,
                    start_from_listing=args.start_from_listing,
                    resume_incomplete=args.resume_incomplete,
                )
            )
    return requests


def manifest_payload(
    *,
    args: argparse.Namespace,
    symbols: list[str],
    run_id: str,
    summaries: list[DownloadSummary],
    failures: list[FailedDownload],
) -> dict[str, Any]:
    return {
        "exchange": "binance",
        "market_type": "spot",
        "run_id": run_id,
        "symbol_source": "exchangeInfo" if args.all_spot_symbols else "manual",
        "requested_symbols": symbols,
        "requested_intervals": args.intervals,
        "requested_start": (
            datetime.fromtimestamp(resolve_start_ms(args) / 1000, tz=UTC)
            .isoformat()
            .replace("+00:00", "Z")
        ),
        "requested_end_exclusive": parse_datetime_utc(
            args.end_date, end_value=True
        ).isoformat().replace("+00:00", "Z"),
        "start_from_listing": args.start_from_listing,
        "fallback_base_urls": args.fallback_base_urls,
        "quote_assets": args.quote_assets,
        "resume_incomplete": args.resume_incomplete,
        "continue_on_error": args.continue_on_error,
        "success_count": len(summaries),
        "failure_count": len(failures),
        "downloads": [asdict(summary) for summary in summaries],
        "failed_downloads": [asdict(failure) for failure in failures],
    }


def resolve_symbols(args: argparse.Namespace) -> list[str]:
    if not args.all_spot_symbols:
        return list(args.symbols)

    allowed_statuses = (
        {value.upper() for value in args.symbol_statuses} if args.symbol_statuses else None
    )
    allowed_quote_assets = (
        {value.upper() for value in args.quote_assets} if args.quote_assets else None
    )
    return fetch_exchange_symbols(
        base_url=args.exchange_info_base_url,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        retry_delay_seconds=args.retry_delay_seconds,
        allowed_statuses=allowed_statuses,
        allowed_quote_assets=allowed_quote_assets,
        max_symbols=args.max_symbols,
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        validate_args(args)
    except ValueError as exc:
        print(f"argument error: {exc}", file=sys.stderr)
        return 2

    symbols = resolve_symbols(args)
    if not symbols:
        print("[symbols] no symbols resolved", file=sys.stderr)
        return 1

    if args.all_spot_symbols:
        status_text = ",".join(args.symbol_statuses) if args.symbol_statuses else "ALL"
        quote_text = ",".join(args.quote_assets) if args.quote_assets else "ALL"
        print(
            f"[symbols] discovered={len(symbols)} source=exchangeInfo "
            f"statuses={status_text} quote_assets={quote_text}",
            file=sys.stderr,
        )

    requests = build_requests(args, symbols)
    run_id = make_run_id()
    summaries: list[DownloadSummary] = []
    failures: list[FailedDownload] = []

    for request in requests:
        print(
            f"[download] {request.symbol} {request.interval} "
            f"{milliseconds_to_iso(request.start_ms)} -> {milliseconds_to_iso(request.end_ms)}",
            file=sys.stderr,
        )
        try:
            summary = download_klines(request, run_id=run_id)
        except Exception as exc:
            failure = failure_from_exception(request, exc)
            failures.append(failure)
            print(
                f"[failed] {request.symbol} {request.interval} "
                f"error={failure.error_type}:{failure.error_message}",
                file=sys.stderr,
            )
            if not args.continue_on_error:
                print("[abort] stopping batch after first failed task", file=sys.stderr)
                break
            continue

        summaries.append(summary)
        print(
            f"[saved] {summary.symbol} {summary.interval} "
            f"rows={summary.row_count} pages={summary.page_count} file={summary.data_path}",
            file=sys.stderr,
        )

    manifest_path = Path(args.output_root) / f"{run_id}.manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = manifest_payload(
        args=args,
        symbols=symbols,
        run_id=run_id,
        summaries=summaries,
        failures=failures,
    )
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"[manifest] {manifest_path}", file=sys.stderr)
    print(
        f"[summary] success={len(summaries)} failed={len(failures)}",
        file=sys.stderr,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
