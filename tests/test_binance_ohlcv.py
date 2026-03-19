from __future__ import annotations

import http.client
import io
import json
import socket
import ssl
from argparse import Namespace
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
from urllib.error import URLError

from src.ingestion.binance_ohlcv import (
    DownloadRequest,
    build_candidate_urls,
    datetime_to_milliseconds,
    fetch_exchange_symbols,
    fetch_json_payload,
    initialize_download_session,
    interval_to_milliseconds,
    kline_row_to_record,
    parse_datetime_utc,
    retry_delay_with_retry_after,
    resolve_start_ms,
    resolve_symbols,
    write_checkpoint,
)


class BinanceOhlcvTests(unittest.TestCase):
    def test_interval_to_milliseconds(self) -> None:
        self.assertEqual(interval_to_milliseconds("5m"), 300_000)
        self.assertEqual(interval_to_milliseconds("1h"), 3_600_000)
        self.assertEqual(interval_to_milliseconds("1w"), 604_800_000)

    def test_parse_datetime_utc_start_date(self) -> None:
        parsed = parse_datetime_utc("2026-03-18", end_value=False)
        self.assertEqual(parsed.isoformat(), "2026-03-18T00:00:00+00:00")

    def test_parse_datetime_utc_end_date_is_end_of_day(self) -> None:
        parsed = parse_datetime_utc("2026-03-18", end_value=True)
        self.assertEqual(parsed.isoformat(), "2026-03-19T00:00:00+00:00")

    def test_parse_datetime_utc_normalizes_timezone(self) -> None:
        parsed = parse_datetime_utc("2026-03-18T08:00:00+08:00", end_value=False)
        self.assertEqual(parsed.isoformat(), "2026-03-18T00:00:00+00:00")

    def test_kline_row_to_record(self) -> None:
        row = [
            1_710_720_000_000,
            "62000.10",
            "62500.00",
            "61888.80",
            "62333.30",
            "123.45",
            1_710_723_599_999,
            "7700000.00",
            4567,
            "61.00",
            "3800000.00",
            "0",
        ]
        record = kline_row_to_record(
            row,
            symbol="BTCUSDT",
            interval="1h",
            fetched_at="2026-03-18T01:02:03Z",
        )

        self.assertEqual(record["symbol"], "BTCUSDT")
        self.assertEqual(record["interval"], "1h")
        self.assertEqual(record["open"], "62000.10")
        self.assertEqual(record["number_of_trades"], 4567)
        self.assertEqual(record["fetched_at"], "2026-03-18T01:02:03Z")

    def test_datetime_to_milliseconds(self) -> None:
        parsed = parse_datetime_utc("1970-01-01T00:00:01+00:00", end_value=False)
        self.assertEqual(datetime_to_milliseconds(parsed), 1000)

    def test_resolve_start_ms_defaults_to_epoch_floor_for_listing_mode(self) -> None:
        args = Namespace(
            start_date=None,
            start_from_listing=True,
        )
        self.assertEqual(resolve_start_ms(args), 0)

    def test_resolve_symbols_uses_exchange_info_when_all_spot_requested(self) -> None:
        args = Namespace(
            all_spot_symbols=True,
            symbols=["BTCUSDT"],
            symbol_statuses=["TRADING"],
            quote_assets=["USDT"],
            exchange_info_base_url="https://api.binance.com",
            timeout_seconds=30.0,
            max_retries=3,
            retry_delay_seconds=1.0,
            max_symbols=2,
        )

        with patch(
            "src.ingestion.binance_ohlcv.fetch_exchange_symbols",
            return_value=["ADAUSDT", "BTCUSDT"],
        ) as mocked:
            symbols = resolve_symbols(args)

        self.assertEqual(symbols, ["ADAUSDT", "BTCUSDT"])
        mocked.assert_called_once()

    def test_retry_delay_uses_retry_after_when_larger(self) -> None:
        error = SimpleNamespace(headers={"Retry-After": "7"})
        delay = retry_delay_with_retry_after(
            error,
            base_delay_seconds=1.0,
            attempt=1,
            jitter_seconds=0.0,
        )
        self.assertEqual(delay, 7.0)

    @patch("src.ingestion.binance_ohlcv.fetch_json_payload")
    def test_fetch_exchange_symbols_filters_quote_asset(self, mocked_fetch: object) -> None:
        mocked_fetch.return_value = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                },
                {
                    "symbol": "BTCFDUSD",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "FDUSD",
                },
            ]
        }

        symbols = fetch_exchange_symbols(
            base_url="https://api.binance.com",
            timeout_seconds=30.0,
            max_retries=3,
            retry_delay_seconds=1.0,
            allowed_statuses={"TRADING"},
            allowed_quote_assets={"USDT"},
            max_symbols=None,
        )
        self.assertEqual(symbols, ["BTCUSDT"])

    def test_initialize_download_session_resumes_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir)
            request = DownloadRequest(
                symbol="BTCUSDT",
                interval="1h",
                start_ms=0,
                end_ms=1000,
                base_url="https://data-api.binance.vision",
                fallback_base_urls=("https://api.binance.com",),
                output_root=output_root,
                limit=1000,
                timeout_seconds=30.0,
                sleep_seconds=0.0,
                max_retries=3,
                retry_delay_seconds=1.0,
                retry_jitter_seconds=0.0,
                start_from_listing=False,
                resume_incomplete=True,
            )

            first = initialize_download_session(request, run_id="run_a")
            first.data_path.write_text("{}", encoding="utf-8")
            first.current_start_ms = 500
            first.row_count = 10
            write_checkpoint(request, first)

            with patch("sys.stderr", new=io.StringIO()):
                resumed = initialize_download_session(request, run_id="run_b")

            self.assertTrue(resumed.resumed_from_checkpoint)
            self.assertEqual(resumed.run_id, "run_a")
            self.assertEqual(resumed.current_start_ms, 500)
            self.assertEqual(resumed.row_count, 10)
            self.assertEqual(resumed.data_path, first.data_path)

    def test_build_candidate_urls_dedupes_primary_and_fallbacks(self) -> None:
        urls = build_candidate_urls(
            "https://data-api.binance.vision",
            "/api/v3/klines",
            {"symbol": "BTCUSDT"},
            fallback_base_urls=(
                "https://api.binance.com",
                "https://data-api.binance.vision",
            ),
        )
        self.assertEqual(
            urls,
            [
                "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT",
                "https://api.binance.com/api/v3/klines?symbol=BTCUSDT",
            ],
        )

    @patch("src.ingestion.binance_ohlcv.time.sleep")
    @patch("src.ingestion.binance_ohlcv.urlopen")
    def test_fetch_json_payload_rotates_to_fallback_after_url_error(
        self,
        mocked_urlopen: object,
        mocked_sleep: object,
    ) -> None:
        mocked_urlopen.side_effect = [
            URLError("EOF"),
            io.StringIO('{"ok": true}'),
        ]

        payload = fetch_json_payload(
            [
                "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT",
                "https://api.binance.com/api/v3/klines?symbol=BTCUSDT",
            ],
            timeout_seconds=30.0,
            max_retries=1,
            retry_delay_seconds=0.0,
            retry_jitter_seconds=0.0,
            request_label="BTCUSDT 5m",
        )

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(
            mocked_urlopen.call_args_list[0].args[0].full_url,
            "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT",
        )
        self.assertEqual(
            mocked_urlopen.call_args_list[1].args[0].full_url,
            "https://api.binance.com/api/v3/klines?symbol=BTCUSDT",
        )
        mocked_sleep.assert_called_once_with(0.0)

    @patch("src.ingestion.binance_ohlcv.time.sleep")
    @patch("src.ingestion.binance_ohlcv.urlopen")
    def test_fetch_json_payload_retries_direct_ssl_error(
        self,
        mocked_urlopen: object,
        mocked_sleep: object,
    ) -> None:
        mocked_urlopen.side_effect = [
            ssl.SSLEOFError(8, "EOF occurred in violation of protocol"),
            io.StringIO("[]"),
        ]

        payload = fetch_json_payload(
            ["https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT"],
            timeout_seconds=30.0,
            max_retries=1,
            retry_delay_seconds=0.0,
            retry_jitter_seconds=0.0,
            request_label="BTCUSDT 5m",
        )

        self.assertEqual(payload, [])
        mocked_sleep.assert_called_once_with(0.0)

    @patch("src.ingestion.binance_ohlcv.time.sleep")
    @patch("src.ingestion.binance_ohlcv.urlopen")
    def test_fetch_json_payload_retries_read_timeout(
        self,
        mocked_urlopen: object,
        mocked_sleep: object,
    ) -> None:
        class TimeoutResponse:
            def __enter__(self) -> "TimeoutResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
                return False

            def read(self) -> bytes:
                raise socket.timeout("The read operation timed out")

        class OkResponse:
            def __enter__(self) -> "OkResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
                return False

            def read(self) -> bytes:
                return b'{"rows": 1}'

        mocked_urlopen.side_effect = [TimeoutResponse(), OkResponse()]

        payload = fetch_json_payload(
            ["https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT"],
            timeout_seconds=30.0,
            max_retries=1,
            retry_delay_seconds=0.0,
            retry_jitter_seconds=0.0,
            request_label="BTCUSDT 5m",
        )

        self.assertEqual(payload, {"rows": 1})
        mocked_sleep.assert_called_once_with(0.0)

    @patch("src.ingestion.binance_ohlcv.time.sleep")
    @patch("src.ingestion.binance_ohlcv.urlopen")
    def test_fetch_json_payload_retries_incomplete_read(
        self,
        mocked_urlopen: object,
        mocked_sleep: object,
    ) -> None:
        class IncompleteResponse:
            def __enter__(self) -> "IncompleteResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
                return False

            def read(self) -> bytes:
                raise http.client.IncompleteRead(b'{"ok":', 10)

        class OkResponse:
            def __enter__(self) -> "OkResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
                return False

            def read(self) -> bytes:
                return b'{"ok": true}'

        mocked_urlopen.side_effect = [IncompleteResponse(), OkResponse()]

        payload = fetch_json_payload(
            ["https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT"],
            timeout_seconds=30.0,
            max_retries=1,
            retry_delay_seconds=0.0,
            retry_jitter_seconds=0.0,
            request_label="BTCUSDT 5m",
        )

        self.assertEqual(payload, {"ok": True})
        mocked_sleep.assert_called_once_with(0.0)

    @patch("src.ingestion.binance_ohlcv.time.sleep")
    @patch("src.ingestion.binance_ohlcv.urlopen")
    def test_fetch_json_payload_retries_json_decode_error(
        self,
        mocked_urlopen: object,
        mocked_sleep: object,
    ) -> None:
        class BadJsonResponse:
            def __enter__(self) -> "BadJsonResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
                return False

            def read(self) -> bytes:
                return b'{"ok":'

        class OkResponse:
            def __enter__(self) -> "OkResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
                return False

            def read(self) -> bytes:
                return json.dumps({"ok": True}).encode("utf-8")

        mocked_urlopen.side_effect = [BadJsonResponse(), OkResponse()]

        payload = fetch_json_payload(
            ["https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT"],
            timeout_seconds=30.0,
            max_retries=1,
            retry_delay_seconds=0.0,
            retry_jitter_seconds=0.0,
            request_label="BTCUSDT 5m",
        )

        self.assertEqual(payload, {"ok": True})
        mocked_sleep.assert_called_once_with(0.0)


if __name__ == "__main__":
    unittest.main()
