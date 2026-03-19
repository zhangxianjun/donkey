from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.normalize.market_ohlcv import (
    discover_raw_files,
    load_and_dedupe_records,
    normalize_raw_record,
)


class MarketOhlcvNormalizeTests(unittest.TestCase):
    def test_normalize_raw_record_maps_fields(self) -> None:
        raw = {
            "exchange": "binance",
            "market_type": "spot",
            "symbol": "BTCUSDT",
            "interval": "1h",
            "open_time_iso": "2026-03-18T00:00:00.000Z",
            "open": "100.1",
            "high": "101.2",
            "low": "99.9",
            "close": "100.5",
            "volume": "123.45",
            "quote_asset_volume": "12400.0",
            "number_of_trades": 55,
            "fetched_at": "2026-03-18T00:10:00Z",
        }

        normalized = normalize_raw_record(
            raw,
            source_file="data/raw/binance/spot/BTCUSDT/1h/example.jsonl",
            line_no=1,
            data_version="v1",
            created_at="2026-03-18T01:00:00Z",
        )

        self.assertEqual(normalized.ts, "2026-03-18T00:00:00.000Z")
        self.assertEqual(normalized.symbol, "BTCUSDT")
        self.assertEqual(normalized.interval, "1h")
        self.assertEqual(normalized.open, 100.1)
        self.assertEqual(normalized.trade_count, 55)
        self.assertEqual(
            normalized.source_file,
            "data/raw/binance/spot/BTCUSDT/1h/example.jsonl",
        )

    def test_discover_raw_files_filters_only_symbol_interval_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "BTCUSDT" / "1h").mkdir(parents=True)
            (root / "ETHUSDT" / "5m").mkdir(parents=True)

            (root / "BTCUSDT" / "1h" / "a.jsonl").write_text("", encoding="utf-8")
            (root / "BTCUSDT" / "1h" / "a.meta.json").write_text("{}", encoding="utf-8")
            (root / "run.manifest.json").write_text("{}", encoding="utf-8")
            (root / "ETHUSDT" / "5m" / "b.jsonl").write_text("", encoding="utf-8")

            paths = discover_raw_files(
                root,
                symbols={"BTCUSDT"},
                intervals={"1h"},
            )

            self.assertEqual([path.name for path in paths], ["a.jsonl"])

    def test_load_and_dedupe_records_prefers_latest_fetched_at(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            input_root = repo_root / "data" / "raw" / "binance" / "spot" / "ETHUSDT" / "5m"
            input_root.mkdir(parents=True)

            older_file = input_root / "older.jsonl"
            newer_file = input_root / "newer.jsonl"

            older_file.write_text(
                json.dumps(
                    {
                        "exchange": "binance",
                        "market_type": "spot",
                        "symbol": "ETHUSDT",
                        "interval": "5m",
                        "open_time_iso": "2026-03-17T00:00:00.000Z",
                        "open": "10",
                        "high": "11",
                        "low": "9",
                        "close": "10.5",
                        "volume": "100",
                        "quote_asset_volume": "1000",
                        "number_of_trades": 1,
                        "fetched_at": "2026-03-18T01:00:00Z",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            newer_file.write_text(
                json.dumps(
                    {
                        "exchange": "binance",
                        "market_type": "spot",
                        "symbol": "ETHUSDT",
                        "interval": "5m",
                        "open_time_iso": "2026-03-17T00:00:00.000Z",
                        "open": "20",
                        "high": "21",
                        "low": "19",
                        "close": "20.5",
                        "volume": "200",
                        "quote_asset_volume": "2000",
                        "number_of_trades": 2,
                        "fetched_at": "2026-03-18T02:00:00Z",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            by_interval, duplicate_counter = load_and_dedupe_records(
                [older_file, newer_file],
                data_version="v1",
                created_at="2026-03-18T03:00:00Z",
                repo_root=repo_root,
            )

            self.assertEqual(duplicate_counter["5m"], 1)
            deduped = list(by_interval["5m"].values())
            self.assertEqual(len(deduped), 1)
            self.assertEqual(deduped[0].open, 20.0)
            self.assertEqual(
                deduped[0].source_file,
                "data/raw/binance/spot/ETHUSDT/5m/newer.jsonl",
            )


if __name__ == "__main__":
    unittest.main()
