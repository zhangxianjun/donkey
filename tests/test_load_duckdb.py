from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

from src.warehouse.load_duckdb import (
    discover_normalized_files,
    normalized_record_to_row,
)

HAS_DUCKDB = importlib.util.find_spec("duckdb") is not None


class WarehouseLoaderUnitTests(unittest.TestCase):
    def test_discover_normalized_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            version_dir = root / "v1"
            version_dir.mkdir(parents=True)

            (version_dir / "market_ohlcv_1h.jsonl").write_text("", encoding="utf-8")
            (version_dir / "market_ohlcv_5m.parquet").write_text("", encoding="utf-8")
            (version_dir / "normalize_manifest.json").write_text("{}", encoding="utf-8")

            discovered = discover_normalized_files(
                root,
                data_version="v1",
                intervals={"1h"},
            )

            self.assertEqual(len(discovered), 1)
            self.assertEqual(discovered[0].interval, "1h")
            self.assertEqual(discovered[0].file_format, "jsonl")

    def test_normalized_record_to_row(self) -> None:
        record = {
            "ts": "2026-03-18T00:00:00.000Z",
            "symbol": "BTCUSDT",
            "exchange": "binance",
            "market_type": "spot",
            "interval": "1h",
            "open": 100,
            "high": 110,
            "low": 90,
            "close": 105,
            "volume": 123.45,
            "quote_volume": 1000.0,
            "trade_count": 50,
            "source_file": "data/normalized/v1/market_ohlcv_1h.jsonl",
            "data_version": "v1",
            "created_at": "2026-03-18T01:00:00Z",
        }

        row = normalized_record_to_row(
            record,
            source_path=Path("data/normalized/v1/market_ohlcv_1h.jsonl"),
            line_no=1,
        )

        self.assertEqual(row[1], "BTCUSDT")
        self.assertEqual(row[4], "1h")
        self.assertEqual(row[9], 123.45)
        self.assertEqual(row[12], "data/normalized/v1/market_ohlcv_1h.jsonl")
        self.assertEqual(row[13], "v1")


@unittest.skipUnless(HAS_DUCKDB, "duckdb package is not installed")
class WarehouseLoaderIntegrationTests(unittest.TestCase):
    def test_load_script_is_idempotent_for_same_version_interval(self) -> None:
        import duckdb

        from src.warehouse.load_duckdb import initialize_database, load_normalized_file, NormalizedFile

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            db_dir = repo_root / "db"
            sql_dir = repo_root / "sql"
            normalized_dir = repo_root / "data" / "normalized" / "v1"
            db_dir.mkdir(parents=True)
            sql_dir.mkdir(parents=True)
            normalized_dir.mkdir(parents=True)

            init_sql = sql_dir / "init_v1.sql"
            init_sql.write_text(
                """
                CREATE TABLE IF NOT EXISTS market_ohlcv (
                    ts TIMESTAMP NOT NULL,
                    symbol VARCHAR NOT NULL,
                    exchange VARCHAR NOT NULL,
                    market_type VARCHAR NOT NULL,
                    interval VARCHAR NOT NULL,
                    open DOUBLE NOT NULL,
                    high DOUBLE NOT NULL,
                    low DOUBLE NOT NULL,
                    close DOUBLE NOT NULL,
                    volume DOUBLE NOT NULL,
                    quote_volume DOUBLE,
                    trade_count BIGINT,
                    source_file VARCHAR,
                    data_version VARCHAR NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                ATTACH 'db/experiments.duckdb' AS exp;
                CREATE TABLE IF NOT EXISTS exp.experiments (
                    experiment_id VARCHAR
                );
                CREATE TABLE IF NOT EXISTS exp.artifacts (
                    experiment_id VARCHAR
                );
                DETACH exp;
                """.strip()
                + "\n",
                encoding="utf-8",
            )

            normalized_path = normalized_dir / "market_ohlcv_1h.jsonl"
            normalized_path.write_text(
                json.dumps(
                    {
                        "ts": "2026-03-18T00:00:00.000Z",
                        "symbol": "BTCUSDT",
                        "exchange": "binance",
                        "market_type": "spot",
                        "interval": "1h",
                        "open": 100,
                        "high": 110,
                        "low": 90,
                        "close": 105,
                        "volume": 123.45,
                        "quote_volume": 1000.0,
                        "trade_count": 50,
                        "source_file": "data/normalized/v1/market_ohlcv_1h.jsonl",
                        "data_version": "v1",
                        "created_at": "2026-03-18T01:00:00Z",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            db_path = db_dir / "quant.duckdb"
            connection = duckdb.connect(str(db_path))
            try:
                initialize_database(connection, init_sql_path=init_sql, repo_root=repo_root)
                normalized_file = NormalizedFile(
                    interval="1h",
                    file_format="jsonl",
                    path=normalized_path,
                )

                first = load_normalized_file(
                    connection,
                    normalized_file,
                    data_version="v1",
                    repo_root=repo_root,
                )
                second = load_normalized_file(
                    connection,
                    normalized_file,
                    data_version="v1",
                    repo_root=repo_root,
                )

                row_count = connection.execute(
                    "SELECT COUNT(*) FROM market_ohlcv WHERE data_version = 'v1' AND interval = '1h'"
                ).fetchone()[0]

                self.assertEqual(first.inserted_rows, 1)
                self.assertEqual(first.replaced_rows, 0)
                self.assertEqual(second.inserted_rows, 1)
                self.assertEqual(second.replaced_rows, 1)
                self.assertEqual(row_count, 1)
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
