from __future__ import annotations

import base64
import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.request import Request, urlopen
from unittest.mock import patch

from src.admin.pairs_dashboard import (
    add_persisted_strategy_root,
    BacktestJob,
    BacktestRunRequest,
    build_strategy_config_payload,
    build_pair_preference_key,
    clone_strategy_config,
    DashboardConfig,
    DuckDBLoadJob,
    DownloadJob,
    find_strategy_entry_by_path,
    fetch_charting_library_asset,
    KlineDownloadRequest,
    LocalPair,
    load_persisted_strategy_roots,
    NormalizeJob,
    parse_simple_yaml_file,
    parse_strategy_clone_payload,
    parse_strategy_root_payload,
    PairAdminHTTPServer,
    SourcePair,
    discover_strategy_entries,
    discover_local_pairs,
    load_pair_preferences,
    parse_pair_preference_payload,
    update_pair_preference,
)


class PairsDashboardTests(unittest.TestCase):
    class MockResponse:
        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        def read(self) -> bytes:
            return self._payload

        def __enter__(self) -> "PairsDashboardTests.MockResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    @staticmethod
    def serve_requests(server: PairAdminHTTPServer, request_count: int) -> None:
        for _ in range(request_count):
            server.handle_request()

    @staticmethod
    def make_config(workspace_root: Path) -> DashboardConfig:
        return DashboardConfig(
            workspace_root=workspace_root,
            raw_root=workspace_root / "data" / "raw" / "binance" / "spot",
            normalized_root=workspace_root / "data" / "normalized",
            strategies_root=workspace_root / "config" / "strategies",
            extra_strategy_roots=tuple(),
            strategy_root_store_path=workspace_root / "data" / "admin" / "strategy_roots.json",
            backtests_root=workspace_root / "data" / "backtests",
            logs_root=workspace_root / "logs",
            quant_db_path=workspace_root / "db" / "quant.duckdb",
            experiments_db_path=workspace_root / "db" / "experiments.duckdb",
            local_trading_store_path=workspace_root / "data" / "admin" / "local_trading_pairs.json",
            pair_preferences_store_path=workspace_root / "data" / "admin" / "pair_preferences.json",
            currency_icon_root=workspace_root / "data" / "admin" / "currency_icons",
            exchange_info_base_url="https://api.binance.com",
            market_data_base_url="https://data-api.binance.vision",
            fallback_base_urls=("https://api.binance.com",),
            timeout_seconds=30.0,
            max_retries=3,
            retry_delay_seconds=1.0,
            retry_jitter_seconds=0.5,
            limit=1000,
            sleep_seconds=0.0,
            default_quote_asset="USDT",
            default_tradeable_only=True,
        )

    def test_discover_strategy_entries_includes_external_roots(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, tempfile.TemporaryDirectory() as external_tmpdir:
            workspace_root = Path(tmpdir)
            external_root = Path(external_tmpdir)
            config = self.make_config(workspace_root)
            config = DashboardConfig(**{**config.__dict__, "extra_strategy_roots": (external_root,)})

            normalized_version_dir = config.normalized_root / "v1"
            normalized_version_dir.mkdir(parents=True)
            (normalized_version_dir / "market_ohlcv_1h.jsonl").write_text(
                json.dumps(
                    {
                        "ts": "2026-03-18T00:00:00.000Z",
                        "symbol": "BNBUSDT",
                        "interval": "1h",
                        "open": 100,
                        "high": 110,
                        "low": 90,
                        "close": 105,
                        "volume": 123.45,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            local_strategy_dir = config.strategies_root
            local_strategy_dir.mkdir(parents=True)
            (local_strategy_dir / "demo_v1.yaml").write_text(
                "\n".join(
                    [
                        "strategy_name: demo",
                        "strategy_version: v1",
                        "data:",
                        "  data_version: v1",
                        "universe:",
                        "  symbols:",
                        "    - BTCUSDT",
                        "  interval: 1h",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            external_strategy_dir = external_root / "configs"
            external_strategy_dir.mkdir(parents=True)
            external_strategy_path = external_strategy_dir / "jingdaihuakai_v1.yaml"
            external_strategy_path.write_text(
                "\n".join(
                    [
                        'strategy_name: jingdaihuakai',
                        'strategy_version: v1',
                        'display_name: "静待花开策略"',
                        'description: "private strategy"',
                        "data:",
                        "  data_version: v1",
                        "universe:",
                        "  symbols:",
                        "    - BNBUSDT",
                        "  interval: 1h",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            entries = discover_strategy_entries(config)
            external_entry = find_strategy_entry_by_path(config, str(external_strategy_path))

        self.assertEqual(len(entries), 2)
        assert external_entry is not None
        self.assertEqual(external_entry.display_name, "静待花开策略")
        self.assertEqual(external_entry.location_kind, "external")
        self.assertEqual(external_entry.strategy_root, str(external_root.resolve()))
        self.assertTrue(external_entry.input_exists)

    def test_clone_strategy_config_saves_into_external_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, tempfile.TemporaryDirectory() as external_tmpdir:
            workspace_root = Path(tmpdir)
            external_root = Path(external_tmpdir)
            config = self.make_config(workspace_root)
            external_configs_root = external_root / "configs"
            config = DashboardConfig(**{**config.__dict__, "extra_strategy_roots": (external_configs_root,)})

            normalized_version_dir = config.normalized_root / "v1"
            normalized_version_dir.mkdir(parents=True)
            (normalized_version_dir / "market_ohlcv_1h.jsonl").write_text(
                json.dumps(
                    {
                        "ts": "2026-03-18T00:00:00.000Z",
                        "symbol": "BNBUSDT",
                        "interval": "1h",
                        "open": 100,
                        "high": 110,
                        "low": 90,
                        "close": 105,
                        "volume": 123.45,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            external_configs_root.mkdir(parents=True)
            strategy_path = external_configs_root / "jingdaihuakai_v1.yaml"
            strategy_path.write_text(
                "\n".join(
                    [
                        "strategy_name: jingdaihuakai",
                        "strategy_version: v1",
                        'display_name: "静待花开策略"',
                        'description: "private strategy"',
                        "module:",
                        "  path: ../strategies/jingdaihuakai.py",
                        "  factory_name: build_strategy",
                        "universe:",
                        "  symbols:",
                        "    - BNBUSDT",
                        "  interval: 1h",
                        "data:",
                        "  data_version: v1",
                        "signal:",
                        "  min_swing_amplitude: 0.18",
                        "  fib_tolerance_pct: 0.05",
                        "execution:",
                        "  fee_bps: 10",
                        "risk:",
                        "  max_position_per_symbol: 0.5",
                        "backtest:",
                        "  engine: native",
                        "artifacts:",
                        "  signal_path: data/signals/jingdaihuakai_v1/{symbol}_1h_signal.jsonl",
                        "  trades_path: data/backtests/jingdaihuakai_v1/trades.jsonl",
                        "  equity_path: data/backtests/jingdaihuakai_v1/portfolio_equity.jsonl",
                        "  summary_path: data/backtests/jingdaihuakai_v1/summary.json",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            config_payload = build_strategy_config_payload(config, str(strategy_path))
            clone_request = parse_strategy_clone_payload(
                {
                    "source_strategy_path": str(strategy_path),
                    "target_filename": "jingdaihuakai_v1_cal.yaml",
                    "strategy_name": "jingdaihuakai",
                    "strategy_version": "v1_cal",
                    "display_name": "静待花开策略 调参版",
                    "description": "calibrated clone",
                    "updates": {
                        "signal.min_swing_amplitude": 0.2,
                        "signal.fib_tolerance_pct": 0.06,
                        "universe.symbols": ["BNBUSDT", "ETHUSDT"],
                    },
                }
            )
            result = clone_strategy_config(config, clone_request)
            cloned_path = Path(result["strategy_path"])
            cloned_config = parse_simple_yaml_file(cloned_path)
            cloned_entry = find_strategy_entry_by_path(config, str(cloned_path))

        self.assertEqual(config_payload["clone_defaults"]["strategy_version"], "v1_cal")
        self.assertEqual(config_payload["strategy"]["display_name"], "静待花开策略")
        self.assertEqual(cloned_path.parent, external_configs_root.resolve())
        self.assertEqual(cloned_config["strategy_version"], "v1_cal")
        self.assertEqual(cloned_config["display_name"], "静待花开策略 调参版")
        self.assertEqual(cloned_config["signal"]["min_swing_amplitude"], 0.2)
        self.assertEqual(cloned_config["signal"]["fib_tolerance_pct"], 0.06)
        self.assertEqual(cloned_config["universe"]["symbols"], ["BNBUSDT", "ETHUSDT"])
        self.assertEqual(
            cloned_config["artifacts"]["summary_path"],
            "data/backtests/jingdaihuakai_v1_cal/summary.json",
        )
        self.assertEqual(
            cloned_config["artifacts"]["signal_path"],
            "data/signals/jingdaihuakai_v1_cal/{symbol}_1h_signal.jsonl",
        )
        assert cloned_entry is not None
        self.assertEqual(cloned_entry.location_kind, "external")
        self.assertEqual(cloned_entry.display_name, "静待花开策略 调参版")

    def test_add_persisted_strategy_root_saves_store_and_discovers_yml(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, tempfile.TemporaryDirectory() as external_tmpdir:
            workspace_root = Path(tmpdir)
            external_root = Path(external_tmpdir) / "configs"
            external_root.mkdir(parents=True)
            config = self.make_config(workspace_root)

            strategy_path = external_root / "private_demo_v1.yml"
            strategy_path.write_text(
                "\n".join(
                    [
                        "strategy_name: private_demo",
                        "strategy_version: v1",
                        'display_name: "外部示例策略"',
                        "universe:",
                        "  symbols:",
                        "    - BTCUSDT",
                        "  interval: 1h",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            result = add_persisted_strategy_root(
                config,
                parse_strategy_root_payload({"root_path": str(external_root)}),
            )
            persisted_roots = load_persisted_strategy_roots(config)
            discovered = discover_strategy_entries(config)
            store_exists = config.strategy_root_store_path.exists()

        self.assertTrue(result["created"])
        self.assertEqual(result["strategy_count"], 1)
        self.assertEqual(persisted_roots, (external_root.resolve(),))
        self.assertTrue(store_exists)
        self.assertIn(str(external_root.resolve()), [item.strategy_root for item in discovered])
        self.assertEqual(discovered[0].display_name, "外部示例策略")

    def test_discover_local_pairs_aggregates_intervals_and_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_root = Path(tmpdir)
            btc_1h = raw_root / "BTCUSDT" / "1h"
            btc_4h = raw_root / "BTCUSDT" / "4h"
            eth_5m = raw_root / "ETHUSDT" / "5m"
            btc_1h.mkdir(parents=True)
            btc_4h.mkdir(parents=True)
            eth_5m.mkdir(parents=True)

            (btc_1h / "run_a.jsonl").write_text("{}", encoding="utf-8")
            (btc_1h / "run_a.meta.json").write_text("{}", encoding="utf-8")
            (btc_1h / "_checkpoint.json").write_text("{}", encoding="utf-8")
            (btc_4h / "run_b.jsonl").write_text("{}", encoding="utf-8")
            (eth_5m / "run_c.jsonl").write_text("{}", encoding="utf-8")

            discovered = discover_local_pairs(raw_root)

        self.assertEqual([item.symbol for item in discovered], ["BTCUSDT", "ETHUSDT"])
        self.assertEqual(discovered[0].source, "binance")
        self.assertEqual(discovered[0].source_label, "币安")
        self.assertEqual(discovered[0].display_symbol, "BTCUSDT")
        self.assertEqual(discovered[0].intervals, ["1h", "4h"])
        self.assertEqual(discovered[0].interval_count, 2)
        self.assertEqual(discovered[0].data_file_count, 2)
        self.assertEqual(discovered[0].metadata_file_count, 1)
        self.assertEqual(discovered[0].checkpoint_count, 1)
        self.assertIsNotNone(discovered[0].last_updated)

    def test_fetch_charting_library_asset_caches_remote_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = b"console.log('chart');"

            with patch(
                "src.admin.pairs_dashboard.urlopen",
                return_value=self.MockResponse(payload),
            ) as mocked_urlopen:
                asset_path = fetch_charting_library_asset("bundles/runtime.js", root=root, timeout_seconds=1.0)

            self.assertEqual(asset_path, (root / "bundles" / "runtime.js").resolve())
            self.assertEqual(asset_path.read_bytes(), payload)
            request = mocked_urlopen.call_args.args[0]
            self.assertEqual(
                request.full_url,
                "https://charting-library.tradingview-widget.com/charting_library/bundles/runtime.js",
            )

    def test_fetch_charting_library_asset_allows_empty_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            with patch(
                "src.admin.pairs_dashboard.urlopen",
                return_value=self.MockResponse(b""),
            ):
                asset_path = fetch_charting_library_asset("bundles/empty.css", root=root, timeout_seconds=1.0)

            self.assertEqual(asset_path.read_bytes(), b"")

    def test_pair_preferences_are_persisted_and_removed_when_cleared(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace_root = Path(tmpdir)
            config = self.make_config(workspace_root)

            updated = update_pair_preference(
                config,
                parse_pair_preference_payload(
                    {
                        "kind": "duckdb",
                        "key": build_pair_preference_key(
                            "duckdb",
                            data_version="v1",
                            interval="1h",
                            symbol="BTCUSDT",
                        ),
                        "hidden": True,
                        "pinned": True,
                    }
                ),
            )
            loaded = load_pair_preferences(config)
            cleared = update_pair_preference(
                config,
                parse_pair_preference_payload(
                    {
                        "kind": "duckdb",
                        "key": build_pair_preference_key(
                            "duckdb",
                            data_version="v1",
                            interval="1h",
                            symbol="BTCUSDT",
                        ),
                        "hidden": False,
                        "pinned": False,
                    }
                ),
            )

        assert updated is not None
        self.assertEqual(updated.key, "duckdb:v1:1h:BTCUSDT")
        self.assertTrue(updated.hidden)
        self.assertTrue(updated.pinned)
        self.assertEqual(len(loaded), 1)
        self.assertIsNone(cleared)

    def test_http_endpoints_expose_dashboard_resources(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workspace_root = Path(tmpdir)
            config = self.make_config(workspace_root)

            interval_dir = config.raw_root / "BTCUSDT" / "1h"
            interval_dir.mkdir(parents=True)
            (interval_dir / "run_a.jsonl").write_text("{}", encoding="utf-8")
            (interval_dir / "run_a.meta.json").write_text("{}", encoding="utf-8")
            okx_interval_dir = workspace_root / "data" / "raw" / "okx" / "spot" / "BTC-USDT" / "1h"
            okx_interval_dir.mkdir(parents=True)
            (okx_interval_dir / "run_okx.jsonl").write_text("{}", encoding="utf-8")

            config.normalized_root.mkdir(parents=True)
            normalized_version_dir = config.normalized_root / "v1"
            normalized_version_dir.mkdir(parents=True)
            (normalized_version_dir / "market_ohlcv_1h.jsonl").write_text(
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
                        "source_file": "data/raw/binance/spot/BTCUSDT/1h/run_a.jsonl",
                        "data_version": "v1",
                        "created_at": "2026-03-18T01:00:00Z",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (normalized_version_dir / "normalize_manifest.json").write_text(
                json.dumps(
                    {
                        "data_version": "v1",
                        "output_format": "jsonl",
                        "created_at": "2026-03-20T00:00:00Z",
                        "raw_file_count": 1,
                        "interval_outputs": [
                            {
                                "interval": "1h",
                                "output_path": str(normalized_version_dir / "market_ohlcv_1h.jsonl"),
                                "row_count": 1,
                                "source_file_count": 1,
                                "duplicate_rows_removed": 0,
                            }
                        ],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            config.logs_root.mkdir(parents=True)
            (config.logs_root / "admin.log").write_text("ok", encoding="utf-8")
            config.quant_db_path.parent.mkdir(parents=True)
            config.quant_db_path.write_text("", encoding="utf-8")
            config.experiments_db_path.write_text("", encoding="utf-8")

            strategy_dir = config.strategies_root
            strategy_dir.mkdir(parents=True)
            (strategy_dir / "demo_v1.yaml").write_text(
                "\n".join(
                    [
                        "strategy_name: demo",
                        "strategy_version: v1",
                        'display_name: "演示策略"',
                        'description: "demo strategy"',
                        "universe:",
                        "  exchange: binance",
                        "  market_type: spot",
                        "  symbols:",
                        "    - BTCUSDT",
                        "  interval: 1d",
                        "backtest:",
                        '  start_date: "2020-01-01"',
                        '  end_date: "2025-12-31"',
                        "artifacts:",
                        "  trades_path: data/backtests/demo_v1/trades.parquet",
                        "  equity_path: data/backtests/demo_v1/portfolio_equity.parquet",
                        "  summary_path: data/backtests/demo_v1/summary.json",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            backtest_dir = config.backtests_root / "demo_v1"
            backtest_dir.mkdir(parents=True)
            (backtest_dir / "summary.json").write_text(
                json.dumps({"sharpe": 1.2, "total_return": 0.34}),
                encoding="utf-8",
            )
            (backtest_dir / "trades.parquet").write_text("", encoding="utf-8")
            (backtest_dir / "portfolio_equity.parquet").write_text("", encoding="utf-8")
            config.currency_icon_root.mkdir(parents=True)
            (config.currency_icon_root / "DEFAULT.svg").write_text(
                "<svg xmlns='http://www.w3.org/2000/svg'><text>DEFAULT</text></svg>",
                encoding="utf-8",
            )
            (config.currency_icon_root / "BTC.svg").write_text(
                "<svg xmlns='http://www.w3.org/2000/svg'><text>BTC</text></svg>",
                encoding="utf-8",
            )
            (config.currency_icon_root / "catalog.json").write_text(
                json.dumps(
                    {
                        "generated_at": "2026-03-20T00:00:00Z",
                        "sources": ["binance"],
                        "source_counts": {"binance": 2},
                        "asset_count": 2,
                        "assets": ["BTC", "ETH"],
                    }
                ),
                encoding="utf-8",
            )

            server = PairAdminHTTPServer(("127.0.0.1", 0), config=config)
            thread = threading.Thread(
                target=self.serve_requests,
                args=(server, 58),
                daemon=True,
            )
            thread.start()

            remote_pair = SourcePair(
                source="binance",
                source_label="币安",
                symbol="BTCUSDT",
                display_symbol="BTCUSDT",
                base_asset="BTC",
                quote_asset="USDT",
                status="TRADING",
                tradeable=True,
                source_kind="remote",
            )

            job = DownloadJob(
                job_id="job_123456",
                symbol="BTCUSDT",
                intervals=["1h", "4h"],
                start_date=None,
                end_date="2026-03-20",
                start_from_listing=True,
                status="queued",
                created_at="2026-03-20T00:00:00Z",
            )

            normalize_job = NormalizeJob(
                job_id="norm_123456",
                source="binance",
                symbols=["BTCUSDT"],
                intervals=["1h"],
                data_version="v1",
                output_format="jsonl",
                status="queued",
                created_at="2026-03-20T00:00:00Z",
            )

            duckdb_job = DuckDBLoadJob(
                job_id="duck_123456",
                data_version="v1",
                intervals=["1h"],
                status="queued",
                created_at="2026-03-20T00:00:00Z",
                db_path=str(config.quant_db_path),
            )

            backtest_job = BacktestJob(
                job_id="btjob_123456",
                run_id="20260320_000000_abcd12",
                strategy_id="demo_v1",
                strategy_name="demo",
                display_name="演示策略",
                strategy_version="v1",
                engine="backtrader",
                interval="1d",
                data_version="v1",
                input_path=str(normalized_version_dir / "market_ohlcv_1h.jsonl"),
                skip_signal_write=False,
                status="queued",
                created_at="2026-03-20T00:00:00Z",
                strategy_path=str(strategy_dir / "demo_v1.yaml"),
            )

            try:
                host, port = server.server_address
                base_url = f"http://{host}:{port}"

                with patch(
                    "src.admin.pairs_dashboard.fetch_source_pairs",
                    return_value=[remote_pair],
                ) as mocked_fetch, patch.object(
                    server.download_jobs,
                    "create_job",
                    return_value=job,
                ) as mocked_create, patch.object(
                    server.download_jobs,
                    "list_jobs",
                    return_value=[job],
                ), patch.object(
                    server.normalize_jobs,
                    "create_job",
                    return_value=normalize_job,
                ) as mocked_normalize_create, patch.object(
                    server.normalize_jobs,
                    "list_jobs",
                    return_value=[normalize_job],
                ), patch.object(
                    server.duckdb_load_jobs,
                    "create_job",
                    return_value=duckdb_job,
                ) as mocked_duckdb_create, patch.object(
                    server.duckdb_load_jobs,
                    "list_jobs",
                    return_value=[duckdb_job],
                ), patch.object(
                    server.backtest_jobs,
                    "create_job",
                    return_value=backtest_job,
                ) as mocked_backtest_create, patch.object(
                    server.backtest_jobs,
                    "list_jobs",
                    return_value=[backtest_job],
                ), patch(
                    "src.admin.pairs_dashboard.query_duckdb_overview",
                    return_value={
                        "db_path": str(config.quant_db_path),
                        "db_exists": True,
                        "duckdb_available": True,
                        "table_exists": True,
                        "total_rows": 1,
                        "versions": [
                            {
                                "data_version": "v1",
                                "interval": "1h",
                                "row_count": 1,
                                "symbol_count": 1,
                                "first_ts": "2026-03-18T00:00:00Z",
                                "last_ts": "2026-03-18T00:00:00Z",
                            }
                        ],
                        "symbols": ["BTCUSDT"],
                        "intervals": ["1h"],
                        "charting_library": {
                            "root": "charting_library",
                            "bundle_path": "charting_library/charting_library.js",
                            "bundle_exists": False,
                            "fetched_at": "2026-03-20T00:00:00Z",
                        },
                        "fetched_at": "2026-03-20T00:00:00Z",
                    },
                ), patch(
                    "src.admin.pairs_dashboard.query_duckdb_symbol_catalog",
                    return_value={
                        "count": 1,
                        "symbols": [
                            {
                                "symbol": "BTCUSDT",
                                "exchange": "binance",
                                "market_type": "spot",
                                "row_count": 1,
                                "first_ts": "2026-03-18T00:00:00Z",
                                "last_ts": "2026-03-18T00:00:00Z",
                            }
                        ],
                        "data_version": "v1",
                        "interval": "1h",
                        "fetched_at": "2026-03-20T00:00:00Z",
                    },
                ), patch(
                    "src.admin.pairs_dashboard.query_market_bars",
                    return_value={
                        "symbol": "BTCUSDT",
                        "interval": "1h",
                        "data_version": "v1",
                        "count": 1,
                        "bars": [
                            {
                                "time": 1710720000000,
                                "open": 100.0,
                                "high": 110.0,
                                "low": 90.0,
                                "close": 105.0,
                                "volume": 123.45,
                            }
                        ],
                        "fetched_at": "2026-03-20T00:00:00Z",
                    },
                ):
                    with urlopen(f"{base_url}/api/source-pairs?source=binance") as response:
                        source_payload = json.loads(response.read().decode("utf-8"))

                    manual_request = Request(
                        f"{base_url}/api/local-trading-pairs",
                        data=json.dumps(
                            {
                                "source": "binance",
                                "symbol": "BTCUSDT",
                                "quote_asset": "USDT",
                                "note": "watch",
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urlopen(manual_request) as response:
                        manual_create_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/local-trading-pairs?source=binance") as response:
                        manual_list_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/local-pairs") as response:
                        local_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/local-pairs?source=okx") as response:
                        okx_local_payload = json.loads(response.read().decode("utf-8"))

                    pair_preference_request = Request(
                        f"{base_url}/api/pair-preferences",
                        data=json.dumps(
                            {
                                "kind": "local",
                                "key": build_pair_preference_key(
                                    "local",
                                    source="binance",
                                    symbol="BTCUSDT",
                                ),
                                "hidden": True,
                                "pinned": True,
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urlopen(pair_preference_request) as response:
                        pair_preference_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/pair-preferences") as response:
                        pair_preferences_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/strategies") as response:
                        strategy_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(
                        f"{base_url}/api/strategy-config?strategy_path={strategy_dir / 'demo_v1.yaml'}"
                    ) as response:
                        strategy_config_payload = json.loads(response.read().decode("utf-8"))

                    clone_strategy_request = Request(
                        f"{base_url}/api/clone-strategy-config",
                        data=json.dumps(
                            {
                                "source_strategy_path": str(strategy_dir / "demo_v1.yaml"),
                                "target_filename": "demo_v1_cal.yaml",
                                "strategy_name": "demo",
                                "strategy_version": "v1_cal",
                                "display_name": "演示策略 调参版",
                                "description": "demo strategy calibrated",
                                "updates": {
                                    "backtest.initial_capital": 200000,
                                    "universe.symbols": ["BTCUSDT", "ETHUSDT"],
                                },
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urlopen(clone_strategy_request) as response:
                        clone_strategy_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/strategies") as response:
                        strategy_payload_after_clone = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/backtest-records") as response:
                        backtest_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/backtest-report?record_id=strategy:demo_v1") as response:
                        backtest_report_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/system-settings") as response:
                        settings_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/currency-icons") as response:
                        icon_payload = json.loads(response.read().decode("utf-8"))

                    download_request = Request(
                        f"{base_url}/api/download-klines",
                        data=json.dumps(
                            {
                                "symbol": "BTCUSDT",
                                "intervals": "1h,4h",
                                "end_date": "2026-03-20",
                                "start_from_listing": True,
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urlopen(download_request) as response:
                        download_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/download-jobs") as response:
                        jobs_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/normalized-datasets") as response:
                        normalized_payload = json.loads(response.read().decode("utf-8"))

                    normalize_request = Request(
                        f"{base_url}/api/normalize",
                        data=json.dumps(
                            {
                                "source": "binance",
                                "symbol": "BTCUSDT",
                                "intervals": "1h",
                                "data_version": "v1",
                                "output_format": "jsonl",
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urlopen(normalize_request) as response:
                        normalize_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/normalize-jobs") as response:
                        normalize_jobs_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/duckdb-status") as response:
                        duckdb_status_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/duckdb-symbols?data_version=v1&interval=1h") as response:
                        duckdb_symbols_payload = json.loads(response.read().decode("utf-8"))

                    duckdb_request = Request(
                        f"{base_url}/api/load-duckdb",
                        data=json.dumps(
                            {
                                "data_version": "v1",
                                "intervals": "1h",
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urlopen(duckdb_request) as response:
                        duckdb_load_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/duckdb-load-jobs") as response:
                        duckdb_jobs_payload = json.loads(response.read().decode("utf-8"))

                    backtest_request = Request(
                        f"{base_url}/api/run-backtest",
                        data=json.dumps(
                            {
                                "strategy_path": str(strategy_dir / "demo_v1.yaml"),
                                "engine": "backtrader",
                                "skip_signal_write": False,
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urlopen(backtest_request) as response:
                        backtest_run_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/backtest-jobs") as response:
                        backtest_jobs_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(
                        f"{base_url}/api/chart-bars?symbol=BTCUSDT&interval=1h&data_version=v1"
                    ) as response:
                        chart_bars_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/") as response:
                        html = response.read().decode("utf-8")

                    with urlopen(f"{base_url}/favicon.svg") as response:
                        favicon = response.read().decode("utf-8")
                        favicon_content_type = response.headers.get_content_type()

                    upload_icon_request = Request(
                        f"{base_url}/api/currency-icons",
                        data=json.dumps(
                            {
                                "asset": "ETH",
                                "filename": "eth.svg",
                                "mime_type": "image/svg+xml",
                                "content_base64": base64.b64encode(
                                    b"<svg xmlns='http://www.w3.org/2000/svg'><text>ETH</text></svg>"
                                ).decode("ascii"),
                            }
                        ).encode("utf-8"),
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    )
                    with urlopen(upload_icon_request) as response:
                        upload_icon_payload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/api/currency-icons") as response:
                        icon_payload_after_upload = json.loads(response.read().decode("utf-8"))

                    with urlopen(f"{base_url}/currency-icons/ETH") as response:
                        eth_icon = response.read().decode("utf-8")
                        eth_icon_content_type = response.headers.get_content_type()
            finally:
                server.server_close()
                thread.join(timeout=1)

        self.assertEqual(source_payload["count"], 1)
        self.assertEqual(source_payload["filters"]["quote_asset"], ["USDT"])
        self.assertTrue(source_payload["filters"]["tradeable_only"])
        mocked_fetch.assert_called_once_with(
            source="binance",
            config=config,
            allowed_quote_assets={"USDT"},
            tradeable_only=True,
        )

        self.assertEqual(manual_create_payload["pair"]["symbol"], "BTCUSDT")
        self.assertEqual(manual_list_payload["count"], 1)
        self.assertEqual(manual_list_payload["pairs"][0]["source"], "binance")

        self.assertEqual(local_payload["count"], 2)
        self.assertEqual(local_payload["count_by_source"]["binance"], 1)
        self.assertEqual(local_payload["count_by_source"]["okx"], 1)
        self.assertEqual(local_payload["pairs"][0]["source"], "binance")
        self.assertEqual(okx_local_payload["count"], 1)
        self.assertEqual(okx_local_payload["pairs"][0]["source"], "okx")
        self.assertEqual(okx_local_payload["pairs"][0]["symbol"], "BTCUSDT")
        self.assertEqual(okx_local_payload["pairs"][0]["display_symbol"], "BTC-USDT")
        self.assertTrue(pair_preference_payload["entry"]["hidden"])
        self.assertTrue(pair_preference_payload["entry"]["pinned"])
        self.assertEqual(pair_preferences_payload["count"], 1)
        self.assertEqual(pair_preferences_payload["entries"][0]["key"], "local:binance:BTCUSDT")

        self.assertEqual(strategy_payload["count"], 1)
        self.assertEqual(strategy_payload["strategies"][0]["strategy_name"], "demo")
        self.assertEqual(strategy_payload["strategies"][0]["display_name"], "演示策略")
        self.assertEqual(strategy_config_payload["strategy"]["strategy_name"], "demo")
        self.assertEqual(strategy_config_payload["clone_defaults"]["strategy_version"], "v1_cal")
        self.assertEqual(clone_strategy_payload["strategy_id"], "demo_v1_cal")
        self.assertTrue(clone_strategy_payload["strategy_path"].endswith("demo_v1_cal.yaml"))
        self.assertEqual(strategy_payload_after_clone["count"], 2)
        self.assertIn(
            "演示策略 调参版",
            [item["display_name"] for item in strategy_payload_after_clone["strategies"]],
        )

        self.assertEqual(backtest_payload["count"], 2)
        original_record = next(item for item in backtest_payload["records"] if item["record_id"] == "strategy:demo_v1")
        self.assertEqual(original_record["status"], "ready")
        self.assertEqual(original_record["metrics"]["sharpe"], 1.2)
        self.assertEqual(original_record["display_name"], "演示策略")
        self.assertTrue(original_record["report_available"])
        self.assertEqual(backtest_report_payload["record"]["record_id"], "strategy:demo_v1")
        self.assertEqual(backtest_report_payload["record"]["display_name"], "演示策略")
        self.assertTrue(backtest_report_payload["record"]["strategy_path"].endswith("demo_v1.yaml"))
        self.assertEqual(backtest_report_payload["summary"]["total_return"], 0.34)

        self.assertEqual(settings_payload["default_quote_asset"], "USDT")
        self.assertEqual(settings_payload["log_count"], 1)
        self.assertEqual(settings_payload["local_trading_pair_count"], 1)
        self.assertEqual(settings_payload["pair_preference_count"], 1)
        self.assertEqual(settings_payload["currency_icon_count"], 1)
        self.assertEqual(settings_payload["currency_icon_missing_count"], 1)
        self.assertEqual(settings_payload["normalize_job_count"], 1)
        self.assertEqual(settings_payload["duckdb_load_job_count"], 1)
        self.assertEqual(settings_payload["backtest_job_count"], 1)
        self.assertEqual(str(config.pair_preferences_store_path), settings_payload["pair_preferences_store_path"])

        self.assertEqual(icon_payload["available_count"], 1)
        self.assertEqual(icon_payload["missing_count"], 1)
        self.assertEqual(icon_payload["entries"][0]["asset"], "BTC")
        self.assertEqual(icon_payload["entries"][1]["asset"], "ETH")
        self.assertFalse(icon_payload["entries"][1]["exists"])

        self.assertEqual(download_payload["job"]["symbol"], "BTCUSDT")
        mocked_create.assert_called_once_with(
            KlineDownloadRequest(
                symbol="BTCUSDT",
                intervals=["1h", "4h"],
                start_date=None,
                end_date="2026-03-20",
                start_from_listing=True,
            )
        )

        self.assertEqual(jobs_payload["count"], 1)
        self.assertEqual(jobs_payload["jobs"][0]["job_id"], "job_123456")
        self.assertEqual(normalized_payload["version_count"], 1)
        self.assertEqual(normalized_payload["files"][0]["interval"], "1h")
        self.assertEqual(normalize_payload["job"]["job_id"], "norm_123456")
        self.assertEqual(normalize_jobs_payload["count"], 1)
        self.assertEqual(duckdb_status_payload["total_rows"], 1)
        self.assertEqual(duckdb_symbols_payload["symbols"][0]["symbol"], "BTCUSDT")
        self.assertEqual(duckdb_load_payload["job"]["job_id"], "duck_123456")
        self.assertEqual(duckdb_jobs_payload["count"], 1)
        self.assertEqual(backtest_run_payload["job"]["job_id"], "btjob_123456")
        self.assertEqual(backtest_jobs_payload["count"], 1)
        self.assertEqual(backtest_jobs_payload["jobs"][0]["engine"], "backtrader")
        self.assertEqual(chart_bars_payload["count"], 1)
        self.assertEqual(chart_bars_payload["bars"][0]["close"], 105.0)
        self.assertIn("首页", html)
        self.assertIn("数据源", html)
        self.assertIn("数据管理", html)
        self.assertIn("系统设置", html)
        self.assertIn("格式数据", html)
        self.assertIn("DuckDB", html)
        self.assertIn("K线图表", html)
        self.assertNotIn("刷新全部", html)
        self.assertIn('href="/favicon.svg"', html)
        self.assertIn('src="/icon.svg"', html)
        self.assertEqual(favicon_content_type, "image/svg+xml")
        self.assertIn("<svg", favicon)
        self.assertEqual(upload_icon_payload["entry"]["asset"], "ETH")
        self.assertEqual(icon_payload_after_upload["available_count"], 2)
        self.assertEqual(icon_payload_after_upload["missing_count"], 0)
        self.assertEqual(eth_icon_content_type, "image/svg+xml")
        self.assertIn("ETH", eth_icon)
        mocked_normalize_create.assert_called_once()
        mocked_duckdb_create.assert_called_once()
        mocked_backtest_create.assert_called_once_with(
            BacktestRunRequest(
                strategy_path=str(strategy_dir / "demo_v1.yaml"),
                engine="backtrader",
                skip_signal_write=False,
            )
        )

    def test_http_endpoint_adds_external_strategy_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, tempfile.TemporaryDirectory() as external_tmpdir:
            workspace_root = Path(tmpdir)
            external_root = Path(external_tmpdir) / "configs"
            external_root.mkdir(parents=True)
            config = self.make_config(workspace_root)

            strategy_dir = config.strategies_root
            strategy_dir.mkdir(parents=True)
            (strategy_dir / "demo_v1.yaml").write_text(
                "strategy_name: demo\nstrategy_version: v1\n",
                encoding="utf-8",
            )
            (external_root / "external_v1.yaml").write_text(
                "\n".join(
                    [
                        "strategy_name: external_demo",
                        "strategy_version: v1",
                        'display_name: "外部策略"',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            server = PairAdminHTTPServer(("127.0.0.1", 0), config=config)
            thread = threading.Thread(
                target=self.serve_requests,
                args=(server, 6),
                daemon=True,
            )
            thread.start()

            try:
                host, port = server.server_address
                base_url = f"http://{host}:{port}"

                add_root_request = Request(
                    f"{base_url}/api/strategy-roots",
                    data=json.dumps({"root_path": str(external_root)}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(add_root_request) as response:
                    add_root_payload = json.loads(response.read().decode("utf-8"))

                with urlopen(f"{base_url}/api/system-settings") as response:
                    settings_payload = json.loads(response.read().decode("utf-8"))

                with urlopen(f"{base_url}/api/strategies") as response:
                    strategies_payload = json.loads(response.read().decode("utf-8"))
            finally:
                server.server_close()
                thread.join(timeout=1)

        self.assertTrue(add_root_payload["created"])
        self.assertEqual(add_root_payload["strategy_count"], 1)
        self.assertIn(str(external_root.resolve()), settings_payload["persisted_extra_strategy_roots"])
        self.assertIn(str(external_root.resolve()), settings_payload["extra_strategy_roots"])
        self.assertEqual(str(config.strategy_root_store_path), settings_payload["strategy_root_store_path"])
        self.assertEqual(str(external_root.resolve()), strategies_payload["strategies"][1]["strategy_root"])
        self.assertEqual("外部策略", strategies_payload["strategies"][1]["display_name"])


if __name__ == "__main__":
    unittest.main()
