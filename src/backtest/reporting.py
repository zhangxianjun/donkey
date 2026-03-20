from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def as_path(value: str | None) -> Path | None:
    if value is None:
        return None
    candidate = Path(str(value)).expanduser()
    return candidate.resolve()


def read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def read_record_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    if path.suffix == ".jsonl":
        rows: list[dict[str, Any]] = []
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                payload = json.loads(line)
                if isinstance(payload, dict):
                    rows.append(payload)
        except (OSError, json.JSONDecodeError):
            return []
        return rows

    if path.suffix == ".parquet":
        try:
            import pyarrow.parquet as pq
        except ImportError:
            return []
        try:
            return [
                dict(item)
                for item in pq.read_table(path).to_pylist()
                if isinstance(item, dict)
            ]
        except Exception:
            return []

    return []


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def compute_drawdown_series(equity_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    peak = None
    series: list[dict[str, Any]] = []
    for row in equity_rows:
        total_equity = safe_float(row.get("total_equity"))
        ts = row.get("ts")
        if total_equity is None or ts is None:
            continue
        peak = total_equity if peak is None else max(peak, total_equity)
        drawdown = total_equity / peak - 1.0 if peak and peak > 0 else 0.0
        series.append(
            {
                "ts": str(ts),
                "total_equity": total_equity,
                "cash": safe_float(row.get("cash")),
                "market_value": safe_float(row.get("market_value")),
                "gross_exposure": safe_float(row.get("gross_exposure")),
                "drawdown": drawdown,
            }
        )
    return series


def downsample_rows(rows: list[dict[str, Any]], *, max_points: int) -> list[dict[str, Any]]:
    if len(rows) <= max_points:
        return rows
    if max_points <= 1:
        return [rows[0], rows[-1]]
    step = max(len(rows) / float(max_points - 1), 1.0)
    sampled: list[dict[str, Any]] = []
    index = 0.0
    while round(index) < len(rows):
        sampled.append(rows[int(round(index))])
        index += step
    if sampled[-1] is not rows[-1]:
        sampled.append(rows[-1])
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for row in sampled:
        key = tuple(sorted(row.items()))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def artifact_entry(label: str, path: Path | None) -> dict[str, Any]:
    exists = bool(path and path.exists())
    size_bytes = path.stat().st_size if exists and path is not None else None
    return {
        "label": label,
        "path": str(path) if path is not None else None,
        "exists": exists,
        "size_bytes": size_bytes,
    }


def summarize_trades(trades: list[dict[str, Any]]) -> dict[str, Any]:
    if not trades:
        return {
            "count": 0,
            "wins": 0,
            "losses": 0,
            "avg_net_pnl": None,
            "avg_return_pct": None,
            "avg_bars_held": None,
            "avg_win_net_pnl": None,
            "avg_loss_net_pnl": None,
            "best_trade": None,
            "worst_trade": None,
            "recent": [],
        }

    normalized: list[dict[str, Any]] = []
    for trade in trades:
        item = dict(trade)
        item["net_pnl"] = safe_float(trade.get("net_pnl"))
        item["return_pct"] = safe_float(trade.get("return_pct"))
        item["bars_held"] = safe_int(trade.get("bars_held"))
        normalized.append(item)

    wins = [trade for trade in normalized if (trade.get("net_pnl") or 0.0) > 0]
    losses = [trade for trade in normalized if (trade.get("net_pnl") or 0.0) < 0]
    net_pnls = [trade["net_pnl"] for trade in normalized if trade.get("net_pnl") is not None]
    returns = [trade["return_pct"] for trade in normalized if trade.get("return_pct") is not None]
    bars_held = [trade["bars_held"] for trade in normalized if trade.get("bars_held") is not None]

    sorted_by_exit = sorted(
        normalized,
        key=lambda item: str(item.get("exit_ts", "")),
        reverse=True,
    )
    best_trade = max(normalized, key=lambda item: item.get("net_pnl") or float("-inf"))
    worst_trade = min(normalized, key=lambda item: item.get("net_pnl") or float("inf"))
    return {
        "count": len(normalized),
        "wins": len(wins),
        "losses": len(losses),
        "avg_net_pnl": sum(net_pnls) / len(net_pnls) if net_pnls else None,
        "avg_return_pct": sum(returns) / len(returns) if returns else None,
        "avg_bars_held": sum(bars_held) / len(bars_held) if bars_held else None,
        "avg_win_net_pnl": (
            sum(trade["net_pnl"] for trade in wins if trade.get("net_pnl") is not None) / len(wins)
            if wins
            else None
        ),
        "avg_loss_net_pnl": (
            sum(trade["net_pnl"] for trade in losses if trade.get("net_pnl") is not None) / len(losses)
            if losses
            else None
        ),
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "recent": sorted_by_exit[:10],
    }


def build_backtest_report(record: Mapping[str, Any]) -> dict[str, Any]:
    summary_path = as_path(record.get("summary_path"))
    trades_path = as_path(record.get("trades_path"))
    equity_path = as_path(record.get("equity_path"))
    manifest_path = as_path(record.get("manifest_path"))

    summary = read_json_object(summary_path)
    trades = read_record_rows(trades_path)
    equity_rows = read_record_rows(equity_path)
    equity_series = compute_drawdown_series(equity_rows)
    trade_summary = summarize_trades(trades)

    peak_equity = max((point["total_equity"] for point in equity_series), default=safe_float(summary.get("final_equity")))
    latest_drawdown = equity_series[-1]["drawdown"] if equity_series else None

    report = {
        "report_generated_at": utc_now_iso(),
        "record": {
            "record_id": record.get("record_id"),
            "run_id": record.get("run_id"),
            "strategy_id": record.get("strategy_id"),
            "strategy_name": record.get("strategy_name"),
            "strategy_version": record.get("strategy_version"),
            "engine": record.get("engine"),
            "status": record.get("status"),
            "input_path": record.get("input_path"),
            "manifest_path": record.get("manifest_path"),
            "created_at": record.get("created_at"),
            "started_at": record.get("started_at"),
            "finished_at": record.get("finished_at"),
            "updated_at": record.get("updated_at"),
            "error": record.get("error"),
        },
        "summary": summary,
        "metrics": {
            "initial_capital": safe_float(summary.get("initial_capital")),
            "final_equity": safe_float(summary.get("final_equity")),
            "total_return": safe_float(summary.get("total_return")),
            "cagr": safe_float(summary.get("cagr")),
            "max_drawdown": safe_float(summary.get("max_drawdown")),
            "latest_drawdown": latest_drawdown,
            "sharpe": safe_float(summary.get("sharpe")),
            "win_rate": safe_float(summary.get("win_rate")),
            "profit_factor": safe_float(summary.get("profit_factor")),
            "trade_count": safe_int(summary.get("trade_count")) or trade_summary["count"],
            "avg_net_pnl": trade_summary["avg_net_pnl"],
            "avg_return_pct": trade_summary["avg_return_pct"],
            "avg_bars_held": trade_summary["avg_bars_held"],
            "peak_equity": peak_equity,
        },
        "equity": {
            "point_count": len(equity_series),
            "points": downsample_rows(equity_series, max_points=240),
        },
        "trades": trade_summary,
        "artifacts": [
            artifact_entry("manifest", manifest_path),
            artifact_entry("summary", summary_path),
            artifact_entry("trades", trades_path),
            artifact_entry("equity", equity_path),
        ],
    }
    return report
