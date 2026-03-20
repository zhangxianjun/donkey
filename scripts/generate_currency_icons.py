from __future__ import annotations

import argparse
import html
import hashlib
import json
from pathlib import Path
import sys

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from src.admin.pairs_dashboard import (
    DEFAULT_CURRENCY_ICON_ASSET,
    DEFAULT_CURRENCY_ICON_ROOT,
    DashboardConfig,
    SOURCE_ORDER,
    fetch_source_pairs,
    normalize_asset_code,
)
from src.ingestion.binance_ohlcv import (
    DEFAULT_EXCHANGE_INFO_BASE_URL,
    DEFAULT_KLINE_FALLBACK_BASE_URLS,
    DEFAULT_MARKET_DATA_BASE_URL,
    MAX_LIMIT,
    utc_now_iso,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate built-in currency icon SVG files.")
    parser.add_argument(
        "--output-root",
        default=str(DEFAULT_CURRENCY_ICON_ROOT),
        help="Currency icon output directory.",
    )
    parser.add_argument(
        "--sources",
        nargs="*",
        default=list(SOURCE_ORDER),
        help="Sources used to build the asset universe.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=30.0,
        help="HTTP timeout per source request.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="Retry count per source request.",
    )
    return parser.parse_args()


def build_config(workspace_root: Path, output_root: Path, timeout_seconds: float, max_retries: int) -> DashboardConfig:
    return DashboardConfig(
        workspace_root=workspace_root,
        raw_root=(workspace_root / "data" / "raw" / "binance" / "spot").resolve(),
        normalized_root=(workspace_root / "data" / "normalized").resolve(),
        strategies_root=(workspace_root / "config" / "strategies").resolve(),
        backtests_root=(workspace_root / "data" / "backtests").resolve(),
        logs_root=(workspace_root / "logs").resolve(),
        quant_db_path=(workspace_root / "db" / "quant.duckdb").resolve(),
        experiments_db_path=(workspace_root / "db" / "experiments.duckdb").resolve(),
        local_trading_store_path=(workspace_root / "data" / "admin" / "local_trading_pairs.json").resolve(),
        currency_icon_root=output_root.resolve(),
        exchange_info_base_url=DEFAULT_EXCHANGE_INFO_BASE_URL,
        market_data_base_url=DEFAULT_MARKET_DATA_BASE_URL,
        fallback_base_urls=tuple(DEFAULT_KLINE_FALLBACK_BASE_URLS),
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_delay_seconds=1.0,
        retry_jitter_seconds=0.25,
        limit=MAX_LIMIT,
        sleep_seconds=0.0,
        default_quote_asset="USDT",
        default_tradeable_only=True,
    )


def hsl(hue: int, saturation: int, lightness: int) -> str:
    return f"hsl({hue} {saturation}% {lightness}%)"


def palette_for_asset(asset: str) -> tuple[str, str, str, str]:
    digest = hashlib.sha256(asset.encode("utf-8")).hexdigest()
    hue = int(digest[:2], 16) * 360 // 255
    hue_shift = int(digest[2:4], 16) % 48 + 12
    saturation = 64 + int(digest[4:6], 16) % 18
    lightness = 44 + int(digest[6:8], 16) % 10
    accent_hue = (hue + hue_shift) % 360
    background_a = hsl(hue, saturation, lightness)
    background_b = hsl((hue + 26) % 360, min(88, saturation + 8), min(66, lightness + 12))
    accent = hsl(accent_hue, 86, 72)
    outline = hsl((hue + 180) % 360, 42, 24)
    return background_a, background_b, accent, outline


def split_asset_label(asset: str) -> list[str]:
    if len(asset) <= 5:
        return [asset]
    if len(asset) <= 10:
        split_index = (len(asset) + 1) // 2
        return [asset[:split_index], asset[split_index:]]
    return [asset[:5], asset[5:10]]


def render_currency_icon(asset: str) -> str:
    line_a, *rest = split_asset_label(asset)
    line_b = rest[0] if rest else ""
    background_a, background_b, accent, outline = palette_for_asset(asset)
    font_size_a = 24 if len(line_a) <= 4 else 18
    font_size_b = 18 if len(line_b) <= 4 else 14
    asset_id = f"asset-{hashlib.sha1(asset.encode('utf-8')).hexdigest()[:12]}"
    return """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 96 96" role="img" aria-labelledby="{asset_id}-title {asset_id}-desc">
  <title id="{asset_id}-title">{asset} icon</title>
  <desc id="{asset_id}-desc">Generated built-in icon for asset {asset}.</desc>
  <defs>
    <linearGradient id="{asset_id}-gradient" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="{background_a}" />
      <stop offset="100%" stop-color="{background_b}" />
    </linearGradient>
  </defs>
  <rect width="96" height="96" rx="28" fill="#0b0e11" />
  <rect x="7" y="7" width="82" height="82" rx="24" fill="url(#{asset_id}-gradient)" stroke="{outline}" stroke-width="1.5" />
  <circle cx="70" cy="24" r="14" fill="{accent}" fill-opacity="0.22" />
  <circle cx="26" cy="73" r="12" fill="#ffffff" fill-opacity="0.09" />
  <text x="48" y="{line_a_y}" text-anchor="middle" fill="#f7f9fb" font-family="'Avenir Next', 'IBM Plex Sans', 'Noto Sans', sans-serif" font-size="{font_size_a}" font-weight="800" letter-spacing="0.04em">{line_a}</text>
  {line_b_text}
</svg>
""".format(
        asset=html.escape(asset),
        asset_id=asset_id,
        background_a=background_a,
        background_b=background_b,
        accent=accent,
        outline=outline,
        line_a=html.escape(line_a),
        line_a_y="54" if not line_b else "44",
        font_size_a=font_size_a,
        line_b_text=(
            ""
            if not line_b
            else (
                f'<text x="48" y="66" text-anchor="middle" fill="#f7f9fb" '
                f'font-family="\'Avenir Next\', \'IBM Plex Sans\', \'Noto Sans\', sans-serif" '
                f'font-size="{font_size_b}" font-weight="700" letter-spacing="0.05em">{html.escape(line_b)}</text>'
            )
        ),
        font_size_b=font_size_b,
    )


def render_default_icon() -> str:
    return """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 96 96" role="img" aria-labelledby="default-title default-desc">
  <title id="default-title">Default asset icon</title>
  <desc id="default-desc">Fallback icon used when a specific asset icon is unavailable.</desc>
  <defs>
    <linearGradient id="default-gradient" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#1d2630" />
      <stop offset="100%" stop-color="#3d4b5f" />
    </linearGradient>
  </defs>
  <rect width="96" height="96" rx="28" fill="#0b0e11" />
  <rect x="7" y="7" width="82" height="82" rx="24" fill="url(#default-gradient)" stroke="#5c6d82" stroke-width="1.5" />
  <circle cx="70" cy="24" r="14" fill="#f0b90b" fill-opacity="0.22" />
  <text x="48" y="48" text-anchor="middle" fill="#f7f9fb" font-family="'Avenir Next', 'IBM Plex Sans', 'Noto Sans', sans-serif" font-size="24" font-weight="800" letter-spacing="0.06em">ICON</text>
  <text x="48" y="67" text-anchor="middle" fill="#d8e1eb" font-family="'Avenir Next', 'IBM Plex Sans', 'Noto Sans', sans-serif" font-size="13" font-weight="700" letter-spacing="0.12em">DEFAULT</text>
</svg>
"""


def fetch_assets(config: DashboardConfig, sources: list[str]) -> tuple[list[str], dict[str, int]]:
    source_counts: dict[str, int] = {}
    assets: set[str] = set()
    for source in sources:
        pairs = fetch_source_pairs(
            source=source,
            config=config,
            allowed_quote_assets=None,
            tradeable_only=False,
        )
        source_assets = {
            normalize_asset_code(pair.base_asset)
            for pair in pairs
            if pair.base_asset
        } | {
            normalize_asset_code(pair.quote_asset)
            for pair in pairs
            if pair.quote_asset
        }
        source_counts[source] = len(source_assets)
        assets.update(source_assets)
    return sorted(assets), source_counts


def write_icons(output_root: Path, assets: list[str]) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / f"{DEFAULT_CURRENCY_ICON_ASSET}.svg").write_text(render_default_icon(), encoding="utf-8")
    for asset in assets:
        (output_root / f"{asset}.svg").write_text(render_currency_icon(asset), encoding="utf-8")


def main() -> int:
    args = parse_args()
    workspace_root = WORKSPACE_ROOT
    output_root = Path(args.output_root).expanduser().resolve()
    config = build_config(
        workspace_root=workspace_root,
        output_root=output_root,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
    )

    sources = [source.strip().lower() for source in args.sources if source.strip()]
    unsupported_sources = [source for source in sources if source not in SOURCE_ORDER]
    if unsupported_sources:
        raise SystemExit(f"Unsupported sources: {', '.join(unsupported_sources)}")

    assets, source_counts = fetch_assets(config, sources)
    write_icons(output_root, assets)

    catalog = {
        "generated_at": utc_now_iso(),
        "sources": sources,
        "source_counts": source_counts,
        "asset_count": len(assets),
        "assets": assets,
    }
    (output_root / "catalog.json").write_text(
        json.dumps(catalog, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"generated {len(assets)} currency icons in {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
