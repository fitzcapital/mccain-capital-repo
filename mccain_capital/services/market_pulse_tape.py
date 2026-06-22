"""Market Pulse tape range and sparkline helpers."""

from __future__ import annotations

import html
import math
from typing import Any, Dict, List


def range_payload(
    rows_or_points: List[Dict[str, Any]],
    *,
    source: str,
) -> Dict[str, Any]:
    highs: List[float] = []
    lows: List[float] = []
    values: List[float] = []
    for row in rows_or_points:
        if not isinstance(row, dict):
            continue
        high = row.get("high")
        low = row.get("low")
        if isinstance(high, (int, float)) and isinstance(low, (int, float)):
            highs.append(float(high))
            lows.append(float(low))
            continue
        value = row.get("v", row.get("close"))
        if isinstance(value, (int, float)):
            values.append(float(value))
    if highs and lows:
        low_value = min(lows)
        high_value = max(highs)
    elif len(values) >= 2:
        low_value = min(values)
        high_value = max(values)
    else:
        return {}
    compact = (
        f"{high_value:.2f}"
        if abs(high_value - low_value) < 0.01
        else f"{low_value:.2f}-{high_value:.2f}"
    )
    return {
        "day_low": low_value,
        "day_high": high_value,
        "day_range": f"{low_value:.2f} to {high_value:.2f}",
        "day_range_compact": compact,
        "range_display": compact,
        "day_range_source": source,
    }


def apply_range_payload(
    quote: Dict[str, Any],
    payload: Dict[str, Any],
) -> None:
    if not payload:
        quote.setdefault("day_range", "—")
        quote.setdefault("day_range_compact", "—")
        quote.setdefault("range_display", "—")
        return
    quote["day_low"] = payload["day_low"]
    quote["day_high"] = payload["day_high"]
    quote["day_range"] = payload["day_range"]
    quote["day_range_compact"] = payload["day_range_compact"]
    quote["range_display"] = payload["range_display"]
    quote["day_range_source"] = payload["day_range_source"]


def _spark_symbol_seed(symbol: str) -> int:
    value = 2166136261
    for char in str(symbol or "TAPE").upper():
        value ^= ord(char)
        value = (value * 16777619) & 0xFFFFFFFF
    return value


def _spark_seeded_unit(seed: int, offset: int) -> float:
    value = (seed + ((offset + 1) * 0x9E3779B9)) & 0xFFFFFFFF
    value ^= (value << 13) & 0xFFFFFFFF
    value ^= value >> 17
    value ^= (value << 5) & 0xFFFFFFFF
    return float((value & 0xFFFFFFFF) % 1000) / 1000.0


def _spark_ambient_layers(symbol: str, width: float, height: float) -> str:
    seed = _spark_symbol_seed(symbol)
    band_count = 3 + (seed % 3)
    center_y = height / 2.0
    parts: List[str] = []
    for idx in range(band_count):
        unit = _spark_seeded_unit(seed, idx)
        band_width = 74.0 + (unit * 48.0)
        x = 8.0 + (_spark_seeded_unit(seed, idx + 7) * max(1.0, width - band_width - 16.0))
        y = center_y - 11.0 + (idx * 5.2) + ((_spark_seeded_unit(seed, idx + 13) - 0.5) * 3.6)
        opacity = 0.08 + (_spark_seeded_unit(seed, idx + 23) * 0.08)
        parts.append(
            f'<rect class="marketMiniSparkAmbientBand marketMiniSparkAmbientBand--{idx + 1}" '
            f'x="{x:.2f}" y="{y:.2f}" width="{band_width:.2f}" '
            f'height="{(5.8 + (unit * 2.6)):.2f}" rx="6" '
            f'style="--spark-band-opacity:{opacity:.3f}" />'
        )
    for idx in range(4):
        x = 12.0 + (_spark_seeded_unit(seed, idx + 31) * (width - 24.0))
        y = 14.0 + (_spark_seeded_unit(seed, idx + 41) * (height - 28.0))
        radius = 0.55 + (_spark_seeded_unit(seed, idx + 51) * 0.55)
        opacity = 0.10 + (_spark_seeded_unit(seed, idx + 61) * 0.08)
        parts.append(
            f'<circle class="marketMiniSparkParticle" cx="{x:.2f}" cy="{y:.2f}" '
            f'r="{radius:.2f}" style="--spark-particle-opacity:{opacity:.3f}" />'
        )
    safe_symbol = html.escape(str(symbol or "TAPE").upper(), quote=True)
    return (
        f'<g class="marketMiniSparkAmbient" data-ambient-symbol="{safe_symbol}">'
        + "".join(parts)
        + "</g>"
    )


def sparkline_svg(series: List[float], tone: str, symbol: str = "") -> str:
    values = [float(v) for v in series if isinstance(v, (int, float))]
    if len(values) < 4:
        return '<div class="marketMiniSparkEmpty">No trend</div>'

    target_bars = min(10, max(8, int(math.ceil(len(values) / 2))))
    chunk_size = max(1, int(math.ceil(len(values) / target_bars)))
    candles: List[Dict[str, float]] = []
    for start in range(0, len(values), chunk_size):
        chunk = values[start : start + chunk_size]
        if not chunk:
            continue
        candles.append(
            {
                "open": float(chunk[0]),
                "high": float(max(chunk)),
                "low": float(min(chunk)),
                "close": float(chunk[-1]),
            }
        )
    if len(candles) < 2:
        return '<div class="marketMiniSparkEmpty">No trend</div>'

    width = 138.0
    height = 60.0
    min_v = min(float(c["low"]) for c in candles)
    max_v = max(float(c["high"]) for c in candles)
    if abs(max_v - min_v) < 1e-9:
        max_v = min_v + 1.0

    def _y(value: float) -> float:
        return ((max_v - value) / (max_v - min_v)) * (height - 12) + 6

    baseline_y = height / 2.0
    plot_width = width - 6.0
    plot_start = 3.0
    slot_width = plot_width / max(len(candles), 1)
    candle_gap = 1.1
    candle_width = min(10.8, max(6.2, slot_width - candle_gap))
    path_points = [
        (
            plot_start + (((idx + 0.5) * plot_width) / max(len(candles), 1)),
            _y(float(candle["close"])),
        )
        for idx, candle in enumerate(candles)
    ]
    trend_path = ""
    if path_points:
        trend_path = f"M {path_points[0][0]:.2f} {path_points[0][1]:.2f}"
        for idx in range(1, len(path_points)):
            prev_x, prev_y = path_points[idx - 1]
            point_x, point_y = path_points[idx]
            mid_x = (prev_x + point_x) / 2.0
            mid_y = (prev_y + point_y) / 2.0
            trend_path += (
                f" Q {prev_x:.2f} {prev_y:.2f} {mid_x:.2f} {mid_y:.2f}"
                f" T {point_x:.2f} {point_y:.2f}"
            )
    candle_markup: List[str] = []
    center_x = plot_start
    for idx, candle in enumerate(candles):
        center_x = plot_start + (((idx + 0.5) * plot_width) / max(len(candles), 1))
        open_y = _y(candle["open"])
        close_y = _y(candle["close"])
        high_y = _y(candle["high"])
        low_y = _y(candle["low"])
        top_y = min(open_y, close_y)
        body_height = max(3.2, abs(close_y - open_y))
        cls = (
            "up"
            if candle["close"] > candle["open"]
            else "down" if candle["close"] < candle["open"] else "flat"
        )
        current_cls = " current" if idx == len(candles) - 1 else ""
        candle_markup.append(
            f'<line class="marketMiniSparkWick {cls}{current_cls}" '
            f'x1="{center_x:.2f}" y1="{high_y:.2f}" '
            f'x2="{center_x:.2f}" y2="{low_y:.2f}" />'
        )
        candle_markup.append(
            f'<rect class="marketMiniSparkBody {cls}{current_cls}" '
            f'x="{(center_x - candle_width / 2):.2f}" '
            f'y="{top_y:.2f}" width="{candle_width:.2f}" '
            f'height="{body_height:.2f}" rx=".08" ry=".08" />'
        )
    last_close_y = _y(candles[-1]["close"])
    last_close_class = (
        "up"
        if candles[-1]["close"] > candles[-1]["open"]
        else "down" if candles[-1]["close"] < candles[-1]["open"] else "flat"
    )
    return (
        '<svg viewBox="0 0 138 60" class="marketMiniSpark" aria-hidden="true">'
        + '<defs><linearGradient id="dashboardTapeAmbientGradient" x1="0%" y1="50%" x2="100%" y2="50%">'
        + '<stop offset="0%" stop-color="#7e5cff" />'
        + '<stop offset="52%" stop-color="#5484ff" />'
        + '<stop offset="100%" stop-color="#54f6eb" />'
        + "</linearGradient></defs>"
        + _spark_ambient_layers(symbol, width, height)
        + f'<line class="marketMiniSparkGuide marketMiniSparkBaseline" x1="3" y1="{baseline_y:.2f}" x2="135" y2="{baseline_y:.2f}" />'
        + (f'<path class="marketMiniSparkTrend {last_close_class}" d="{trend_path}" />' if trend_path else "")
        + f'<circle class="marketMiniSparkCurrentGlow {last_close_class}" cx="{center_x:.2f}" cy="{last_close_y:.2f}" r="10.5" />'
        + "".join(candle_markup)
        + f'<line class="marketMiniSparkPriceMarker {last_close_class}" x1="{max(3.0, center_x - 5.0):.2f}" y1="{last_close_y:.2f}" x2="135" y2="{last_close_y:.2f}" />'
        + f'<circle class="marketMiniSparkPoint {last_close_class}" cx="{center_x:.2f}" cy="{last_close_y:.2f}" r="2.7" />'
        + "</svg>"
    )
