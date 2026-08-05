"""Market Pulse tape range and sparkline helpers."""

from __future__ import annotations

import html
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


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


def _spark_ambient_layers(
    symbol: str,
    width: float,
    height: float,
    *,
    soft_glow: bool = False,
) -> str:
    seed = _spark_symbol_seed(symbol)
    band_count = 3 + (seed % 3)
    center_y = height / 2.0
    parts: List[str] = []
    for idx in range(band_count):
        unit = _spark_seeded_unit(seed, idx)
        opacity = 0.08 + (_spark_seeded_unit(seed, idx + 23) * 0.08)
        if soft_glow:
            cx = 28.0 + (_spark_seeded_unit(seed, idx + 7) * (width - 56.0))
            cy = center_y - 6.0 + (idx * 4.0) + ((_spark_seeded_unit(seed, idx + 13) - 0.5) * 5.0)
            rx = 16.0 + (unit * 14.0)
            ry = 2.6 + (_spark_seeded_unit(seed, idx + 17) * 1.8)
            parts.append(
                f'<ellipse class="marketMiniSparkAmbientGlow marketMiniSparkAmbientGlow--{idx + 1}" '
                f'cx="{cx:.2f}" cy="{cy:.2f}" rx="{rx:.2f}" ry="{ry:.2f}" '
                f'style="--spark-glow-opacity:{opacity:.3f}" />'
            )
        else:
            band_width = 74.0 + (unit * 48.0)
            x = 8.0 + (_spark_seeded_unit(seed, idx + 7) * max(1.0, width - band_width - 16.0))
            y = center_y - 11.0 + (idx * 5.2) + ((_spark_seeded_unit(seed, idx + 13) - 0.5) * 3.6)
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


def _float_or_none(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except Exception:
        return None


def _parse_row_time(row: Dict[str, Any]) -> Optional[datetime]:
    raw = (
        row.get("ts")
        or row.get("datetime")
        or row.get("timestamp")
        or row.get("time")
        or row.get("date")
        or row.get("as_of")
        or row.get("asof")
    )
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw), tz=timezone.utc)
        except Exception:
            return None
    if not isinstance(raw, str) or not raw.strip():
        return None
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


TIMEFRAME_WINDOWS: Dict[str, int] = {
    "15M": 15,
    "30M": 30,
    "1H": 60,
    "6H": 360,
    "24H": 1440,
}


def _window_rows(rows: List[Dict[str, Any]], minutes: int) -> List[Dict[str, Any]]:
    timed = [(dt, row) for row in rows if (dt := _parse_row_time(row)) is not None]
    if len(timed) >= 2:
        latest = max(dt for dt, _row in timed)
        cutoff = latest - timedelta(minutes=max(1, int(minutes)))
        filtered = [row for dt, row in timed if dt >= cutoff]
        if len(filtered) >= 2:
            return filtered
    fallback_count = min(max(2, int(minutes)), 390)
    return rows[-fallback_count:]


def _last_hour_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _window_rows(rows, TIMEFRAME_WINDOWS["1H"])


def _compact_ohlc_rows(rows: List[Dict[str, float]], target: int = 12) -> List[Dict[str, float]]:
    if len(rows) <= target:
        return rows
    chunk_size = max(1, int(math.ceil(len(rows) / target)))
    candles: List[Dict[str, float]] = []
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start : start + chunk_size]
        if not chunk:
            continue
        candles.append(
            {
                "open": float(chunk[0]["open"]),
                "high": max(float(row["high"]) for row in chunk),
                "low": min(float(row["low"]) for row in chunk),
                "close": float(chunk[-1]["close"]),
            }
        )
    return candles[-target:]


def _compact_close_points(values: List[float], target: int = 24) -> List[float]:
    clean = [float(value) for value in values if isinstance(value, (int, float))]
    if len(clean) <= target:
        return clean
    step = (len(clean) - 1) / float(target - 1)
    return [clean[min(len(clean) - 1, round(idx * step))] for idx in range(target)]


def timeframe_payload(
    rows_or_points: List[Dict[str, Any]],
    *,
    symbol: str = "",
    label: str = "1H",
    minutes: int = 60,
) -> Dict[str, Any]:
    rows = [dict(row) for row in rows_or_points if isinstance(row, dict)]
    rows = _window_rows(rows, minutes)
    ohlc_rows: List[Dict[str, float]] = []
    close_values: List[float] = []
    for row in rows:
        open_v = _float_or_none(row.get("open"))
        if open_v is None:
            open_v = _float_or_none(row.get("o"))
        high_v = _float_or_none(row.get("high"))
        if high_v is None:
            high_v = _float_or_none(row.get("h"))
        low_v = _float_or_none(row.get("low"))
        if low_v is None:
            low_v = _float_or_none(row.get("l"))
        close_v = _float_or_none(row.get("close"))
        if close_v is None:
            close_v = _float_or_none(row.get("c"))
        if close_v is None:
            close_v = _float_or_none(row.get("v"))
        if close_v is not None:
            close_values.append(close_v)
        if open_v is None or high_v is None or low_v is None or close_v is None:
            continue
        ohlc_rows.append(
            {
                "open": open_v,
                "high": max(open_v, high_v, low_v, close_v),
                "low": min(open_v, high_v, low_v, close_v),
                "close": close_v,
            }
        )

    first = close_values[0] if close_values else None
    last = close_values[-1] if close_values else None
    change = last - first if first is not None and last is not None else None
    pct_change = (change / first) * 100.0 if change is not None and first not in (None, 0) else None
    tone = (
        "up"
        if change is not None and change > 0
        else "down" if change is not None and change < 0 else "flat"
    )
    candles = _compact_ohlc_rows(ohlc_rows, 22) if len(ohlc_rows) >= 2 else []
    line_points = _compact_close_points(close_values, 24) if len(close_values) >= 2 else []
    payload = {
        "label": label,
        "change": change,
        "change_display": f"{change:+.2f}" if change is not None else "—",
        "pct_change": pct_change,
        "pct_display": f"{pct_change:+.2f}%" if pct_change is not None else "—",
        "tone": tone,
        "status": "ohlc" if candles else "close-only" if line_points else "missing",
        "candles": candles,
        "line_points": [] if candles else line_points,
    }
    payload["svg"] = last_hour_svg(payload, symbol=symbol)
    return payload


def timeframe_payloads(
    rows_or_points: List[Dict[str, Any]], *, symbol: str = ""
) -> Dict[str, Dict[str, Any]]:
    return {
        label: timeframe_payload(rows_or_points, symbol=symbol, label=label, minutes=minutes)
        for label, minutes in TIMEFRAME_WINDOWS.items()
    }


def last_hour_payload(rows_or_points: List[Dict[str, Any]], *, symbol: str = "") -> Dict[str, Any]:
    return timeframe_payload(
        rows_or_points,
        symbol=symbol,
        label="1H",
        minutes=TIMEFRAME_WINDOWS["1H"],
    )


def last_hour_svg(payload: Dict[str, Any], *, symbol: str = "") -> str:
    candles = [dict(row) for row in payload.get("candles") or [] if isinstance(row, dict)]
    line_values = [
        float(v) for v in payload.get("line_points") or [] if isinstance(v, (int, float))
    ]
    tone = str(payload.get("tone") or "flat")
    width = 360.0
    height = 128.0
    plot_start = 16.0
    plot_end = width - 16.0
    plot_top = 12.0
    plot_bottom = height - 16.0
    plot_height = plot_bottom - plot_top

    if candles:
        lows = [float(row["low"]) for row in candles]
        highs = [float(row["high"]) for row in candles]
        closes = [float(row["close"]) for row in candles]
    elif len(line_values) >= 2:
        lows = list(line_values)
        highs = list(line_values)
        closes = list(line_values)
    else:
        return '<div class="dashboardTapeHourEmpty">No trend</div>'

    min_v = min(lows)
    max_v = max(highs)
    center_v = closes[-1] if closes else 0.0
    visual_floor = max(abs(center_v) * 0.0009, 0.12)
    if abs(max_v - min_v) < visual_floor:
        mid_v = (max_v + min_v) / 2.0
        min_v = mid_v - (visual_floor / 2.0)
        max_v = mid_v + (visual_floor / 2.0)

    def _y(value: float) -> float:
        return ((max_v - value) / (max_v - min_v)) * plot_height + plot_top

    baseline_y = _y(closes[0])
    markup: List[str] = []
    if candles:
        slot = (plot_end - plot_start) / max(len(candles), 1)
        candle_width = min(17.0, max(8.5, slot * 0.58))
        for idx, candle in enumerate(candles):
            center_x = plot_start + ((idx + 0.5) * slot)
            open_y = _y(float(candle["open"]))
            close_y = _y(float(candle["close"]))
            high_y = _y(float(candle["high"]))
            low_y = _y(float(candle["low"]))
            cls = (
                "up"
                if candle["close"] > candle["open"]
                else "down" if candle["close"] < candle["open"] else "flat"
            )
            current = " current" if idx == len(candles) - 1 else ""
            top_y = min(open_y, close_y)
            body_h = max(5.5, abs(close_y - open_y))
            markup.append(
                f'<line class="dashboardTapeHourWick {cls}{current}" x1="{center_x:.2f}" '
                f'y1="{high_y:.2f}" x2="{center_x:.2f}" y2="{low_y:.2f}" />'
            )
            markup.append(
                f'<rect class="dashboardTapeHourBody {cls}{current}" '
                f'x="{center_x - (candle_width / 2.0):.2f}" y="{top_y:.2f}" '
                f'width="{candle_width:.2f}" height="{body_h:.2f}" rx=".2" />'
            )
        last_x = plot_start + ((len(candles) - 0.5) * slot)
        last_y = _y(float(candles[-1]["close"]))
    else:
        coords = [
            (
                plot_start + ((idx * (plot_end - plot_start)) / max(len(line_values) - 1, 1)),
                _y(value),
            )
            for idx, value in enumerate(line_values)
        ]
        path = f"M {coords[0][0]:.2f} {coords[0][1]:.2f}"
        for idx in range(1, len(coords)):
            prev_x, prev_y = coords[idx - 1]
            point_x, point_y = coords[idx]
            mid_x = (prev_x + point_x) / 2.0
            path += (
                f" C {mid_x:.2f} {prev_y:.2f} {mid_x:.2f} {point_y:.2f}"
                f" {point_x:.2f} {point_y:.2f}"
            )
        markup.append(f'<path class="dashboardTapeHourLine {tone}" d="{path}" />')
        last_x, last_y = coords[-1]

    safe_tone = "up" if tone == "up" else "down" if tone == "down" else "flat"
    return (
        f'<svg viewBox="0 0 {width:.0f} {height:.0f}" '
        f'class="dashboardTapeHourChart {"is-ohlc" if candles else "is-close-only"}" aria-hidden="true">'
        + f'<line class="dashboardTapeHourBaseline" x1="{plot_start:.2f}" y1="{baseline_y:.2f}" '
        + f'x2="{plot_end:.2f}" y2="{baseline_y:.2f}" />'
        + "".join(markup)
        + (
            f'<circle class="dashboardTapeHourPoint {safe_tone}" '
            f'cx="{last_x:.2f}" cy="{last_y:.2f}" r="3.4" />'
        )
        + "</svg>"
    )


def dashboard_sparkline_svg(series: List[float], tone: str, symbol: str = "") -> str:
    values = [float(v) for v in series if isinstance(v, (int, float))]
    if len(values) < 2:
        return '<div class="marketMiniSparkEmpty">No trend</div>'

    values = values[-40:]
    width = 138.0
    height = 60.0
    plot_start = 5.0
    plot_end = width - 5.0
    plot_width = plot_end - plot_start
    min_v = min(values)
    max_v = max(values)
    center_v = values[-1] if values else 0.0
    visual_floor = max(abs(center_v) * 0.0012, 0.18)
    if abs(max_v - min_v) < visual_floor:
        mid_v = (max_v + min_v) / 2.0
        min_v = mid_v - (visual_floor / 2.0)
        max_v = mid_v + (visual_floor / 2.0)

    def _y(value: float) -> float:
        return ((max_v - value) / (max_v - min_v)) * (height - 14.0) + 7.0

    points = [
        (
            plot_start + ((idx * plot_width) / max(len(values) - 1, 1)),
            _y(value),
        )
        for idx, value in enumerate(values)
    ]
    line_path = f"M {points[0][0]:.2f} {points[0][1]:.2f}"
    for idx in range(1, len(points)):
        prev_x, prev_y = points[idx - 1]
        point_x, point_y = points[idx]
        mid_x = (prev_x + point_x) / 2.0
        line_path += (
            f" C {mid_x:.2f} {prev_y:.2f} {mid_x:.2f} {point_y:.2f}" f" {point_x:.2f} {point_y:.2f}"
        )
    area_path = (
        f"{line_path} L {points[-1][0]:.2f} {height - 6.0:.2f}"
        f" L {points[0][0]:.2f} {height - 6.0:.2f} Z"
    )
    baseline_y = _y(values[0])
    last_x, last_y = points[-1]
    first = values[0]
    last = values[-1]
    cls = "up" if last > first else "down" if last < first else "flat"
    return (
        '<svg viewBox="0 0 138 60" class="marketMiniSpark marketMiniSpark--line" aria-hidden="true">'
        + '<defs><linearGradient id="dashboardTapeAmbientGradient" x1="0%" y1="50%" x2="100%" y2="50%">'
        + '<stop offset="0%" stop-color="#7e5cff" />'
        + '<stop offset="52%" stop-color="#5484ff" />'
        + '<stop offset="100%" stop-color="#54f6eb" />'
        + "</linearGradient></defs>"
        + _spark_ambient_layers(symbol, width, height)
        + f'<line class="marketMiniSparkGuide marketMiniSparkBaseline" x1="{plot_start:.2f}" y1="{baseline_y:.2f}" x2="{plot_end:.2f}" y2="{baseline_y:.2f}" />'
        + f'<path class="marketMiniSparkArea {cls}" d="{area_path}" />'
        + f'<path class="marketMiniSparkLine {cls}" d="{line_path}" />'
        + f'<circle class="marketMiniSparkCurrentGlow {cls}" cx="{last_x:.2f}" cy="{last_y:.2f}" r="10.5" />'
        + f'<line class="marketMiniSparkPriceMarker {cls}" x1="{max(plot_start, last_x - 5.0):.2f}" y1="{last_y:.2f}" x2="{plot_end:.2f}" y2="{last_y:.2f}" />'
        + f'<circle class="marketMiniSparkPoint {cls}" cx="{last_x:.2f}" cy="{last_y:.2f}" r="2.7" />'
        + "</svg>"
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
        + (
            f'<path class="marketMiniSparkTrend {last_close_class}" d="{trend_path}" />'
            if trend_path
            else ""
        )
        + f'<circle class="marketMiniSparkCurrentGlow {last_close_class}" cx="{center_x:.2f}" cy="{last_close_y:.2f}" r="10.5" />'
        + "".join(candle_markup)
        + f'<line class="marketMiniSparkPriceMarker {last_close_class}" x1="{max(3.0, center_x - 5.0):.2f}" y1="{last_close_y:.2f}" x2="135" y2="{last_close_y:.2f}" />'
        + f'<circle class="marketMiniSparkPoint {last_close_class}" cx="{center_x:.2f}" cy="{last_close_y:.2f}" r="2.7" />'
        + "</svg>"
    )
