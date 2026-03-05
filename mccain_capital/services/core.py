"""Core domain service gateway.

Core routes still rely on legacy implementations in ``app_core``. This module
keeps that dependency localized behind explicit delegator functions.
"""

from __future__ import annotations

from calendar import Calendar, monthrange
from datetime import date
from datetime import datetime
from datetime import timedelta
import json
import os
import shutil
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from typing import Any, Dict, List, Optional, Tuple

from flask import (
    abort,
    current_app,
    flash,
    jsonify,
    make_response,
    Response,
    redirect,
    render_template,
    request,
    send_file,
    stream_with_context,
    url_for,
)

from mccain_capital.auth import auth_enabled, effective_username, is_authenticated
from mccain_capital import runtime as app_runtime
from mccain_capital.services.ui import (
    get_forex_factory_feed,
    get_system_status,
    render_page,
    simple_msg,
)
from mccain_capital.services.viewmodels import (
    balance_state_badges,
    dashboard_data_trust,
    sync_state_badges,
)

MULTIPLIER = 100
DEFAULT_STOP_PCT = 20.0
DEFAULT_TARGET_PCT = 30.0
DEFAULT_FEE_PER_CONTRACT = 0.70
DAY_OPEN_INTERVALS = tuple(range(2, 13))
WEEK_OPEN_INTERVALS = (2, 3, 4, 5, 6)
MONTH_OPEN_INTERVALS = (2,)
MARKET_PULSE_CACHE_TTL_SECONDS = 300
MARKET_PULSE_UNSAFE_CRITICAL_THRESHOLD = 2
MARKET_PULSE_CACHE_FILE = os.path.join(app_runtime.UPLOAD_DIR, ".market_pulse_cache.json")
MARKET_NEWS_CACHE_TTL_SECONDS = 900
MARKET_NEWS_CACHE_FILE = os.path.join(app_runtime.UPLOAD_DIR, ".market_news_cache.json")
MILESTONE_PROFIT_SOURCES: Tuple[str, ...] = ("today", "week", "mtd", "ytd")
FINNHUB_API_KEY = (os.environ.get("FINNHUB_API_KEY") or "").strip()
FINNHUB_BASE_URL = "https://finnhub.io/api/v1"
MARKET_PULSE_QUOTES_URLS: Tuple[str, ...] = (
    "https://query2.finance.yahoo.com/v7/finance/quote",
    "https://query1.finance.yahoo.com/v7/finance/quote",
)
MARKET_PULSE_CHART_URLS: Tuple[str, ...] = (
    "https://query2.finance.yahoo.com/v8/finance/chart/",
    "https://query1.finance.yahoo.com/v8/finance/chart/",
)
MARKET_PULSE_SYMBOLS: Tuple[Dict[str, str], ...] = (
    {
        "symbol": "^GSPC",
        "label": "SPX",
        "group": "core",
        "focus": "Primary cash index proxy for SPX options context.",
    },
    {
        "symbol": "SPY",
        "label": "SPY",
        "group": "core",
        "focus": "S&P ETF liquidity and tape confirmation.",
    },
    {
        "symbol": "QQQ",
        "label": "QQQ",
        "group": "core",
        "focus": "Large-cap tech leadership and risk-on read.",
    },
    {
        "symbol": "IWM",
        "label": "IWM",
        "group": "core",
        "focus": "Small-cap breadth and participation.",
    },
    {
        "symbol": "^VIX",
        "label": "VIX",
        "group": "core",
        "focus": "Volatility regime and gamma proxy anchor.",
    },
    {
        "symbol": "NVDA",
        "label": "NVDA",
        "group": "leaders",
        "focus": "AI beta and high-beta leadership.",
    },
    {
        "symbol": "MSFT",
        "label": "MSFT",
        "group": "leaders",
        "focus": "Mega-cap software leadership.",
    },
    {
        "symbol": "AAPL",
        "label": "AAPL",
        "group": "leaders",
        "focus": "Consumer/mega-cap breadth signal.",
    },
    {
        "symbol": "AMZN",
        "label": "AMZN",
        "group": "leaders",
        "focus": "Consumer + cloud leadership check.",
    },
    {
        "symbol": "META",
        "label": "META",
        "group": "leaders",
        "focus": "Ad-tech and momentum leadership.",
    },
    {
        "symbol": "TSLA",
        "label": "TSLA",
        "group": "leaders",
        "focus": "EV beta and retail-momentum leadership pulse.",
    },
)
MARKET_PULSE_WATCHLIST_NEWS_SYMBOLS: Tuple[str, ...] = (
    "SPY",
    "QQQ",
    "IWM",
    "NVDA",
    "MSFT",
    "AAPL",
    "AMZN",
    "META",
    "TSLA",
)
_market_pulse_cache: Dict[str, Any] = {"fetched_at": None, "payload": None}
_market_news_cache: Dict[str, Any] = {"fetched_at": None, "payload": None}


def _legacy():
    from mccain_capital import app_core

    return app_core


def _load_market_pulse_disk_cache() -> Dict[str, Any] | None:
    try:
        with open(MARKET_PULSE_CACHE_FILE, "r", encoding="utf-8") as f:
            parsed = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _save_market_pulse_disk_cache(payload: Dict[str, Any]) -> None:
    try:
        os.makedirs(app_runtime.UPLOAD_DIR, exist_ok=True)
        with open(MARKET_PULSE_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except OSError:
        return


def _load_market_news_disk_cache() -> Dict[str, Any] | None:
    try:
        with open(MARKET_NEWS_CACHE_FILE, "r", encoding="utf-8") as f:
            parsed = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _save_market_news_disk_cache(payload: Dict[str, Any]) -> None:
    try:
        os.makedirs(app_runtime.UPLOAD_DIR, exist_ok=True)
        with open(MARKET_NEWS_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except OSError:
        return


def _market_pulse_yahoo_href(symbol: str) -> str:
    return "https://finance.yahoo.com/quote/" + urllib.parse.quote(symbol, safe="")


def _market_pulse_json_request_any(url: str, params: Dict[str, Any], timeout: int = 4) -> Any:
    try:
        req = urllib.request.Request(
            url + "?" + urllib.parse.urlencode(params),
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json,*/*",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.load(resp)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError):
        return None


def _market_pulse_json_request(
    url: str, params: Dict[str, Any], timeout: int = 4
) -> Dict[str, Any] | None:
    parsed = _market_pulse_json_request_any(url, params, timeout=timeout)
    return parsed if isinstance(parsed, dict) else None


def _market_pulse_yahoo_quote_payload(symbol_csv: str) -> Dict[str, Any] | None:
    for base_url in MARKET_PULSE_QUOTES_URLS:
        payload = _market_pulse_json_request(
            base_url,
            {"symbols": symbol_csv},
        )
        if isinstance(payload, dict):
            return payload
    return None


def _market_pulse_yahoo_chart_payload(symbol: str) -> Dict[str, Any] | None:
    encoded = urllib.parse.quote(symbol, safe="")
    params = {"interval": "1m", "range": "1d"}
    for base_url in MARKET_PULSE_CHART_URLS:
        payload = _market_pulse_json_request(base_url + encoded, params)
        if isinstance(payload, dict):
            return payload
    return None


def _market_pulse_cached_row_map(
    cached_payload: Dict[str, Any] | None
) -> Dict[str, Dict[str, Any]]:
    if not isinstance(cached_payload, dict):
        return {}
    rows = cached_payload.get("quotes") or []
    return {
        str(row.get("label") or ""): row
        for row in rows
        if isinstance(row, dict) and str(row.get("label") or "")
    }


def _market_pulse_has_value(row: Dict[str, Any] | None) -> bool:
    if not isinstance(row, dict):
        return False
    return isinstance(row.get("price"), (int, float))


def _market_pulse_normalized_cached_row(
    cached_row: Dict[str, Any] | None, spec: Dict[str, str]
) -> Dict[str, Any] | None:
    if not isinstance(cached_row, dict):
        return None
    row = dict(cached_row)
    row["symbol"] = spec["symbol"]
    row["label"] = spec["label"]
    row["group"] = spec["group"]
    row["focus"] = spec["focus"]
    row["yahoo_href"] = _market_pulse_yahoo_href(spec["symbol"])
    row["data_state"] = str(row.get("data_state") or "cached")
    row["data_status_label"] = str(row.get("data_status_label") or "Cached")
    return row


def _market_pulse_quote_record(raw: Dict[str, Any], spec: Dict[str, str]) -> Dict[str, Any]:
    fallback = raw if isinstance(raw, dict) else {}
    price = fallback.get("regularMarketPrice")
    if price is None:
        price = fallback.get("postMarketPrice")
    if price is None:
        price = fallback.get("preMarketPrice")
    change = fallback.get("regularMarketChange")
    change_pct = fallback.get("regularMarketChangePercent")
    day_low = fallback.get("regularMarketDayLow")
    day_high = fallback.get("regularMarketDayHigh")
    return {
        "symbol": spec["symbol"],
        "label": spec["label"],
        "group": spec["group"],
        "focus": spec["focus"],
        "name": str(fallback.get("shortName") or fallback.get("longName") or spec["label"]),
        "price": float(price) if isinstance(price, (int, float)) else None,
        "change": float(change) if isinstance(change, (int, float)) else 0.0,
        "change_pct": float(change_pct) if isinstance(change_pct, (int, float)) else 0.0,
        "volume": int(fallback.get("regularMarketVolume") or 0),
        "avg_volume": int(fallback.get("averageDailyVolume3Month") or 0),
        "market_state": str(fallback.get("marketState") or "UNKNOWN").replace("_", " ").title(),
        "day_range": (
            f"{float(day_low):,.2f} to {float(day_high):,.2f}"
            if isinstance(day_low, (int, float)) and isinstance(day_high, (int, float))
            else "—"
        ),
        "yahoo_href": _market_pulse_yahoo_href(spec["symbol"]),
        "data_state": "missing",
        "data_status_label": "Missing",
    }


def _market_pulse_yahoo_chart_record(
    payload: Dict[str, Any] | None,
    spec: Dict[str, str],
    cached_row: Dict[str, Any] | None = None,
    fetched_label: str = "",
    fetched_epoch: int = 0,
) -> Dict[str, Any]:
    fallback = cached_row if isinstance(cached_row, dict) else {}
    chart = payload.get("chart") if isinstance(payload, dict) else {}
    result_rows = chart.get("result") if isinstance(chart, dict) else []
    row0 = (
        result_rows[0]
        if isinstance(result_rows, list) and result_rows and isinstance(result_rows[0], dict)
        else {}
    )
    meta = row0.get("meta") if isinstance(row0.get("meta"), dict) else {}
    indicators = row0.get("indicators") if isinstance(row0.get("indicators"), dict) else {}
    quote_rows = indicators.get("quote") if isinstance(indicators.get("quote"), list) else []
    quote0 = quote_rows[0] if quote_rows and isinstance(quote_rows[0], dict) else {}

    price = meta.get("regularMarketPrice")
    prev_close = meta.get("previousClose")
    day_low = meta.get("regularMarketDayLow")
    day_high = meta.get("regularMarketDayHigh")

    highs = quote0.get("high") if isinstance(quote0.get("high"), list) else []
    lows = quote0.get("low") if isinstance(quote0.get("low"), list) else []
    volumes = quote0.get("volume") if isinstance(quote0.get("volume"), list) else []
    closes = quote0.get("close") if isinstance(quote0.get("close"), list) else []
    stamps = row0.get("timestamp") if isinstance(row0.get("timestamp"), list) else []

    if day_low is None:
        numeric_lows = [float(v) for v in lows if isinstance(v, (int, float))]
        day_low = min(numeric_lows) if numeric_lows else None
    if day_high is None:
        numeric_highs = [float(v) for v in highs if isinstance(v, (int, float))]
        day_high = max(numeric_highs) if numeric_highs else None

    price_source = "live"
    if not isinstance(price, (int, float)):
        if isinstance(prev_close, (int, float)):
            price = float(prev_close)
            price_source = "delayed"
        elif isinstance(cached_row, dict) and isinstance(cached_row.get("price"), (int, float)):
            price = float(cached_row.get("price"))
            price_source = "cached"

    change = 0.0
    change_pct = 0.0
    if (
        isinstance(price, (int, float))
        and isinstance(prev_close, (int, float))
        and float(prev_close) != 0.0
    ):
        change = float(price) - float(prev_close)
        change_pct = (change / float(prev_close)) * 100.0

    if isinstance(price, (int, float)):
        mini_series = [float(v) for v in closes if isinstance(v, (int, float))]
        out: Dict[str, Any] = {
            "symbol": spec["symbol"],
            "label": spec["label"],
            "group": spec["group"],
            "focus": spec["focus"],
            "name": str(
                meta.get("shortName")
                or meta.get("longName")
                or fallback.get("name")
                or spec["label"]
            ),
            "price": float(price),
            "change": float(change),
            "change_pct": float(change_pct),
            "volume": int(meta.get("regularMarketVolume") or 0),
            "avg_volume": int(
                meta.get("averageDailyVolume3Month") or fallback.get("avg_volume") or 0
            ),
            "market_state": str(
                meta.get("marketState") or fallback.get("market_state") or "Unknown"
            )
            .replace("_", " ")
            .title(),
            "day_range": (
                f"{float(day_low):,.2f} to {float(day_high):,.2f}"
                if isinstance(day_low, (int, float)) and isinstance(day_high, (int, float))
                else str(fallback.get("day_range") or "—")
            ),
            "yahoo_href": _market_pulse_yahoo_href(spec["symbol"]),
            "data_state": (
                "live"
                if price_source == "live"
                else ("delayed" if price_source == "delayed" else "cached")
            ),
            "data_status_label": (
                "Live"
                if price_source == "live"
                else ("Delayed" if price_source == "delayed" else "Cached")
            ),
            "asof": (
                str(fallback.get("asof") or fetched_label)
                if price_source == "cached"
                else fetched_label
            ),
            "asof_epoch": (
                int(fallback.get("asof_epoch") or fetched_epoch)
                if price_source == "cached"
                else fetched_epoch
            ),
            "mini_series": (
                mini_series[-60:] if mini_series else list(fallback.get("mini_series") or [])
            ),
        }
        if spec["label"] == "SPX":
            series = []
            for stamp, close_v, volume_v in zip(stamps, closes, volumes):
                if not isinstance(stamp, (int, float)) or not isinstance(close_v, (int, float)):
                    continue
                ts = datetime.fromtimestamp(int(stamp), tz=app_runtime.TZ)
                series.append(
                    {
                        "label": ts.strftime("%H:%M"),
                        "stamp": int(stamp),
                        "o": float(close_v),
                        "h": float(close_v),
                        "l": float(close_v),
                        "c": float(close_v),
                        "v": int(volume_v) if isinstance(volume_v, (int, float)) else 0,
                    }
                )
            if series:
                out["series"] = series
        return out

    if isinstance(cached_row, dict):
        normalized = _market_pulse_normalized_cached_row(cached_row, spec)
        if isinstance(normalized, dict):
            normalized["asof"] = fetched_label
            normalized["asof_epoch"] = int(cached_row.get("asof_epoch") or fetched_epoch)
            return normalized
    missing = _market_pulse_quote_record({}, spec)
    missing["asof"] = fetched_label
    missing["asof_epoch"] = fetched_epoch
    return missing


def _market_pulse_finnhub_quote_record(
    raw: Dict[str, Any] | None,
    spec: Dict[str, str],
    cached_row: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    fallback = cached_row if isinstance(cached_row, dict) else {}
    normalized_fallback = _market_pulse_normalized_cached_row(cached_row, spec)
    if not isinstance(raw, dict):
        raw = {}

    price = raw.get("c")
    change = raw.get("d")
    change_pct = raw.get("dp")
    day_high = raw.get("h")
    day_low = raw.get("l")
    prev_close = raw.get("pc")
    market_state = (
        "Live"
        if isinstance(raw.get("t"), (int, float)) and raw.get("t")
        else str(fallback.get("market_state") or "UNKNOWN")
    )

    if not isinstance(price, (int, float)):
        if isinstance(normalized_fallback, dict):
            return normalized_fallback
        return _market_pulse_quote_record({}, spec)

    record = {
        "symbol": spec["symbol"],
        "label": spec["label"],
        "group": spec["group"],
        "focus": spec["focus"],
        "name": str(fallback.get("name") or spec["label"]),
        "price": float(price),
        "change": float(change) if isinstance(change, (int, float)) else 0.0,
        "change_pct": float(change_pct) if isinstance(change_pct, (int, float)) else 0.0,
        "volume": int(fallback.get("volume") or 0),
        "avg_volume": int(fallback.get("avg_volume") or 0),
        "market_state": str(market_state).replace("_", " ").title(),
        "day_range": (
            f"{float(day_low):,.2f} to {float(day_high):,.2f}"
            if isinstance(day_low, (int, float)) and isinstance(day_high, (int, float))
            else str(fallback.get("day_range") or "—")
        ),
        "yahoo_href": _market_pulse_yahoo_href(spec["symbol"]),
    }
    if isinstance(prev_close, (int, float)):
        record["prev_close"] = float(prev_close)
    if "series" in fallback and isinstance(fallback.get("series"), list):
        record["series"] = fallback["series"]
    return record


def _market_pulse_spx_proxy_ratio(
    cached_spx: Dict[str, Any] | None,
    cached_spy: Dict[str, Any] | None,
) -> float:
    try:
        spx_price = float((cached_spx or {}).get("price"))
        spy_price = float((cached_spy or {}).get("price"))
        if spy_price > 0:
            return spx_price / spy_price
    except (TypeError, ValueError, ZeroDivisionError):
        pass
    return 10.0


def _market_pulse_spx_proxy_record(
    spy_raw: Dict[str, Any] | None,
    spec: Dict[str, str],
    cached_spx: Dict[str, Any] | None,
    cached_spy: Dict[str, Any] | None,
) -> Dict[str, Any]:
    fallback = _market_pulse_normalized_cached_row(cached_spx, spec)
    if _market_pulse_has_value(fallback):
        if isinstance(fallback, dict):
            fallback["market_state"] = str(fallback.get("market_state") or "Cached SPX")
            return fallback
    if not isinstance(spy_raw, dict) or not isinstance(spy_raw.get("c"), (int, float)):
        return fallback if isinstance(fallback, dict) else _market_pulse_quote_record({}, spec)

    ratio = _market_pulse_spx_proxy_ratio(cached_spx, cached_spy)
    price = float(spy_raw.get("c")) * ratio
    change = float(spy_raw.get("d") or 0.0) * ratio
    change_pct = float(spy_raw.get("dp") or 0.0)
    day_high = spy_raw.get("h")
    day_low = spy_raw.get("l")
    market_state = "Proxy via Spy"
    row = {
        "symbol": spec["symbol"],
        "label": spec["label"],
        "group": spec["group"],
        "focus": spec["focus"],
        "name": "SPX proxy via SPY",
        "price": round(price, 2),
        "change": round(change, 2),
        "change_pct": change_pct,
        "volume": int((cached_spx or {}).get("volume") or 0),
        "avg_volume": int((cached_spx or {}).get("avg_volume") or 0),
        "market_state": market_state,
        "day_range": (
            f"{float(day_low) * ratio:,.2f} to {float(day_high) * ratio:,.2f}"
            if isinstance(day_low, (int, float)) and isinstance(day_high, (int, float))
            else str((cached_spx or {}).get("day_range") or "—")
        ),
        "yahoo_href": _market_pulse_yahoo_href(spec["symbol"]),
    }
    if isinstance((cached_spx or {}).get("series"), list):
        row["series"] = (cached_spx or {}).get("series")
    return row


def _market_pulse_scale_series(series: List[Dict[str, Any]], ratio: float) -> List[Dict[str, Any]]:
    scaled: List[Dict[str, Any]] = []
    for point in series:
        if not isinstance(point, dict):
            continue
        try:
            value = float(point.get("v"))
        except (TypeError, ValueError):
            continue
        scaled.append({"label": str(point.get("label") or ""), "v": round(value * ratio, 2)})
    return scaled


def _market_pulse_scale_candles(
    candles: List[Dict[str, Any]], ratio: float
) -> List[Dict[str, Any]]:
    scaled: List[Dict[str, Any]] = []
    for candle in candles:
        if not isinstance(candle, dict):
            continue
        try:
            scaled.append(
                {
                    "label": str(candle.get("label") or ""),
                    "stamp": int(candle.get("stamp") or 0),
                    "o": round(float(candle.get("o")) * ratio, 2),
                    "h": round(float(candle.get("h")) * ratio, 2),
                    "l": round(float(candle.get("l")) * ratio, 2),
                    "c": round(float(candle.get("c")) * ratio, 2),
                    "v": int(candle.get("v") or 0),
                }
            )
        except (TypeError, ValueError):
            continue
    return scaled


def _market_pulse_preserve_cached_rows(
    quotes: List[Dict[str, Any]],
    cached_rows: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    preserved: List[Dict[str, Any]] = []
    for row in quotes:
        label = str((row or {}).get("label") or "")
        cached = cached_rows.get(label)
        if not _market_pulse_has_value(row) and _market_pulse_has_value(cached):
            spec = next((item for item in MARKET_PULSE_SYMBOLS if item["label"] == label), None)
            if isinstance(spec, dict):
                cached_row = _market_pulse_normalized_cached_row(cached, spec)
                if isinstance(cached_row, dict):
                    cached_row["data_state"] = "cached"
                    cached_row["data_status_label"] = "Cached"
                    preserved.append(cached_row)
                    continue
        preserved.append(row)
    return preserved


def _market_pulse_force_yahoo_source(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "available": False,
            "fetched_at": "",
            "source_label": "Yahoo Finance chart feed",
            "source_note": "",
            "quotes": [],
        }
    normalized = dict(payload)
    normalized["source_label"] = "Yahoo Finance chart feed"
    note = str(normalized.get("source_note") or "").strip()
    if (not note) or ("finnhub" in note.lower()):
        normalized["source_note"] = (
            "Live quote data may be delayed by the upstream feed depending on the symbol."
        )
    return normalized


def _market_pulse_force_symbol_set(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    normalized = _market_pulse_force_yahoo_source(payload)
    cached_rows = _market_pulse_cached_row_map(normalized)
    quotes: List[Dict[str, Any]] = []
    for spec in MARKET_PULSE_SYMBOLS:
        cached = cached_rows.get(spec["label"])
        if isinstance(cached, dict):
            row = _market_pulse_normalized_cached_row(cached, spec)
            if isinstance(row, dict):
                quotes.append(row)
                continue
        quotes.append(_market_pulse_quote_record({}, spec))
    normalized["quotes"] = quotes
    normalized["available"] = any(q.get("price") is not None for q in quotes)
    counts = {"live": 0, "delayed": 0, "cached": 0, "missing": 0}
    for row in quotes:
        state = str(row.get("data_state") or "missing").lower()
        counts[state if state in counts else "missing"] += 1
    normalized["integrity"] = {
        "latency_ms": 0,
        "forced_refresh": False,
        "cached_only": True,
        "live_count": counts["live"],
        "delayed_count": counts["delayed"],
        "cached_count": counts["cached"],
        "missing_count": counts["missing"],
        "tracked_count": len(quotes),
    }
    return normalized


def _market_pulse_finnhub_candles(symbol: str, now_et: datetime) -> List[Dict[str, Any]]:
    if not FINNHUB_API_KEY:
        return []
    end_at = int(now_et.timestamp())
    start_at = end_at - (7 * 60 * 60)
    payload = _market_pulse_json_request(
        FINNHUB_BASE_URL + "/stock/candle",
        {
            "symbol": symbol,
            "resolution": "1",
            "from": start_at,
            "to": end_at,
            "token": FINNHUB_API_KEY,
        },
    )
    if not isinstance(payload, dict) or str(payload.get("s") or "").lower() != "ok":
        return []
    opens = payload.get("o") or []
    highs = payload.get("h") or []
    lows = payload.get("l") or []
    closes = payload.get("c") or []
    volumes = payload.get("v") or []
    stamps = payload.get("t") or []
    if not all(isinstance(item, list) for item in (opens, highs, lows, closes, volumes, stamps)):
        return []
    series: List[Dict[str, Any]] = []
    for open_v, high_v, low_v, close_v, volume_v, stamp in zip(
        opens, highs, lows, closes, volumes, stamps
    ):
        if not all(
            isinstance(item, (int, float)) for item in (open_v, high_v, low_v, close_v, stamp)
        ):
            continue
        ts = datetime.fromtimestamp(int(stamp), tz=app_runtime.TZ)
        series.append(
            {
                "label": ts.strftime("%H:%M"),
                "stamp": int(stamp),
                "o": float(open_v),
                "h": float(high_v),
                "l": float(low_v),
                "c": float(close_v),
                "v": int(volume_v) if isinstance(volume_v, (int, float)) else 0,
            }
        )
    return series


def _market_pulse_snapshot(force_refresh: bool = False) -> Dict[str, Any]:
    started = time.perf_counter()
    now_et = app_runtime.now_et()
    fetched_label = now_et.strftime("%b %d, %Y %I:%M:%S %p ET")
    fetched_epoch = int(now_et.timestamp())
    fetched_at = _market_pulse_cache.get("fetched_at")
    cached_payload = _market_pulse_cache.get("payload")
    if (
        (not force_refresh)
        and isinstance(fetched_at, datetime)
        and isinstance(cached_payload, dict)
        and (now_et - fetched_at).total_seconds() < MARKET_PULSE_CACHE_TTL_SECONDS
    ):
        return _market_pulse_force_symbol_set(cached_payload)

    disk_payload = _load_market_pulse_disk_cache()
    cache_seed = cached_payload if isinstance(cached_payload, dict) else disk_payload
    cached_rows = _market_pulse_cached_row_map(cache_seed)

    quotes: List[Dict[str, Any]] = []
    live_count = 0
    for spec in MARKET_PULSE_SYMBOLS:
        payload = _market_pulse_yahoo_chart_payload(spec["symbol"])
        row = _market_pulse_yahoo_chart_record(
            payload,
            spec,
            cached_rows.get(spec["label"]),
            fetched_label=fetched_label,
            fetched_epoch=fetched_epoch,
        )
        if str(row.get("data_state") or "") == "live":
            live_count += 1
        quotes.append(row)
    quotes = _market_pulse_preserve_cached_rows(quotes, cached_rows)
    counts = {"live": 0, "delayed": 0, "cached": 0, "missing": 0}
    for row in quotes:
        state = str(row.get("data_state") or "missing").lower()
        counts[state if state in counts else "missing"] += 1
    latency_ms = int((time.perf_counter() - started) * 1000)

    if not live_count:
        if isinstance(cached_payload, dict):
            return _market_pulse_force_symbol_set(cached_payload)
        if isinstance(disk_payload, dict):
            _market_pulse_cache["payload"] = disk_payload
            return _market_pulse_force_symbol_set(disk_payload)
        return {
            "available": False,
            "fetched_at": "",
            "source_label": "Yahoo Finance chart feed",
            "source_note": (
                "Live market data is unavailable because the app runtime cannot currently reach the Yahoo quote host. "
                "A cached snapshot will be shown when available."
            ),
            "quotes": [],
            "integrity": {
                "latency_ms": latency_ms,
                "forced_refresh": bool(force_refresh),
                "cached_only": True,
                "live_count": 0,
                "delayed_count": 0,
                "cached_count": 0,
                "missing_count": len(MARKET_PULSE_SYMBOLS),
                "tracked_count": len(MARKET_PULSE_SYMBOLS),
            },
        }

    result = {
        "available": any(q.get("price") is not None for q in quotes),
        "fetched_at": fetched_label,
        "source_label": "Yahoo Finance chart feed",
        "source_note": (
            "Live quote data is being derived from Yahoo chart metadata."
            if live_count == len(MARKET_PULSE_SYMBOLS)
            else "Yahoo chart data is partially available; missing fields may use the last cached snapshot."
        ),
        "quotes": quotes,
        "integrity": {
            "latency_ms": latency_ms,
            "forced_refresh": bool(force_refresh),
            "cached_only": False,
            "live_count": counts["live"],
            "delayed_count": counts["delayed"],
            "cached_count": counts["cached"],
            "missing_count": counts["missing"],
            "tracked_count": len(quotes),
        },
    }
    _market_pulse_cache["fetched_at"] = now_et
    _market_pulse_cache["payload"] = result
    _save_market_pulse_disk_cache(result)
    return result


def _market_pulse_context(quotes: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_label = {str(q.get("label") or ""): q for q in quotes}
    vix_val = float((by_label.get("VIX") or {}).get("price") or 0.0)
    spy_pct = float((by_label.get("SPY") or {}).get("change_pct") or 0.0)
    qqq_pct = float((by_label.get("QQQ") or {}).get("change_pct") or 0.0)
    iwm_pct = float((by_label.get("IWM") or {}).get("change_pct") or 0.0)
    spx_pct = float((by_label.get("SPX") or {}).get("change_pct") or 0.0)

    if vix_val and vix_val < 16:
        gamma_label = "Likely pin / lower-vol"
        gamma_tone = "positive"
        gamma_note = "Calmer vol regime. Expect tighter rotations unless catalysts break range."
    elif vix_val and vix_val < 21:
        gamma_label = "Balanced / two-way"
        gamma_tone = ""
        gamma_note = "Mixed regime. Expect cleaner reactions at key levels, but less pinning."
    elif vix_val:
        gamma_label = "Higher-vol / expansion"
        gamma_tone = "negative"
        gamma_note = "Higher vol regime. Expect faster range expansion and weaker pin behavior."
    else:
        gamma_label = "Proxy unavailable"
        gamma_tone = ""
        gamma_note = "Gamma proxy could not be derived because VIX data is unavailable."

    if qqq_pct > spy_pct and qqq_pct > iwm_pct:
        leadership = "Tech-led"
    elif iwm_pct > spy_pct and iwm_pct > qqq_pct:
        leadership = "Broad risk-on"
    elif spy_pct >= 0 and qqq_pct < 0 and iwm_pct < 0:
        leadership = "Defensive large-cap"
    else:
        leadership = "Mixed tape"

    breadth_delta = round(iwm_pct - spy_pct, 2)
    breadth_label = "Broadening"
    if breadth_delta < -0.3:
        breadth_label = "Narrowing"
    elif abs(breadth_delta) <= 0.3:
        breadth_label = "Balanced"

    return {
        "gamma_label": gamma_label,
        "gamma_tone": gamma_tone,
        "gamma_note": gamma_note,
        "vix_value": vix_val,
        "leadership": leadership,
        "breadth_label": breadth_label,
        "breadth_delta": breadth_delta,
        "spx_pct": spx_pct,
        "headline_note": (
            f"SPX {spx_pct:+.2f}% · QQQ {qqq_pct:+.2f}% · IWM {iwm_pct:+.2f}% vs SPY {spy_pct:+.2f}%."
            if quotes
            else "Live quote data is unavailable right now."
        ),
    }


def _market_pulse_stats(quotes: List[Dict[str, Any]]) -> Dict[str, Any]:
    advancers = 0
    decliners = 0
    unchanged = 0
    missing = 0
    biggest_label = "—"
    biggest_move = 0.0
    for q in quotes:
        try:
            price = q.get("price")
            if not isinstance(price, (int, float)):
                missing += 1
                continue
            pct = float(q.get("change_pct") or 0.0)
            if pct > 0:
                advancers += 1
            elif pct < 0:
                decliners += 1
            else:
                unchanged += 1
            if abs(pct) > abs(biggest_move):
                biggest_move = pct
                biggest_label = str(q.get("label") or "—")
        except (TypeError, ValueError):
            missing += 1
            continue
    return {
        "advancers": advancers,
        "decliners": decliners,
        "unchanged": unchanged,
        "missing": missing,
        "biggest_label": biggest_label,
        "biggest_move": biggest_move,
        "tracked": len(quotes),
    }


def _market_pulse_sparkline_svg(series: List[float], tone: str) -> str:
    values = [float(v) for v in series if isinstance(v, (int, float))]
    if len(values) < 2:
        return '<div class="marketMiniSparkEmpty">No trend</div>'
    width = 120.0
    height = 28.0
    min_v = min(values)
    max_v = max(values)
    if abs(max_v - min_v) < 1e-9:
        max_v = min_v + 1.0
    step = width / max(len(values) - 1, 1)
    points = []
    for idx, value in enumerate(values):
        x = idx * step
        y = ((max_v - value) / (max_v - min_v)) * (height - 2) + 1
        points.append(f"{x:.2f},{y:.2f}")
    cls = "up" if tone == "up" else "down" if tone == "down" else "flat"
    return (
        '<svg viewBox="0 0 120 28" class="marketMiniSpark" aria-hidden="true">'
        f'<polyline class="marketMiniSparkLine {cls}" points="{" ".join(points)}" />'
        "</svg>"
    )


def _market_pulse_enrich_quotes(
    quotes: List[Dict[str, Any]], now_et: datetime
) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    now_epoch = int(now_et.timestamp())
    for row in quotes:
        if not isinstance(row, dict):
            continue
        q = dict(row)
        state = str(q.get("data_state") or "missing").lower()
        asof_epoch = int(q.get("asof_epoch") or 0) if str(q.get("asof_epoch") or "").strip() else 0
        age_s = max(0, now_epoch - asof_epoch) if asof_epoch else 999999
        if state == "missing":
            band = "critical"
            fresh_label = "No live data"
        elif age_s <= 60:
            band = "live"
            fresh_label = f"Live · {age_s}s old"
        elif age_s <= 180:
            band = "warn"
            fresh_label = f"Stale · {age_s}s old"
        else:
            band = "critical"
            fresh_label = f"Critical · {age_s}s old"
        q["freshness_band"] = band
        q["freshness_age_s"] = age_s
        q["freshness_label"] = fresh_label

        mini = q.get("mini_series")
        if not isinstance(mini, list):
            mini = []
        if not mini and isinstance(q.get("series"), list):
            mini = [
                float(p.get("v"))
                for p in q.get("series")
                if isinstance(p, dict) and isinstance(p.get("v"), (int, float))
            ]
        tone = "flat"
        if len(mini) >= 2:
            delta = float(mini[-1]) - float(mini[0])
            if delta > 0:
                tone = "up"
            elif delta < 0:
                tone = "down"
        q["sparkline_svg"] = _market_pulse_sparkline_svg(mini[-40:], tone)
        enriched.append(q)
    return enriched


def _market_pulse_alert(quotes: List[Dict[str, Any]]) -> Dict[str, Any]:
    warn = [q for q in quotes if str(q.get("freshness_band") or "") == "warn"]
    critical = [q for q in quotes if str(q.get("freshness_band") or "") == "critical"]
    if not warn and not critical:
        return {"show": False, "tone": "ok", "message": "All ticker data is fresh."}
    if critical:
        names = ", ".join(str(q.get("label") or "") for q in critical[:4])
        more = f" +{len(critical)-4} more" if len(critical) > 4 else ""
        return {
            "show": True,
            "tone": "critical",
            "message": f"Critical stale data on {len(critical)} tickers: {names}{more}. Verify before entry.",
        }
    names = ", ".join(str(q.get("label") or "") for q in warn[:4])
    more = f" +{len(warn)-4} more" if len(warn) > 4 else ""
    return {
        "show": True,
        "tone": "warn",
        "message": f"Stale data on {len(warn)} tickers: {names}{more}.",
    }


def _market_pulse_guardrail(quotes: List[Dict[str, Any]]) -> Dict[str, Any]:
    critical = [q for q in quotes if str(q.get("freshness_band") or "") == "critical"]
    active = len(critical) >= MARKET_PULSE_UNSAFE_CRITICAL_THRESHOLD
    labels = [str(q.get("label") or "—") for q in critical]
    msg = (
        f"Data Unsafe: {len(critical)} critical-stale tickers ({', '.join(labels[:5])}). Trading actions are locked."
        if active
        else ""
    )
    return {
        "active": active,
        "critical_count": len(critical),
        "threshold": MARKET_PULSE_UNSAFE_CRITICAL_THRESHOLD,
        "labels": labels,
        "message": msg,
    }


def _market_pulse_market_hours(now_et: datetime) -> bool:
    if int(now_et.weekday()) >= 5:
        return False
    minute_of_day = (int(now_et.hour) * 60) + int(now_et.minute)
    return (9 * 60 + 30) <= minute_of_day < (16 * 60)


def _market_news_timestamp_label(stamp: Any) -> str:
    if not isinstance(stamp, (int, float)):
        return ""
    return datetime.fromtimestamp(int(stamp), tz=app_runtime.TZ).strftime("%b %-d, %-I:%M %p ET")


def _market_news_theme(text: str) -> Tuple[str, str]:
    raw = text.lower()
    themes = [
        (
            ("fed", "powell", "rates", "yield", "treasury", "bond"),
            ("Rates", "Rates / liquidity backdrop"),
        ),
        (
            ("cpi", "pce", "inflation", "jobs", "payrolls", "ism"),
            ("Macro", "Macro release with index impact"),
        ),
        (
            ("oil", "iran", "middle east", "crude"),
            ("Energy", "Energy and geopolitics can move the tape"),
        ),
        (("vix", "volatility", "options"), ("Vol", "Volatility regime shift")),
        (
            ("nvidia", "ai", "semiconductor", "chip"),
            ("AI", "AI leadership / semis can drag QQQ and SPX"),
        ),
        (("apple", "microsoft", "amazon", "meta"), ("Mega-cap", "Mega-cap leadership watch")),
        (
            ("s&p", "spx", "spy", "qqq", "iwm", "nasdaq", "dow"),
            ("Index", "Direct index / ETF driver"),
        ),
    ]
    for keywords, result in themes:
        if any(word in raw for word in keywords):
            return result
    return ("Market", "General market-moving headline")


def _market_news_score(row: Dict[str, Any]) -> int:
    text = (
        f"{row.get('headline') or ''} {row.get('summary') or ''} {row.get('related') or ''}".lower()
    )
    score = 0
    weighted = {
        "fed": 5,
        "powell": 5,
        "rates": 4,
        "yield": 4,
        "treasury": 4,
        "cpi": 5,
        "pce": 5,
        "inflation": 5,
        "jobs": 4,
        "payroll": 4,
        "ism": 4,
        "oil": 3,
        "iran": 3,
        "vix": 4,
        "volatility": 4,
        "s&p": 5,
        "spx": 5,
        "spy": 4,
        "qqq": 4,
        "iwm": 4,
        "nvidia": 4,
        "apple": 3,
        "microsoft": 3,
        "amazon": 3,
        "meta": 3,
        "ai": 3,
        "earnings": 2,
    }
    for term, value in weighted.items():
        if term in text:
            score += value
    return score


def _market_news_item(
    row: Dict[str, Any], *, symbol: str = "", forced_tag: str = ""
) -> Dict[str, Any]:
    headline = str(row.get("headline") or "").strip()
    summary = str(row.get("summary") or "").strip()
    source = str(row.get("source") or "Source").strip() or "Source"
    url = str(row.get("url") or "").strip()
    tag, why = _market_news_theme(f"{headline} {summary} {row.get('related') or ''}")
    return {
        "headline": headline or "Market headline",
        "summary": summary or why,
        "source": source,
        "url": url,
        "published_label": _market_news_timestamp_label(row.get("datetime")),
        "tag": forced_tag or tag,
        "why": why,
        "symbol": symbol,
    }


def _market_news_snapshot() -> Dict[str, Any]:
    now_et = app_runtime.now_et()
    fetched_at = _market_news_cache.get("fetched_at")
    cached_payload = _market_news_cache.get("payload")
    if (
        isinstance(fetched_at, datetime)
        and isinstance(cached_payload, dict)
        and (now_et - fetched_at).total_seconds() < MARKET_NEWS_CACHE_TTL_SECONDS
    ):
        return cached_payload

    if not FINNHUB_API_KEY:
        disk = _load_market_news_disk_cache()
        if isinstance(disk, dict):
            _market_news_cache["payload"] = disk
            return disk
        return {
            "available": False,
            "source_note": "Finnhub news feed is not configured.",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
        }

    general_payload = _market_pulse_json_request_any(
        FINNHUB_BASE_URL + "/news",
        {"category": "general", "token": FINNHUB_API_KEY},
        timeout=8,
    )
    market_rows = general_payload if isinstance(general_payload, list) else []
    relevant_general = [
        row for row in market_rows if isinstance(row, dict) and _market_news_score(row) >= 4
    ]
    relevant_general.sort(
        key=lambda row: (_market_news_score(row), int(row.get("datetime") or 0)),
        reverse=True,
    )
    market_items = [_market_news_item(row) for row in relevant_general[:8]]

    watchlist_items: List[Dict[str, Any]] = []
    from_day = (now_et.date() - timedelta(days=5)).isoformat()
    to_day = now_et.date().isoformat()
    for symbol in MARKET_PULSE_WATCHLIST_NEWS_SYMBOLS:
        payload = _market_pulse_json_request_any(
            FINNHUB_BASE_URL + "/company-news",
            {"symbol": symbol, "from": from_day, "to": to_day, "token": FINNHUB_API_KEY},
            timeout=8,
        )
        if not isinstance(payload, list):
            continue
        best = next(
            (
                row
                for row in payload
                if isinstance(row, dict) and str(row.get("headline") or "").strip()
            ),
            None,
        )
        if best is None:
            continue
        item = _market_news_item(best, symbol=symbol, forced_tag=symbol)
        watchlist_items.append(item)

    macro_overlay = _forex_factory_usd_week_events(now_et.date())
    macro_events = []
    for event in list(macro_overlay.get("events") or [])[:6]:
        macro_events.append(
            {
                "headline": str(event.get("title") or "USD event"),
                "summary": f"{event.get('impact') or ''} impact scheduled for {event.get('time_label') or ''}.",
                "source": "Forex Factory",
                "url": str(event.get("jump_href") or "/candle-opens"),
                "published_label": str(event.get("date_label") or ""),
                "tag": "Macro",
                "why": str(event.get("tooltip") or "Calendar event"),
            }
        )

    result = {
        "available": bool(market_items or watchlist_items or macro_events),
        "source_note": "Finnhub market news plus Forex Factory macro triggers.",
        "macro_events": macro_events,
        "market_items": market_items,
        "watchlist_items": watchlist_items,
    }
    _market_news_cache["fetched_at"] = now_et
    _market_news_cache["payload"] = result
    _save_market_news_disk_cache(result)
    return result


def home():
    return _legacy().home()


def setup_page():
    from mccain_capital.services import auth as auth_svc

    return auth_svc.setup_page()


def login_page():
    from mccain_capital.services import auth as auth_svc

    return auth_svc.login_page()


def logout_page():
    from mccain_capital.services import auth as auth_svc

    return auth_svc.logout_page()


def healthz():
    return _legacy().healthz()


def favicon():
    return _legacy().favicon()


def _load_dashboard_milestone_settings() -> Dict[str, Any]:
    name = str(
        app_runtime.get_setting_value("dashboard_milestone_name", "Profit Milestone") or ""
    ).strip()
    if not name:
        name = "Profit Milestone"
    profit_goal = float(app_runtime.get_setting_float("dashboard_milestone_profit_goal", 5000.0))
    target_balance = float(app_runtime.get_setting_float("dashboard_milestone_target_balance", 0.0))
    profit_source = (
        str(app_runtime.get_setting_value("dashboard_milestone_profit_source", "ytd") or "ytd")
        .strip()
        .lower()
    )
    if profit_source not in MILESTONE_PROFIT_SOURCES:
        profit_source = "ytd"
    return {
        "name": name,
        "profit_goal": max(0.0, profit_goal),
        "target_balance": max(0.0, target_balance),
        "profit_source": profit_source,
    }


def _milestone_profit_value(
    source: str, *, today_net: float, this_week_total: float, mtd_net: float, ytd_net: float
) -> float:
    if source == "today":
        return float(today_net)
    if source == "week":
        return float(this_week_total)
    if source == "mtd":
        return float(mtd_net)
    return float(ytd_net)


def _dashboard_milestone_viewmodel(
    settings: Dict[str, Any],
    *,
    today_net: float,
    this_week_total: float,
    mtd_net: float,
    ytd_net: float,
    overall_balance: float,
    starting_balance: float,
    avg_daily_profit: float,
) -> Dict[str, Any]:
    source = str(settings.get("profit_source") or "ytd")
    profit_current = _milestone_profit_value(
        source,
        today_net=today_net,
        this_week_total=this_week_total,
        mtd_net=mtd_net,
        ytd_net=ytd_net,
    )
    profit_goal = float(settings.get("profit_goal") or 0.0)
    target_balance = float(settings.get("target_balance") or 0.0)

    profit_progress_pct = 0.0
    if profit_goal > 0.0:
        profit_progress_pct = max(0.0, min(100.0, (profit_current / profit_goal) * 100.0))

    balance_progress_pct = 0.0
    if target_balance > 0.0:
        if target_balance <= starting_balance:
            balance_progress_pct = 100.0 if overall_balance >= target_balance else 0.0
        else:
            balance_progress_pct = max(
                0.0,
                min(
                    100.0,
                    ((overall_balance - starting_balance) / (target_balance - starting_balance))
                    * 100.0,
                ),
            )

    if profit_goal > 0.0 and target_balance > 0.0:
        overall_progress_pct = min(profit_progress_pct, balance_progress_pct)
    elif profit_goal > 0.0:
        overall_progress_pct = profit_progress_pct
    elif target_balance > 0.0:
        overall_progress_pct = balance_progress_pct
    else:
        overall_progress_pct = 0.0

    source_labels = {"today": "Today", "week": "Week", "mtd": "MTD", "ytd": "YTD"}
    pace = float(avg_daily_profit)
    projected_days_profit: Optional[int] = None
    projected_days_balance: Optional[int] = None
    projected_days_overall: Optional[int] = None
    if pace > 0.0:
        if profit_goal > 0.0:
            projected_days_profit = int((max(0.0, profit_goal - profit_current) / pace) + 0.9999)
        if target_balance > 0.0:
            projected_days_balance = int(
                (max(0.0, target_balance - overall_balance) / pace) + 0.9999
            )
        if projected_days_profit is not None and projected_days_balance is not None:
            projected_days_overall = max(projected_days_profit, projected_days_balance)
        elif projected_days_profit is not None:
            projected_days_overall = projected_days_profit
        elif projected_days_balance is not None:
            projected_days_overall = projected_days_balance

    return {
        "name": str(settings.get("name") or "Profit Milestone"),
        "profit_source": source,
        "profit_source_label": source_labels.get(source, "YTD"),
        "profit_current": float(profit_current),
        "profit_goal": profit_goal,
        "profit_remaining": max(0.0, profit_goal - profit_current),
        "target_balance": target_balance,
        "balance_remaining": max(0.0, target_balance - overall_balance),
        "overall_progress_pct": overall_progress_pct,
        "profit_progress_pct": profit_progress_pct,
        "balance_progress_pct": balance_progress_pct,
        "profit_done": profit_goal > 0.0 and profit_current >= profit_goal,
        "balance_done": target_balance > 0.0 and overall_balance >= target_balance,
        "has_profit_goal": profit_goal > 0.0,
        "has_balance_goal": target_balance > 0.0,
        "avg_daily_profit": pace,
        "projected_days_profit": projected_days_profit,
        "projected_days_balance": projected_days_balance,
        "projected_days_overall": projected_days_overall,
    }


def dashboard_milestone_update():
    name = str(request.form.get("milestone_name") or "").strip()[:80]
    if not name:
        name = "Profit Milestone"
    profit_source = str(request.form.get("milestone_profit_source") or "ytd").strip().lower()
    if profit_source not in MILESTONE_PROFIT_SOURCES:
        profit_source = "ytd"
    profit_goal = app_runtime.parse_float(request.form.get("milestone_profit_goal") or "") or 0.0
    target_balance = (
        app_runtime.parse_float(request.form.get("milestone_target_balance") or "") or 0.0
    )

    app_runtime.set_setting_value("dashboard_milestone_name", name)
    app_runtime.set_setting_value("dashboard_milestone_profit_source", profit_source)
    app_runtime.set_setting_value("dashboard_milestone_profit_goal", f"{max(0.0, profit_goal):.2f}")
    app_runtime.set_setting_value(
        "dashboard_milestone_target_balance", f"{max(0.0, target_balance):.2f}"
    )
    flash("Milestone updated.", "success")

    y = str(request.form.get("y") or "").strip()
    m = str(request.form.get("m") or "").strip()
    scope = str(request.form.get("scope") or "").strip().lower()
    params: Dict[str, str] = {}
    if y:
        params["y"] = y
    if m:
        params["m"] = m
    if scope in {"active", "all"}:
        params["scope"] = scope
    return redirect(url_for("dashboard", **params))


def dashboard():
    from mccain_capital.repositories import analytics as analytics_repo
    from mccain_capital.repositories import trades as trades_repo
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import market_worker
    from mccain_capital.services import options_panel_service

    scope = trades_repo.account_scope_snapshot()
    scope_enabled = bool(scope.get("enabled"))
    scope_mode_raw = (request.args.get("scope") or "").strip().lower()
    scope_active = scope_enabled and scope_mode_raw != "all"
    scope_start = str(scope.get("start_date") or "")
    scope_starting_balance = float(scope.get("starting_balance") or 50000.0)
    anchor = trades_repo.latest_trade_day() or app_runtime.now_et().date()
    year = int(request.args.get("y") or anchor.year)
    month = max(1, min(12, int(request.args.get("m") or anchor.month)))

    heat = trades_repo.month_heatmap(year, month)
    prev_y, prev_m = (year, month - 1)
    next_y, next_m = (year, month + 1)
    if prev_m == 0:
        prev_m = 12
        prev_y -= 1
    if next_m == 13:
        next_m = 1
        next_y += 1

    month_name = date(year, month, 1).strftime("%B %Y")
    balance_integrity = trades_repo.balance_integrity_snapshot(
        start_date=scope_start if scope_active else None,
        starting_balance=scope_starting_balance if scope_active else None,
    )
    overall_balance = float(balance_integrity.get("canonical_balance") or 0.0)
    sync_status = get_system_status()
    data_trust = dashboard_data_trust(sync_status, balance_integrity)
    balance_badges = balance_state_badges(balance_integrity)
    sync_badges = sync_state_badges(
        sync_status,
        status_key="last_sync_status",
        stage_key="last_sync_stage",
        updated_key="last_sync_updated_human",
    )
    admin_recompute_allowed = auth_enabled() and is_authenticated()

    week_anchor = (
        anchor.isoformat()
        if (year == anchor.year and month == anchor.month)
        else date(year, month, 1).isoformat()
    )
    this_week_total = trades_repo.week_total_net(week_anchor)
    mtd_net = trades_repo.month_total_net(year, month)
    ytd_net = trades_repo.ytd_total_net(year)
    mtd_trades = trades_repo.month_trade_count(year, month)
    ytd_trades = trades_repo.ytd_trade_count(year)
    if scope_active and scope_start:
        with app_runtime.db() as conn:
            this_week_range_start, this_week_range_end = trades_repo.week_range_for(week_anchor)
            this_week_row = conn.execute(
                """
                SELECT COALESCE(SUM(net_pl), 0) AS net
                FROM trades
                WHERE trade_date >= ? AND trade_date < ?
                  AND trade_date >= ?
                """,
                (this_week_range_start, this_week_range_end, scope_start),
            ).fetchone()
            mtd_first = date(year, month, 1).isoformat()
            mtd_next = date(year + (month == 12), 1 if month == 12 else month + 1, 1).isoformat()
            mtd_row = conn.execute(
                """
                SELECT COALESCE(SUM(net_pl), 0) AS net, COUNT(*) AS count
                FROM trades
                WHERE trade_date >= ? AND trade_date < ?
                  AND trade_date >= ?
                """,
                (mtd_first, mtd_next, scope_start),
            ).fetchone()
            ytd_first = date(year, 1, 1).isoformat()
            ytd_next = date(year + 1, 1, 1).isoformat()
            ytd_row = conn.execute(
                """
                SELECT COALESCE(SUM(net_pl), 0) AS net, COUNT(*) AS count
                FROM trades
                WHERE trade_date >= ? AND trade_date < ?
                  AND trade_date >= ?
                """,
                (ytd_first, ytd_next, scope_start),
            ).fetchone()
        this_week_total = float((this_week_row["net"] if this_week_row else 0.0) or 0.0)
        mtd_net = float((mtd_row["net"] if mtd_row else 0.0) or 0.0)
        ytd_net = float((ytd_row["net"] if ytd_row else 0.0) or 0.0)
        mtd_trades = int((mtd_row["count"] if mtd_row else 0) or 0)
        ytd_trades = int((ytd_row["count"] if ytd_row else 0) or 0)
    proj = trades_repo.projections_from_daily(
        trades_repo.last_n_trading_day_totals(20, since_date=scope_start if scope_active else ""),
        overall_balance,
    )

    ytd_trades_list = [
        dict(r)
        for r in trades_repo.fetch_trades_range(
            date(year, 1, 1).isoformat(), date(year + 1, 1, 1).isoformat()
        )
    ]
    if scope_active and scope_start:
        ytd_trades_list = [
            r for r in ytd_trades_list if str(r.get("trade_date") or "") >= scope_start
        ]
    ytd_stats = trades_repo.trade_day_stats(ytd_trades_list)
    ytd_cons = trades_repo.calc_consistency(ytd_trades_list)
    ytd_wins = int(ytd_stats.get("wins", 0) or 0)
    ytd_losses = int(ytd_stats.get("losses", 0) or 0)
    ytd_win_rate = float(ytd_stats.get("win_rate", 0.0))
    today_rows = [dict(r) for r in trades_repo.fetch_trades(d=app_runtime.today_iso(), q="")]
    if scope_active and scope_start and app_runtime.today_iso() < scope_start:
        today_rows = []
    today_stats = trades_repo.trade_day_stats(today_rows)
    today_net = float(today_stats.get("total", 0.0))
    today_win_rate = float(today_stats.get("win_rate", 0.0))
    today_wins = int(today_stats.get("wins", 0) or 0)
    today_losses = int(today_stats.get("losses", 0) or 0)
    today_count = len(today_rows)
    capital_pulse = max(8.0, min(100.0, 50.0 + ((mtd_net / 3000.0) * 50.0)))
    discipline_pulse = max(8.0, min(100.0, today_win_rate if today_count else 18.0))
    discipline_label = (
        "Locked in"
        if today_win_rate >= 60 and today_net >= 0
        else "Stabilize process" if today_count else "No session logged"
    )
    recent_start = max(date(year, month, 1), anchor - timedelta(days=45))
    recent_rows = analytics_repo.fetch_analytics_rows(recent_start.isoformat(), anchor.isoformat())
    recent_rule_breaks = analytics_repo.rule_break_counts(recent_rows)
    recent_setup_rows = [
        row
        for row in analytics_repo.group_table(recent_rows, "setup_tag")
        if str(row.get("k") or "").strip() and str(row.get("k") or "").strip() != "Unlabeled"
    ]
    top_rule_break = recent_rule_breaks[0] if recent_rule_breaks else None
    top_setup = recent_setup_rows[0] if recent_setup_rows else None
    payout_focus = (
        f"5-day pace projects {app_runtime.money(proj['p5']['est_balance'])}."
        if proj.get("p5")
        else "Need more daily history for payout pace."
    )
    payout_focus_detail = (
        f"10-day estimate {app_runtime.money(proj['p10']['est_balance'])} · Avg day {app_runtime.money(proj['avg'])}."
        if proj.get("p10")
        else "Upload more trades to stabilize projections."
    )
    risk_posture_title = (
        "Attack window"
        if today_count
        and today_net > 0
        and (ytd_cons.get("ratio") is None or ytd_cons.get("ratio", 1.0) <= 0.30)
        else "Protect capital" if today_count and today_net < 0 else "Wait for clean signal"
    )
    risk_posture_detail = (
        f"Today {today_wins}W/{today_losses}L · Consistency "
        + (f"{float(ytd_cons['ratio']) * 100.0:.1f}%" if ytd_cons.get("ratio") is not None else "—")
        + "."
    )
    pattern_watch = (
        f"Most common breach: {str(top_rule_break['tag']).replace('-', ' ').title()} ({top_rule_break['count']})."
        if top_rule_break
        else "No recurring rule-break tag is dominating recent sessions."
    )
    setup_focus = (
        f"Lead setup {top_setup['k']} · {top_setup['count']} trades · {app_runtime.money(top_setup['net'])}."
        if top_setup
        else "No dominant labeled setup yet."
    )
    milestone_settings = _load_dashboard_milestone_settings()
    milestone = _dashboard_milestone_viewmodel(
        milestone_settings,
        today_net=today_net,
        this_week_total=this_week_total,
        mtd_net=mtd_net,
        ytd_net=ytd_net,
        overall_balance=overall_balance,
        starting_balance=float(balance_integrity.get("starting_balance") or 50000.0),
        avg_daily_profit=float(proj.get("avg") or 0.0),
    )
    market_snapshot = market_worker.get_market_snapshot()
    market_updated_at = str(market_snapshot.get("updated_at") or "")
    market_updated_at_human = ""
    if market_updated_at:
        try:
            dt = datetime.fromisoformat(market_updated_at)
            market_updated_at_human = (
                dt.astimezone(app_runtime.TZ)
                .strftime("%b %d, %Y %I:%M:%S %p ET")
                .replace(" 0", " ")
            )
        except Exception:
            market_updated_at_human = market_updated_at

    options_snapshot = options_panel_service.get_options_snapshot()
    options_asof = str(options_snapshot.get("asof") or "")
    options_asof_human = ""
    if options_asof:
        try:
            dt = datetime.fromisoformat(options_asof)
            options_asof_human = (
                dt.astimezone(app_runtime.TZ)
                .strftime("%b %d, %Y %I:%M:%S %p ET")
                .replace(" 0", " ")
            )
        except Exception:
            options_asof_human = options_asof
    options_spx = dict((options_snapshot.get("symbols") or {}).get("SPX") or {})
    options_underlying = dict(options_spx.get("underlying") or {})
    options_contracts = list(options_spx.get("contracts") or [])
    gamma_snapshot = gamma_map_service.get_gamma_snapshot()
    options_gamma = {
        "gamma_flip": gamma_snapshot.get("gamma_flip"),
        "call_wall": gamma_snapshot.get("call_wall"),
        "put_wall": gamma_snapshot.get("put_wall"),
        "net_gamma": gamma_snapshot.get("net_gamma_label") or gamma_snapshot.get("net_gex") or "—",
        "regime": gamma_snapshot.get("regime") or "unavailable",
        "bias": gamma_snapshot.get("bias") or "insufficient_data",
        "gamma_walls_top3": list(gamma_snapshot.get("gamma_walls_top3") or []),
        "void_zone": dict(gamma_snapshot.get("void_zone") or {"start": None, "end": None}),
    }

    if not current_app.config.get("TESTING"):
        market_worker.start_market_worker_once()
        options_panel_service.start_options_worker_once()
        gamma_map_service.start_gamma_worker_once()

    content = render_template(
        "dashboard.html",
        heat=heat,
        prev_y=prev_y,
        prev_m=prev_m,
        next_y=next_y,
        next_m=next_m,
        month_name=month_name,
        overall_balance=overall_balance,
        balance_integrity=balance_integrity,
        balance_badges=balance_badges,
        sync_status=sync_status,
        sync_badges=sync_badges,
        data_trust=data_trust,
        admin_recompute_allowed=admin_recompute_allowed,
        this_week_total=this_week_total,
        mtd_net=mtd_net,
        ytd_net=ytd_net,
        mtd_trades=mtd_trades,
        ytd_trades=ytd_trades,
        ytd_wins=ytd_wins,
        ytd_losses=ytd_losses,
        ytd_win_rate=ytd_win_rate,
        ytd_cons=ytd_cons,
        cons_threshold=0.30,
        today_net=today_net,
        today_win_rate=today_win_rate,
        today_wins=today_wins,
        today_losses=today_losses,
        today_count=today_count,
        capital_pulse=capital_pulse,
        discipline_pulse=discipline_pulse,
        discipline_label=discipline_label,
        payout_focus=payout_focus,
        payout_focus_detail=payout_focus_detail,
        risk_posture_title=risk_posture_title,
        risk_posture_detail=risk_posture_detail,
        pattern_watch=pattern_watch,
        setup_focus=setup_focus,
        proj=proj,
        account_scope=scope,
        scope_mode=("active" if scope_active else "all"),
        scope_active_href=f"/dashboard?y={year}&m={month}&scope=active",
        scope_all_href=f"/dashboard?y={year}&m={month}&scope=all",
        dashboard_year=year,
        dashboard_month=month,
        milestone=milestone,
        market_watchlist=list(market_worker.WATCHLIST),
        market_prices=market_snapshot.get("prices") or {},
        market_alerts=market_snapshot.get("alerts") or [],
        market_updated_at=market_updated_at,
        market_updated_at_human=market_updated_at_human,
        options_underlying=options_underlying,
        options_gamma=options_gamma,
        options_contracts=options_contracts,
        options_asof=options_asof,
        options_asof_human=options_asof_human,
        gamma_snapshot=gamma_snapshot,
        money=app_runtime.money,
        money_compact=_money_compact,
    )
    return render_page(content, active="dashboard")


def stream_market():
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import market_worker
    from mccain_capital.services import options_panel_service

    is_testing = bool(current_app.config.get("TESTING"))
    if not is_testing:
        market_worker.start_market_worker_once()
        options_panel_service.start_options_worker_once()
        gamma_map_service.start_gamma_worker_once()

    @stream_with_context
    def generate():
        while True:
            payload = market_worker.get_market_snapshot()
            payload["options"] = options_panel_service.get_options_snapshot()
            payload["gamma_map"] = gamma_map_service.get_gamma_snapshot()
            yield f"data: {json.dumps(payload)}\\n\\n"
            if is_testing:
                break
            time.sleep(2)

    response = Response(generate(), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Connection"] = "keep-alive"
    return response


def stream_options_panel():
    from mccain_capital.services import options_panel_service

    is_testing = bool(current_app.config.get("TESTING"))
    if not is_testing:
        options_panel_service.start_options_worker_once()

    @stream_with_context
    def generate():
        while True:
            payload = options_panel_service.get_options_snapshot()
            yield f"data: {json.dumps(payload)}\\n\\n"
            if is_testing:
                break
            time.sleep(2)

    response = Response(generate(), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Connection"] = "keep-alive"
    return response


def gamma_map_page():
    from mccain_capital.services import gamma_map_service

    is_testing = bool(current_app.config.get("TESTING"))
    if not is_testing:
        gamma_map_service.start_gamma_worker_once()

    snapshot = gamma_map_service.get_gamma_snapshot()
    if not snapshot.get("asof") and not is_testing:
        try:
            snapshot = gamma_map_service.run_gamma_refresh_once()
        except Exception:
            snapshot = gamma_map_service.get_gamma_snapshot()

    content = render_template(
        "core/gamma_map.html",
        gamma=snapshot,
        money=app_runtime.money,
        money_compact=_money_compact,
    )
    return render_page(content, active="market-pulse", title="McCain Capital · Gamma Map")


def market_pulse_page():
    force_refresh = (request.args.get("refresh") or "").strip().lower() in {"1", "true", "yes"}
    now_et = app_runtime.now_et()
    snapshot = _market_pulse_snapshot(force_refresh=force_refresh)
    news_snapshot = _market_news_snapshot()
    quotes = _market_pulse_enrich_quotes(list(snapshot.get("quotes") or []), now_et)
    alert = _market_pulse_alert(quotes)
    guardrail = _market_pulse_guardrail(quotes)
    context = _market_pulse_context(quotes)
    integrity = dict(snapshot.get("integrity") or {})
    stats = _market_pulse_stats(quotes)
    core_quotes = list(quotes)
    leader_quotes = [q for q in quotes if str(q.get("group") or "") == "leaders"]

    content = render_template(
        "core/market_pulse.html",
        available=bool(snapshot.get("available")),
        fetched_at=str(snapshot.get("fetched_at") or ""),
        source_label=str(snapshot.get("source_label") or "Yahoo Finance chart feed"),
        source_note=str(snapshot.get("source_note") or ""),
        core_quotes=core_quotes,
        leader_quotes=leader_quotes,
        context=context,
        integrity=integrity,
        alert=alert,
        guardrail=guardrail,
        market_hours=bool(_market_pulse_market_hours(now_et)),
        stats=stats,
        news_available=bool(news_snapshot.get("available")),
        news_source_note=str(news_snapshot.get("source_note") or ""),
        macro_events=list(news_snapshot.get("macro_events") or []),
        market_items=list(news_snapshot.get("market_items") or []),
        watchlist_items=list(news_snapshot.get("watchlist_items") or []),
        money=app_runtime.money,
        money_compact=_money_compact,
    )
    resp = make_response(
        render_page(content, active="market-pulse", title="McCain Capital · Market Pulse")
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


def command_calendar_page():
    from mccain_capital.repositories import analytics as analytics_repo
    from mccain_capital.repositories import goals as goals_repo
    from mccain_capital.repositories import journal as journal_repo
    from mccain_capital.repositories import trades as trades_repo

    anchor = trades_repo.latest_trade_day() or app_runtime.now_et().date()
    year = int(request.args.get("y") or anchor.year)
    month = max(1, min(12, int(request.args.get("m") or anchor.month)))
    first = date(year, month, 1)
    next_month = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    month_end = next_month - timedelta(days=1)

    heat = trades_repo.month_heatmap(year, month)
    journal_rows = journal_repo.fetch_entry_day_rollups(first.isoformat(), month_end.isoformat())
    goal_rows = goals_repo.fetch_daily_goals(first.isoformat(), month_end.isoformat())
    analytics_rows = analytics_repo.fetch_analytics_rows(first.isoformat(), month_end.isoformat())

    journal_map = {str(row["entry_date"]): row for row in journal_rows}
    goal_map = {str(row["track_date"]): dict(row) for row in goal_rows}
    analytics_map = _analytics_rows_by_day(analytics_rows)

    activity_days = 0
    journal_days = 0
    project_days = 0
    project_signals = 0
    debrief_count = 0
    state_rollup: Dict[str, int] = {}
    mistake_rollup: Dict[str, int] = {}

    for week in heat["weeks"]:
        for day in week["days"]:
            iso = str(day.get("iso") or "")
            if not iso:
                continue
            journal = journal_map.get(iso) or {}
            goals = goal_map.get(iso) or {}
            day_analytics = analytics_map.get(iso) or []
            goal_signal_count = _goal_signal_count(goals)
            if day.get("has_trades") or journal or goal_signal_count:
                activity_days += 1
            if journal:
                journal_days += 1
                debrief_count += int(journal.get("entry_count") or 0)
            if goal_signal_count:
                project_days += 1
                project_signals += goal_signal_count
            day["journal"] = journal
            day["goals"] = goals
            day["goal_signal_count"] = goal_signal_count
            day["has_projects"] = goal_signal_count > 0
            day["activity_level"] = sum(
                [
                    1 if day.get("has_trades") else 0,
                    1 if journal else 0,
                    1 if goal_signal_count else 0,
                ]
            )
            day["focus_label"] = _day_focus_label(day, journal, goals)
            day["project_summary"] = _project_summary(goals)
            day["journal_summary"] = _journal_summary(journal)
            day["mistake_summary"] = _day_mistake_summary(day_analytics)
            day["day_state"] = _day_state(day, journal, goals, day_analytics)
            day["day_state_label"] = _day_state_label(day["day_state"])
            state_rollup[day["day_state"]] = int(state_rollup.get(day["day_state"], 0)) + 1
            if day["mistake_summary"]:
                mistake_rollup[day["mistake_summary"]] = (
                    int(mistake_rollup.get(day["mistake_summary"], 0)) + 1
                )

    month_net = trades_repo.month_total_net(year, month)
    month_trade_count = trades_repo.month_trade_count(year, month)
    overall_balance = trades_repo.latest_balance_overall()
    month_name = first.strftime("%B %Y")
    prev_y, prev_m = (year, month - 1)
    next_y, next_m = (year, month + 1)
    if prev_m == 0:
        prev_m = 12
        prev_y -= 1
    if next_m == 13:
        next_m = 1
        next_y += 1

    content = render_template(
        "core/command_calendar.html",
        heat=heat,
        month_name=month_name,
        month_net=month_net,
        month_trade_count=month_trade_count,
        overall_balance=overall_balance,
        prev_y=prev_y,
        prev_m=prev_m,
        next_y=next_y,
        next_m=next_m,
        activity_days=activity_days,
        journal_days=journal_days,
        debrief_count=debrief_count,
        project_days=project_days,
        project_signals=project_signals,
        state_rollup=state_rollup,
        top_mistake=max(mistake_rollup.items(), key=lambda kv: kv[1])[0] if mistake_rollup else "",
        money=app_runtime.money,
        money_compact=_money_compact,
    )
    return render_page(content, active="calendar", title=f"{month_name} Calendar")


def dashboard_recompute_balances():
    if not auth_enabled():
        flash("Enable authentication to use admin recompute actions.", "warn")
        return redirect(url_for("dashboard"))
    if not is_authenticated():
        abort(403)

    from mccain_capital.repositories import trades as trades_repo

    starting = float(app_runtime.get_setting_float("starting_balance", 50000.0))
    trades_repo.recompute_balances(starting_balance=starting)
    try:
        from mccain_capital.services.trades import record_admin_audit

        record_admin_audit(
            "dashboard_recompute_balances",
            {"starting_balance": starting},
            actor=effective_username(),
        )
    except Exception:
        pass
    flash("Stored trade balances recomputed from canonical ledger math.", "success")
    return redirect(url_for("dashboard"))


def candle_opens_page():
    anchor = app_runtime.now_et().date()
    year = int(request.args.get("y") or anchor.year)
    month = max(1, min(12, int(request.args.get("m") or anchor.month)))
    model = _build_candle_open_calendar(year, month)
    content = render_template("core/candle_opens.html", **model)
    return render_page(
        content,
        active="candle-opens",
        title=f"{model['month_name']} Candle Opens",
        top_notice=model["top_notice"],
    )


def analytics_page():
    from mccain_capital.services import analytics as analytics_svc

    return analytics_svc.analytics_page()


def session_replay_page():
    from mccain_capital.services import analytics as analytics_svc

    return analytics_svc.session_replay_page()


def calculator():
    context = _calculator_context(request.form if request.method == "POST" else None)

    if request.method == "POST" and request.headers.get("X-Requested-With") == "XMLHttpRequest":
        results_html = render_template(
            "calculator_results.html",
            out=context["out"],
            money=app_runtime.money,
        )
        return jsonify(
            {
                "ok": context["err"] is None,
                "err": context["err"],
                "results_html": results_html,
            }
        )

    content = render_template(
        "calculator.html",
        out=context["out"],
        err=context["err"],
        vals=context["vals"],
        money=app_runtime.money,
        current_balance=context["current_balance"],
        current_consistency=context["current_consistency"],
    )
    return render_page(content, active="calc")


def links_page():
    content = render_template("core/links.html")
    return render_page(content, active="links")


def export_json():
    return _legacy().export_json()


def backup_data():
    stamp = app_runtime.now_et().strftime("%Y%m%d_%H%M%S")
    fd, out_path = tempfile.mkstemp(prefix="mccain_backup_", suffix=".zip")
    os.close(fd)
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if os.path.exists(app_runtime.DB_PATH):
            zf.write(str(app_runtime.DB_PATH), arcname="data/journal.db")

        if os.path.isdir(app_runtime.UPLOAD_DIR):
            for root, _, files in os.walk(str(app_runtime.UPLOAD_DIR)):
                for name in files:
                    full = os.path.join(root, name)
                    rel = os.path.relpath(full, str(app_runtime.UPLOAD_DIR))
                    zf.write(full, arcname=f"data/uploads/{rel}")

        meta = {
            "exported_at": app_runtime.now_iso(),
            "db_path": str(app_runtime.DB_PATH),
            "upload_dir": str(app_runtime.UPLOAD_DIR),
            "app": "mccain-capital",
        }
        zf.writestr("data/meta.json", json.dumps(meta, ensure_ascii=False, indent=2))

    try:
        from mccain_capital.services.trades import record_admin_audit

        record_admin_audit(
            "manual_backup_downloaded",
            {"file": f"mccain_capital_backup_{stamp}.zip"},
            actor=(
                _legacy()._effective_username()
                if _legacy().auth_enabled()
                else _legacy().APP_USERNAME
            ),
        )
    except Exception:
        pass
    return send_file(
        out_path,
        as_attachment=True,
        download_name=f"mccain_capital_backup_{stamp}.zip",
        mimetype="application/zip",
    )


def restore_data():
    if request.method == "GET":
        content = render_template(
            "core/restore_backup.html",
            db_path=str(app_runtime.DB_PATH),
            upload_dir=str(app_runtime.UPLOAD_DIR),
        )
        return render_page(content, active="dashboard")

    f = request.files.get("backup_zip")
    if not f or not f.filename:
        return render_page(simple_msg("Please choose a backup zip file."), active="dashboard")

    try:
        with zipfile.ZipFile(f.stream) as zf:
            names = zf.namelist()
            if not names:
                return render_page(simple_msg("Backup zip is empty."), active="dashboard")

            allowed_prefixes = ("data/journal.db", "data/uploads/", "data/meta.json")
            for n in names:
                if n.startswith("/") or ".." in n:
                    return render_page(
                        simple_msg("Backup zip contains unsafe paths."), active="dashboard"
                    )
                if not any(n == p or n.startswith(p) for p in allowed_prefixes):
                    return render_page(
                        simple_msg("Backup zip contains unsupported files."), active="dashboard"
                    )

            db_member = "data/journal.db"
            if db_member in names:
                db_path = str(app_runtime.DB_PATH)
                os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
                db_dir = os.path.dirname(db_path) or "."
                fd, tmp_db = tempfile.mkstemp(prefix="restore_db_", suffix=".tmp", dir=db_dir)
                os.close(fd)
                try:
                    with zf.open(db_member) as src, open(tmp_db, "wb") as dst:
                        shutil.copyfileobj(src, dst, length=1024 * 1024)
                    os.replace(tmp_db, db_path)
                finally:
                    if os.path.exists(tmp_db):
                        os.unlink(tmp_db)

            upload_dir = str(app_runtime.UPLOAD_DIR)
            os.makedirs(upload_dir, exist_ok=True)
            for n in names:
                if not n.startswith("data/uploads/") or n.endswith("/"):
                    continue
                rel = n[len("data/uploads/") :]
                out_path = os.path.join(upload_dir, rel)
                out_dir = os.path.dirname(out_path)
                if out_dir:
                    os.makedirs(out_dir, exist_ok=True)
                with zf.open(n) as src, open(out_path, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
    except zipfile.BadZipFile:
        return render_page(simple_msg("Invalid zip file."), active="dashboard")
    except Exception as e:
        return render_page(simple_msg(f"Restore failed: {e}"), active="dashboard")

    try:
        from mccain_capital.services.trades import record_admin_audit

        record_admin_audit(
            "manual_backup_restored",
            {"source_filename": f.filename if f else ""},
            actor=(
                _legacy()._effective_username()
                if _legacy().auth_enabled()
                else _legacy().APP_USERNAME
            ),
        )
    except Exception:
        pass
    return render_page(simple_msg("Backup restore completed."), active="dashboard")


def strat_page():
    from mccain_capital.services import strat as strat_svc

    return strat_svc.strat_page()


def _money_compact(val: Any) -> str:
    if val is None or val == "":
        return ""
    try:
        n = float(val)
    except Exception:
        return ""
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n >= 10000:
        return f"{sign}${n / 1000:.0f}k"
    if n >= 1000:
        return f"{sign}${n / 1000:.1f}k"
    if n >= 100:
        return f"{sign}${n:.0f}"
    return f"{sign}${n:.2f}"


def _calc_stop_takeprofit(entry: float, stop_pct: float, target_pct: float) -> Tuple[float, float]:
    stop_price = round(entry * (1 - stop_pct / 100.0), 2)
    tp_price = round(entry * (1 + target_pct / 100.0), 2)
    return stop_price, tp_price


def _calc_risk_reward(
    entry: float, contracts: int, stop_price: float, tp_price: float, fee_per_contract: float
) -> Dict[str, float]:
    fees = round(contracts * fee_per_contract, 2)
    risk_gross = (entry - stop_price) * MULTIPLIER * contracts
    reward_gross = (tp_price - entry) * MULTIPLIER * contracts
    risk_net = round(risk_gross + fees, 2)
    reward_net = round(reward_gross - fees, 2)
    rr = round((reward_net / risk_net), 2) if risk_net > 0 else 0.0
    return {"fees": fees, "risk_net": risk_net, "reward_net": reward_net, "rr": rr}


def _calculator_context(form_data: Optional[Any] = None) -> Dict[str, Any]:
    from mccain_capital.repositories import trades as trades_repo

    current_balance = trades_repo.latest_balance_overall() or 50000.0
    base_trades = trades_repo.fetch_trades(d="", q="")
    current_consistency = trades_repo.calc_consistency(base_trades)
    vals = {
        "entry": "",
        "contracts": "1",
        "stop_pct": str(DEFAULT_STOP_PCT),
        "target_pct": str(DEFAULT_TARGET_PCT),
        "fee_per_contract": str(DEFAULT_FEE_PER_CONTRACT),
    }
    out = None
    err = None
    if form_data is not None:
        vals["entry"] = (form_data.get("entry") or "").strip()
        vals["contracts"] = (form_data.get("contracts") or "1").strip()
        vals["stop_pct"] = (form_data.get("stop_pct") or str(DEFAULT_STOP_PCT)).strip()
        vals["target_pct"] = (form_data.get("target_pct") or str(DEFAULT_TARGET_PCT)).strip()
        vals["fee_per_contract"] = (
            form_data.get("fee_per_contract") or str(DEFAULT_FEE_PER_CONTRACT)
        ).strip()

        entry = app_runtime.parse_float(vals["entry"])
        contracts = app_runtime.parse_int(vals["contracts"]) or 1
        stop_pct = app_runtime.parse_float(vals["stop_pct"]) or DEFAULT_STOP_PCT
        target_pct = app_runtime.parse_float(vals["target_pct"]) or DEFAULT_TARGET_PCT
        fee = app_runtime.parse_float(vals["fee_per_contract"]) or DEFAULT_FEE_PER_CONTRACT

        if not entry or entry <= 0:
            err = "Entry premium must be > 0."
        elif contracts <= 0:
            err = "Contracts must be >= 1."
        else:
            stop_price, tp_price = _calc_stop_takeprofit(entry, stop_pct, target_pct)
            rr = _calc_risk_reward(entry, contracts, stop_price, tp_price, fee)
            ladder = []
            for p in range(10, 101, 10):
                ladder_tp = round(entry * (1 + p / 100.0), 2)
                ladder_rr = _calc_risk_reward(entry, contracts, stop_price, ladder_tp, fee)
                ladder.append({"pct": p, "tp": ladder_tp, "net": ladder_rr["reward_net"]})

            out = {
                "entry": entry,
                "contracts": contracts,
                "total_spend": round(entry * MULTIPLIER * contracts + (fee * contracts), 2),
                "stop_pct": stop_pct,
                "target_pct": target_pct,
                "fee": fee,
                "stop_price": stop_price,
                "tp_price": tp_price,
                "current_balance": float(current_balance),
                "balance_if_stop": round(float(current_balance) - float(rr["risk_net"]), 2),
                "balance_if_target": round(float(current_balance) + float(rr["reward_net"]), 2),
                "consistency_current": current_consistency,
                "consistency_if_stop": trades_repo.calc_consistency(
                    list(base_trades) + [{"net_pl": -float(rr["risk_net"])}]
                ),
                "consistency_if_target": trades_repo.calc_consistency(
                    list(base_trades) + [{"net_pl": float(rr["reward_net"])}]
                ),
                "risk_pct_balance": (
                    round((float(rr["risk_net"]) / float(current_balance) * 100.0), 2)
                    if current_balance
                    else 0.0
                ),
                "reward_pct_balance": (
                    round((float(rr["reward_net"]) / float(current_balance) * 100.0), 2)
                    if current_balance
                    else 0.0
                ),
                "profit_pct": (
                    round(
                        (
                            float(rr["reward_net"])
                            / float(entry * MULTIPLIER * contracts + (fee * contracts))
                            * 100.0
                        ),
                        1,
                    )
                    if (entry * MULTIPLIER * contracts + (fee * contracts))
                    else 0.0
                ),
                "plan_state": (
                    "Sharp"
                    if rr["rr"] >= 2.0 and float(rr["risk_net"]) <= float(current_balance) * 0.01
                    else (
                        "Manageable"
                        if rr["rr"] >= 1.5
                        and float(rr["risk_net"]) <= float(current_balance) * 0.02
                        else "Too hot"
                    )
                ),
                **rr,
                "ladder": ladder,
            }

    return {
        "out": out,
        "err": err,
        "vals": vals,
        "current_balance": current_balance,
        "current_consistency": current_consistency,
    }


def _build_candle_open_calendar(year: int, month: int) -> Dict[str, Any]:
    cal = Calendar(firstweekday=6)
    session_index = _trading_day_index_map(year)
    week_index, week_open_dates = _trading_week_index_map(year)
    month_index, month_open_dates = _trading_month_index_map(year)
    now_et = app_runtime.now_et()
    news_overlay = _forex_factory_usd_week_events(now_et.date())
    prev_y, prev_m = (year, month - 1)
    next_y, next_m = (year, month + 1)
    if prev_m == 0:
        prev_m = 12
        prev_y -= 1
    if next_m == 13:
        next_m = 1
        next_y += 1

    weeks = []
    total_signals = 0
    trading_days = 0
    for week in cal.monthdatescalendar(year, month):
        cells = []
        for day in week:
            in_month = day.month == month
            holiday_name = _market_holiday_name(day)
            is_weekend = day.weekday() >= 5
            is_holiday = bool(holiday_name)
            is_trading = in_month and not is_weekend and not is_holiday
            day_labels = []
            week_labels = []
            month_labels = []
            if is_trading:
                trading_days += 1
                idx = session_index.get(day)
                if idx is not None:
                    day_labels = [f"{span}D" for span in DAY_OPEN_INTERVALS if idx % span == 1]
                if day in week_open_dates:
                    widx = week_index.get(day)
                    if widx is not None:
                        week_labels = [
                            f"{span}W" for span in WEEK_OPEN_INTERVALS if widx % span == 1
                        ]
                if day in month_open_dates:
                    midx = month_index.get(day)
                    if midx is not None:
                        month_labels = [
                            f"{span}M" for span in MONTH_OPEN_INTERVALS if midx % span == 1
                        ]
                total_signals += len(day_labels) + len(week_labels) + len(month_labels)
            cells.append(
                {
                    "day": day.day,
                    "iso": day.isoformat(),
                    "weekday_label": day.strftime("%a"),
                    "in_month": in_month,
                    "is_weekend": is_weekend,
                    "is_holiday": is_holiday,
                    "is_trading": is_trading,
                    "holiday_name": holiday_name,
                    "day_labels": day_labels,
                    "week_labels": week_labels,
                    "month_labels": month_labels,
                    "news_events": news_overlay["events_by_day"].get(day.isoformat(), []),
                    "labels": day_labels + week_labels + month_labels,
                }
            )
        weeks.append(cells)

    month_name = date(year, month, 1).strftime("%B %Y")
    return {
        "month_name": month_name,
        "year": year,
        "month": month,
        "weeks": weeks,
        "prev_y": prev_y,
        "prev_m": prev_m,
        "next_y": next_y,
        "next_m": next_m,
        "trading_days": trading_days,
        "signal_count": total_signals,
        "day_legend": ", ".join(f"{span}D" for span in DAY_OPEN_INTERVALS),
        "week_legend": ", ".join(f"{span}W" for span in WEEK_OPEN_INTERVALS),
        "month_legend": ", ".join(f"{span}M" for span in MONTH_OPEN_INTERVALS),
        "news_week_range": news_overlay["week_range_label"],
        "news_summary": news_overlay["summary"],
        "news_total": news_overlay["total"],
        "news_high": news_overlay["high_count"],
        "news_medium": news_overlay["medium_count"],
        "news_events": news_overlay["events"],
        "news_days": news_overlay["days"],
        "news_available": news_overlay["available"],
        "top_notice": _candle_page_top_notice(now_et, news_overlay["events"]),
    }


def _candle_page_top_notice(
    now_et: datetime,
    news_events: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    for event in news_events:
        raw = str(event.get("starts_at") or "")
        if not raw or str(event.get("impact_class") or "") != "high":
            continue
        try:
            starts_at = datetime.fromisoformat(raw)
        except ValueError:
            continue
        if starts_at < now_et:
            continue
        day_prefix = "" if starts_at.date() == now_et.date() else f"{starts_at.strftime('%a')} "
        return {
            "label": "Red Folder",
            "text": f"🔴 {day_prefix}{event['time_label']}",
            "detail": event["tooltip"],
            "href": event.get("jump_href") or "",
            "level": "high",
        }
    return None


def _forex_factory_usd_week_events(anchor: date) -> Dict[str, Any]:
    week_start = anchor - timedelta(days=(anchor.weekday() + 1) % 7)
    week_end = week_start + timedelta(days=6)
    result: Dict[str, Any] = {
        "available": False,
        "week_range_label": f"{week_start.strftime('%b %-d')} to {week_end.strftime('%b %-d')}",
        "events": [],
        "events_by_day": {},
        "days": [],
        "total": 0,
        "high_count": 0,
        "medium_count": 0,
        "summary": "Live USD red/orange events unavailable right now.",
    }
    payload = get_forex_factory_feed()
    if payload is None:
        return result

    events: List[Dict[str, Any]] = []
    events_by_day: Dict[str, List[Dict[str, Any]]] = {}
    high_count = 0
    medium_count = 0
    for row in payload:
        if not isinstance(row, dict):
            continue
        if str(row.get("country") or "").upper() != "USD":
            continue
        impact = str(row.get("impact") or "").title()
        if impact not in {"High", "Medium"}:
            continue
        raw_date = str(row.get("date") or "").strip()
        if not raw_date:
            continue
        try:
            dt = datetime.fromisoformat(raw_date)
        except ValueError:
            continue
        event_day = dt.date()
        if event_day < week_start or event_day > week_end:
            continue
        time_label = dt.strftime("%-I:%M %p ET")
        item = {
            "title": str(row.get("title") or "USD event").strip() or "USD event",
            "impact": impact,
            "iso": event_day.isoformat(),
            "date_label": event_day.strftime("%a, %b %-d"),
            "starts_at": raw_date,
            "time_label": time_label,
            "impact_class": "high" if impact == "High" else "medium",
            "icon": "🔴" if impact == "High" else "🟠",
            "jump_href": (
                f"/candle-opens?y={event_day.year}&m={event_day.month}"
                f"#news-day-{event_day.isoformat()}"
            ),
            "tooltip": f"{impact} impact • {time_label} • {str(row.get('title') or 'USD event').strip() or 'USD event'}",
        }
        events.append(item)
        events_by_day.setdefault(item["iso"], []).append(item)
        if impact == "High":
            high_count += 1
        else:
            medium_count += 1

    events.sort(key=lambda item: (item["iso"], item["time_label"], item["title"]))
    for items in events_by_day.values():
        items.sort(key=lambda item: (item["time_label"], item["title"]))

    news_days: List[Dict[str, Any]] = []
    for iso in sorted(events_by_day.keys()):
        day_events = list(events_by_day.get(iso, []))
        if not day_events:
            continue
        news_days.append(
            {
                "iso": iso,
                "date_label": day_events[0]["date_label"],
                "high_count": len([e for e in day_events if e.get("impact_class") == "high"]),
                "medium_count": len([e for e in day_events if e.get("impact_class") == "medium"]),
                "events": day_events,
            }
        )

    if events:
        result.update(
            {
                "available": True,
                "events": events,
                "events_by_day": events_by_day,
                "days": news_days,
                "total": len(events),
                "high_count": high_count,
                "medium_count": medium_count,
                "summary": f"{len(events)} USD red/orange events this week.",
            }
        )
    return result


def _goal_signal_count(goal_row: Dict[str, Any]) -> int:
    if not goal_row:
        return 0
    count = 0
    count += 1 if float(goal_row.get("debt_paid") or 0.0) > 0 else 0
    count += 1 if int(goal_row.get("upwork_proposals") or 0) > 0 else 0
    count += 1 if int(goal_row.get("upwork_interviews") or 0) > 0 else 0
    count += 1 if float(goal_row.get("upwork_hours") or 0.0) > 0 else 0
    count += 1 if float(goal_row.get("upwork_earnings") or 0.0) > 0 else 0
    count += 1 if float(goal_row.get("other_income") or 0.0) > 0 else 0
    count += 1 if str(goal_row.get("notes") or "").strip() else 0
    return count


def _project_summary(goal_row: Dict[str, Any]) -> List[str]:
    if not goal_row:
        return []
    items: List[str] = []
    proposals = int(goal_row.get("upwork_proposals") or 0)
    interviews = int(goal_row.get("upwork_interviews") or 0)
    hours = float(goal_row.get("upwork_hours") or 0.0)
    debt_paid = float(goal_row.get("debt_paid") or 0.0)
    other_income = float(goal_row.get("other_income") or 0.0)
    if proposals:
        items.append(f"{proposals} proposals")
    if interviews:
        items.append(f"{interviews} interviews")
    if hours:
        items.append(f"{hours:.1f}h outside work")
    if debt_paid:
        items.append(f"Debt {app_runtime.money(debt_paid)}")
    if other_income:
        items.append(f"Other {app_runtime.money(other_income)}")
    if str(goal_row.get("notes") or "").strip():
        items.append("project note")
    return items[:3]


def _journal_summary(journal_row: Dict[str, Any]) -> List[str]:
    if not journal_row:
        return []
    items: List[str] = []
    entry_count = int(journal_row.get("entry_count") or 0)
    if entry_count:
        items.append(f"{entry_count} debrief{'s' if entry_count != 1 else ''}")
    moods = list(journal_row.get("moods") or [])
    if moods:
        items.append(moods[0].title())
    setups = list(journal_row.get("setups") or [])
    if setups:
        items.append(setups[0])
    return items[:3]


def _day_focus_label(
    day_row: Dict[str, Any], journal_row: Dict[str, Any], goal_row: Dict[str, Any]
) -> str:
    if day_row.get("has_trades") and journal_row and goal_row:
        return "Full stack day"
    if day_row.get("has_trades") and journal_row:
        return "Traded and debriefed"
    if day_row.get("has_trades") and goal_row:
        return "Traded and built"
    if journal_row and goal_row:
        return "Review and project push"
    if day_row.get("has_trades"):
        return "Trading session"
    if journal_row:
        return "Debrief day"
    if goal_row:
        return "Project day"
    return "No signal"


def _analytics_rows_by_day(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        iso = str(row.get("trade_date") or "")
        if not iso:
            continue
        out.setdefault(iso, []).append(row)
    return out


def _day_mistake_summary(rows: List[Dict[str, Any]]) -> str:
    counts: Dict[str, int] = {}
    for row in rows:
        tags = str(row.get("rule_break_tags") or "")
        for tag in [part.strip().lower() for part in tags.split(",") if part.strip()]:
            counts[tag] = int(counts.get(tag, 0)) + 1
    if not counts:
        return ""
    return max(counts.items(), key=lambda kv: kv[1])[0][0:36]


def _day_state(
    day_row: Dict[str, Any],
    journal_row: Dict[str, Any],
    goal_row: Dict[str, Any],
    analytics_rows: List[Dict[str, Any]],
) -> str:
    has_trades = bool(day_row.get("has_trades"))
    net = float(day_row.get("net") or 0.0)
    has_journal = bool(journal_row)
    has_projects = bool(goal_row)
    mistake = _day_mistake_summary(analytics_rows)
    if has_trades and net > 0 and has_journal and not mistake:
        return "clean_win"
    if has_trades and net > 0:
        return "sloppy_win" if mistake else "green_day"
    if has_trades and net < 0:
        return "impulsive_loss" if mistake else "controlled_loss"
    if has_trades and net == 0:
        return "flat_session"
    if has_journal and has_projects:
        return "review_build"
    if has_journal:
        return "debrief_day"
    if has_projects:
        return "project_day"
    return "quiet_day"


def _day_state_label(value: str) -> str:
    labels = {
        "clean_win": "Clean win",
        "sloppy_win": "Review win",
        "green_day": "Green day",
        "controlled_loss": "Controlled loss",
        "impulsive_loss": "Impulsive loss",
        "flat_session": "Flat session",
        "review_build": "Review + build",
        "debrief_day": "Debrief",
        "project_day": "Project",
        "quiet_day": "",
    }
    return labels.get(value, "Day state")


def _trading_day_index_map(year: int) -> Dict[date, int]:
    idx = 0
    out: Dict[date, int] = {}
    cursor = date(year, 1, 1)
    end = date(year, 12, 31)
    while cursor <= end:
        if _is_market_session(cursor):
            idx += 1
            out[cursor] = idx
        cursor += timedelta(days=1)
    return out


def _trading_week_index_map(year: int) -> Tuple[Dict[date, int], set[date]]:
    idx = 0
    out: Dict[date, int] = {}
    week_open_dates: set[date] = set()
    current_week_key = None
    cursor = date(year, 1, 1)
    end = date(year, 12, 31)
    while cursor <= end:
        if _is_market_session(cursor):
            week_key = cursor - timedelta(days=cursor.weekday())
            if week_key != current_week_key:
                current_week_key = week_key
                idx += 1
                week_open_dates.add(cursor)
            out[cursor] = idx
        cursor += timedelta(days=1)
    return out, week_open_dates


def _trading_month_index_map(year: int) -> Tuple[Dict[date, int], set[date]]:
    idx = 0
    out: Dict[date, int] = {}
    month_open_dates: set[date] = set()
    for month in range(1, 13):
        cursor = date(year, month, 1)
        end = date(year, month, monthrange(year, month)[1])
        while cursor <= end:
            if _is_market_session(cursor):
                idx += 1
                out[cursor] = idx
                month_open_dates.add(cursor)
                break
            cursor += timedelta(days=1)
    return out, month_open_dates


def _is_market_session(day: date) -> bool:
    return day.weekday() < 5 and not _market_holiday_name(day)


def _market_holiday_name(day: date) -> str:
    return _market_holidays(day.year).get(day, "")


def _market_holidays(year: int) -> Dict[date, str]:
    easter = _easter_sunday(year)
    holidays = {
        _observed_fixed_holiday(year, 1, 1): "New Years Day",
        _nth_weekday_of_month(year, 1, 0, 3): "Martin Luther King Jr. Day",
        _nth_weekday_of_month(year, 2, 0, 3): "Presidents Day",
        easter - timedelta(days=2): "Good Friday",
        _last_weekday_of_month(year, 5, 0): "Memorial Day",
        _observed_fixed_holiday(year, 6, 19): "Juneteenth",
        _observed_fixed_holiday(year, 7, 4): "Independence Day",
        _nth_weekday_of_month(year, 9, 0, 1): "Labor Day",
        _nth_weekday_of_month(year, 11, 3, 4): "Thanksgiving",
        _observed_fixed_holiday(year, 12, 25): "Christmas Day",
    }
    return holidays


def _observed_fixed_holiday(year: int, month: int, day_num: int) -> date:
    holiday = date(year, month, day_num)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    return first + timedelta(days=delta + ((n - 1) * 7))


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    if month == 12:
        cursor = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        cursor = date(year, month + 1, 1) - timedelta(days=1)
    while cursor.weekday() != weekday:
        cursor -= timedelta(days=1)
    return cursor


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    weekday_offset = (32 + (2 * e) + (2 * i) - h - k) % 7
    m = (a + (11 * h) + (22 * weekday_offset)) // 451
    month = (h + weekday_offset - (7 * m) + 114) // 31
    day_num = ((h + weekday_offset - (7 * m) + 114) % 31) + 1
    return date(year, month, day_num)
