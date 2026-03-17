"""Core domain service gateway.

Core routes still rely on legacy implementations in ``app_core``. This module
keeps that dependency localized behind explicit delegator functions.
"""

from __future__ import annotations

from calendar import Calendar, monthrange
from datetime import date
from datetime import datetime
from datetime import timedelta
from email.utils import parsedate_to_datetime
import json
import os
import shutil
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
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
    get_trading_window_state,
    get_forex_factory_feed,
    get_forex_factory_month_feed,
    get_forex_factory_next_week_feed,
    get_system_status,
    render_page,
    save_trading_window_settings,
    simple_msg,
)
from mccain_capital.services.viewmodels import (
    balance_state_badges,
    dashboard_data_trust,
    sync_state_badges,
)
from mccain_capital.services.market_pulse_health import build_market_source_health
from mccain_capital.services.gamma_context_service import build_spx_priority_context

MULTIPLIER = 100
DEFAULT_STOP_PCT = 20.0
DEFAULT_TARGET_PCT = 30.0
DEFAULT_FEE_PER_CONTRACT = 0.70
DAY_OPEN_INTERVALS = tuple(range(2, 13))
WEEK_OPEN_INTERVALS = (2, 3, 4, 5, 6)
MONTH_OPEN_INTERVALS = (2,)
MARKET_PULSE_CACHE_TTL_SECONDS = 300
MARKET_PULSE_UNSAFE_CRITICAL_THRESHOLD = 2
MARKET_NEWS_CACHE_TTL_SECONDS = 900
MARKET_NEWS_RSS_TIMEOUT_SECONDS = 1.25
MARKET_NEWS_RSS_SYMBOL_LIMIT = 5
MARKET_NEWS_FRESH_SECONDS = 12 * 60 * 60
MARKET_NEWS_MAX_AGE_SECONDS = 36 * 60 * 60
WATCHLIST_NEWS_MAX_AGE_SECONDS = 48 * 60 * 60
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
    {
        "symbol": "CSCO",
        "label": "CSCO",
        "group": "leaders",
        "focus": "Enterprise networking and infrastructure signal.",
    },
    {
        "symbol": "INTC",
        "label": "INTC",
        "group": "leaders",
        "focus": "Semiconductor cycle and broad tech demand proxy.",
    },
)
MARKET_PULSE_WATCHLIST_NEWS_SYMBOLS: Tuple[str, ...] = tuple(
    dict.fromkeys(
        ["SPX", "VIX"]
        + [
            str(spec.get("label") or "").strip().upper()
            for spec in MARKET_PULSE_SYMBOLS
            if str(spec.get("label") or "").strip().upper() not in {"", "SPX", "VIX"}
        ]
    )
)
YAHOO_RSS_SYMBOL_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline"
_market_pulse_cache: Dict[str, Any] = {"fetched_at": None, "payload": None}
_market_news_cache: Dict[str, Any] = {"fetched_at": None, "payload": None}
USD_CALENDAR_FALLBACK_EVENTS: Tuple[Tuple[str, str, str], ...] = (
    ("2026-03-11T08:30:00-04:00", "High", "Core CPI m/m"),
    ("2026-03-11T08:30:00-04:00", "High", "CPI m/m"),
    ("2026-03-11T08:30:00-04:00", "High", "CPI y/y"),
    ("2026-03-12T08:30:00-04:00", "High", "Unemployment Claims"),
    ("2026-03-13T08:30:00-04:00", "High", "Core PCE Price Index m/m"),
    ("2026-03-13T08:30:00-04:00", "High", "GDP (Second Estimate) q/q"),
    ("2026-03-13T10:00:00-04:00", "High", "JOLTS Job Openings"),
    ("2026-03-13T10:00:00-04:00", "Medium", "Prelim UoM Consumer Sentiment"),
    ("2026-03-13T10:00:00-04:00", "Medium", "Prelim UoM Inflation Expectations"),
    ("2026-03-16T09:15:00-04:00", "Medium", "Industrial Production m/m"),
    ("2026-03-18T08:30:00-04:00", "High", "PPI m/m"),
    ("2026-03-18T08:30:00-04:00", "High", "Core PPI m/m"),
    ("2026-03-18T14:00:00-04:00", "High", "FOMC Rate Decision"),
    ("2026-03-18T14:30:00-04:00", "High", "FOMC Press Conference"),
    ("2026-03-19T08:30:00-04:00", "High", "Unemployment Claims"),
    ("2026-03-24T08:30:00-04:00", "Medium", "Productivity and Costs q/q"),
    ("2026-03-25T08:30:00-04:00", "Medium", "Import Price Index m/m"),
    ("2026-03-25T08:30:00-04:00", "Medium", "Export Price Index m/m"),
    ("2026-03-26T08:30:00-04:00", "High", "Unemployment Claims"),
    ("2026-03-31T10:00:00-04:00", "High", "JOLTS Job Openings"),
)


def _legacy():
    from mccain_capital import app_core

    return app_core


def _market_pulse_cache_file() -> str:
    return app_runtime.upload_path(".market_pulse_cache.json")


def _market_news_cache_file() -> str:
    return app_runtime.upload_path(".market_news_cache.json")


def _load_market_pulse_disk_cache() -> Dict[str, Any] | None:
    try:
        with open(_market_pulse_cache_file(), "r", encoding="utf-8") as f:
            parsed = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _save_market_pulse_disk_cache(payload: Dict[str, Any]) -> None:
    try:
        os.makedirs(app_runtime.upload_root(), exist_ok=True)
        with open(_market_pulse_cache_file(), "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except OSError:
        return


def _load_market_news_disk_cache() -> Dict[str, Any] | None:
    try:
        with open(_market_news_cache_file(), "r", encoding="utf-8") as f:
            parsed = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _save_market_news_disk_cache(payload: Dict[str, Any]) -> None:
    try:
        os.makedirs(app_runtime.upload_root(), exist_ok=True)
        with open(_market_news_cache_file(), "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except OSError:
        return


def _market_pulse_yahoo_href(symbol: str) -> str:
    return "https://finance.yahoo.com/quote/" + urllib.parse.quote(symbol, safe="")


def _format_iso_et_label(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=app_runtime.TZ)
        return dt.astimezone(app_runtime.TZ).strftime("%b %d, %Y %I:%M:%S %p ET")
    except Exception:
        return text


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
    row["data_reason"] = str(row.get("data_reason") or "cached_snapshot")
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
    from mccain_capital.services import market_data_service

    started = time.perf_counter()
    now_et = app_runtime.now_et()
    fetched_label = now_et.strftime("%b %d, %Y %I:%M:%S %p ET")
    fetched_at = _market_pulse_cache.get("fetched_at")
    cached_payload = _market_pulse_cache.get("payload")
    if (
        (not force_refresh)
        and isinstance(fetched_at, datetime)
        and isinstance(cached_payload, dict)
        and (now_et - fetched_at).total_seconds() < MARKET_PULSE_CACHE_TTL_SECONDS
    ):
        normalized_cache = _market_pulse_force_symbol_set(cached_payload)
        normalized_cache["source_label"] = "Massive market feed (cached snapshot)"
        normalized_cache["source_note"] = "Using recent cached Massive snapshot within refresh TTL."
        return normalized_cache

    symbols = [str(spec.get("symbol") or "").strip().upper() for spec in MARKET_PULSE_SYMBOLS]
    quotes_by_symbol = market_data_service.get_watchlist(symbols, allow_yf_fallback=False)
    if not quotes_by_symbol:
        disk_payload = _load_market_pulse_disk_cache()
        if isinstance(cached_payload, dict):
            fallback = _market_pulse_force_symbol_set(cached_payload)
            fallback["source_label"] = "Massive market feed (cached fallback)"
            fallback["source_note"] = (
                "Massive live quote request returned no data. Showing last cached snapshot."
            )
            return fallback
        if isinstance(disk_payload, dict):
            _market_pulse_cache["payload"] = disk_payload
            fallback = _market_pulse_force_symbol_set(disk_payload)
            fallback["source_label"] = "Massive market feed (cached fallback)"
            fallback["source_note"] = (
                "Massive live quote request returned no data. Showing last cached snapshot."
            )
            return fallback
        return {
            "available": False,
            "fetched_at": "",
            "source_label": "Massive market feed",
            "source_note": "Massive feed unavailable or missing entitlement for requested symbols.",
            "quotes": [],
            "integrity": {
                "latency_ms": int((time.perf_counter() - started) * 1000),
                "forced_refresh": bool(force_refresh),
                "cached_only": False,
                "live_count": 0,
                "delayed_count": 0,
                "cached_count": 0,
                "missing_count": len(MARKET_PULSE_SYMBOLS),
                "tracked_count": len(MARKET_PULSE_SYMBOLS),
            },
        }

    quotes: List[Dict[str, Any]] = []
    counts = {"live": 0, "delayed": 0, "cached": 0, "missing": 0}
    fallback_count = 0
    for spec in MARKET_PULSE_SYMBOLS:
        symbol = str(spec.get("symbol") or "").strip().upper()
        quote = dict(quotes_by_symbol.get(symbol) or {})
        provider = str(quote.get("provider") or "").strip().lower()
        reason = str(quote.get("reason") or "").strip()
        price = quote.get("price")
        pct = quote.get("pct_change")
        as_of = str(quote.get("as_of") or "")
        asof_epoch = 0
        if as_of:
            try:
                asof_epoch = int(datetime.fromisoformat(as_of).timestamp())
            except Exception:
                asof_epoch = 0
        price_num = float(price) if isinstance(price, (int, float)) else None
        pct_num = float(pct) if isinstance(pct, (int, float)) else 0.0
        prev_close = (
            price_num / (1.0 + (pct_num / 100.0))
            if (price_num is not None and abs(100.0 + pct_num) > 1e-9)
            else None
        )
        change = (
            (price_num - prev_close) if (price_num is not None and prev_close is not None) else 0.0
        )
        mini_series: List[float] = []
        if price_num is not None:
            if prev_close is not None and isinstance(prev_close, (int, float)):
                mini_series = [float(prev_close), float(price_num)]
            else:
                mini_series = [float(price_num), float(price_num)]
        if price_num is None:
            state = "missing"
        elif provider == "tradier":
            state = "live"
        else:
            state = "delayed"
            fallback_count += 1
        counts[state] += 1
        quotes.append(
            {
                "symbol": symbol,
                "label": spec["label"],
                "group": spec["group"],
                "focus": spec["focus"],
                "name": spec["label"],
                "provider": provider,
                "reason": reason,
                "price": price_num,
                "change": change,
                "change_pct": pct_num,
                "volume": 0,
                "avg_volume": 0,
                "market_state": (
                    "Live"
                    if state == "live"
                    else "Provider fallback" if state == "delayed" else "Unavailable"
                ),
                "day_range": "—",
                "day_open": None,
                "day_high": None,
                "day_low": None,
                "vwap": None,
                "prior_day_high": None,  # TODO(api): wire prior-day high from upstream provider payload.
                "prior_day_low": None,  # TODO(api): wire prior-day low from upstream provider payload.
                "overnight_high": None,  # TODO(api): wire overnight high from upstream provider payload.
                "overnight_low": None,  # TODO(api): wire overnight low from upstream provider payload.
                "yahoo_href": _market_pulse_yahoo_href(spec["symbol"]),
                "as_of": as_of or fetched_label,
                "asof": as_of or fetched_label,
                "asof_epoch": asof_epoch,
                "data_state": state,
                "data_status_label": (
                    "Live" if state == "live" else "Delayed" if state == "delayed" else "Missing"
                ),
                "data_reason": reason,
                "mini_series": mini_series,
                "series": [],
            }
        )

    def _rows_session_day(rows: List[Dict[str, Any]]) -> Optional[date]:
        for row in reversed(rows):
            if not isinstance(row, dict):
                continue
            ts_raw = str(row.get("ts") or "").strip()
            if not ts_raw:
                continue
            try:
                parsed = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=app_runtime.TZ)
                return parsed.astimezone(app_runtime.TZ).date()
            except Exception:
                continue
        return None

    # Build richer refresh-time curves so cards feel like Yahoo-style micro charts.
    for q in quotes:
        symbol = str(q.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        try:
            rows = market_data_service.get_intraday(symbol)
        except Exception:
            rows = []
        try:
            prior_rows = market_data_service.get_prior_session_intraday(
                symbol,
                anchor_session_day=_rows_session_day(rows),
            )
        except Exception:
            prior_rows = []
        if prior_rows:
            prior_highs = [
                float(r.get("high"))
                for r in prior_rows
                if isinstance(r, dict) and r.get("high") is not None
            ]
            prior_lows = [
                float(r.get("low"))
                for r in prior_rows
                if isinstance(r, dict) and r.get("low") is not None
            ]
            if prior_highs and prior_lows:
                q["prior_day_high"] = max(prior_highs)
                q["prior_day_low"] = min(prior_lows)
        if not rows:
            continue
        points = [
            {"ts": str(r.get("ts") or ""), "v": float(r.get("close"))}
            for r in rows[-240:]
            if isinstance(r, dict) and r.get("close") is not None
        ]
        curve = [float(p["v"]) for p in points]
        if len(curve) >= 8:
            q["mini_series"] = curve
            q["series"] = points
            first_open = None
            for r in rows:
                if isinstance(r, dict) and isinstance(r.get("open"), (int, float)):
                    first_open = float(r.get("open"))
                    break
            if first_open is not None:
                q["day_open"] = first_open
            highs = [
                float(r.get("high"))
                for r in rows
                if isinstance(r, dict) and r.get("high") is not None
            ]
            lows = [
                float(r.get("low"))
                for r in rows
                if isinstance(r, dict) and r.get("low") is not None
            ]
            if highs and lows:
                day_low = min(lows)
                day_high = max(highs)
                q["day_low"] = day_low
                q["day_high"] = day_high
                q["day_range"] = f"{day_low:,.2f} to {day_high:,.2f}"
            vwap_num = 0.0
            vwap_den = 0.0
            for r in rows:
                if not isinstance(r, dict):
                    continue
                close_v = r.get("close")
                vol_v = r.get("volume")
                if (
                    isinstance(close_v, (int, float))
                    and isinstance(vol_v, (int, float))
                    and float(vol_v) > 0
                ):
                    vwap_num += float(close_v) * float(vol_v)
                    vwap_den += float(vol_v)
            if vwap_den > 0:
                q["vwap"] = vwap_num / vwap_den
    if counts["live"] == 0:
        disk_payload = _load_market_pulse_disk_cache()
        if isinstance(cached_payload, dict):
            fallback = _market_pulse_force_symbol_set(cached_payload)
            fallback["source_label"] = "Massive market feed (cached fallback)"
            fallback["source_note"] = (
                "Massive returned symbols but no usable live prices. Showing last cached snapshot."
            )
            return fallback
        if isinstance(disk_payload, dict):
            _market_pulse_cache["payload"] = disk_payload
            fallback = _market_pulse_force_symbol_set(disk_payload)
            fallback["source_label"] = "Massive market feed (cached fallback)"
            fallback["source_note"] = (
                "Massive returned symbols but no usable live prices. Showing last cached snapshot."
            )
            return fallback
    latency_ms = int((time.perf_counter() - started) * 1000)
    tradier_live = sum(
        1
        for spec in MARKET_PULSE_SYMBOLS
        if str(
            (quotes_by_symbol.get(str(spec.get("symbol") or "").strip().upper()) or {}).get(
                "provider"
            )
            or ""
        )
        .strip()
        .lower()
        == "tradier"
    )
    source_note = "Live quote data is being served by Tradier."
    source_label = "Tradier market feed"
    if tradier_live == 0:
        source_note = "Live quote data is being served by Massive."
        source_label = "Massive market feed"
    if fallback_count:
        source_label = "Mixed provider feed"
        source_note = f"{fallback_count} symbol(s) are on non-Tradier fallback quotes."
    result = {
        "available": any(q.get("price") is not None for q in quotes),
        "fetched_at": fetched_label,
        "source_label": source_label,
        "source_note": source_note,
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
    vix_row = dict(by_label.get("VIX") or {})
    vix_val = float(vix_row.get("price") or 0.0)
    vix_pct = float(vix_row.get("change_pct") or 0.0)
    spy_pct = float((by_label.get("SPY") or {}).get("change_pct") or 0.0)
    qqq_pct = float((by_label.get("QQQ") or {}).get("change_pct") or 0.0)
    iwm_pct = float((by_label.get("IWM") or {}).get("change_pct") or 0.0)
    spx_pct = float((by_label.get("SPX") or {}).get("change_pct") or 0.0)
    if vix_pct > 0.15:
        vix_direction = "rising"
    elif vix_pct < -0.15:
        vix_direction = "falling"
    else:
        vix_direction = "flat"

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
        "vix_direction": vix_direction,
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


def _gamma_data_quality(
    gamma_snapshot: Dict[str, Any], quotes: List[Dict[str, Any]], now_et: datetime
) -> Dict[str, str]:
    by_label = {str(q.get("label") or ""): q for q in quotes if isinstance(q, dict)}
    spx = dict(by_label.get("SPX") or {})
    spx_state = str(spx.get("data_state") or "").lower()
    spx_reason = str(spx.get("data_reason") or "").lower()
    spot_live = spx_state == "live" and spx_reason.startswith("tradier")
    diagnostics = dict(gamma_snapshot.get("diagnostics") or {})
    gamma_status = str(diagnostics.get("status") or "waiting").lower()

    asof_raw = str(gamma_snapshot.get("asof") or "").strip()
    oi_age = "unknown"
    age_s = None
    if asof_raw:
        try:
            asof_dt = datetime.fromisoformat(asof_raw.replace("Z", "+00:00")).astimezone(
                app_runtime.TZ
            )
            age_s = max(0, int((now_et - asof_dt).total_seconds()))
            if age_s < 60:
                oi_age = f"{age_s}s"
            elif age_s < 3600:
                oi_age = f"{age_s // 60}m"
            else:
                oi_age = f"{age_s // 3600}h"
        except Exception:
            oi_age = "unknown"

    contracts_used = int(((gamma_snapshot.get("diagnostics") or {}).get("contracts_used")) or 0)
    warning = ""
    if gamma_status == "error":
        tone = "critical"
        warning = "Gamma refresh error"
    elif age_s is None:
        tone = "critical"
        warning = "Gamma timestamp missing"
    elif age_s > 900:
        tone = "critical"
        warning = "Gamma stale >15m"
    elif age_s > 300:
        tone = "warn"
        warning = "Gamma stale >5m"
    elif not spot_live:
        tone = "warn"
        warning = "Spot not live"
    elif contracts_used < 50:
        tone = "warn"
        warning = "Thin contract coverage"
    else:
        tone = "ok"

    return {
        "tone": tone,
        "summary": (
            f"{'Live spot' if spot_live else 'Non-live spot'} · "
            f"OI age {oi_age} · {contracts_used} contracts"
        ),
        "warning": warning,
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
    area_points = "0.00,28.00 " + " ".join(points) + " 120.00,28.00"
    cls = "up" if tone == "up" else "down" if tone == "down" else "flat"
    return (
        '<svg viewBox="0 0 120 28" class="marketMiniSpark" aria-hidden="true">'
        f'<polygon class="marketMiniSparkArea {cls}" points="{area_points}" />'
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
        elif state == "delayed":
            band = "warn"
            fresh_label = "Delayed fallback"
        elif state == "cached":
            band = "critical"
            fresh_label = "Cached snapshot"
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
        q["freshness_reason"] = str(q.get("data_reason") or "")
        source_badge = _quote_source_badge(q)
        q["source_badge_label"] = source_badge["label"]
        q["source_badge_tone"] = source_badge["tone"]

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


def _quote_source_badge(quote: Dict[str, Any]) -> Dict[str, str]:
    provider = str((quote or {}).get("provider") or "").strip().lower()
    reason = (
        str((quote or {}).get("reason") or (quote or {}).get("data_reason") or "").strip().lower()
    )
    if provider == "tradier" and reason.startswith("tradier_stream_"):
        return {"label": "Tradier Stream", "tone": "positive"}
    if provider == "tradier" and reason.startswith("tradier_live"):
        return {"label": "Tradier Live Quote", "tone": "positive"}
    if provider == "tradier" and reason.startswith("tradier_close"):
        return {"label": "Tradier Close", "tone": "warm"}
    if provider == "tradier" and reason.startswith("tradier_"):
        return {"label": "Tradier Feed", "tone": "positive"}
    if provider == "yfinance":
        return {"label": "Yahoo Fallback", "tone": "warm"}
    if provider == "massive":
        return {"label": "Fallback Snapshot", "tone": "warm"}
    if provider:
        return {"label": f"{provider.title()} Fallback", "tone": "warm"}
    return {"label": "Feed unavailable", "tone": "neutral"}


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


def _market_news_age_seconds(stamp: Any, now_et: datetime) -> int | None:
    if not isinstance(stamp, (int, float)):
        return None
    try:
        return max(0, int(now_et.timestamp()) - int(stamp))
    except Exception:
        return None


def _market_news_age_label(stamp: Any, now_et: datetime) -> str:
    age_s = _market_news_age_seconds(stamp, now_et)
    if age_s is None:
        return ""
    if age_s < 3600:
        mins = max(1, age_s // 60)
        return f"{mins}m ago"
    if age_s < 12 * 3600:
        return f"{age_s // 3600}h ago"

    published = datetime.fromtimestamp(int(stamp), tz=app_runtime.TZ)
    if published.date() == now_et.date():
        return published.strftime("Today %-I:%M %p ET")
    if published.date() == (now_et.date() - timedelta(days=1)):
        return published.strftime("Yesterday %-I:%M %p ET")
    return published.strftime("%b %-d, %-I:%M %p ET")


def _market_news_is_recent(stamp: Any, now_et: datetime, max_age_seconds: int) -> bool:
    age_s = _market_news_age_seconds(stamp, now_et)
    return age_s is not None and age_s <= int(max_age_seconds)


def _market_news_row_priority(row: Dict[str, Any], now_et: datetime) -> tuple[int, int, int]:
    score = _market_news_score(row)
    age_s = _market_news_age_seconds(row.get("datetime"), now_et)
    freshness_bucket = 0 if age_s is not None and age_s <= MARKET_NEWS_FRESH_SECONDS else 1
    recency_key = -(int(row.get("datetime") or 0))
    return (freshness_bucket, -score, recency_key)


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
    row: Dict[str, Any], *, now_et: datetime, symbol: str = "", forced_tag: str = ""
) -> Dict[str, Any]:
    headline = str(row.get("headline") or "").strip()
    summary = str(row.get("summary") or "").strip()
    source = str(row.get("source") or "Source").strip() or "Source"
    url = str(row.get("url") or "").strip()
    tag, why = _market_news_theme(f"{headline} {summary} {row.get('related') or ''}")
    age_s = _market_news_age_seconds(row.get("datetime"), now_et)
    stale = bool(age_s is not None and age_s > MARKET_NEWS_FRESH_SECONDS)
    return {
        "headline": headline or "Market headline",
        "summary": summary or why,
        "source": source,
        "url": url,
        "published_label": _market_news_age_label(row.get("datetime"), now_et),
        "absolute_label": _market_news_timestamp_label(row.get("datetime")),
        "tag": forced_tag or tag,
        "why": why,
        "symbol": symbol,
        "stale": stale,
    }


def _market_news_rss_rows(symbol: str, *, limit: int = 8) -> List[Dict[str, Any]]:
    params = {
        "s": symbol,
        "region": "US",
        "lang": "en-US",
    }
    url = YAHOO_RSS_SYMBOL_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/rss+xml,application/xml,text/xml,*/*",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=MARKET_NEWS_RSS_TIMEOUT_SECONDS) as resp:
            xml_body = resp.read()
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
        return []

    try:
        root = ET.fromstring(xml_body)
    except ET.ParseError:
        return []

    rows: List[Dict[str, Any]] = []
    for item in root.findall(".//item")[: max(1, int(limit))]:
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        summary = (item.findtext("description") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        stamp = 0
        if pub:
            try:
                stamp = int(parsedate_to_datetime(pub).timestamp())
            except Exception:
                stamp = 0
        if not title:
            continue
        rows.append(
            {
                "headline": title,
                "summary": summary,
                "source": "Yahoo Finance RSS",
                "url": link,
                "datetime": stamp,
                "related": symbol,
            }
        )
    return rows


def _market_news_snapshot() -> Dict[str, Any]:
    now_et = app_runtime.now_et()
    fetched_at = _market_news_cache.get("fetched_at")
    cached_payload = _market_news_cache.get("payload")
    disk_payload = _load_market_news_disk_cache()
    if (
        isinstance(fetched_at, datetime)
        and isinstance(cached_payload, dict)
        and (now_et - fetched_at).total_seconds() < MARKET_NEWS_CACHE_TTL_SECONDS
    ):
        return cached_payload

    # In environments without Finnhub, prefer cached market news over blocking on
    # multiple RSS network calls every page load.
    if (
        not FINNHUB_API_KEY
        and isinstance(disk_payload, dict)
        and bool(
            (disk_payload.get("market_items") or [])
            or (disk_payload.get("watchlist_items") or [])
            or (disk_payload.get("macro_events") or [])
        )
    ):
        cached = dict(disk_payload)
        cached["source_note"] = str(
            cached.get("source_note") or "Using cached news/macro snapshot (fast path)."
        )
        _market_news_cache["fetched_at"] = now_et
        _market_news_cache["payload"] = cached
        return cached

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

    market_items: List[Dict[str, Any]] = []
    watchlist_items: List[Dict[str, Any]] = []
    source_note = "Fresh drivers from Yahoo Finance RSS plus Forex Factory macro triggers."
    if FINNHUB_API_KEY:
        general_payload = _market_pulse_json_request_any(
            FINNHUB_BASE_URL + "/news",
            {"category": "general", "token": FINNHUB_API_KEY},
            timeout=8,
        )
        market_rows = general_payload if isinstance(general_payload, list) else []
        relevant_general = [
            row
            for row in market_rows
            if isinstance(row, dict)
            and _market_news_score(row) >= 4
            and _market_news_is_recent(row.get("datetime"), now_et, MARKET_NEWS_MAX_AGE_SECONDS)
        ]
        relevant_general.sort(key=lambda row: _market_news_row_priority(row, now_et))
        market_items = [_market_news_item(row, now_et=now_et) for row in relevant_general[:8]]

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
            rows = [
                row
                for row in payload
                if isinstance(row, dict)
                and str(row.get("headline") or "").strip()
                and _market_news_is_recent(
                    row.get("datetime"), now_et, WATCHLIST_NEWS_MAX_AGE_SECONDS
                )
            ]
            rows.sort(key=lambda row: _market_news_row_priority(row, now_et))
            best = rows[0] if rows else None
            if best is None:
                continue
            watchlist_items.append(
                _market_news_item(best, now_et=now_et, symbol=symbol, forced_tag=symbol)
            )
        source_note = "Fresh Finnhub drivers plus Forex Factory macro triggers."
    else:
        all_rows: List[Dict[str, Any]] = []
        for symbol in MARKET_PULSE_WATCHLIST_NEWS_SYMBOLS[:MARKET_NEWS_RSS_SYMBOL_LIMIT]:
            rows = _market_news_rss_rows(symbol, limit=4)
            if rows:
                fresh_rows = [
                    row
                    for row in rows
                    if _market_news_is_recent(
                        row.get("datetime"), now_et, WATCHLIST_NEWS_MAX_AGE_SECONDS
                    )
                ]
                all_rows.extend(fresh_rows)
                if fresh_rows:
                    fresh_rows.sort(key=lambda row: _market_news_row_priority(row, now_et))
                    watchlist_items.append(
                        _market_news_item(
                            fresh_rows[0], now_et=now_et, symbol=symbol, forced_tag=symbol
                        )
                    )
        all_rows = [row for row in all_rows if isinstance(row, dict)]
        all_rows.sort(key=lambda row: _market_news_row_priority(row, now_et))
        market_items = [_market_news_item(row, now_et=now_et) for row in all_rows[:8]]

    result = {
        "available": bool(market_items or watchlist_items or macro_events),
        "source_note": source_note,
        "macro_events": macro_events,
        "market_items": market_items,
        "watchlist_items": watchlist_items,
    }
    if isinstance(disk_payload, dict):
        cached_macro = list(disk_payload.get("macro_events") or [])
        cached_market = list(disk_payload.get("market_items") or [])
        cached_watch = list(disk_payload.get("watchlist_items") or [])
        merged = False
        if not result["macro_events"] and cached_macro:
            result["macro_events"] = cached_macro[:6]
            merged = True
        if not result["market_items"] and cached_market:
            result["market_items"] = cached_market[:8]
            merged = True
        if not result["watchlist_items"] and cached_watch:
            result["watchlist_items"] = cached_watch
            merged = True
        if merged:
            result["available"] = True
            result["source_note"] = (
                "Live + cached merge (restored missing fresh sections where possible)."
            )
    if (
        (not result["available"])
        and isinstance(disk_payload, dict)
        and bool(
            (disk_payload.get("market_items") or [])
            or (disk_payload.get("watchlist_items") or [])
            or (disk_payload.get("macro_events") or [])
        )
    ):
        fallback = dict(disk_payload)
        fallback["source_note"] = (
            "Using cached news/macro snapshot (live fetch unavailable). Headline freshness may be degraded."
        )
        _market_news_cache["fetched_at"] = now_et
        _market_news_cache["payload"] = fallback
        return fallback
    _market_news_cache["fetched_at"] = now_et
    _market_news_cache["payload"] = result
    if result["available"]:
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


def _dashboard_brief_setting_key(day: str) -> str:
    return f"dashboard_daily_brief::{str(day or '').strip()}"


def _load_dashboard_brief_settings(day: str) -> Dict[str, str]:
    raw = str(app_runtime.get_setting_value(_dashboard_brief_setting_key(day), "") or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _save_dashboard_brief_settings(day: str, payload: Dict[str, Any]) -> None:
    clean = {
        "focus": str(payload.get("focus") or "").strip()[:280],
        "plan_a": str(payload.get("plan_a") or "").strip()[:280],
        "plan_b": str(payload.get("plan_b") or "").strip()[:280],
        "no_trade": str(payload.get("no_trade") or "").strip()[:280],
    }
    app_runtime.set_setting_value(_dashboard_brief_setting_key(day), json.dumps(clean))


def _dashboard_daily_brief_viewmodel(
    *,
    now_et: datetime,
    dashboard_spx: Dict[str, Any],
    dashboard_vix: Dict[str, Any],
    gamma_snapshot: Dict[str, Any],
    news_snapshot: Dict[str, Any],
    today_count: int,
    today_net: float,
) -> Dict[str, Any]:
    day_key = now_et.date().isoformat()
    saved = _load_dashboard_brief_settings(day_key)
    is_tuned = any(
        str(saved.get(key) or "").strip() for key in ("focus", "plan_a", "plan_b", "no_trade")
    )

    def _num(value: Any) -> Optional[float]:
        try:
            if value is None or str(value).strip() == "":
                return None
            return float(value)
        except Exception:
            return None

    spot = _num(dashboard_spx.get("price"))
    change_pct = _num(dashboard_spx.get("pct_change"))
    vix = _num(dashboard_vix.get("price"))
    gamma_flip = _num(gamma_snapshot.get("gamma_flip"))
    call_wall = _num(gamma_snapshot.get("call_wall"))
    put_wall = _num(gamma_snapshot.get("put_wall"))
    day_open = _num(dashboard_spx.get("day_open"))

    if (
        spot is not None
        and gamma_flip is not None
        and spot > gamma_flip
        and (change_pct or 0.0) >= 0
    ):
        bias_label = "Bullish above flip"
        bias_tone = "positive"
        bias_summary = f"SPX is trading above gamma flip {gamma_flip:.0f}, so continuation longs have cleaner structure than reactive fades."
    elif (
        spot is not None
        and gamma_flip is not None
        and spot < gamma_flip
        and (change_pct or 0.0) <= 0
    ):
        bias_label = "Defensive below flip"
        bias_tone = "negative"
        bias_summary = f"SPX is below gamma flip {gamma_flip:.0f}, so failed bounces and risk-off structure deserve more respect than impulsive longs."
    else:
        bias_label = "Two-way / responsive"
        bias_tone = ""
        bias_summary = "Price is inside a mixed zone. Stay selective, shorten hold times, and avoid forcing trend conviction before levels confirm."

    if vix is not None and vix >= 22:
        volatility_label = "Elevated vol"
    elif vix is not None and vix >= 18:
        volatility_label = "Active vol"
    elif vix is not None:
        volatility_label = "Contained vol"
    else:
        volatility_label = "Vol unknown"

    macro_events = [
        {
            "headline": str(row.get("headline") or "Macro event"),
            "published_label": str(row.get("published_label") or ""),
            "summary": str(row.get("summary") or ""),
        }
        for row in list(news_snapshot.get("macro_events") or [])[:3]
        if isinstance(row, dict)
    ]
    if not macro_events:
        macro_events = [
            {
                "headline": "No major USD macro trigger loaded",
                "published_label": "Calendar fallback",
                "summary": "Stand down if a surprise catalyst lands while the tape is thin.",
            }
        ]

    key_levels = []
    for label, value, tone in (
        ("Put Wall", put_wall, "positive"),
        ("Gamma Flip", gamma_flip, ""),
        ("Call Wall", call_wall, "negative"),
        ("Day Open", day_open, ""),
    ):
        if value is None:
            continue
        key_levels.append({"label": label, "value": f"{value:.0f}", "tone": tone})
    if not key_levels and spot is not None:
        key_levels.append({"label": "Spot", "value": f"{spot:.2f}", "tone": ""})

    default_focus = f"{bias_label}. Respect {volatility_label.lower()} and trade only when SPX confirms around your levels."
    if gamma_flip is not None:
        plan_a = f"Primary setup: continuation only if price accepts {'above' if bias_tone == 'positive' else 'below' if bias_tone == 'negative' else 'through'} {gamma_flip:.0f} with risk defined before entry."
    else:
        plan_a = "Primary setup: only take the cleanest continuation entry with risk defined before entry."
    if call_wall is not None and put_wall is not None:
        plan_b = f"Secondary setup: responsive fade only at edges near {put_wall:.0f} support or {call_wall:.0f} resistance after rejection is obvious."
    else:
        plan_b = (
            "Secondary setup: responsive fade only at obvious extremes after rejection is obvious."
        )
    if macro_events:
        no_trade = f"No trade during or immediately into {macro_events[0]['headline']} unless structure is already resolved and risk is smaller than usual."
    else:
        no_trade = "No trade if the tape goes stale, structure is mixed, or your invalidation is not obvious before entry."

    focus = str(saved.get("focus") or default_focus).strip() or default_focus
    plan_a_value = str(saved.get("plan_a") or plan_a).strip() or plan_a
    plan_b_value = str(saved.get("plan_b") or plan_b).strip() or plan_b
    no_trade_value = str(saved.get("no_trade") or no_trade).strip() or no_trade

    status_label = "Pre-market prep"
    if _market_pulse_market_hours(now_et):
        status_label = "In-session discipline"
    if today_count > 0:
        status_label = f"{today_count} trade{'s' if today_count != 1 else ''} logged"

    return {
        "day_key": day_key,
        "bias_label": bias_label,
        "bias_tone": bias_tone,
        "bias_summary": bias_summary,
        "volatility_label": volatility_label,
        "status_label": status_label,
        "focus": focus,
        "plan_a": plan_a_value,
        "plan_b": plan_b_value,
        "no_trade": no_trade_value,
        "key_levels": key_levels,
        "macro_events": macro_events,
        "headline": (
            f"{bias_label} · {volatility_label}"
            + (f" · Today {app_runtime.money(today_net)}" if today_count else "")
        ),
        "summary": bias_summary,
        "cta_label": "Trade Gate" if today_count == 0 else "Add Trade",
        "source_label": "Manually tuned" if is_tuned else "Auto-generated",
        "source_detail": (
            "Using your saved brief edits for this day."
            if is_tuned
            else "Generated from live SPX, VIX, gamma structure, and macro context."
        ),
        "is_tuned": is_tuned,
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

    projected_completion_date: Optional[date] = None
    projected_completion_label: str = ""
    if projected_days_overall is not None:
        remaining_sessions = max(0, int(projected_days_overall))
        cursor = app_runtime.now_et().date()
        if remaining_sessions <= 0:
            while not _is_market_session(cursor):
                cursor += timedelta(days=1)
        else:
            while remaining_sessions > 0:
                cursor += timedelta(days=1)
                if _is_market_session(cursor):
                    remaining_sessions -= 1
        projected_completion_date = cursor
        projected_completion_label = cursor.strftime("%a, %b %d, %Y")

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
        "projected_completion_date": projected_completion_date,
        "projected_completion_label": projected_completion_label,
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


def dashboard_brief_update():
    day = (
        str(request.form.get("brief_day") or app_runtime.today_iso()).strip()
        or app_runtime.today_iso()
    )
    reset = str(request.form.get("brief_reset") or "").strip() == "1"
    _save_dashboard_brief_settings(
        day,
        {
            "focus": "" if reset else request.form.get("brief_focus") or "",
            "plan_a": "" if reset else request.form.get("brief_plan_a") or "",
            "plan_b": "" if reset else request.form.get("brief_plan_b") or "",
            "no_trade": "" if reset else request.form.get("brief_no_trade") or "",
        },
    )
    flash("Daily brief saved.", "success")

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
    from mccain_capital.services import market_data_service
    from mccain_capital.services import market_worker
    from mccain_capital.services import gamma_map_service
    from mccain_capital.repositories import analytics as analytics_repo
    from mccain_capital.repositories import trades as trades_repo

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
    from mccain_capital.services.ui import get_vanquish_profit_lock_state

    vanquish_lock = get_vanquish_profit_lock_state()
    if not current_app.config.get("TESTING"):
        try:
            market_worker.start_market_worker_once()
        except Exception:
            pass
    tape_snapshot = market_worker.get_market_snapshot()
    tape_prices = dict(tape_snapshot.get("prices") or {})
    dashboard_spx = dict(tape_prices.get("SPX") or {})
    dashboard_vix = dict(tape_prices.get("VIX") or {})
    # Dashboard tape should prefer broker-native Tradier quotes over generic fallbacks.
    try:
        tradier_quotes = market_data_service.get_watchlist_tradier(["SPX", "VIX"])
    except Exception:
        tradier_quotes = {}
    tradier_spx = dict(tradier_quotes.get("SPX") or {})
    tradier_vix = dict(tradier_quotes.get("VIX") or {})
    if tradier_spx.get("price") is not None:
        dashboard_spx = tradier_spx
    if tradier_vix.get("price") is not None:
        dashboard_vix = tradier_vix
    if dashboard_spx.get("price") is None or dashboard_vix.get("price") is None:
        try:
            fallback = market_data_service.get_watchlist(["SPX", "VIX"], allow_yf_fallback=False)
        except Exception:
            fallback = {}
        if dashboard_spx.get("price") is None:
            dashboard_spx = dict(fallback.get("SPX") or dashboard_spx)
        if dashboard_vix.get("price") is None:
            dashboard_vix = dict(fallback.get("VIX") or dashboard_vix)

    now_et = app_runtime.now_et()
    now_epoch = int(now_et.timestamp())

    def _dashboard_sparkline_svg(
        series: List[float], tone: str, prev_close: Optional[float] = None
    ) -> str:
        values = [float(v) for v in series if isinstance(v, (int, float))]
        if len(values) < 2:
            return '<div class="marketMiniSparkEmpty">No trend</div>'
        # Smooth tiny outlier jumps so the mini line stays readable.
        if len(values) >= 5:
            smoothed: List[float] = []
            for idx in range(len(values)):
                left = max(0, idx - 1)
                right = min(len(values), idx + 2)
                window = values[left:right]
                smoothed.append(sum(window) / float(len(window)))
            values = smoothed
        width = 120.0
        height = 28.0
        domain = list(values)
        if isinstance(prev_close, (int, float)):
            domain.append(float(prev_close))
        min_v = min(domain)
        max_v = max(domain)
        if abs(max_v - min_v) < 1e-9:
            max_v = min_v + 1.0
        step = width / max(len(values) - 1, 1)
        points: List[str] = []
        for idx, value in enumerate(values):
            x = idx * step
            y = ((max_v - value) / (max_v - min_v)) * (height - 2) + 1
            points.append(f"{x:.2f},{y:.2f}")
        cls = "up" if tone == "up" else "down" if tone == "down" else "flat"
        ref_line = ""
        if isinstance(prev_close, (int, float)):
            ref_y = ((max_v - float(prev_close)) / (max_v - min_v)) * (height - 2) + 1
            ref_line = f'<line class="marketMiniSparkRef" x1="0" y1="{ref_y:.2f}" x2="120" y2="{ref_y:.2f}" />'
        return (
            '<svg viewBox="0 0 120 28" class="marketMiniSpark" aria-hidden="true">'
            f"{ref_line}"
            f'<polyline class="marketMiniSparkLine {cls}" points="{" ".join(points)}" />'
            "</svg>"
        )

    def _dashboard_mini_series(symbol: str) -> List[float]:
        points_src = (tape_snapshot.get("series_points") or {}).get(symbol) or []
        points: List[float] = []
        if isinstance(points_src, list):
            for row in points_src:
                if isinstance(row, dict) and isinstance(row.get("v"), (int, float)):
                    points.append(float(row.get("v")))
        if points:
            deduped: List[float] = []
            for value in points[-120:]:
                if not deduped or abs(deduped[-1] - value) > 0.01:
                    deduped.append(value)
            if len(deduped) >= 10:
                return deduped[-40:]
        raw_src = (tape_snapshot.get("series") or {}).get(symbol) or []
        if isinstance(raw_src, list):
            raw = [float(v) for v in raw_src if isinstance(v, (int, float))]
            deduped_raw: List[float] = []
            for value in raw[-120:]:
                if not deduped_raw or abs(deduped_raw[-1] - value) > 0.01:
                    deduped_raw.append(value)
            if len(deduped_raw) >= 10:
                return deduped_raw[-40:]

        # If cached tape points are too sparse/flat, pull a clean intraday curve directly.
        try:
            rows = market_data_service.get_intraday(symbol)
        except Exception:
            rows = []
        intraday = [
            float(r.get("close"))
            for r in rows[-120:]
            if isinstance(r, dict) and isinstance(r.get("close"), (int, float))
        ]
        deduped_intraday: List[float] = []
        for value in intraday:
            if not deduped_intraday or abs(deduped_intraday[-1] - value) > 0.01:
                deduped_intraday.append(value)
        if deduped_intraday:
            return deduped_intraday[-40:]
        return []

    def _float_or_none(value: Any) -> Optional[float]:
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(value)
        except Exception:
            return None

    def _enrich_dashboard_quote(symbol: str, quote: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(quote or {})
        intraday_series = _dashboard_mini_series(symbol)
        if intraday_series:
            day_low = min(intraday_series)
            day_high = max(intraday_series)
            enriched["day_range"] = f"{day_low:.2f} to {day_high:.2f}"
            if abs(day_high - day_low) < 0.01:
                enriched["day_range_compact"] = f"{day_high:.2f}"
            else:
                enriched["day_range_compact"] = f"{day_low:.2f}-{day_high:.2f}"
            enriched["day_open"] = float(intraday_series[0])
        else:
            enriched["day_range"] = "—"
            enriched["day_range_compact"] = "—"
            enriched["day_open"] = None
        reason = str(enriched.get("reason") or "").lower()
        provider = str(enriched.get("provider") or "").lower()
        if enriched.get("price") is None:
            state = "Unavailable"
        elif provider == "tradier" and reason.startswith("tradier_"):
            state = "Live"
        elif "fallback" in reason or "close" in reason or provider not in ("", "tradier"):
            state = "Delayed"
        else:
            state = "Live"
        enriched["market_state"] = state
        enriched["data_status_label"] = state
        price = _float_or_none(enriched.get("price"))
        pct = _float_or_none(enriched.get("pct_change"))
        prev_close = None
        if price is not None and pct is not None and abs(100.0 + pct) > 1e-9:
            prev_close = price / (1.0 + (pct / 100.0))
        enriched["prior_close"] = prev_close

        # Keep the tape line smooth from intraday points and overlay prior-close reference.
        chart_series = list(intraday_series)
        if not chart_series and price is not None:
            chart_series = [float(price), float(price)]

        tone = "flat"
        if len(chart_series) >= 2:
            delta = float(chart_series[-1]) - float(chart_series[0])
            if delta > 0:
                tone = "up"
            elif delta < 0:
                tone = "down"
        enriched["sparkline_svg"] = _dashboard_sparkline_svg(
            chart_series,
            tone,
            prev_close=prev_close,
        )

        change_points = None
        if price is not None and prev_close is not None:
            change_points = price - prev_close
        day_open = _float_or_none(enriched.get("day_open"))
        gap_points = None
        gap_pct = None
        if day_open is not None and prev_close is not None and prev_close > 0:
            gap_points = day_open - prev_close
            gap_pct = (gap_points / prev_close) * 100.0
        enriched["change_points"] = change_points
        enriched["overnight_gap_points"] = gap_points
        enriched["overnight_gap_pct"] = gap_pct
        if gap_points is not None and gap_pct is not None:
            enriched["overnight_gap_label"] = f"{gap_points:+.2f} pts · {gap_pct:+.2f}%"
            enriched["overnight_gap_compact"] = f"{gap_points:+.2f} ({gap_pct:+.2f}%)"
        else:
            enriched["overnight_gap_label"] = "—"
            enriched["overnight_gap_compact"] = "—"
        if not str(enriched.get("day_range_compact") or "").strip():
            day_range_full = str(enriched.get("day_range") or "—")
            enriched["day_range_compact"] = (
                day_range_full.replace(" to ", "-") if day_range_full != "—" else "—"
            )
        if not str(enriched.get("overnight_gap_compact") or "").strip():
            gap_full = str(enriched.get("overnight_gap_label") or "—")
            if " pts · " in gap_full:
                gap_pts, gap_pct_label = gap_full.split(" pts · ", 1)
                enriched["overnight_gap_compact"] = f"{gap_pts} ({gap_pct_label})"
            else:
                enriched["overnight_gap_compact"] = gap_full
        source_badge = _quote_source_badge(enriched)
        enriched["source_label"] = source_badge["label"]
        enriched["source_short"] = source_badge["label"]
        enriched["source_badge_label"] = source_badge["label"]
        enriched["source_badge_tone"] = source_badge["tone"]
        as_of_raw = str(enriched.get("as_of") or "").strip()
        age_s = 0
        if as_of_raw:
            try:
                as_of_dt = datetime.fromisoformat(as_of_raw.replace("Z", "+00:00"))
                age_s = max(0, now_epoch - int(as_of_dt.timestamp()))
                enriched["freshness_label"] = f"{state} · {age_s}s old"
            except Exception:
                enriched["freshness_label"] = f"{state} · 0s old"
        else:
            enriched["freshness_label"] = f"{state} · 0s old"
        if age_s >= 3600:
            age_compact = f"{age_s // 3600}h"
        elif age_s >= 60:
            age_compact = f"{age_s // 60}m"
        else:
            age_compact = f"{age_s}s"
        enriched["freshness_label_compact"] = f"{state} · {age_compact}"
        return enriched

    dashboard_spx = _enrich_dashboard_quote("SPX", dashboard_spx)
    dashboard_vix = _enrich_dashboard_quote("VIX", dashboard_vix)
    try:
        gamma_snapshot = gamma_map_service.get_gamma_snapshot()
    except Exception:
        gamma_snapshot = {}
    try:
        news_snapshot = _market_news_snapshot()
    except Exception:
        news_snapshot = {"macro_events": []}
    daily_brief = _dashboard_daily_brief_viewmodel(
        now_et=now_et,
        dashboard_spx=dashboard_spx,
        dashboard_vix=dashboard_vix,
        gamma_snapshot=gamma_snapshot,
        news_snapshot=news_snapshot,
        today_count=today_count,
        today_net=today_net,
    )

    dashboard_tape_updated_raw = str(tape_snapshot.get("updated_at") or "")
    dashboard_tape_updated_label = _format_iso_et_label(dashboard_tape_updated_raw)
    if dashboard_tape_updated_label:
        # Keep dashboard tape timestamp compact for quick scanning.
        parts = dashboard_tape_updated_label.split(" ", 3)
        if len(parts) >= 4:
            dashboard_tape_updated_label = parts[3]

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
        dashboard_spx=dashboard_spx,
        dashboard_vix=dashboard_vix,
        dashboard_tape_updated=dashboard_tape_updated_raw,
        dashboard_tape_updated_label=dashboard_tape_updated_label,
        daily_brief=daily_brief,
        proj=proj,
        account_scope=scope,
        scope_mode=("active" if scope_active else "all"),
        scope_active_href=f"/dashboard?y={year}&m={month}&scope=active",
        scope_all_href=f"/dashboard?y={year}&m={month}&scope=all",
        dashboard_year=year,
        dashboard_month=month,
        milestone=milestone,
        vanquish_lock=vanquish_lock,
        money=app_runtime.money,
        money_compact=_money_compact,
    )
    return render_page(content, active="dashboard", vanquish_lock=vanquish_lock)


def stream_market():
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import market_worker
    from mccain_capital.services import options_panel_service

    is_testing = bool(current_app.config.get("TESTING"))
    gamma_snapshot = gamma_map_service.get_gamma_snapshot()
    if not is_testing:
        market_worker.start_market_worker_once()
        options_panel_service.start_options_worker_once()
        gamma_map_service.start_gamma_worker_once()
        if not gamma_snapshot.get("asof"):
            try:
                gamma_snapshot = gamma_map_service.run_gamma_refresh_once()
            except Exception:
                gamma_snapshot = gamma_map_service.get_gamma_snapshot()

    @stream_with_context
    def generate():
        started_at = time.time()
        while True:
            payload = market_worker.get_market_snapshot()
            payload["options"] = options_panel_service.get_options_snapshot()
            payload["gamma_map"] = (
                gamma_snapshot if is_testing else gamma_map_service.get_gamma_snapshot()
            )
            payload["server_ts"] = app_runtime.now_iso()
            yield f"data: {json.dumps(payload)}\\n\\n"
            if is_testing:
                break
            try:
                interval = float(
                    app_runtime.get_setting_value("market_stream_seconds", 0.25) or 0.25
                )
            except Exception:
                interval = 0.25
            if interval < 0.20:
                interval = 0.20
            # Keep sync workers healthy by rotating SSE connections before worker timeout.
            try:
                max_stream_s = float(
                    app_runtime.get_setting_value("market_stream_max_seconds", 110) or 110
                )
            except Exception:
                max_stream_s = 110.0
            if max_stream_s < 30:
                max_stream_s = 30.0
            if (time.time() - started_at) >= max_stream_s:
                break
            time.sleep(interval)

    response = Response(generate(), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Connection"] = "keep-alive"
    return response


def stream_market_ws():
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import market_worker
    from mccain_capital.services import options_panel_service

    try:
        from simple_websocket import ConnectionClosed
        from simple_websocket import Server
    except Exception:
        return Response("websocket dependency unavailable", status=501)

    is_testing = bool(current_app.config.get("TESTING"))
    if not is_testing:
        market_worker.start_market_worker_once()
        options_panel_service.start_options_worker_once()
        gamma_map_service.start_gamma_worker_once()

    try:
        ws = Server.accept(request.environ)
    except Exception:
        return Response("websocket upgrade required", status=400)

    try:
        while True:
            payload = market_worker.get_market_snapshot()
            payload["options"] = options_panel_service.get_options_snapshot()
            payload["gamma_map"] = gamma_map_service.get_gamma_snapshot()
            ws.send(json.dumps(payload))
            if is_testing:
                break
            time.sleep(1)
    except ConnectionClosed:
        pass
    except Exception:
        pass
    return ""


def stream_options_panel():
    from mccain_capital.services import options_panel_service

    is_testing = bool(current_app.config.get("TESTING"))
    if not is_testing:
        options_panel_service.start_options_worker_once()

    @stream_with_context
    def generate():
        started_at = time.time()
        while True:
            payload = options_panel_service.get_options_snapshot()
            yield f"data: {json.dumps(payload)}\\n\\n"
            if is_testing:
                break
            # Rotate stream to avoid sync-worker timeout churn.
            if (time.time() - started_at) >= 110:
                break
            time.sleep(2)

    response = Response(generate(), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["Connection"] = "keep-alive"
    return response


def market_pulse_page():
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import market_worker
    from mccain_capital.services import options_panel_service

    if auth_enabled() and not is_authenticated():
        return redirect(url_for("login_page", next="/market-pulse"))
    force_refresh = (request.args.get("refresh") or "").strip().lower() in {"1", "true", "yes"}
    now_et = app_runtime.now_et()
    if not current_app.config.get("TESTING"):
        market_worker.start_market_worker_once()
        options_panel_service.start_options_worker_once()
        gamma_map_service.start_gamma_worker_once()
    snapshot = _market_pulse_snapshot(force_refresh=force_refresh)
    gamma_snapshot = gamma_map_service.get_gamma_snapshot()
    if force_refresh or not gamma_snapshot.get("asof"):
        try:
            gamma_snapshot = gamma_map_service.run_gamma_refresh_once()
        except Exception:
            gamma_snapshot = gamma_map_service.get_gamma_snapshot()
    options_snapshot = options_panel_service.get_options_snapshot()
    options_spx = dict((options_snapshot.get("symbols") or {}).get("SPX") or {})
    options_contracts = list(options_spx.get("contracts") or [])
    if not options_contracts:
        try:
            options_snapshot = options_panel_service.run_options_refresh_once()
            options_spx = dict((options_snapshot.get("symbols") or {}).get("SPX") or {})
            options_contracts = list(options_spx.get("contracts") or [])
        except Exception:
            options_contracts = []
    news_snapshot = _market_news_snapshot()
    gamma_updated_label = _format_iso_et_label(gamma_snapshot.get("asof")) or "—"
    quotes = _market_pulse_enrich_quotes(list(snapshot.get("quotes") or []), now_et)
    if not current_app.config.get("TESTING"):
        try:
            live_snapshot = market_worker.get_market_snapshot()
            live_series = dict(live_snapshot.get("series") or {})
            for q in quotes:
                symbol = str(q.get("symbol") or "")
                series = live_series.get(symbol)
                if isinstance(series, list) and len(series) >= 8:
                    q["mini_series"] = [float(v) for v in series if isinstance(v, (int, float))]
            quotes = _market_pulse_enrich_quotes(quotes, now_et)
        except Exception:
            pass
    spx_quote = next((q for q in quotes if str(q.get("label") or "") == "SPX"), {})
    vix_quote = next((q for q in quotes if str(q.get("label") or "") == "VIX"), {})
    quotes_map = {str(q.get("label") or ""): q for q in quotes if isinstance(q, dict)}
    series_points = {
        str(q.get("label") or q.get("symbol") or ""): list(q.get("series") or [])
        for q in quotes
        if isinstance(q, dict) and str(q.get("label") or q.get("symbol") or "").strip()
    }
    spx_priority_context = build_spx_priority_context(
        spx_quote=spx_quote, gamma_snapshot=gamma_snapshot
    )
    alert = _market_pulse_alert(quotes)
    guardrail = _market_pulse_guardrail(quotes)
    context = _market_pulse_context(quotes)
    gamma_quality = _gamma_data_quality(gamma_snapshot, quotes, now_et)
    source_health = build_market_source_health(snapshot, news_snapshot, gamma_snapshot, now_et)
    integrity = dict(snapshot.get("integrity") or {})
    stats = _market_pulse_stats(quotes)
    core_quotes = [q for q in quotes if str(q.get("label") or "") != "SPX"]
    leader_quotes = [q for q in quotes if str(q.get("group") or "") == "leaders"]
    gamma_csv_path = str(((gamma_snapshot.get("paths") or {}).get("csv")) or "")
    gamma_png_path = str(((gamma_snapshot.get("paths") or {}).get("png")) or "")
    gamma_csv_href = (
        url_for("market_pulse_gamma_artifact", name="gamma_data.csv")
        if gamma_csv_path and os.path.exists(gamma_csv_path)
        else ""
    )
    gamma_png_href = (
        url_for("market_pulse_gamma_artifact", name="gamma_map.png")
        if gamma_png_path and os.path.exists(gamma_png_path)
        else ""
    )

    content = render_template(
        "core/market_pulse.html",
        available=bool(snapshot.get("available")),
        fetched_at=str(snapshot.get("fetched_at") or ""),
        source_label=str(snapshot.get("source_label") or "Yahoo Finance chart feed"),
        source_note=str(snapshot.get("source_note") or ""),
        spx_quote=spx_quote,
        vix_quote=vix_quote,
        quotes_map=quotes_map,
        core_quotes=core_quotes,
        leader_quotes=leader_quotes,
        context=context,
        integrity=integrity,
        alert=alert,
        guardrail=guardrail,
        market_hours=bool(_market_pulse_market_hours(now_et)),
        stats=stats,
        gamma_snapshot=gamma_snapshot,
        spx_priority_context=spx_priority_context,
        gamma_quality=gamma_quality,
        source_health=source_health,
        gamma_updated_label=gamma_updated_label,
        market_now_iso=now_et.isoformat(),
        series_points=series_points,
        gamma_csv_href=gamma_csv_href,
        gamma_png_href=gamma_png_href,
        options_contracts=options_contracts,
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


def system_check_page():
    from mccain_capital.services.viewmodels import StateBadgeViewModel

    checks: List[Dict[str, Any]] = []

    def add_check(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    db_path = app_runtime.DB_PATH
    uploads = app_runtime.UPLOAD_DIR
    books = app_runtime.BOOKS_DIR

    add_check("DB file", os.path.exists(db_path), db_path)
    add_check("Uploads dir", os.path.isdir(uploads), uploads)
    add_check("Books dir", os.path.isdir(books), books)
    add_check(
        "Uploads writable",
        os.access(uploads, os.W_OK),
        uploads if os.path.isdir(uploads) else "missing",
    )

    trades_count = 0
    journal_count = 0
    try:
        with app_runtime.db() as conn:
            row = conn.execute("SELECT COUNT(*) FROM trades").fetchone()
            trades_count = int((row[0] if row else 0) or 0)
            row = conn.execute("SELECT COUNT(*) FROM journal_entries").fetchone()
            journal_count = int((row[0] if row else 0) or 0)
        add_check("Trades table", True, f"{trades_count} rows")
        add_check("Journal table", True, f"{journal_count} rows")
    except Exception as exc:
        add_check("DB tables", False, str(exc))

    backup_files = 0
    if os.path.isdir(uploads):
        try:
            backup_files = len(
                [
                    n
                    for n in os.listdir(uploads)
                    if str(n).lower().endswith((".zip", ".json", ".db"))
                ]
            )
        except OSError:
            backup_files = 0
    add_check("Backup artifacts", backup_files > 0, f"{backup_files} files")

    ok_count = len([c for c in checks if c["ok"]])
    status = "healthy" if ok_count == len(checks) else "degraded"
    hero_title = (
        "Runtime Looks Healthy"
        if status == "healthy"
        else "Resolve Runtime Gaps Before They Compound"
    )
    hero_blurb = (
        "Confirm storage, runtime paths, and backup posture before trusting the rest of the stack."
    )
    support_lead = (
        "All core storage paths are present and writable."
        if status == "healthy"
        else "One or more core runtime checks are degraded."
    )
    support_body = "This page is the fast answer to whether the app can safely store, read, and recover its working state."
    status_badges = [
        StateBadgeViewModel(
            label="Confidence",
            value=("High" if status == "healthy" else "Mixed"),
            tone=("healthy" if status == "healthy" else "caution"),
            title="Overall confidence in current storage and runtime health.",
        ),
        StateBadgeViewModel(
            label="Checks",
            value=f"{ok_count}/{len(checks)} ok",
            tone=("healthy" if status == "healthy" else "caution"),
            title="Passing checks out of the total runtime checklist.",
        ),
        StateBadgeViewModel(
            label="Trades",
            value=str(trades_count),
            tone="healthy",
            title="Trade rows detected in the live database.",
        ),
        StateBadgeViewModel(
            label="Journal",
            value=str(journal_count),
            tone="healthy",
            title="Journal entries detected in the live database.",
        ),
    ]
    content = render_template(
        "core/system_check.html",
        checks=checks,
        status=status,
        ok_count=ok_count,
        total_count=len(checks),
        trades_count=trades_count,
        journal_count=journal_count,
        hero_title=hero_title,
        hero_blurb=hero_blurb,
        support_lead=support_lead,
        support_body=support_body,
        status_badges=status_badges,
    )
    return render_page(content, active="ops", title="McCain Capital · System Check")


def market_pulse_gamma_artifact(name: str):
    safe = str(name or "").strip()
    allowed = {"gamma_data.csv": "text/csv", "gamma_map.png": "image/png"}
    if safe not in allowed:
        abort(404)
    path = os.path.join(app_runtime.UPLOAD_DIR, safe)
    if not os.path.exists(path):
        abort(404)
    return send_file(path, mimetype=allowed[safe], as_attachment=False, download_name=safe)


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
    from mccain_capital.services.ui import get_vanquish_profit_lock_state

    vanquish_lock = get_vanquish_profit_lock_state()
    content = render_template("core/links.html", vanquish_lock=vanquish_lock)
    return render_page(content, active="links", vanquish_lock=vanquish_lock)


def vanquish_blocklist_download():
    payload = "trade.vanquishtrader.com\nwww.vanquishtrader.com\n"
    resp = make_response(payload)
    resp.headers["Content-Type"] = "text/plain; charset=utf-8"
    resp.headers["Content-Disposition"] = "attachment; filename=vanquish_blocklist.txt"
    return resp


def vanquish_lock_control():
    from mccain_capital.services.ui import clear_manual_vanquish_lock
    from mccain_capital.services.ui import set_manual_vanquish_lock

    if request.method != "POST":
        return redirect(url_for("dashboard"))
    action = str(request.form.get("action") or "start").strip().lower()
    next_href = (
        str(request.form.get("next") or "").strip() or request.referrer or url_for("dashboard")
    )
    if action == "clear":
        clear_manual_vanquish_lock()
        flash("Manual Vanquish lock cleared.", "success")
        return redirect(next_href)
    try:
        minutes = int(request.form.get("duration_minutes") or "1")
    except ValueError:
        minutes = 1
    state = set_manual_vanquish_lock(minutes, source="dashboard_test")
    flash(
        f"Manual Vanquish lock started for {int(state.get('duration_minutes') or minutes)} minute(s).",
        "success",
    )
    return redirect(next_href)


def trading_window_config():
    if request.method == "GET":
        content = render_template(
            "core/trading_window_settings.html",
            trading_window=get_trading_window_state(),
        )
        return render_page(content, active="ops")
    raw_next = str(request.form.get("next") or "").strip()
    next_href = url_for("trading_window_config")
    if raw_next.startswith("/") and not raw_next.startswith("//") and raw_next != request.path:
        next_href = raw_next
    state = save_trading_window_settings(request.form)
    flash(
        f"Trading window saved. {state.get('start_et')} → {state.get('done_by_et')} ET.",
        "success",
    )
    return redirect(next_href)


def vanquish_lock_state():
    from mccain_capital.services.ui import get_vanquish_profit_lock_state

    return jsonify(get_vanquish_profit_lock_state())


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
    from mccain_capital.services import trades as trades_svc
    async_requested = (request.args.get("async") or "").strip() == "1"

    def render_restore_page(
        *,
        message: str = "",
        tone: str = "info",
        restore_job_id: str = "",
        restore_filename: str = "",
    ):
        content = render_template(
            "core/restore_backup.html",
            db_path=str(app_runtime.DB_PATH),
            upload_dir=str(app_runtime.UPLOAD_DIR),
            message=str(message or "").strip(),
            tone=str(tone or "info").strip() or "info",
            restore_job_id=str(restore_job_id or "").strip(),
            restore_filename=str(restore_filename or "").strip(),
        )
        return render_page(content, active="dashboard")

    if request.method == "GET":
        return render_restore_page(
            restore_job_id=(request.args.get("job") or "").strip(),
            restore_filename=(request.args.get("file") or "").strip(),
        )

    f = request.files.get("backup_zip")
    if not f or not f.filename:
        if async_requested:
            return jsonify(
                {"ok": False, "error": "missing_backup_zip", "message": "Please choose a backup zip file."}
            ), 400
        return render_restore_page(message="Please choose a backup zip file.", tone="warning")

    filename = str(getattr(f, "filename", "") or "").strip()
    actor = effective_username() if auth_enabled() else _legacy().APP_USERNAME
    try:
        restore_path = trades_svc._save_uploaded_restore_archive(f)
    except ValueError as e:
        if async_requested:
            return jsonify({"ok": False, "error": "invalid_backup_zip", "message": str(e)}), 400
        return render_restore_page(message=str(e), tone="warning")
    except Exception as e:
        if async_requested:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "restore_upload_failed",
                        "message": f"Restore upload failed: {e}",
                    }
                ),
                500,
            )
        return render_restore_page(message=f"Restore upload failed: {e}", tone="danger")

    job = trades_svc._start_restore_job(
        restore_path,
        actor=actor,
        cleanup_source=True,
    )
    if async_requested:
        return jsonify({"ok": True, "job": trades_svc._job_response_payload(job)})
    return render_restore_page(
        message=f"Restore started for {filename or os.path.basename(restore_path)}.",
        tone="info",
        restore_job_id=str(job.get("id") or ""),
        restore_filename=filename or os.path.basename(restore_path),
    )


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
    first = date(year, month, 1)
    month_end = date(year, month, monthrange(year, month)[1])
    news_overlay = _forex_factory_usd_window_events(first, month_end)
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
    news_day_meta = {
        str(day.get("iso") or ""): day
        for day in list(news_overlay.get("days") or [])
        if isinstance(day, dict)
    }
    for week in cal.monthdatescalendar(year, month):
        cells = []
        for day in week:
            in_month = day.month == month
            holiday_name = _market_holiday_name(day)
            is_weekend = day.weekday() >= 5
            is_holiday = bool(holiday_name)
            is_trading = in_month and not is_weekend and not is_holiday
            iso = day.isoformat()
            day_news = list(news_overlay["events_by_day"].get(iso, []))
            news_meta = dict(news_day_meta.get(iso) or {})
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
                    "iso": iso,
                    "weekday_label": day.strftime("%a"),
                    "in_month": in_month,
                    "is_weekend": is_weekend,
                    "is_holiday": is_holiday,
                    "is_trading": is_trading,
                    "holiday_name": holiday_name,
                    "day_labels": day_labels,
                    "week_labels": week_labels,
                    "month_labels": month_labels,
                    "news_events": day_news,
                    "news_focus_key": str(news_meta.get("focus_key") or ""),
                    "news_focus_label": str(news_meta.get("focus_label") or ""),
                    "news_headline": str(news_meta.get("headline") or ""),
                    "news_priority_score": int(news_meta.get("priority_score") or 0),
                    "is_key_news_day": bool(news_meta.get("is_key_day")),
                    "has_curated_news": bool(news_meta.get("curated_count")),
                    "is_quiet_day": bool(
                        in_month
                        and is_trading
                        and not day_labels
                        and not week_labels
                        and not month_labels
                        and not day_news
                    ),
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
        "news_provider_count": news_overlay["provider_count"],
        "news_fallback_count": news_overlay["fallback_count"],
        "news_source_mode": news_overlay["source_mode"],
        "news_source_note": news_overlay["source_note"],
        "news_confidence_label": news_overlay["confidence_label"],
        "news_top_days": news_overlay["top_days"],
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


def _calendar_event_title_key(title: str) -> str:
    return " ".join(str(title or "").strip().lower().split())


def _calendar_event_sort_key(item: Dict[str, Any]) -> tuple[str, str, str]:
    raw = str(item.get("starts_at") or "").strip()
    try:
        starts_key = datetime.fromisoformat(raw).isoformat()
    except ValueError:
        starts_key = raw
    return (str(item.get("iso") or ""), starts_key, str(item.get("title") or ""))


def _calendar_event_focus(title: str) -> tuple[str, str, int]:
    text = _calendar_event_title_key(title)
    if "fomc" in text or "fed chair" in text or "rate decision" in text:
        return ("federal", "Fed Day", 8)
    if any(token in text for token in ("cpi", "ppi", "pce", "inflation")):
        return ("inflation", "Inflation", 6)
    if any(token in text for token in ("nfp", "job openings", "unemployment", "payroll")):
        return ("labor", "Labor", 5)
    if any(token in text for token in ("gdp", "retail", "industrial production")):
        return ("growth", "Growth", 4)
    return ("macro", "Macro", 2)


def _forex_factory_usd_window_events(start_day: date, end_day: date) -> Dict[str, Any]:
    if end_day < start_day:
        start_day, end_day = end_day, start_day
    result: Dict[str, Any] = {
        "available": False,
        "week_range_label": f"{start_day.strftime('%b %-d')} to {end_day.strftime('%b %-d')}",
        "events": [],
        "events_by_day": {},
        "days": [],
        "total": 0,
        "high_count": 0,
        "medium_count": 0,
        "summary": "USD red/orange events unavailable for this month.",
        "fallback_used": False,
        "fallback_count": 0,
        "provider_count": 0,
        "source_mode": "unavailable",
        "source_note": "Forex Factory month coverage is unavailable right now.",
        "confidence_label": "Unavailable",
        "top_days": [],
    }
    payload_candidates: List[List[Dict[str, Any]]] = []
    for source_payload in (
        get_forex_factory_month_feed(),
        get_forex_factory_feed(),
        get_forex_factory_next_week_feed(),
    ):
        if isinstance(source_payload, list) and source_payload:
            payload_candidates.append(source_payload)
    seen_keys: set[tuple[str, str, str]] = set()
    payload: List[Dict[str, Any]] = []
    for rows in payload_candidates:
        for row in rows:
            if not isinstance(row, dict):
                continue
            key = (
                str(row.get("date") or "").strip(),
                str(row.get("title") or "").strip().lower(),
                str(row.get("impact") or "").strip().lower(),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            payload.append(row)

    events: List[Dict[str, Any]] = []
    events_by_day: Dict[str, List[Dict[str, Any]]] = {}
    high_count = 0
    medium_count = 0
    provider_count = 0
    existing_event_keys: set[tuple[str, str]] = set()
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
        if event_day < start_day or event_day > end_day:
            continue
        time_label = dt.strftime("%-I:%M %p ET")
        title = str(row.get("title") or "USD event").strip() or "USD event"
        item = {
            "title": title,
            "impact": impact,
            "iso": event_day.isoformat(),
            "date_label": event_day.strftime("%a, %b %-d"),
            "starts_at": raw_date,
            "time_label": time_label,
            "impact_class": "high" if impact == "High" else "medium",
            "icon": "🔴" if impact == "High" else "🟠",
            "source": "feed",
            "source_label": "Live calendar",
            "jump_href": (
                f"/candle-opens?y={event_day.year}&m={event_day.month}"
                f"#news-day-{event_day.isoformat()}"
            ),
            "tooltip": f"{impact} impact • {time_label} • {title}",
        }
        events.append(item)
        events_by_day.setdefault(item["iso"], []).append(item)
        existing_event_keys.add((item["iso"], _calendar_event_title_key(title)))
        provider_count += 1
        if impact == "High":
            high_count += 1
        else:
            medium_count += 1

    # Provider can occasionally publish an incomplete month window.
    # Fill known high-value days with titled backup events instead of anonymous markers.
    fallback_events: List[tuple[datetime, str, str]] = []
    for starts_at, impact, title in USD_CALENDAR_FALLBACK_EVENTS:
        try:
            fallback_events.append(
                (
                    datetime.fromisoformat(str(starts_at)),
                    str(impact).title(),
                    str(title).strip() or "USD backup event",
                )
            )
        except ValueError:
            continue
    fallback_count = 0
    for fallback_at, impact, title in fallback_events:
        fallback_day = fallback_at.date()
        if fallback_day < start_day or fallback_day > end_day:
            continue
        iso = fallback_day.isoformat()
        title_key = _calendar_event_title_key(title)
        if (iso, title_key) in existing_event_keys:
            continue
        impact_class = "high" if impact == "High" else "medium"
        time_label = fallback_at.strftime("%-I:%M %p ET")
        item = {
            "title": title,
            "impact": impact,
            "iso": iso,
            "date_label": fallback_day.strftime("%a, %b %-d"),
            "starts_at": fallback_at.isoformat(),
            "time_label": time_label,
            "impact_class": impact_class,
            "icon": "🔴" if impact_class == "high" else "🟠",
            "source": "curated",
            "source_label": "Curated backup",
            "jump_href": (
                f"/candle-opens?y={fallback_day.year}&m={fallback_day.month}" f"#news-day-{iso}"
            ),
            "tooltip": f"{impact} impact • {time_label} • {title} • Curated backup",
        }
        events.append(item)
        events_by_day.setdefault(iso, []).append(item)
        existing_event_keys.add((iso, title_key))
        fallback_count += 1
        if impact_class == "high":
            high_count += 1
        else:
            medium_count += 1

    events.sort(key=_calendar_event_sort_key)
    for items in events_by_day.values():
        items.sort(key=_calendar_event_sort_key)

    news_days: List[Dict[str, Any]] = []
    event_week_keys: set[tuple[int, int]] = set()
    for iso in sorted(events_by_day.keys()):
        day_events = list(events_by_day.get(iso, []))
        if not day_events:
            continue
        try:
            d = date.fromisoformat(iso)
            event_week_keys.add((int(d.isocalendar().year), int(d.isocalendar().week)))
        except ValueError:
            pass
        top_focus = ("macro", "Macro", 0)
        for event in day_events:
            focus = _calendar_event_focus(str(event.get("title") or ""))
            if focus[2] > top_focus[2]:
                top_focus = focus
        priority_score = (
            sum(4 for e in day_events if e.get("impact_class") == "high")
            + sum(2 for e in day_events if e.get("impact_class") == "medium")
            + sum(_calendar_event_focus(str(e.get("title") or ""))[2] for e in day_events)
        )
        news_days.append(
            {
                "iso": iso,
                "date_label": day_events[0]["date_label"],
                "high_count": len([e for e in day_events if e.get("impact_class") == "high"]),
                "medium_count": len([e for e in day_events if e.get("impact_class") == "medium"]),
                "events": day_events,
                "placeholder": False,
                "curated_count": len([e for e in day_events if e.get("source") == "curated"]),
                "priority_score": priority_score,
                "focus_key": top_focus[0],
                "focus_label": top_focus[1],
                "headline": str(day_events[0].get("title") or "USD event"),
                "is_key_day": priority_score >= 10,
            }
        )

    # If provider payload is sparse, still show each week with a date anchor.
    week_anchor: Dict[tuple[int, int], date] = {}
    cursor = start_day
    while cursor <= end_day:
        week_key = (int(cursor.isocalendar().year), int(cursor.isocalendar().week))
        week_anchor.setdefault(week_key, cursor)
        cursor += timedelta(days=1)
    for week_key, anchor_day in sorted(week_anchor.items(), key=lambda item: item[1]):
        if week_key in event_week_keys:
            continue
        news_days.append(
            {
                "iso": anchor_day.isoformat(),
                "date_label": anchor_day.strftime("%a, %b %-d"),
                "high_count": 0,
                "medium_count": 0,
                "events": [],
                "placeholder": True,
                "curated_count": 0,
                "priority_score": 0,
                "focus_key": "macro",
                "focus_label": "Macro",
                "headline": "",
                "is_key_day": False,
            }
        )
    news_days.sort(key=lambda row: str(row.get("iso") or ""))
    top_days = sorted(
        [row for row in news_days if not row.get("placeholder") and row.get("events")],
        key=lambda row: (
            -int(row.get("priority_score") or 0),
            -int(row.get("high_count") or 0),
            -int(row.get("medium_count") or 0),
            str(row.get("iso") or ""),
        ),
    )[:3]

    if events:
        summary = f"{len(events)} USD red/orange events in selected month."
        source_mode = "live"
        source_note = "Live calendar coverage is carrying the full month."
        confidence_label = "High confidence"
        if fallback_count > 0 and provider_count > 0:
            source_mode = "mixed"
            source_note = (
                f"Mixed coverage: {provider_count} live calendar event"
                f"{'s' if provider_count != 1 else ''} + {fallback_count} curated backup"
                f" event{'s' if fallback_count != 1 else ''}."
            )
            confidence_label = "Mixed confidence"
        elif fallback_count > 0:
            source_mode = "curated"
            source_note = (
                f"Using {fallback_count} curated backup event"
                f"{'s' if fallback_count != 1 else ''} because month coverage is sparse."
            )
            confidence_label = "Backup coverage"
        result.update(
            {
                "available": True,
                "events": events,
                "events_by_day": events_by_day,
                "days": news_days,
                "total": len(events),
                "high_count": high_count,
                "medium_count": medium_count,
                "summary": summary,
                "fallback_used": bool(fallback_count),
                "fallback_count": fallback_count,
                "provider_count": provider_count,
                "source_mode": source_mode,
                "source_note": source_note,
                "confidence_label": confidence_label,
                "top_days": top_days,
            }
        )
    return result


def _forex_factory_usd_week_events(anchor: date) -> Dict[str, Any]:
    week_start = anchor - timedelta(days=(anchor.weekday() + 1) % 7)
    week_end = week_start + timedelta(days=6)
    return _forex_factory_usd_window_events(week_start, week_end)


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
