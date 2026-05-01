"""Core domain service gateway.

Core routes still rely on legacy implementations in ``app_core``. This module
keeps that dependency localized behind explicit delegator functions.
"""

from __future__ import annotations

from calendar import Calendar, monthrange
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
import copy
from datetime import date
from datetime import datetime
from datetime import timedelta
from email.utils import parsedate_to_datetime
import html
import json
import logging
import math
import os
import re
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
    Response,
    current_app,
    flash,
    jsonify,
    make_response,
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
)
from mccain_capital.services.viewmodels import (
    balance_state_badges,
    dashboard_data_trust,
    sync_state_badges,
)
from mccain_capital.services.market_pulse_health import build_market_source_health
from mccain_capital.services import market_data_service
from mccain_capital.services.gamma_context_service import (
    _extract_candidate_ladder,
    _infer_level_step,
    build_spx_priority_context,
)
from mccain_capital.services.twitter_feed_service import build_twitter_feed_snapshot

MULTIPLIER = 100
DEFAULT_STOP_PCT = 20.0
DEFAULT_TARGET_PCT = 30.0
DEFAULT_FEE_PER_CONTRACT = 0.70
DAY_OPEN_INTERVALS = tuple(range(2, 13))
WEEK_OPEN_INTERVALS = (2, 3, 4, 5, 6)
MONTH_OPEN_INTERVALS = (2,)
MARKET_PULSE_CACHE_TTL_SECONDS = 300
MARKET_PULSE_UNSAFE_CRITICAL_THRESHOLD = 2
MARKET_PULSE_LAST_VALID_MAX_AGE_SECONDS = 20 * 60 * 60
MARKET_PULSE_LAST_GOOD_SNAPSHOT_MAX_AGE_SECONDS = 20 * 60 * 60
MARKET_NEWS_CACHE_TTL_SECONDS = 60
MARKET_NEWS_RSS_TIMEOUT_SECONDS = 1.25
MARKET_NEWS_RSS_SYMBOL_LIMIT = 5
MARKET_NEWS_FRESH_SECONDS = 12 * 60 * 60
MARKET_NEWS_MAX_AGE_SECONDS = 36 * 60 * 60
WATCHLIST_NEWS_MAX_AGE_SECONDS = 48 * 60 * 60
MARKET_NEWS_FEED_LIMIT = 100
MARKET_PULSE_X_FEED_LIMIT = 8
MARKET_PULSE_X_MIN_RELEVANCE = 6
MARKET_PULSE_X_PER_ACCOUNT_LIMIT = 2
MARKET_PULSE_X_API_PER_ACCOUNT_LIMIT = 4
MARKET_PULSE_X_RSS_URLS: Tuple[str, ...] = (
    "https://rsshub.app/twitter/user/{handle}",
    "https://nitter.poast.org/{handle}/rss",
    "https://nitter.privacydev.net/{handle}/rss",
)
MARKET_PULSE_X_ACCOUNTS: Tuple[Dict[str, str], ...] = (
    {"handle": "unusual_whales", "label": "Unusual Whales", "lane": "Options Flow"},
)
SUPPORTED_PLAYBOOK_TICKERS: Tuple[str, ...] = ("QQQ", "SPY", "SPX")
DEFAULT_PLAYBOOK_TICKER = "QQQ"
PLAYBOOK_TICKER_STORAGE_KEY = "mc_playbook_ticker"
MILESTONE_PROFIT_SOURCES: Tuple[str, ...] = ("today", "week", "mtd", "ytd")
GAMMA_SPOT_MISMATCH_POINTS_THRESHOLD = 5.0
GAMMA_SPOT_TIMESTAMP_DRIFT_SECONDS = 120
FINNHUB_API_KEY = (os.environ.get("FINNHUB_API_KEY") or "").strip()
FINNHUB_BASE_URL = "https://finnhub.io/api/v1"
X_BEARER_TOKEN = urllib.parse.unquote(
    (
        os.environ.get("X_BEARER_TOKEN")
        or os.environ.get("X_API_BEARER_TOKEN")
        or os.environ.get("TWITTER_BEARER_TOKEN")
        or ""
    ).strip()
)
X_API_BASE_URL = (os.environ.get("X_API_BASE_URL") or "https://api.x.com/2").strip().rstrip("/")
X_API_TIMEOUT_SECONDS = 4.0
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
    {
        "symbol": "LLY",
        "label": "LLY",
        "group": "leaders",
        "focus": "Healthcare leadership and defensive-growth tape signal.",
    },
    {
        "symbol": "AMD",
        "label": "AMD",
        "group": "leaders",
        "focus": "Semiconductor beta and AI hardware breadth signal.",
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
INVESTING_RSS_URLS: Tuple[str, ...] = (
    "https://www.investing.com/rss/news_25.rss",
    "https://www.investing.com/rss/market_overview.rss",
)
_market_pulse_cache: Dict[str, Any] = {"fetched_at": None, "payload": None}
_market_news_cache: Dict[str, Any] = {"fetched_at": None, "payload": None}
_market_pulse_x_user_cache: Dict[str, Any] = {"fetched_at": None, "payload": {}}
_market_pulse_playbook_cache: Dict[str, Any] = {"generated_at": None, "payload": None}

LOGGER = logging.getLogger(__name__)
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
    ("2026-04-13T10:00:00-04:00", "Medium", "Existing Home Sales"),
    ("2026-04-13T14:00:00-04:00", "Medium", "Federal Budget Balance"),
    ("2026-04-14T08:00:00-04:00", "Medium", "NFIB Small Business Index"),
    ("2026-04-14T08:15:00-04:00", "Medium", "ADP Weekly Employment Change"),
    ("2026-04-14T08:30:00-04:00", "High", "Core PPI m/m"),
    ("2026-04-14T08:30:00-04:00", "High", "PPI m/m"),
    ("2026-04-15T08:30:00-04:00", "Medium", "Empire State Manufacturing Index"),
    ("2026-04-15T08:30:00-04:00", "Medium", "Import Prices m/m"),
    ("2026-04-15T10:00:00-04:00", "Medium", "NAHB Housing Market Index"),
    ("2026-04-15T10:30:00-04:00", "Medium", "Crude Oil Inventories"),
    ("2026-04-15T14:00:00-04:00", "High", "Beige Book"),
    ("2026-04-16T08:30:00-04:00", "High", "Philly Fed Manufacturing Index"),
    ("2026-04-16T08:30:00-04:00", "High", "Unemployment Claims"),
    ("2026-04-16T09:15:00-04:00", "Medium", "Capacity Utilization Rate"),
    ("2026-04-16T09:15:00-04:00", "Medium", "Industrial Production m/m"),
)


def _legacy():
    from mccain_capital import app_core

    return app_core


def _market_pulse_cache_file() -> str:
    return app_runtime.upload_path(".market_pulse_cache.json")


def _market_pulse_replay_cache_file() -> str:
    return app_runtime.upload_path(".market_pulse_replay_cache.json")


def _market_news_cache_file() -> str:
    return app_runtime.upload_path(".market_news_cache.json")


def _load_market_pulse_disk_cache() -> Dict[str, Any] | None:
    try:
        with open(_market_pulse_cache_file(), "r", encoding="utf-8") as f:
            parsed = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _load_market_pulse_replay_cache() -> Dict[str, Any] | None:
    try:
        with open(_market_pulse_replay_cache_file(), "r", encoding="utf-8") as f:
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


def _save_market_pulse_replay_cache(payload: Dict[str, Any]) -> None:
    try:
        os.makedirs(app_runtime.upload_root(), exist_ok=True)
        with open(_market_pulse_replay_cache_file(), "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except OSError:
        return


def _market_pulse_cached_replay_series(symbol: str) -> tuple[List[Dict[str, Any]], Optional[str]]:
    def _valid_replay_points(points: List[Dict[str, Any]]) -> bool:
        prices = [
            float((row or {}).get("v"))
            for row in points
            if isinstance((row or {}).get("v"), (int, float))
        ]
        if len(prices) < 8:
            return False
        positive = [v for v in prices if v > 0]
        if len(positive) < max(4, int(len(prices) * 0.8)):
            return False
        peak = max(positive) if positive else 0.0
        trough = min(positive) if positive else 0.0
        return 10.0 <= trough <= peak <= 100000.0

    def _normalize_cached_points(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        points: List[Dict[str, Any]] = []
        seen_ts: set[str] = set()
        for row in rows[-420:]:
            if not isinstance(row, dict):
                continue
            ts_value = str(row.get("ts") or "").strip()
            if not ts_value:
                stamp = row.get("stamp")
                if isinstance(stamp, (int, float)):
                    try:
                        ts_value = datetime.fromtimestamp(int(stamp), tz=app_runtime.TZ).isoformat()
                    except Exception:
                        ts_value = ""
            price = row.get("close")
            if not isinstance(price, (int, float)):
                price = row.get("c")
            if not isinstance(price, (int, float)):
                price = row.get("price")
            if not isinstance(price, (int, float)) and "ts" in row:
                price = row.get("v")
            if not ts_value or not isinstance(price, (int, float)) or ts_value in seen_ts:
                continue
            seen_ts.add(ts_value)
            points.append(
                {
                    "ts": ts_value,
                    "label": str(row.get("label") or ""),
                    "v": float(price),
                    "close": float(price),
                    "volume": (
                        int(row.get("volume") or 0)
                        if isinstance(row.get("volume"), (int, float))
                        else 0
                    ),
                }
            )
        return points

    payload = _load_market_pulse_replay_cache() or {}
    symbols = payload.get("symbols") or {}
    entry = symbols.get(str(symbol or "").strip().upper()) if isinstance(symbols, dict) else None
    if isinstance(entry, dict):
        points = _normalize_cached_points(list(entry.get("points") or []))
        session_day = str(entry.get("session_day") or "").strip() or None
        if _valid_replay_points(points):
            return (points, session_day)

    symbol_key = str(symbol or "").strip().upper()
    candidate_files = [_market_pulse_cache_file()]
    legacy_cache = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "uploads", ".market_pulse_cache.json")
    )
    candidate_files.append(legacy_cache)

    best_points: List[Dict[str, Any]] = []
    best_session_day: Optional[str] = None
    best_session_key = ""
    seen: set[str] = set()
    for path in candidate_files:
        normalized = os.path.abspath(path)
        if normalized in seen:
            continue
        seen.add(normalized)
        try:
            with open(normalized, "r", encoding="utf-8") as f:
                parsed = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            continue
        if not isinstance(parsed, dict):
            continue
        quotes = list(parsed.get("quotes") or [])
        row = next(
            (
                q
                for q in quotes
                if symbol_key
                in {
                    str((q or {}).get("symbol") or "").strip().upper(),
                    str((q or {}).get("label") or "").strip().upper(),
                }
            ),
            {},
        )
        if not isinstance(row, dict):
            continue
        points = _normalize_cached_points(list(row.get("series") or []))
        if not _valid_replay_points(points):
            continue
        session_day_obj = _market_pulse_points_session_day(points)
        session_day = session_day_obj.isoformat() if session_day_obj else ""
        session_key = session_day or str(row.get("asof") or "")
        if session_key <= best_session_key:
            continue
        best_points = points
        best_session_day = session_day or None
        best_session_key = session_key

    if len(best_points) >= 8 and best_session_day:
        try:
            _store_market_pulse_replay_series(
                symbol_key,
                session_day=date.fromisoformat(best_session_day),
                points=best_points,
            )
        except Exception:
            pass
    return (best_points, best_session_day)


def _store_market_pulse_replay_series(
    symbol: str,
    *,
    session_day: date,
    points: List[Dict[str, Any]],
) -> None:
    symbol_key = str(symbol or "").strip().upper()
    if not symbol_key or len(points) < 8:
        return
    payload = _load_market_pulse_replay_cache() or {}
    symbols = payload.get("symbols")
    if not isinstance(symbols, dict):
        symbols = {}
        payload["symbols"] = symbols
    session_day_iso = session_day.isoformat()
    existing = symbols.get(symbol_key)
    if (
        isinstance(existing, dict)
        and str(existing.get("session_day") or "").strip() == session_day_iso
        and list(existing.get("points") or []) == list(points)
    ):
        return
    symbols[symbol_key] = {
        "session_day": session_day_iso,
        "points": list(points)[-240:],
        "saved_at": app_runtime.now_et().isoformat(),
    }
    _save_market_pulse_replay_cache(payload)


def _market_pulse_attach_replay_cache(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    normalized = dict(payload or {})
    quotes_out: List[Dict[str, Any]] = []
    for row in list(normalized.get("quotes") or []):
        if not isinstance(row, dict):
            continue
        q = dict(row)
        symbol = str(q.get("symbol") or q.get("label") or "").strip().upper()
        series = list(q.get("series") or [])
        session_day = _market_pulse_points_session_day(series)
        if session_day is not None and len(series) >= 8:
            try:
                _store_market_pulse_replay_series(symbol, session_day=session_day, points=series)
            except Exception:
                pass
        if not list(q.get("prior_session_series") or []):
            replay_points, replay_day = _market_pulse_cached_replay_series(symbol)
            if len(replay_points) >= 2:
                q["prior_session_series"] = replay_points
                if replay_day:
                    q["prior_session_day"] = replay_day
        quotes_out.append(q)
    normalized["quotes"] = quotes_out
    return normalized


def _market_pulse_rows_session_day(rows: List[Dict[str, Any]]) -> Optional[date]:
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


def _market_pulse_rows_to_points(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    points: List[Dict[str, Any]] = []
    for row in rows[-240:]:
        if not isinstance(row, dict) or row.get("close") is None:
            continue
        ts_raw = str(row.get("ts") or "").strip()
        if not ts_raw:
            continue
        try:
            label = (
                datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                .astimezone(app_runtime.TZ)
                .strftime("%-I:%M")
            )
        except Exception:
            label = ""
        points.append(
            {
                "ts": ts_raw,
                "label": label,
                "v": float(row.get("close")),
                "open": (
                    float(row.get("open")) if isinstance(row.get("open"), (int, float)) else None
                ),
                "high": (
                    float(row.get("high")) if isinstance(row.get("high"), (int, float)) else None
                ),
                "low": float(row.get("low")) if isinstance(row.get("low"), (int, float)) else None,
                "close": (
                    float(row.get("close")) if isinstance(row.get("close"), (int, float)) else None
                ),
                "volume": (
                    int(row.get("volume")) if isinstance(row.get("volume"), (int, float)) else 0
                ),
            }
        )
    return points


def _market_pulse_series_vwap(rows: List[Dict[str, Any]]) -> Optional[float]:
    vwap_num = 0.0
    vwap_den = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        price = row.get("close")
        if not isinstance(price, (int, float)):
            price = row.get("price")
        if not isinstance(price, (int, float)):
            price = row.get("v")
        volume = row.get("volume")
        if not isinstance(volume, (int, float)):
            volume = row.get("vol")
        if (
            isinstance(price, (int, float))
            and isinstance(volume, (int, float))
            and float(volume) > 0
        ):
            vwap_num += float(price) * float(volume)
            vwap_den += float(volume)
    if vwap_den <= 0:
        return None
    return vwap_num / vwap_den


def _market_pulse_iter_market_sessions_backward(
    start_day: Optional[date],
    *,
    limit: int = 12,
) -> List[date]:
    if start_day is None:
        return []
    out: List[date] = []
    cursor = start_day
    while len(out) < max(1, int(limit)):
        if _is_market_session(cursor):
            out.append(cursor)
        cursor -= timedelta(days=1)
    return out


def _market_pulse_fetch_session_points_for_day(
    symbol: str,
    session_day: date,
) -> List[Dict[str, Any]]:
    anchor_day = session_day + timedelta(days=1)
    try:
        rows = market_data_service.get_prior_session_intraday(
            symbol,
            anchor_session_day=anchor_day,
        )
    except Exception:
        rows = []
    if _market_pulse_rows_session_day(rows) != session_day:
        return []
    return _market_pulse_rows_to_points(rows)


def _market_pulse_resolve_replay_session(
    *,
    symbol: str,
    phase: str,
    now_et: datetime,
    current_points: List[Dict[str, Any]],
    replay_points: List[Dict[str, Any]],
    replay_session_day: Optional[date],
) -> Dict[str, Any]:
    today = now_et.date()
    current_day = _market_pulse_points_session_day(current_points)
    last_valid_day = _market_pulse_expected_replay_session_day(phase=phase, now_et=now_et)
    stored_replay_day = replay_session_day or _market_pulse_points_session_day(replay_points)
    stored_points = list(replay_points or [])
    same_day_session_available = bool(current_points) and current_day == today

    if phase == "open" and same_day_session_available:
        return {
            "mode": "live_session",
            "points": current_points,
            "session_day": current_day,
            "last_valid_day": last_valid_day,
            "stored_replay_day": stored_replay_day,
            "replay_source": "live",
        }

    if phase == "afterhours" and same_day_session_available:
        return {
            "mode": "last_session_replay",
            "points": current_points,
            "session_day": current_day,
            "last_valid_day": today,
            "stored_replay_day": stored_replay_day,
            "replay_source": "live_close",
        }

    # After 8 PM ET the phase rolls to "closed", but the just-finished session's
    # SPX series is still the most relevant replay source for the hero chart.
    if phase == "closed" and same_day_session_available:
        return {
            "mode": "last_session_replay",
            "points": current_points,
            "session_day": current_day,
            "last_valid_day": current_day,
            "stored_replay_day": stored_replay_day,
            "replay_source": "live_close",
        }

    if isinstance(last_valid_day, date):
        if stored_points and stored_replay_day == last_valid_day:
            return {
                "mode": "last_session_replay",
                "points": stored_points,
                "session_day": last_valid_day,
                "last_valid_day": last_valid_day,
                "stored_replay_day": stored_replay_day,
                "replay_source": "stored",
            }
        fetched_points = _market_pulse_fetch_session_points_for_day(symbol, last_valid_day)
        if len(fetched_points) >= 2:
            return {
                "mode": "last_session_replay",
                "points": fetched_points,
                "session_day": last_valid_day,
                "last_valid_day": last_valid_day,
                "stored_replay_day": stored_replay_day,
                "replay_source": "provider",
            }

    return {
        "mode": "unavailable",
        "points": [],
        "session_day": None,
        "last_valid_day": last_valid_day,
        "stored_replay_day": stored_replay_day,
        "replay_source": "missing",
    }


def _market_pulse_replay_archive_rows(
    *,
    symbol: str,
    last_valid_day: Optional[date],
    stored_replay_day: Optional[date],
    stored_points: List[Dict[str, Any]],
    limit: int = 8,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(last_valid_day, date):
        return rows
    for session_day in _market_pulse_iter_market_sessions_backward(last_valid_day, limit=limit):
        available = False
        source = "missing"
        if stored_points and stored_replay_day == session_day:
            available = True
            source = "stored"
        else:
            fetched_points = _market_pulse_fetch_session_points_for_day(symbol, session_day)
            if len(fetched_points) >= 2:
                available = True
                source = "provider"
        rows.append(
            {
                "session_day": session_day.isoformat(),
                "label": session_day.strftime("%a %b %-d"),
                "available": available,
                "source": source,
                "is_latest_valid": session_day == last_valid_day,
                "status": "Replay stored" if available else "Replay not stored",
            }
        )
    return rows


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


def _parse_iso_et(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=app_runtime.TZ)
        return dt.astimezone(app_runtime.TZ)
    except Exception:
        return None


def _market_pulse_log(event: str, **fields: Any) -> None:
    payload = {"event": event}
    payload.update({key: value for key, value in fields.items() if value is not None})
    try:
        LOGGER.info("market_pulse_snapshot %s", json.dumps(payload, default=str, sort_keys=True))
    except Exception:
        LOGGER.info("market_pulse_snapshot %s %s", event, payload)


def _market_pulse_positive_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if numeric > 0 else None


def _market_pulse_quote_prev_close(spx_quote: Dict[str, Any]) -> Optional[float]:
    for key in ("prev_close", "previous_close", "prior_close"):
        numeric = _market_pulse_positive_float(spx_quote.get(key))
        if numeric is not None:
            return numeric

    price = _market_pulse_positive_float(spx_quote.get("price"))
    pct = (
        float(spx_quote.get("change_pct"))
        if isinstance(spx_quote.get("change_pct"), (int, float))
        else (
            float(spx_quote.get("pct_change"))
            if isinstance(spx_quote.get("pct_change"), (int, float))
            else None
        )
    )
    if price is not None and pct is not None and abs(100.0 + pct) > 1e-9:
        prev_close = price / (1.0 + (pct / 100.0))
        if prev_close > 0:
            return prev_close

    prior_session_series = list(spx_quote.get("prior_session_series") or [])
    for row in reversed(prior_session_series):
        if not isinstance(row, dict):
            continue
        numeric = _market_pulse_positive_float(row.get("v", row.get("close")))
        if numeric is not None:
            return numeric
    return None


def _market_pulse_cached_playbook_snapshot(now_et: datetime) -> Optional[Dict[str, Any]]:
    generated_at = _market_pulse_playbook_cache.get("generated_at")
    payload = _market_pulse_playbook_cache.get("payload")
    if not isinstance(generated_at, datetime) or not isinstance(payload, dict):
        return None
    age = max(0.0, (now_et - generated_at).total_seconds())
    if age > MARKET_PULSE_LAST_GOOD_SNAPSHOT_MAX_AGE_SECONDS:
        return None
    return copy.deepcopy(payload)


def _market_pulse_store_playbook_snapshot(now_et: datetime, payload: Dict[str, Any]) -> None:
    _market_pulse_playbook_cache["generated_at"] = now_et
    _market_pulse_playbook_cache["payload"] = copy.deepcopy(payload)


def _market_pulse_level_payload(
    *,
    value: Any,
    source: str,
    as_of: str,
    freshness_status: str,
    confidence: str,
    derived_from_session: bool,
    validation_status: str,
    reason: str = "",
) -> Dict[str, Any]:
    return {
        "value": _market_pulse_positive_float(value),
        "source": source,
        "as_of": str(as_of or ""),
        "freshness_status": freshness_status,
        "confidence": confidence,
        "derived_from_session": bool(derived_from_session),
        "validation_status": validation_status,
        "reason": reason,
    }


def _market_pulse_secondary_resolution_source_label(
    next_call_source: str,
    next_put_source: str,
) -> str:
    sources = {
        str(next_call_source or "").strip().lower(),
        str(next_put_source or "").strip().lower(),
    }
    sources.discard("")
    sources.discard("unresolved")
    if not sources:
        return "Unavailable"
    if sources == {"provider"}:
        return "Provider-backed"
    if sources == {"inferred"}:
        return "Inferred from strike ladder"
    return "Mixed provider + inferred"


def _market_pulse_normalize_structure_levels(levels: Dict[str, Any]) -> Dict[str, Optional[float]]:
    return {
        key: _market_pulse_positive_float(levels.get(key))
        for key in (
            "main_flip",
            "local_flip",
            "call_wall",
            "put_wall",
            "next_call_wall",
            "next_put_wall",
            "spot",
        )
    }


def _market_pulse_secondary_display_state(
    levels: Dict[str, Any],
    *,
    next_call_source: str,
    next_put_source: str,
    next_put_confidence: str,
    degraded_reason: str,
) -> Dict[str, Any]:
    next_call = _market_pulse_positive_float(levels.get("next_call_wall"))
    next_put = _market_pulse_positive_float(levels.get("next_put_wall"))
    complete = next_call is not None and next_put is not None
    displayable = complete and next_put_confidence in {"high", "medium"}
    resolution_source = _market_pulse_secondary_resolution_source_label(
        next_call_source=next_call_source,
        next_put_source=next_put_source,
    )
    return {
        "secondary_structure_complete": complete,
        "secondary_structure_displayable": displayable,
        "resolution_source": resolution_source,
        "degraded_reason": (
            "" if displayable else degraded_reason or "secondary_structure_incomplete"
        ),
    }


def _market_pulse_infer_next_call_wall(
    *,
    call_wall: Optional[float],
    put_wall: Optional[float],
    candidates: List[float],
) -> Dict[str, Any]:
    if call_wall is None:
        return {"value": None, "source": "unresolved", "reason": "missing_call_wall"}

    ordered = sorted(
        {float(value) for value in candidates if _market_pulse_positive_float(value) is not None}
    )
    higher = [value for value in ordered if value > float(call_wall) + 0.001]
    if higher:
        candidate = min(higher)
        gap = candidate - float(call_wall)
        step = _infer_level_step(call_wall=call_wall, put_wall=put_wall, candidates=ordered)
        max_gap = max(25.0, min(125.0, float(step) * 4.0))
        if gap <= max_gap:
            return {
                "value": candidate,
                "source": "inferred",
                "reason": "candidate_ladder_above_call_wall",
            }

    if put_wall is None:
        return {"value": None, "source": "unresolved", "reason": "missing_next_call_wall"}

    step = _infer_level_step(call_wall=call_wall, put_wall=put_wall, candidates=ordered)
    candidate = float(call_wall) + float(step)
    gap = candidate - float(call_wall)
    if gap <= max(25.0, min(125.0, float(step) * 4.0)):
        return {
            "value": candidate,
            "source": "inferred",
            "reason": "heuristic_spacing_above_call_wall",
        }
    return {"value": None, "source": "unresolved", "reason": "missing_next_call_wall"}


def _market_pulse_infer_next_put_wall(
    *,
    put_wall: Optional[float],
    call_wall: Optional[float],
    spot: Optional[float],
    next_call_wall: Optional[float],
    candidates: List[float],
    preferred_candidates: List[float],
) -> Dict[str, Any]:
    if put_wall is None:
        return {
            "value": None,
            "source": "unresolved",
            "confidence": "low",
            "reason": "missing_put_wall",
        }

    ordered = sorted(
        {float(value) for value in candidates if _market_pulse_positive_float(value) is not None}
    )
    lower = [value for value in ordered if value < float(put_wall) - 0.001]
    if not lower:
        return {
            "value": None,
            "source": "unresolved",
            "confidence": "low",
            "reason": "no_put_side_candidates_below_put_wall",
        }

    step = _infer_level_step(call_wall=call_wall, put_wall=put_wall, candidates=ordered)
    wall_span = (
        abs(float(call_wall) - float(put_wall))
        if call_wall is not None and put_wall is not None
        else float(step) * 4.0
    )
    min_gap = max(5.0, min(15.0, float(step) * 0.65))
    max_gap = max(25.0, min(150.0, max(float(step) * 4.0, wall_span * 1.5)))
    cluster_radius = max(5.0, min(20.0, float(step) * 1.1))
    preferred = {
        round(float(value), 4)
        for value in preferred_candidates
        if _market_pulse_positive_float(value) is not None
    }
    expected_gap = float(step)
    next_call_gap = (
        float(next_call_wall) - float(call_wall)
        if next_call_wall is not None and call_wall is not None
        else None
    )
    scored: List[Dict[str, Any]] = []
    for candidate in sorted(lower, reverse=True):
        gap = float(put_wall) - float(candidate)
        if gap <= 0.0 or gap < min_gap or gap > max_gap:
            continue
        if spot is not None and abs(float(spot) - float(candidate)) > max(
            abs(float(spot) - float(put_wall)) + max_gap, max_gap * 2.0
        ):
            continue
        neighbor_support = sum(
            1
            for other in lower
            if other != candidate and abs(float(other) - float(candidate)) <= cluster_radius
        )
        gap_alignment = 1.0 - min(1.0, abs(gap - expected_gap) / max(expected_gap, 1.0))
        score = gap_alignment
        if round(float(candidate), 4) in preferred:
            score += 1.5
        if neighbor_support >= 1:
            score += 1.0
        if neighbor_support >= 2:
            score += 0.5
        asymmetry_penalty = 0.0
        if next_call_gap is not None and next_call_gap > 0:
            ratio = max(gap, next_call_gap) / max(min(gap, next_call_gap), 1.0)
            if ratio > 3.0:
                asymmetry_penalty = 0.75
            elif ratio > 2.25:
                asymmetry_penalty = 0.35
        score -= asymmetry_penalty
        if score < 1.0:
            continue
        confidence = "medium"
        if neighbor_support >= 1 and (
            round(float(candidate), 4) in preferred or gap_alignment >= 0.6
        ):
            confidence = "high"
        scored.append(
            {
                "value": float(candidate),
                "score": score,
                "gap": gap,
                "neighbor_support": neighbor_support,
                "preferred": round(float(candidate), 4) in preferred,
                "confidence": confidence,
            }
        )

    if scored:
        scored.sort(
            key=lambda row: (
                row["score"],
                row["neighbor_support"],
                1 if row["preferred"] else 0,
                -abs(row["gap"] - expected_gap),
            ),
            reverse=True,
        )
        best = scored[0]
        return {
            "value": best["value"],
            "source": "inferred",
            "confidence": best["confidence"],
            "reason": (
                "clustered_put_side_candidate"
                if best["neighbor_support"]
                else "candidate_ladder_below_put_wall"
            ),
        }

    heuristic = float(put_wall) - float(step)
    heuristic_gap = float(put_wall) - heuristic
    if heuristic_gap >= min_gap and heuristic_gap <= max_gap:
        nearby_support = sum(
            1 for candidate in lower if abs(float(candidate) - heuristic) <= cluster_radius
        )
        if nearby_support >= 1:
            return {
                "value": heuristic,
                "source": "inferred",
                "confidence": "medium",
                "reason": "heuristic_spacing_supported_by_put_cluster",
            }
        return {
            "value": None,
            "source": "unresolved",
            "confidence": "low",
            "reason": "heuristic_next_put_wall_not_supported",
        }
    return {
        "value": None,
        "source": "unresolved",
        "confidence": "low",
        "reason": "no_trustworthy_next_put_wall_candidate",
    }


def _market_pulse_secondary_candidate_ladder(
    gamma_snapshot: Dict[str, Any]
) -> List[Dict[str, Any]]:
    approved = []
    for row in _extract_candidate_ladder(gamma_snapshot):
        strike = _market_pulse_positive_float(row.get("strike"))
        if strike is None:
            continue
        approved.append(
            {
                "strike": strike,
                "abs_gex": _market_pulse_positive_float(row.get("abs_gex")) or 0.0,
                "oi": _market_pulse_positive_float(row.get("oi")) or 0.0,
                "significance_score": _market_pulse_positive_float(row.get("significance_score"))
                or 0.0,
                "expiry_basket_membership": str(row.get("expiry_basket_membership") or "approved"),
                "sources": list(row.get("sources") or []),
            }
        )
    return approved


def _market_pulse_resolve_canonical_market_structure(
    *,
    levels: Dict[str, Any],
    gamma_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    normalized = _market_pulse_normalize_structure_levels(levels)
    primary = {
        key: normalized.get(key)
        for key in ("main_flip", "local_flip", "call_wall", "put_wall", "spot")
    }
    gamma_snapshot = dict(gamma_snapshot or {})
    call_wall = primary.get("call_wall")
    put_wall = primary.get("put_wall")
    spot = primary.get("spot")

    explicit_next_call = _market_pulse_positive_float(normalized.get("next_call_wall"))
    explicit_next_put = _market_pulse_positive_float(normalized.get("next_put_wall"))
    provider_next_call = _market_pulse_positive_float(
        gamma_snapshot.get("next_call_wall_above")
        if gamma_snapshot.get("next_call_wall_above") is not None
        else gamma_snapshot.get("next_call_wall")
    )
    provider_next_put = _market_pulse_positive_float(
        gamma_snapshot.get("next_put_wall_below")
        if gamma_snapshot.get("next_put_wall_below") is not None
        else gamma_snapshot.get("next_put_wall")
    )
    if provider_next_call is not None and call_wall is not None and provider_next_call <= call_wall:
        provider_next_call = None
    if provider_next_put is not None and put_wall is not None and provider_next_put >= put_wall:
        provider_next_put = None
    candidate_ladder = (
        _market_pulse_secondary_candidate_ladder(gamma_snapshot) if gamma_snapshot else []
    )
    candidates = [float(row["strike"]) for row in candidate_ladder]
    preferred_candidates = (
        list(gamma_snapshot.get("gamma_walls_top3") or []) if gamma_snapshot else []
    )

    next_call_value = explicit_next_call
    next_call_source = (
        "provider"
        if provider_next_call is not None and explicit_next_call is not None
        else "mixed" if explicit_next_call is not None else "unresolved"
    )
    next_call_reason = ""
    if next_call_value is None:
        if provider_next_call is not None:
            next_call_value = provider_next_call
            next_call_source = "provider"
            next_call_reason = "provider_next_call_wall"
        else:
            inferred_call = _market_pulse_infer_next_call_wall(
                call_wall=call_wall,
                put_wall=put_wall,
                candidates=candidates,
            )
            next_call_value = inferred_call.get("value")
            next_call_source = str(inferred_call.get("source") or "unresolved")
            next_call_reason = str(inferred_call.get("reason") or "")

    next_put_confidence = (
        "high" if provider_next_put is not None and explicit_next_put is not None else "none"
    )
    next_put_source = (
        "provider"
        if provider_next_put is not None and explicit_next_put is not None
        else "mixed" if explicit_next_put is not None else "unresolved"
    )
    next_put_reason = ""
    next_put_value = explicit_next_put
    if next_put_value is None:
        if provider_next_put is not None:
            next_put_value = provider_next_put
            next_put_source = "provider"
            next_put_confidence = "high"
            next_put_reason = "provider_next_put_wall"
        else:
            inferred_put = _market_pulse_infer_next_put_wall(
                put_wall=put_wall,
                call_wall=call_wall,
                spot=spot,
                next_call_wall=next_call_value,
                candidates=candidates,
                preferred_candidates=preferred_candidates,
            )
            next_put_value = inferred_put.get("value")
            next_put_source = str(inferred_put.get("source") or "unresolved")
            next_put_confidence = str(inferred_put.get("confidence") or "low")
            next_put_reason = str(inferred_put.get("reason") or "")

    degraded_reason = ""
    if next_call_value is None and next_put_value is None:
        degraded_reason = next_put_reason or next_call_reason or "secondary_structure_unresolved"
    elif next_call_value is not None and next_put_value is None:
        degraded_reason = next_put_reason or "missing_next_put_wall"
    elif next_call_value is None and next_put_value is not None:
        degraded_reason = next_call_reason or "missing_next_call_wall"
    elif next_put_source == "inferred" and next_put_confidence not in {"high", "medium"}:
        degraded_reason = next_put_reason or "next_put_wall_confidence_too_low"

    secondary_state = _market_pulse_secondary_display_state(
        {"next_call_wall": next_call_value, "next_put_wall": next_put_value},
        next_call_source=next_call_source,
        next_put_source=next_put_source,
        next_put_confidence=next_put_confidence,
        degraded_reason=degraded_reason,
    )
    if not secondary_state["secondary_structure_displayable"]:
        # Provider asymmetry is allowed upstream, but the final decision board
        # must never present one-sided secondary structure as authoritative.
        next_call_value = None
        next_put_value = None
        next_call_source = "unresolved"
        next_put_source = "unresolved"
        next_put_confidence = "low" if degraded_reason else "none"
        secondary_state = _market_pulse_secondary_display_state(
            {"next_call_wall": next_call_value, "next_put_wall": next_put_value},
            next_call_source=next_call_source,
            next_put_source=next_put_source,
            next_put_confidence=next_put_confidence,
            degraded_reason=degraded_reason or "secondary_structure_suppressed",
        )

    return {
        "main_flip": primary.get("main_flip"),
        "local_flip": primary.get("local_flip"),
        "call_wall": call_wall,
        "put_wall": put_wall,
        "next_call_wall": next_call_value,
        "next_put_wall": next_put_value,
        "next_call_wall_source": next_call_source,
        "next_put_wall_source": next_put_source,
        "next_put_wall_confidence": next_put_confidence,
        "next_call_wall_reason": next_call_reason,
        "next_put_wall_reason": next_put_reason,
        "secondary_candidate_ladder": candidate_ladder,
        **secondary_state,
    }


def _market_pulse_structure_invariant_check(
    *,
    levels: Dict[str, Any],
    next_call_wall: Any = None,
    next_put_wall: Any = None,
) -> Dict[str, Any]:
    main_flip = _market_pulse_positive_float(levels.get("main_flip"))
    local_flip = _market_pulse_positive_float(levels.get("local_flip"))
    call_wall = _market_pulse_positive_float(levels.get("call_wall"))
    put_wall = _market_pulse_positive_float(levels.get("put_wall"))
    next_call = _market_pulse_positive_float(
        next_call_wall if next_call_wall is not None else levels.get("next_call_wall")
    )
    next_put = _market_pulse_positive_float(
        next_put_wall if next_put_wall is not None else levels.get("next_put_wall")
    )

    hard_issues: List[str] = []
    soft_issues: List[str] = []

    if call_wall is not None and put_wall is not None and call_wall <= put_wall:
        hard_issues.append("wall_order_invalid")

    if (
        local_flip is not None
        and call_wall is not None
        and put_wall is not None
        and not (put_wall <= local_flip <= call_wall)
    ):
        hard_issues.append("local_flip_outside_wall_span")

    if next_call is not None and call_wall is not None and next_call <= call_wall:
        soft_issues.append("next_call_wall_not_above_call_wall")
        next_call = None

    if next_put is not None and put_wall is not None and next_put >= put_wall:
        soft_issues.append("next_put_wall_not_below_put_wall")
        next_put = None

    return {
        "valid": not hard_issues,
        "status": "hard_invalid" if hard_issues else "soft_invalid" if soft_issues else "valid",
        "hard_issues": hard_issues,
        "soft_issues": soft_issues,
        "issues": hard_issues + soft_issues,
        "levels": {
            "main_flip": main_flip,
            "local_flip": local_flip,
            "call_wall": call_wall,
            "put_wall": put_wall,
            "next_call_wall": next_call,
            "next_put_wall": next_put,
        },
    }


def _market_pulse_resolve_spot_snapshot(
    *,
    session_mode: str,
    spx_quote: Dict[str, Any],
    gamma_snapshot: Dict[str, Any],
    now_et: datetime,
    last_good_snapshot: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    quote_price = _market_pulse_positive_float(spx_quote.get("price"))
    quote_as_of = str(spx_quote.get("asof") or spx_quote.get("as_of") or "")
    quote_band = str(spx_quote.get("freshness_band") or "")
    quote_state = str(spx_quote.get("data_state") or "")
    official_close = _market_pulse_quote_prev_close(spx_quote)
    gamma_spot = _market_pulse_positive_float(
        gamma_snapshot.get("spot")
        if gamma_snapshot.get("spot") is not None
        else gamma_snapshot.get("spot_price_used")
    )
    gamma_spot_source = str(gamma_snapshot.get("spot_source") or "").strip().lower()
    gamma_spot_source_label = (
        str(gamma_snapshot.get("spot_source_label") or "").strip() or "Last Valid Snapshot"
    )
    gamma_build_status = str(gamma_snapshot.get("build_status") or "").strip().lower()
    gamma_as_of = str(
        gamma_snapshot.get("spot_source_timestamp")
        or gamma_snapshot.get("last_successful_compute")
        or gamma_snapshot.get("computed_at")
        or ""
    )
    last_good_meta = dict(
        (last_good_snapshot or {}).get("market_structure_snapshot", {}).get("spot_meta") or {}
    )
    last_good_value = _market_pulse_positive_float(last_good_meta.get("value"))
    last_good_as_of = str(
        last_good_meta.get("as_of")
        or (last_good_snapshot or {}).get("market_structure_snapshot", {}).get("snapshot_timestamp")
        or ""
    )

    candidates: List[Dict[str, Any]] = []
    if session_mode == "regular" and quote_price is not None and quote_state != "missing":
        candidates.append(
            {
                "value": quote_price,
                "source": "live",
                "source_label": "Live Session",
                "as_of": quote_as_of,
                "freshness_status": "live" if quote_band == "live" else "stale",
                "confidence": "high" if quote_band == "live" else "medium",
                "state": "LIVE_SESSION",
                "spot_is_fallback": False,
                "fallback_reason": "",
            }
        )
    if quote_price is not None:
        candidates.append(
            {
                "value": quote_price,
                "source": "session_anchor",
                "source_label": "Last Valid Session",
                "as_of": quote_as_of,
                "freshness_status": (
                    "stale" if session_mode != "regular" or quote_band != "live" else "live"
                ),
                "confidence": "medium" if session_mode != "regular" else "high",
                "state": "AFTER_HOURS_VALID" if session_mode != "regular" else "LIVE_SESSION",
                "spot_is_fallback": session_mode != "regular" or quote_band != "live",
                "fallback_reason": (
                    "Using the most recent trusted SPX session quote."
                    if session_mode != "regular" or quote_band != "live"
                    else ""
                ),
            }
        )
    if official_close is not None:
        candidates.append(
            {
                "value": official_close,
                "source": "official_close",
                "source_label": "Official Close",
                "as_of": quote_as_of,
                "freshness_status": "stale",
                "confidence": "medium",
                "state": "AFTER_HOURS_VALID" if session_mode != "regular" else "LIVE_SESSION",
                "spot_is_fallback": True,
                "fallback_reason": "Using official prior close as the SPX anchor.",
            }
        )
    if gamma_spot is not None:
        candidates.append(
            {
                "value": gamma_spot,
                "source": gamma_spot_source or "last_valid_snapshot",
                "source_label": gamma_spot_source_label,
                "as_of": gamma_as_of,
                "freshness_status": "stale",
                "confidence": (
                    "high"
                    if gamma_build_status == "live_valid"
                    and gamma_spot_source == "live_quote"
                    and session_mode == "regular"
                    else "medium" if gamma_build_status == "fallback_valid" else "low"
                ),
                "state": (
                    "LIVE_SESSION"
                    if gamma_build_status == "live_valid"
                    and gamma_spot_source == "live_quote"
                    and session_mode == "regular"
                    else "AFTER_HOURS_VALID"
                ),
                "spot_is_fallback": gamma_build_status != "live_valid"
                or gamma_spot_source != "live_quote",
                "fallback_reason": str(gamma_snapshot.get("fallback_reason") or ""),
            }
        )
    if last_good_value is not None:
        candidates.append(
            {
                "value": last_good_value,
                "source": "last_good_snapshot",
                "source_label": "Last Good Snapshot",
                "as_of": last_good_as_of,
                "freshness_status": "stale",
                "confidence": "low",
                "state": "AFTER_HOURS_VALID" if session_mode != "regular" else "LIVE_SESSION",
                "spot_is_fallback": True,
                "fallback_reason": "Using the last cached playbook snapshot anchor.",
            }
        )

    selected = next(
        (candidate for candidate in candidates if candidate.get("value") is not None), None
    )
    if selected:
        _market_pulse_log(
            "spot_selected",
            session_mode=session_mode,
            source=selected.get("source"),
            freshness_status=selected.get("freshness_status"),
            confidence=selected.get("confidence"),
            as_of=selected.get("as_of"),
            value=selected.get("value"),
        )
        return selected

    _market_pulse_log("spot_unavailable", session_mode=session_mode)
    return {
        "value": None,
        "source": "unavailable",
        "source_label": "Unavailable",
        "as_of": "",
        "freshness_status": "unavailable",
        "confidence": "none",
        "state": "UNAVAILABLE",
        "spot_is_fallback": False,
        "fallback_reason": "",
    }


def _market_pulse_resolve_gamma_payload(
    *,
    gamma_snapshot: Dict[str, Any],
    last_good_snapshot: Optional[Dict[str, Any]],
    session_mode: str,
    now_et: datetime,
) -> Dict[str, Any]:
    snapshot_status = str(gamma_snapshot.get("snapshot_status") or "").strip().lower()
    gamma_as_of = str(
        gamma_snapshot.get("last_successful_compute")
        or gamma_snapshot.get("computed_at")
        or gamma_snapshot.get("asof")
        or ""
    )
    raw_levels = {
        "main_flip": gamma_snapshot.get("gamma_flip_combined_basket"),
        "local_flip": gamma_snapshot.get("local_flip_aggregated_gamma"),
        "call_wall": gamma_snapshot.get("call_wall_aggregated_gamma"),
        "put_wall": gamma_snapshot.get("put_wall_aggregated_gamma"),
        "next_call_wall": gamma_snapshot.get("next_call_wall_above"),
        "next_put_wall": gamma_snapshot.get("next_put_wall_below"),
    }
    required_level_count = sum(
        1
        for key in ("main_flip", "local_flip", "call_wall", "put_wall")
        if _market_pulse_positive_float(raw_levels.get(key)) is not None
    )
    raw_invariants = _market_pulse_structure_invariant_check(
        levels=raw_levels,
        next_call_wall=raw_levels.get("next_call_wall"),
        next_put_wall=raw_levels.get("next_put_wall"),
    )
    current_snapshot_usable = (
        snapshot_status in {"healthy", "degraded", "stale"}
        and required_level_count >= 3
        and bool(raw_invariants.get("valid"))
    )

    cached_structure = dict((last_good_snapshot or {}).get("market_structure_snapshot") or {})
    fallback_level_meta = dict(cached_structure.get("level_meta") or {})
    fallback_levels = {
        key: _market_pulse_positive_float(dict(fallback_level_meta.get(key) or {}).get("value"))
        for key in (
            "main_flip",
            "local_flip",
            "call_wall",
            "put_wall",
            "next_call_wall",
            "next_put_wall",
        )
    }
    fallback_required_present = all(
        fallback_levels.get(key) is not None for key in ("local_flip", "call_wall", "put_wall")
    )
    fallback_invariants = _market_pulse_structure_invariant_check(
        levels=fallback_levels,
        next_call_wall=fallback_levels.get("next_call_wall"),
        next_put_wall=fallback_levels.get("next_put_wall"),
    )
    fallback_time = _parse_iso_et(
        cached_structure.get("last_valid_snapshot_time")
        or cached_structure.get("snapshot_timestamp")
    )
    fallback_age_seconds = (
        max(0.0, (now_et - fallback_time).total_seconds()) if fallback_time is not None else None
    )
    fallback_usable = bool(
        fallback_required_present
        and bool(fallback_invariants.get("valid"))
        and fallback_time is not None
        and fallback_age_seconds is not None
        and fallback_age_seconds <= MARKET_PULSE_LAST_GOOD_SNAPSHOT_MAX_AGE_SECONDS
    )
    use_fallback = (
        session_mode in {"after_hours", "premarket", "closed"}
        and not current_snapshot_usable
        and fallback_usable
    )
    selected_invariants = raw_invariants
    if use_fallback:
        selected_invariants = fallback_invariants
    elif snapshot_status in {"", "invalid"} and fallback_invariants.get("issues"):
        selected_invariants = fallback_invariants

    if use_fallback:
        selected_levels = dict(fallback_invariants.get("levels") or {})
        levels_source = "last_valid_snapshot"
        gamma_data_status = (
            "partial" if fallback_invariants.get("soft_issues") else "stale_but_usable"
        )
        gamma_regime_meta = {
            "value": "unconfirmed",
            "label": "Unconfirmed",
            "subtitle": "Using last valid session structure",
            "source": "last_valid_snapshot",
            "as_of": str(cached_structure.get("last_valid_snapshot_time") or ""),
            "freshness_status": "stale",
            "confidence": "low",
            "derived_from_session": True,
            "validation_status": (
                "fallback_sanitized" if fallback_invariants.get("soft_issues") else "fallback_valid"
            ),
        }
    elif snapshot_status in {"healthy", "degraded", "stale"} and not raw_invariants.get(
        "hard_issues"
    ):
        selected_levels = dict(raw_invariants.get("levels") or {})
        levels_source = (
            "live_session_snapshot"
            if session_mode == "regular" and snapshot_status == "healthy"
            else (
                "stale_snapshot"
                if snapshot_status == "stale"
                else "last_valid_snapshot" if session_mode != "regular" else "live_session_snapshot"
            )
        )
        gamma_data_status = _market_pulse_gamma_data_status(
            gamma_snapshot,
            level_count=sum(
                1
                for value in raw_levels.values()
                if _market_pulse_positive_float(value) is not None
            ),
        )
        if raw_invariants.get("soft_issues"):
            gamma_data_status = "partial"
        regime_view = _market_pulse_gamma_regime_viewmodel(
            gamma_snapshot,
            gamma_data_status=gamma_data_status,
        )
        gamma_regime_meta = {
            "value": regime_view["gamma_regime"],
            "label": regime_view["gamma_regime_label"],
            "subtitle": regime_view["gamma_regime_subtitle"],
            "source": "validated_gamma_snapshot",
            "as_of": gamma_as_of,
            "freshness_status": (
                "live" if snapshot_status == "healthy" and session_mode == "regular" else "stale"
            ),
            "confidence": (
                "high"
                if snapshot_status == "healthy" and session_mode == "regular"
                else "medium" if snapshot_status in {"healthy", "degraded"} else "low"
            ),
            "derived_from_session": session_mode != "regular",
            "validation_status": (
                "invariant_soft_failed"
                if raw_invariants.get("soft_issues")
                else snapshot_status or "validated"
            ),
        }
    elif (
        session_mode != "regular"
        and snapshot_status in {"healthy", "degraded", "stale"}
        and sum(
            1
            for value in raw_levels.values()
            if _market_pulse_positive_float(value) is not None
        )
        >= 4
    ):
        selected_levels = dict(raw_invariants.get("levels") or raw_levels or {})
        levels_source = "last_valid_snapshot"
        gamma_data_status = "partial"
        gamma_regime_meta = {
            "value": "unconfirmed",
            "label": "Degraded",
            "subtitle": "Using degraded session structure",
            "source": "validated_gamma_snapshot",
            "as_of": gamma_as_of,
            "freshness_status": "stale",
            "confidence": "low",
            "derived_from_session": True,
            "validation_status": "invariant_hard_failed",
        }
    else:
        selected_levels = {
            key: None
            for key in (
                "main_flip",
                "local_flip",
                "call_wall",
                "put_wall",
                "next_call_wall",
                "next_put_wall",
            )
        }
        levels_source = "unavailable"
        gamma_data_status = "invalid"
        gamma_regime_meta = {
            "value": "unavailable",
            "label": "Regime Unavailable",
            "subtitle": "Gamma snapshot unavailable",
            "source": "unavailable",
            "as_of": gamma_as_of,
            "freshness_status": "unavailable",
            "confidence": "none",
            "derived_from_session": False,
            "validation_status": "invalid",
        }

    canonical_structure = _market_pulse_resolve_canonical_market_structure(
        levels={
            **selected_levels,
            "spot": (
                gamma_snapshot.get("spot_price_used")
                if gamma_snapshot.get("spot_price_used") is not None
                else gamma_snapshot.get("spot")
            ),
        },
        gamma_snapshot=gamma_snapshot,
    )
    selected_levels = {
        key: canonical_structure.get(key)
        for key in (
            "main_flip",
            "local_flip",
            "call_wall",
            "put_wall",
            "next_call_wall",
            "next_put_wall",
        )
    }

    level_meta: Dict[str, Dict[str, Any]] = {}
    for key, value in selected_levels.items():
        if use_fallback:
            source = "last_good_snapshot" if key == "main_flip" else "prior_valid_session"
            as_of = str(cached_structure.get("last_valid_snapshot_time") or "")
            freshness_status = "stale"
            confidence = "low"
            validation_status = (
                "fallback_sanitized" if fallback_invariants.get("soft_issues") else "fallback_valid"
            )
            derived_from_session = True
            reason = ",".join(fallback_invariants.get("issues") or [])
        elif levels_source != "unavailable":
            source = (
                "live_session"
                if session_mode == "regular" and snapshot_status == "healthy"
                else "prior_valid_session"
            )
            as_of = gamma_as_of
            freshness_status = (
                "live" if session_mode == "regular" and snapshot_status == "healthy" else "stale"
            )
            confidence = "high" if freshness_status == "live" else "medium"
            validation_status = (
                "invariant_soft_failed"
                if raw_invariants.get("soft_issues")
                else snapshot_status or "validated"
            )
            derived_from_session = session_mode != "regular"
            reason = ",".join(raw_invariants.get("issues") or [])
        else:
            source = "unavailable"
            as_of = ""
            freshness_status = "unavailable"
            confidence = "none"
            validation_status = "invalid"
            derived_from_session = False
            reason = ",".join(
                raw_invariants.get("hard_issues") or fallback_invariants.get("hard_issues") or []
            )
        if key == "next_call_wall":
            source = str(canonical_structure.get("next_call_wall_source") or source)
            validation_status = (
                "secondary_structure_resolved"
                if canonical_structure.get("secondary_structure_displayable")
                else "secondary_structure_suppressed"
            )
            reason = str(
                canonical_structure.get("degraded_reason")
                or canonical_structure.get("next_call_wall_reason")
                or reason
            )
        elif key == "next_put_wall":
            source = str(canonical_structure.get("next_put_wall_source") or source)
            confidence = str(canonical_structure.get("next_put_wall_confidence") or confidence)
            validation_status = (
                "secondary_structure_resolved"
                if canonical_structure.get("secondary_structure_displayable")
                else "secondary_structure_suppressed"
            )
            reason = str(
                canonical_structure.get("degraded_reason")
                or canonical_structure.get("next_put_wall_reason")
                or reason
            )
        level_meta[key] = _market_pulse_level_payload(
            value=value,
            source=source,
            as_of=as_of,
            freshness_status=freshness_status,
            confidence=confidence,
            derived_from_session=derived_from_session,
            validation_status=validation_status,
            reason=reason,
        )

    local_flip_meta = dict(
        level_meta.get("local_flip")
        or _market_pulse_level_payload(
            value=None,
            source="unavailable",
            as_of="",
            freshness_status="unavailable",
            confidence="none",
            derived_from_session=False,
            validation_status="invalid",
        )
    )
    if local_flip_meta.get("value") is not None:
        local_flip_meta["source"] = (
            "live_session"
            if session_mode == "regular" and current_snapshot_usable
            else "prior_valid_session"
        )
        local_flip_meta["reason"] = (
            "validated_live_local_flip"
            if local_flip_meta["source"] == "live_session"
            else "using_last_valid_session_local_flip"
        )
    else:
        local_flip_meta["reason"] = "no_valid_local_flip"

    _market_pulse_log(
        "gamma_selected",
        session_mode=session_mode,
        snapshot_status=snapshot_status,
        levels_source=levels_source,
        gamma_data_status=gamma_data_status,
        gamma_regime=gamma_regime_meta.get("value"),
        invariant_status=selected_invariants.get("status"),
        invariant_issues=selected_invariants.get("issues"),
        fallback_used=use_fallback,
        fallback_age_seconds=fallback_age_seconds,
    )
    return {
        "levels_source": levels_source,
        "gamma_data_status": gamma_data_status,
        "gamma_regime_meta": gamma_regime_meta,
        "level_meta": level_meta,
        "local_flip_meta": local_flip_meta,
        "selected_as_of": gamma_regime_meta.get("as_of") or gamma_as_of,
        "structure_invariant_status": selected_invariants.get("status"),
        "structure_invariant_issues": list(selected_invariants.get("issues") or []),
        "canonical_structure": canonical_structure,
    }


def _market_pulse_apply_resolved_levels(
    gamma_snapshot: Dict[str, Any],
    gamma_resolution: Dict[str, Any],
) -> Dict[str, Any]:
    out = dict(gamma_snapshot or {})
    level_meta = dict(gamma_resolution.get("level_meta") or {})
    out["gamma_flip_combined_basket"] = dict(level_meta.get("main_flip") or {}).get("value")
    out["local_flip_aggregated_gamma"] = dict(level_meta.get("local_flip") or {}).get("value")
    out["call_wall_aggregated_gamma"] = dict(level_meta.get("call_wall") or {}).get("value")
    out["put_wall_aggregated_gamma"] = dict(level_meta.get("put_wall") or {}).get("value")
    out["next_call_wall_above"] = dict(level_meta.get("next_call_wall") or {}).get("value")
    out["next_put_wall_below"] = dict(level_meta.get("next_put_wall") or {}).get("value")
    out["local_flip_found"] = dict(level_meta.get("local_flip") or {}).get("value") is not None
    out["resolved_gamma_data_status"] = str(gamma_resolution.get("gamma_data_status") or "")
    return out


def _market_pulse_apply_resolved_spot(
    spx_quote: Dict[str, Any],
    spot_resolution: Dict[str, Any],
) -> Dict[str, Any]:
    out = dict(spx_quote or {})
    out["price"] = spot_resolution.get("value")
    if spot_resolution.get("as_of"):
        out["asof"] = spot_resolution.get("as_of")
    out["as_of"] = spot_resolution.get("as_of")
    out["spot_source"] = spot_resolution.get("source")
    out["spot_source_label"] = spot_resolution.get("source_label")
    out["spot_is_fallback"] = bool(spot_resolution.get("spot_is_fallback"))
    out["fallback_reason"] = str(spot_resolution.get("fallback_reason") or "")
    return out


def _market_pulse_chart_source_viewmodel(
    *,
    execution_chart: Dict[str, Any],
    session_mode: str,
) -> Dict[str, Any]:
    mode = str(execution_chart.get("mode") or "unavailable")
    points = list(execution_chart.get("points") or [])
    bars_as_of = ""
    if points and isinstance(points[-1], dict):
        bars_as_of = str(points[-1].get("ts") or "")
    if mode == "live_session":
        chart_state = "live_session"
        bars_source = "live_session_bars"
        freshness_status = "live"
    elif mode == "last_session_replay":
        chart_state = "last_valid_session"
        bars_source = str(execution_chart.get("replay_source") or "last_valid_session")
        freshness_status = "stale" if session_mode != "regular" else "live"
    else:
        chart_state = "unavailable"
        bars_source = "unavailable"
        freshness_status = "unavailable"
    _market_pulse_log(
        "chart_selected",
        session_mode=session_mode,
        chart_state=chart_state,
        bars_source=bars_source,
        bars_as_of=bars_as_of,
    )
    return {
        "chart_state": chart_state,
        "bars_source": bars_source,
        "bars_as_of": bars_as_of,
        "session_label": str(
            execution_chart.get("session_label")
            or execution_chart.get("last_valid_session_label")
            or ""
        ),
        "freshness_status": freshness_status,
    }


def _market_pulse_playbook_snapshot_valid(payload: Dict[str, Any]) -> bool:
    structure = dict(payload.get("market_structure_snapshot") or {})
    spot_meta = dict(structure.get("spot_meta") or {})
    spot_value = _market_pulse_positive_float(spot_meta.get("value"))
    app_state = str(structure.get("app_state") or "")
    levels_source = str(structure.get("levels_source") or "")
    if app_state == "UNAVAILABLE" or spot_value is None:
        return False
    if levels_source != "unavailable":
        level_meta = dict(structure.get("level_meta") or {})
        invariant_check = _market_pulse_structure_invariant_check(
            levels={
                key: _market_pulse_positive_float(dict(level_meta.get(key) or {}).get("value"))
                for key in (
                    "main_flip",
                    "local_flip",
                    "call_wall",
                    "put_wall",
                    "next_call_wall",
                    "next_put_wall",
                )
            }
        )
        if invariant_check.get("issues"):
            return False
        for key in ("call_wall", "put_wall"):
            if _market_pulse_positive_float(dict(level_meta.get(key) or {}).get("value")) is None:
                return False
    return True


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


def _market_pulse_payload_has_current_symbols(payload: Dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    cached_labels = {
        str(row.get("label") or "").strip().upper()
        for row in list(payload.get("quotes") or [])
        if isinstance(row, dict)
    }
    expected_labels = {
        str(spec.get("label") or "").strip().upper()
        for spec in MARKET_PULSE_SYMBOLS
        if str(spec.get("label") or "").strip()
    }
    return expected_labels.issubset(cached_labels)


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
    anchor = now_et.date()
    fetched_label = now_et.strftime("%b %d, %Y %I:%M:%S %p ET")
    fetched_at = _market_pulse_cache.get("fetched_at")
    cached_payload = _market_pulse_cache.get("payload")
    if (
        (not force_refresh)
        and isinstance(fetched_at, datetime)
        and isinstance(cached_payload, dict)
        and _market_pulse_payload_has_current_symbols(cached_payload)
        and (now_et - fetched_at).total_seconds() < MARKET_PULSE_CACHE_TTL_SECONDS
    ):
        normalized_cache = _market_pulse_attach_replay_cache(
            _market_pulse_force_symbol_set(cached_payload)
        )
        normalized_cache["source_label"] = "Massive market feed (cached snapshot)"
        normalized_cache["source_note"] = "Using recent cached Massive snapshot within refresh TTL."
        return normalized_cache

    if not force_refresh:
        disk_payload = _load_market_pulse_disk_cache()
        if isinstance(disk_payload, dict) and _market_pulse_payload_has_current_symbols(disk_payload):
            _market_pulse_cache["fetched_at"] = now_et
            _market_pulse_cache["payload"] = disk_payload
            fallback = _market_pulse_attach_replay_cache(
                _market_pulse_force_symbol_set(disk_payload)
            )
            fallback["source_label"] = str(
                fallback.get("source_label") or "Market feed cached snapshot"
            )
            fallback["source_note"] = (
                "Using cached market snapshot for fast page load. Use refresh for live data."
            )
            fallback.setdefault("integrity", {})
            if isinstance(fallback["integrity"], dict):
                fallback["integrity"]["cached_only"] = True
                fallback["integrity"]["forced_refresh"] = False
            return fallback

    symbols = [str(spec.get("symbol") or "").strip().upper() for spec in MARKET_PULSE_SYMBOLS]
    quotes_by_symbol = market_data_service.get_watchlist(symbols, allow_yf_fallback=False)
    if not quotes_by_symbol:
        disk_payload = _load_market_pulse_disk_cache()
        if isinstance(cached_payload, dict) and _market_pulse_payload_has_current_symbols(cached_payload):
            fallback = _market_pulse_attach_replay_cache(
                _market_pulse_force_symbol_set(cached_payload)
            )
            fallback["source_label"] = "Massive market feed (cached fallback)"
            fallback["source_note"] = (
                "Massive live quote request returned no data. Showing last cached snapshot."
            )
            return fallback
        if isinstance(disk_payload, dict) and _market_pulse_payload_has_current_symbols(disk_payload):
            _market_pulse_cache["payload"] = disk_payload
            fallback = _market_pulse_attach_replay_cache(
                _market_pulse_force_symbol_set(disk_payload)
            )
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

    # Ensure VIX doesn't disappear when the primary provider misses it.
    vix_symbol = "^VIX"
    vix_quote = dict(quotes_by_symbol.get(vix_symbol) or {})
    if not vix_quote or vix_quote.get("price") in {None, ""}:
        try:
            tradier_vix = market_data_service.get_watchlist_tradier(["VIX"])
            if isinstance(tradier_vix, dict) and tradier_vix.get("VIX"):
                quotes_by_symbol[vix_symbol] = tradier_vix["VIX"]
        except Exception:
            pass
    vix_quote = dict(quotes_by_symbol.get(vix_symbol) or {})
    if not vix_quote or vix_quote.get("price") in {None, ""}:
        try:
            vix_fallback = market_data_service.get_watchlist([vix_symbol], allow_yf_fallback=True)
            if isinstance(vix_fallback, dict) and vix_fallback.get(vix_symbol):
                quotes_by_symbol[vix_symbol] = vix_fallback[vix_symbol]
        except Exception:
            pass
    vix_quote = dict(quotes_by_symbol.get(vix_symbol) or {})
    if not vix_quote or vix_quote.get("price") in {None, ""}:
        try:
            vix_alt = market_data_service.get_watchlist(["VIX"], allow_yf_fallback=True)
            if isinstance(vix_alt, dict) and vix_alt.get("VIX"):
                quotes_by_symbol[vix_symbol] = vix_alt["VIX"]
        except Exception:
            pass

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
                anchor_session_day=_market_pulse_rows_session_day(rows),
            )
        except Exception:
            prior_rows = []
        prior_points = _market_pulse_rows_to_points(prior_rows)
        cached_replay_points: List[Dict[str, Any]] = []
        cached_replay_day: Optional[str] = None
        if len(prior_points) < 2:
            cached_replay_points, cached_replay_day = _market_pulse_cached_replay_series(symbol)
            today_iso = now_et.date().isoformat()
            if cached_replay_day == today_iso:
                cached_replay_points = []
                cached_replay_day = None
        if len(prior_points) >= 2:
            q["prior_session_series"] = prior_points
            prior_session_day = _market_pulse_rows_session_day(prior_rows)
            if prior_session_day is not None:
                q["prior_session_day"] = prior_session_day.isoformat()
        elif len(cached_replay_points) >= 2:
            q["prior_session_series"] = list(cached_replay_points)
            if cached_replay_day:
                q["prior_session_day"] = cached_replay_day
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
        current_vwap = _market_pulse_series_vwap(rows)
        if current_vwap is not None:
            q["vwap"] = current_vwap
        if not rows:
            fallback_vwap_rows = prior_rows or list(q.get("prior_session_series") or [])
            fallback_vwap = _market_pulse_series_vwap(fallback_vwap_rows)
            if fallback_vwap is not None:
                q["vwap"] = fallback_vwap
            continue
        points = _market_pulse_rows_to_points(rows)
        curve = [float(p["v"]) for p in points]
        if len(curve) >= 8:
            q["mini_series"] = curve
            q["series"] = points
            current_session_day = _market_pulse_rows_session_day(rows)
            if current_session_day is not None:
                _store_market_pulse_replay_series(
                    symbol,
                    session_day=current_session_day,
                    points=points,
                )
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
    if counts["live"] == 0:
        disk_payload = _load_market_pulse_disk_cache()
        if isinstance(cached_payload, dict) and _market_pulse_payload_has_current_symbols(cached_payload):
            fallback = _market_pulse_attach_replay_cache(
                _market_pulse_force_symbol_set(cached_payload)
            )
            fallback["source_label"] = "Massive market feed (cached fallback)"
            fallback["source_note"] = (
                "Massive returned symbols but no usable live prices. Showing last cached snapshot."
            )
            return fallback
        if isinstance(disk_payload, dict) and _market_pulse_payload_has_current_symbols(disk_payload):
            _market_pulse_cache["payload"] = disk_payload
            fallback = _market_pulse_attach_replay_cache(
                _market_pulse_force_symbol_set(disk_payload)
            )
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
    result = _market_pulse_attach_replay_cache(result)
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
    snapshot_status = str(gamma_snapshot.get("snapshot_status") or "").strip().lower()
    freshness_basis = (
        "exchange-native"
        if bool(gamma_snapshot.get("exchange_timestamp_available"))
        else "fetch-time proxy"
    )
    asof_raw = str(
        gamma_snapshot.get("source_effective_timestamp") or gamma_snapshot.get("asof") or ""
    ).strip()
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
    gamma_spot = gamma_snapshot.get("spot_price_used", gamma_snapshot.get("spot"))
    gamma_spot_value = None
    try:
        gamma_spot_value = float(gamma_spot) if gamma_spot is not None else None
    except Exception:
        gamma_spot_value = None
    spot_value = spx.get("price")
    live_spot_ts = _parse_iso_et(spx.get("asof") or spx.get("as_of"))
    gamma_spot_ts = _parse_iso_et(gamma_snapshot.get("spot_source_timestamp"))
    spot_mismatch = (
        isinstance(spot_value, (int, float))
        and gamma_spot_value is not None
        and abs(float(spot_value) - float(gamma_spot_value)) > GAMMA_SPOT_MISMATCH_POINTS_THRESHOLD
    )
    timestamp_mismatch = (
        live_spot_ts is not None
        and gamma_spot_ts is not None
        and abs(int((live_spot_ts - gamma_spot_ts).total_seconds()))
        > GAMMA_SPOT_TIMESTAMP_DRIFT_SECONDS
    )
    stale_flags = {str(flag) for flag in (gamma_snapshot.get("stale_flags") or [])}
    warning = ""
    if snapshot_status == "invalid":
        tone = "critical"
        warning = "Invalid snapshot"
    elif snapshot_status == "stale":
        tone = "warn"
        warning = "Stale snapshot"
    elif snapshot_status == "degraded":
        tone = "warn"
        warning = "Degraded gamma basket"
    elif gamma_status == "error":
        tone = "critical"
        warning = "Gamma refresh error"
    elif spot_mismatch:
        tone = "critical"
        warning = "Spot source mismatch"
    elif timestamp_mismatch:
        tone = "warn"
        warning = "Spot timestamp drift"
    elif "stale_gamma_source" in stale_flags:
        tone = "critical"
        warning = "Gamma source stale"
    elif "missing_expiries" in stale_flags:
        tone = "warn"
        warning = "Next expiry missing"
    elif "no_valid_gamma_flip" in stale_flags:
        tone = "warn"
        warning = "No valid gamma flip"
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
            f"{snapshot_status.title() if snapshot_status else 'Unknown'} snapshot · "
            f"{'Live spot' if spot_live else 'Non-live spot'} · "
            f"Gamma source age {oi_age} ({freshness_basis}) · {contracts_used} contracts"
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
    baseline_y = ((max_v - values[0]) / (max_v - min_v)) * (height - 2) + 1
    marker_stride = max(1, len(points) // 8)
    marker_points = [
        (idx, point)
        for idx, point in enumerate(points)
        if idx == 0 or idx == len(points) - 1 or idx % marker_stride == 0
    ]
    markers = "".join(
        f'<circle class="marketMiniSparkPoint {cls}'
        f'{" start" if idx == 0 else " end" if idx == len(points) - 1 else ""}" '
        f'cx="{point.split(",")[0]}" '
        f'cy="{point.split(",")[1]}" r="1.55" />'
        for idx, point in marker_points
    )
    return (
        '<svg viewBox="0 0 120 28" class="marketMiniSpark" aria-hidden="true">'
        '<line class="marketMiniSparkGuide" x1="0" y1="7" x2="120" y2="7" />'
        f'<line class="marketMiniSparkGuide marketMiniSparkBaseline" x1="0" '
        f'y1="{baseline_y:.2f}" x2="120" y2="{baseline_y:.2f}" />'
        '<line class="marketMiniSparkGuide" x1="0" y1="21" x2="120" y2="21" />'
        f'<polygon class="marketMiniSparkArea {cls}" points="{area_points}" />'
        f'<polyline class="marketMiniSparkLine {cls}" points="{" ".join(points)}" />'
        f"{markers}"
        "</svg>"
    )


def _market_pulse_resolve_sparkline_values(quote: Dict[str, Any]) -> List[float]:
    values: List[float] = []
    for source_key in ("mini_series", "series", "prior_session_series"):
        source = quote.get(source_key)
        if not isinstance(source, list):
            continue
        source_values: List[float] = []
        for item in source[-120:]:
            value = item.get("v") if isinstance(item, dict) else item
            if isinstance(value, (int, float)):
                source_values.append(float(value))
        if len(source_values) >= 4:
            values = source_values[-40:]
            break

    if len(values) >= 4:
        return values

    symbol = str(quote.get("label") or quote.get("symbol") or "").strip().upper()
    if not symbol:
        return values
    replay_points, _replay_day = _market_pulse_cached_replay_series(symbol)
    replay_values = [
        float(point.get("v"))
        for point in replay_points[-120:]
        if isinstance(point, dict) and isinstance(point.get("v"), (int, float))
    ]
    return replay_values[-40:] if len(replay_values) >= 8 else values


def _market_pulse_tape_state(symbol: str, pct_change: Any) -> Dict[str, str]:
    ticker = str(symbol or "").strip().upper()
    try:
        pct = float(pct_change) if pct_change is not None else None
    except (TypeError, ValueError):
        pct = None
    if pct is not None and not math.isfinite(pct):
        pct = None
    if ticker in {"SPX", "SPY", "QQQ", "IWM"}:
        if pct is not None and pct >= 0.35:
            return {
                "label": "RISK-ON",
                "display": "Risk-On",
                "tone": "positive",
                "title": "Broad tape supports long risk.",
            }
        if pct is not None and pct <= -0.35:
            return {
                "label": "RISK-OFF",
                "display": "Risk-Off",
                "tone": "negative",
                "title": "Broad tape is defensive; long risk needs extra confirmation.",
            }
    if pct is not None and pct >= 0.75:
        return {
            "label": "STRONG",
            "display": "Strong",
            "tone": "positive",
            "title": "Symbol is leading or showing strong upside pressure.",
        }
    if pct is not None and pct <= -0.75:
        return {
            "label": "WEAK",
            "display": "Weak",
            "tone": "negative",
            "title": "Symbol is lagging or under downside pressure.",
        }
    return {
        "label": "MIXED",
        "display": "Mixed",
        "tone": "neutral",
        "title": "No clean tape edge yet.",
    }


def _market_data_age_label(age_s: Any) -> str:
    try:
        age = max(0, int(float(age_s or 0)))
    except (TypeError, ValueError):
        age = 0
    if age >= 72 * 3600:
        return "72h+ old"
    if age >= 3600:
        return f"{age // 3600}h old"
    if age >= 60:
        return f"{age // 60}m old"
    return f"{age}s old"


def _market_pulse_enrich_quotes(
    quotes: List[Dict[str, Any]], now_et: datetime
) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    now_epoch = int(now_et.timestamp())
    market_open = _market_pulse_market_hours(now_et)
    for row in quotes:
        if not isinstance(row, dict):
            continue
        q = dict(row)
        state = str(q.get("data_state") or "missing").lower()
        asof_epoch = int(q.get("asof_epoch") or 0) if str(q.get("asof_epoch") or "").strip() else 0
        age_s = max(0, now_epoch - asof_epoch) if asof_epoch else 999999
        age_label = _market_data_age_label(age_s)
        if state == "missing":
            band = "critical"
            fresh_label = "No live data"
        elif not market_open and age_s <= 72 * 3600:
            band = "warn"
            if state == "delayed":
                fresh_label = "Closed · delayed fallback"
            elif state == "cached":
                fresh_label = f"Closed · last snapshot {age_label}"
            else:
                fresh_label = f"Closed · last quote {age_label}"
        elif state == "delayed":
            band = "warn"
            fresh_label = "Delayed fallback"
        elif state == "cached":
            band = "critical"
            fresh_label = "Cached snapshot"
        elif age_s <= 60:
            band = "live"
            fresh_label = f"Live · {age_label}"
        elif age_s <= 180:
            band = "warn"
            fresh_label = f"Stale · {age_label}"
        else:
            band = "critical"
            fresh_label = f"Critical · {age_label}"
        q["freshness_band"] = band
        q["freshness_age_s"] = age_s
        q["freshness_label"] = fresh_label
        if not market_open and state != "missing":
            q["market_state"] = "Closed"
        q["freshness_reason"] = str(q.get("data_reason") or "")
        source_badge = _quote_source_badge(q)
        q["source_badge_label"] = source_badge["label"]
        q["source_badge_tone"] = source_badge["tone"]
        tape_state = _market_pulse_tape_state(
            str(q.get("label") or q.get("symbol") or ""),
            q.get("change_pct") if q.get("change_pct") is not None else q.get("pct_change"),
        )
        q["watch_state"] = tape_state["display"]
        q["watch_state_label"] = tape_state["label"]
        q["watch_tone"] = tape_state["tone"]
        q["watch_state_title"] = tape_state["title"]

        mini = _market_pulse_resolve_sparkline_values(q)
        if len(mini) >= 4:
            q["mini_series"] = mini[-40:]
            q["series"] = [
                {"v": float(value), "close": float(value)}
                for value in mini[-40:]
                if isinstance(value, (int, float))
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
    if warn and all(str(q.get("freshness_label") or "").startswith("Closed ·") for q in warn):
        return {
            "show": True,
            "tone": "warn",
            "message": "Closed-session quotes loaded. Use this for planning only until live data resumes.",
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


def _market_pulse_session_phase(now_et: datetime) -> str:
    if int(now_et.weekday()) >= 5:
        return "closed"
    minute_of_day = (int(now_et.hour) * 60) + int(now_et.minute)
    if (9 * 60 + 30) <= minute_of_day < (16 * 60):
        return "open"
    if (4 * 60) <= minute_of_day < (9 * 60 + 30):
        return "premarket"
    if (16 * 60) <= minute_of_day < (20 * 60):
        return "afterhours"
    return "closed"


def _market_pulse_points_session_day(points: List[Dict[str, Any]]) -> Optional[date]:
    for row in reversed(points):
        if not isinstance(row, dict):
            continue
        ts = str(row.get("ts") or "").strip()
        if not ts:
            continue
        try:
            parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=app_runtime.TZ)
            return parsed.astimezone(app_runtime.TZ).date()
        except Exception:
            continue
    return None


def _market_pulse_last_completed_trading_session(anchor_day: date) -> Optional[date]:
    cursor = anchor_day
    for _ in range(14):
        if _is_market_session(cursor):
            return cursor
        cursor -= timedelta(days=1)
    return None


def _market_pulse_expected_replay_session_day(*, phase: str, now_et: datetime) -> Optional[date]:
    anchor = now_et.date()
    if phase != "afterhours":
        anchor -= timedelta(days=1)
    return _market_pulse_last_completed_trading_session(anchor)


def _market_pulse_resolved_session_day(
    *,
    mode: str,
    phase: str,
    points: List[Dict[str, Any]],
    now_et: datetime,
) -> Optional[date]:
    point_day = _market_pulse_points_session_day(points)
    if point_day is not None:
        return point_day
    if mode == "live_session":
        return now_et.date()
    if mode == "last_session_replay":
        return _market_pulse_expected_replay_session_day(phase=phase, now_et=now_et)
    return None


def _market_pulse_resolve_execution_series(
    *,
    symbol: str,
    phase: str,
    now_et: datetime,
    current_points: List[Dict[str, Any]],
    replay_points: List[Dict[str, Any]],
    replay_session_day: Optional[date],
) -> Dict[str, Any]:
    return _market_pulse_resolve_replay_session(
        symbol=symbol,
        phase=phase,
        now_et=now_et,
        current_points=current_points,
        replay_points=replay_points,
        replay_session_day=replay_session_day,
    )


def _market_pulse_execution_session_label(
    points: List[Dict[str, Any]], now_et: datetime, session_day: Optional[date] = None
) -> str:
    effective_day = session_day or _market_pulse_points_session_day(points)
    if effective_day is None:
        return ""
    return effective_day.strftime("%a %b %-d")


def _market_pulse_execution_replay_summary(
    *,
    points: List[Dict[str, Any]],
    levels: Dict[str, Any],
    regime_positive: bool,
) -> str:
    def _num(value: Any) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except Exception:
            return None

    if len(points) < 2:
        return "Review the loaded replay session against gamma structure."

    open_price = _num(points[0].get("price"))
    last_price = _num(points[-1].get("price"))
    price_values = [_num(point.get("price")) for point in points]
    price_values = [value for value in price_values if value is not None]
    if not price_values:
        return "Review the loaded replay session against gamma structure."
    high_price = max(price_values)
    low_price = min(price_values)
    flip = _num(levels.get("gamma_flip"))
    call_wall = _num(levels.get("call_wall"))
    put_wall = _num(levels.get("put_wall"))

    opener = "Opened"
    if flip is not None and open_price is not None:
        if open_price >= flip:
            opener = "Opened above flip"
        else:
            opener = "Opened below flip"

    bias = "mean reversion stayed intact" if regime_positive else "expansion stayed active"
    if flip is not None and low_price <= flip <= high_price:
        if last_price is not None and last_price >= flip:
            bias = "reclaimed flip and closed back above"
        elif last_price is not None and last_price < flip:
            bias = "failed reclaim and closed below flip"

    wall_note = ""
    if call_wall is not None and high_price >= call_wall:
        wall_note = "tested call wall"
    elif put_wall is not None and low_price <= put_wall:
        wall_note = "tested put wall"
    elif flip is not None:
        flip_distance = abs((last_price or open_price or flip) - flip)
        wall_note = "held near flip" if flip_distance <= 8 else "stayed away from extremes"

    summary_parts = [opener]
    if regime_positive:
        summary_parts.append("positive gamma held")
    else:
        summary_parts.append("negative gamma stayed active")
    if wall_note:
        summary_parts.append(wall_note)
    if bias:
        summary_parts.append(bias)
    return ", ".join(summary_parts[:3]) + "."


def _market_pulse_execution_chart_viewmodel(
    *,
    spx_quote: Dict[str, Any],
    gamma_snapshot: Dict[str, Any],
    macro_events: List[Dict[str, Any]],
    now_et: datetime,
    resolved_spot: Any = None,
    resolved_levels: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    raw_points = list(spx_quote.get("series") or [])
    raw_replay_points = list(spx_quote.get("prior_session_series") or [])

    def _normalize_chart_points(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        points: List[Dict[str, Any]] = []
        seen_ts: set[str] = set()
        for row in rows[-420:]:
            if not isinstance(row, dict):
                continue
            ts = str(row.get("ts") or "").strip()
            price = row.get("v", row.get("close"))
            if not ts or not isinstance(price, (int, float)) or ts in seen_ts:
                continue
            seen_ts.add(ts)
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(app_runtime.TZ)
            except Exception:
                continue
            points.append(
                {
                    "ts": ts,
                    "label": dt.strftime("%-I:%M"),
                    "price": float(price),
                    "volume": (
                        int(row.get("volume"))
                        if isinstance(row.get("volume"), (int, float))
                        else int(row.get("vol")) if isinstance(row.get("vol"), (int, float)) else 0
                    ),
                }
            )
        points.sort(key=lambda row: str(row.get("ts") or ""))
        return points

    current_points = _normalize_chart_points(raw_points)
    replay_points = _normalize_chart_points(raw_replay_points)
    replay_session_day = None
    replay_day_raw = str(spx_quote.get("prior_session_day") or "").strip()
    if replay_day_raw:
        try:
            replay_session_day = date.fromisoformat(replay_day_raw)
        except Exception:
            replay_session_day = None
    resolved = _market_pulse_resolve_execution_series(
        symbol=str(spx_quote.get("symbol") or spx_quote.get("label") or "SPX"),
        phase=_market_pulse_session_phase(now_et),
        now_et=now_et,
        current_points=current_points,
        replay_points=replay_points,
        replay_session_day=replay_session_day,
    )
    mode = str(resolved.get("mode") or "unavailable")
    points = list(resolved.get("points") or [])
    resolved_session_day = resolved.get("session_day")
    if not isinstance(resolved_session_day, date):
        resolved_session_day = None
    last_valid_day = resolved.get("last_valid_day")
    if not isinstance(last_valid_day, date):
        last_valid_day = _market_pulse_expected_replay_session_day(
            phase=_market_pulse_session_phase(now_et),
            now_et=now_et,
        )
    stored_replay_day = resolved.get("stored_replay_day")
    if not isinstance(stored_replay_day, date):
        stored_replay_day = replay_session_day
    archive_rows = _market_pulse_replay_archive_rows(
        symbol=str(spx_quote.get("symbol") or spx_quote.get("label") or "SPX"),
        last_valid_day=last_valid_day,
        stored_replay_day=stored_replay_day,
        stored_points=replay_points,
    )
    latest_archive_row = archive_rows[0] if archive_rows else {}
    latest_replay_available = bool(latest_archive_row.get("available"))

    # Keep local reference to the active session phase for labels and client handoff.
    phase = _market_pulse_session_phase(now_et)
    resolved_levels = dict(resolved_levels or {})
    local_flip_value = (
        resolved_levels.get("local_flip")
        if resolved_levels.get("local_flip") is not None
        else gamma_snapshot.get("local_flip_aggregated_gamma")
    )

    levels = {
        "gamma_flip": (
            resolved_levels.get("main_flip")
            if resolved_levels.get("main_flip") is not None
            else gamma_snapshot.get("gamma_flip_combined_basket")
        ),
        "local_flip": local_flip_value,
        "call_wall": (
            resolved_levels.get("call_wall")
            if resolved_levels.get("call_wall") is not None
            else gamma_snapshot.get("call_wall_aggregated_gamma")
        ),
        "put_wall": (
            resolved_levels.get("put_wall")
            if resolved_levels.get("put_wall") is not None
            else gamma_snapshot.get("put_wall_aggregated_gamma")
        ),
        "pdh": spx_quote.get("prior_day_high"),
        "pdl": spx_quote.get("prior_day_low"),
    }
    level_rows = []
    for key, label in (
        ("gamma_flip", "Gamma Flip"),
        ("local_flip", "Local Flip"),
        ("call_wall", "Call Wall"),
        ("put_wall", "Put Wall"),
        ("pdh", "PDH"),
        ("pdl", "PDL"),
    ):
        value = levels.get(key)
        if isinstance(value, (int, float)):
            level_rows.append({"key": key, "label": label, "value": float(value)})

    session_day = resolved_session_day or _market_pulse_resolved_session_day(
        mode=mode,
        phase=phase,
        points=points,
        now_et=now_et,
    )
    event_rows: List[Dict[str, Any]] = []
    for row in list(macro_events or [])[:5]:
        if not isinstance(row, dict):
            continue
        starts_at = str(row.get("starts_at") or row.get("iso") or "").strip()
        if not starts_at:
            continue
        try:
            dt = datetime.fromisoformat(starts_at.replace("Z", "+00:00")).astimezone(app_runtime.TZ)
        except Exception:
            continue
        if session_day is not None and dt.date() != session_day:
            continue
        event_rows.append(
            {
                "ts": dt.isoformat(),
                "label": str(row.get("time_label") or dt.strftime("%-I:%M %p")),
                "headline": str(row.get("headline") or "Macro"),
            }
        )

    latest_price = None
    point_prices = [
        float(point.get("price"))
        for point in points
        if isinstance(point, dict) and isinstance(point.get("price"), (int, float))
    ]
    if point_prices:
        latest_price = point_prices[-1]
    elif isinstance(resolved_spot, (int, float)):
        latest_price = float(resolved_spot)
    elif isinstance(spx_quote.get("price"), (int, float)):
        latest_price = float(spx_quote.get("price"))

    regime_text = str(gamma_snapshot.get("regime") or "").strip()
    net_gamma = gamma_snapshot.get("net_gex")
    regime_positive = False
    if regime_text:
        regime_positive = "positive" in regime_text.lower()
    elif isinstance(net_gamma, (int, float)):
        regime_positive = float(net_gamma) >= 0
    environment = "Mean Reversion" if regime_positive else "Expansion"
    regime_label = regime_text or ("Positive Gamma" if regime_positive else "Negative Gamma")
    session_label = _market_pulse_execution_session_label(points, now_et, session_day)
    session_date_iso = session_day.isoformat() if session_day is not None else ""
    replay_caption = f"Replay • {session_label}" if session_label else "Replay"
    last_valid_label = (
        last_valid_day.strftime("%a %b %-d") if isinstance(last_valid_day, date) else ""
    )
    last_stored_replay_label = (
        stored_replay_day.strftime("%a %b %-d") if isinstance(stored_replay_day, date) else ""
    )
    replay_gap_note = ""
    replay_status_label = (
        "Replay stored"
        if mode == "last_session_replay" and session_label and last_valid_label == session_label
        else "Replay not stored"
    )
    if last_valid_label and mode != "last_session_replay":
        replay_gap_note = f"Replay not stored for {last_valid_label}."
    distance_to_flip = None
    flip_value = levels.get("gamma_flip")
    if isinstance(latest_price, (int, float)) and isinstance(flip_value, (int, float)):
        distance_to_flip = float(latest_price) - float(flip_value)
    if mode == "live_session":
        status_label = (
            f"Live Session • {str(spx_quote.get('freshness_label') or 'Updated just now')}"
        )
        summary = "Current intraday SPX execution map with live gamma reaction levels."
        price_label = "Live Price"
        context_label = "Live Structure"
    elif mode == "last_session_replay":
        prefix = (
            "Awaiting Open • Last Valid Session" if phase == "premarket" else "Last Valid Session"
        )
        primary_label = last_valid_label or session_label
        status_label = f"{prefix} • {primary_label}" if primary_label else prefix
        summary = _market_pulse_execution_replay_summary(
            points=points,
            levels=levels,
            regime_positive=regime_positive,
        )
        price_label = "Last Session Close"
        context_label = "Last Known Structure"
    else:
        unavailable_label = (
            last_valid_label
            or session_label
            or _market_pulse_execution_session_label(
                [],
                now_et,
                _market_pulse_expected_replay_session_day(phase=phase, now_et=now_et),
            )
        )
        status_label = (
            f"No Session Data Available • {unavailable_label}"
            if unavailable_label
            else "No Session Data Available"
        )
        summary = (
            f"Replay not stored for {unavailable_label}. Live structure remains available."
            if unavailable_label
            else "No valid intraday SPX session data is available yet."
        )
        price_label = "Reference Price"
        context_label = "Unavailable"
        replay_caption = (
            f"Replay unavailable • {unavailable_label}"
            if unavailable_label
            else "Replay unavailable"
        )

    return {
        "symbol": "SPX",
        "phase": phase,
        "mode": mode,
        "session_date": session_date_iso,
        "session_label": session_label,
        "primary_session_label": last_valid_label or session_label,
        "last_valid_session_date": (
            last_valid_day.isoformat() if isinstance(last_valid_day, date) else ""
        ),
        "last_valid_session_label": last_valid_label,
        "last_stored_replay_date": (
            stored_replay_day.isoformat() if isinstance(stored_replay_day, date) else ""
        ),
        "last_stored_replay_label": last_stored_replay_label,
        "replay_gap_note": replay_gap_note,
        "replay_status_label": replay_status_label,
        "latest_replay_available": latest_replay_available,
        "replay_source": str(resolved.get("replay_source") or ""),
        "archive_rows": archive_rows,
        "status_label": status_label,
        "summary": summary,
        "replay_caption": replay_caption,
        "archive_summary": (
            f"Last valid {last_valid_label} • replay stored"
            if latest_replay_available and last_valid_label
            else (
                f"Last valid {last_valid_label} • replay not stored"
                if last_valid_label
                else "Replay archive unavailable"
            )
        ),
        "price_label": price_label,
        "context_label": context_label,
        "points": points,
        "levels": level_rows,
        "events": event_rows,
        "latest_price": latest_price,
        "distance_to_flip": distance_to_flip,
        "freshness_label": str(spx_quote.get("freshness_label") or ""),
        "regime": regime_label,
        "environment": environment,
    }


def _market_news_timestamp_label(stamp: Any) -> str:
    if not isinstance(stamp, (int, float)):
        return ""
    return datetime.fromtimestamp(int(stamp), tz=app_runtime.TZ).strftime("%b %-d, %-I:%M %p ET")


def _market_pulse_level_value(rows: List[Dict[str, Any]], key: str) -> Optional[float]:
    for row in rows:
        if str(row.get("key") or "") != key:
            continue
        value = row.get("value")
        if isinstance(value, (int, float)):
            return float(value)
    return None


def _market_pulse_clamp(value: float, minimum: float, maximum: float) -> float:
    return min(maximum, max(minimum, float(value)))


def _market_pulse_score_grade(score: int) -> str:
    if score >= 93:
        return "A"
    if score >= 88:
        return "A-"
    if score >= 80:
        return "B"
    if score >= 72:
        return "B-"
    if score >= 64:
        return "C"
    if score >= 56:
        return "C-"
    if score >= 48:
        return "D"
    return "F"


def _market_pulse_score_status(score: int) -> str:
    if score >= 80:
        return "GO"
    if score >= 60:
        return "WATCH"
    if score >= 40:
        return "CAUTION"
    return "NO TRADE"


def _market_pulse_tone_for_status(status: str) -> str:
    if status == "GO":
        return "positive"
    if status == "WATCH":
        return "warn"
    if status == "CAUTION":
        return "warn"
    return "negative"


def _market_pulse_execution_model(
    *,
    spx_quote: Dict[str, Any],
    gamma_snapshot: Dict[str, Any],
    execution_chart: Dict[str, Any],
    spx_priority_context: Dict[str, Any],
) -> Dict[str, Any]:
    spot = (
        float(spx_quote.get("price"))
        if isinstance(spx_quote.get("price"), (int, float))
        else (
            float(execution_chart.get("latest_price"))
            if isinstance(execution_chart.get("latest_price"), (int, float))
            else None
        )
    )
    main_flip = _market_pulse_level_value(list(execution_chart.get("levels") or []), "gamma_flip")
    local_flip = _market_pulse_level_value(list(execution_chart.get("levels") or []), "local_flip")
    call_wall = _market_pulse_level_value(list(execution_chart.get("levels") or []), "call_wall")
    put_wall = _market_pulse_level_value(list(execution_chart.get("levels") or []), "put_wall")
    raw_net_gamma = (
        float(gamma_snapshot.get("net_gex"))
        if isinstance(gamma_snapshot.get("net_gex"), (int, float))
        else (
            float(gamma_snapshot.get("net_gex_total"))
            if isinstance(gamma_snapshot.get("net_gex_total"), (int, float))
            else None
        )
    )
    local_flip_available = bool(gamma_snapshot.get("local_flip_found")) and isinstance(
        local_flip, (int, float)
    )
    metrics = dict(spx_priority_context.get("metrics") or {})
    structure_range_state = str(metrics.get("trap_zone_state") or "").strip() or "unavailable"
    values = [
        value
        for value in (spot, main_flip, local_flip, call_wall, put_wall)
        if isinstance(value, (int, float))
    ]
    structure_span = (max(values) - min(values)) if len(values) >= 2 else 0.0
    wall_span = (
        abs(float(call_wall) - float(put_wall))
        if isinstance(call_wall, (int, float)) and isinstance(put_wall, (int, float))
        else structure_span
    )
    neutral_band_main = _market_pulse_clamp(max(5.0, wall_span * 0.08), 5.0, 18.0)
    neutral_band_local = _market_pulse_clamp(max(2.5, wall_span * 0.025), 2.5, 6.0)

    if spot is not None and main_flip is not None:
        if spot > (main_flip + neutral_band_main):
            macro_state = "positive"
            macro_title = "POSITIVE GAMMA"
            macro_subtitle = "MEAN REVERSION ACTIVE"
            macro_band_state = "above_main_flip"
        elif spot < (main_flip - neutral_band_main):
            macro_state = "negative"
            macro_title = "NEGATIVE GAMMA"
            macro_subtitle = "TREND / MOMENTUM ACTIVE"
            macro_band_state = "below_main_flip"
        else:
            macro_state = "neutral"
            macro_title = "FLIP ZONE"
            macro_subtitle = "NO CLEAR EDGE"
            macro_band_state = "at_main_flip"
    else:
        macro_state = "unknown"
        macro_title = "REGIME UNKNOWN"
        macro_subtitle = "LEVELS UNAVAILABLE"
        macro_band_state = "unknown"

    if spot is not None and local_flip is not None:
        if spot > (local_flip + neutral_band_local):
            local_state = "above_local"
            local_title = "ABOVE LOCAL FLIP"
            local_action = "BUY DIPS / HOLD ABOVE LOCAL PIVOT"
            local_short = "BUY DIPS"
            local_context = "ABOVE LOCAL FLIP"
        elif spot < (local_flip - neutral_band_local):
            local_state = "below_local"
            local_title = "BELOW LOCAL FLIP"
            local_action = "SELL RIPS / REJECT POPS"
            local_short = "SELL RIPS"
            local_context = "BELOW LOCAL FLIP"
        else:
            local_state = "at_local"
            local_title = "AT LOCAL FLIP"
            local_action = "WAIT FOR RESOLUTION"
            local_short = "WAIT"
            local_context = "AT LOCAL FLIP"
    else:
        local_state = "unknown"
        if (
            gamma_snapshot.get("snapshot_status") in {"healthy", "degraded", "stale"}
            and not local_flip_available
        ):
            local_title = "NONE IN LOCAL BAND"
            local_action = "WAIT FOR NEW LOCAL SIGN CHANGE"
            local_short = "NO BAND"
            local_context = "NONE IN LOCAL BAND"
        else:
            local_title = "LOCAL FLIP UNKNOWN"
            local_action = "WAIT FOR LOCAL PIVOT"
            local_short = "WAIT"
            local_context = "LOCAL FLIP UNKNOWN"

    distances = {
        "to_main_flip": (spot - main_flip) if spot is not None and main_flip is not None else None,
        "to_local_flip": (
            (spot - local_flip) if spot is not None and local_flip is not None else None
        ),
        "to_call_wall": (spot - call_wall) if spot is not None and call_wall is not None else None,
        "to_put_wall": (spot - put_wall) if spot is not None and put_wall is not None else None,
    }

    levels_for_nearest = [
        ("Main Flip", main_flip),
        ("Local Flip", local_flip),
        ("Call Wall", call_wall),
        ("Put Wall", put_wall),
    ]
    nearest_name = ""
    nearest_value = None
    nearest_distance = None
    if spot is not None:
        for label, value in levels_for_nearest:
            if not isinstance(value, (int, float)):
                continue
            distance = abs(float(spot) - float(value))
            if nearest_distance is None or distance < nearest_distance:
                nearest_name = label
                nearest_value = float(value)
                nearest_distance = distance

    inside_walls = (
        spot is not None
        and call_wall is not None
        and put_wall is not None
        and put_wall <= spot <= call_wall
    )
    near_main_flip = (
        nearest_distance is not None
        and nearest_name == "Main Flip"
        and nearest_distance <= neutral_band_main
    )
    near_local_flip = (
        nearest_distance is not None
        and nearest_name == "Local Flip"
        and nearest_distance <= neutral_band_local
    )
    near_call_wall = (
        nearest_distance is not None and nearest_name == "Call Wall" and nearest_distance <= 14.0
    )
    near_put_wall = (
        nearest_distance is not None and nearest_name == "Put Wall" and nearest_distance <= 14.0
    )
    midrange = (
        inside_walls
        and nearest_distance is not None
        and nearest_distance > max(neutral_band_local * 1.5, 22.0)
    )

    if spot is None:
        location_summary = "STRUCTURE UNKNOWN"
        zone_label = "Unknown"
        status_line = "Awaiting price"
        action_read = "Wait for live structure"
    elif put_wall is not None and spot < put_wall:
        location_summary = "BELOW PUT WALL"
        zone_label = "Below Put Wall"
        status_line = "Support nearby"
        action_read = "Wait for support reclaim"
    elif call_wall is not None and spot > call_wall:
        location_summary = "ABOVE CALL WALL"
        zone_label = "Above Call Wall"
        status_line = "Resistance failed"
        action_read = "Avoid chasing extension"
    elif near_local_flip:
        location_summary = "AT LOCAL FLIP"
        zone_label = "Flip Decision Zone"
        status_line = "Decision zone"
        action_read = "Wait for local flip resolution"
    elif near_call_wall:
        location_summary = "NEAR CALL WALL"
        zone_label = "Near Call Wall"
        status_line = "Resistance nearby"
        action_read = "Sell rips near resistance"
    elif near_put_wall:
        location_summary = "NEAR PUT WALL"
        zone_label = "Near Put Wall"
        status_line = "Support nearby"
        action_read = "Buy dips near support"
    elif inside_walls and macro_band_state == "below_main_flip":
        location_summary = "BELOW MAIN FLIP INSIDE RANGE"
        zone_label = "Inside Range / Below Main Flip"
        status_line = "Range with overhead pressure"
        action_read = "Sell rips below main flip"
    elif inside_walls and macro_band_state == "above_main_flip":
        location_summary = "ABOVE MAIN FLIP INSIDE RANGE"
        zone_label = "Inside Range / Above Main Flip"
        status_line = "Supportive range"
        action_read = "Buy dips above main flip"
    elif inside_walls:
        location_summary = "INSIDE RANGE"
        zone_label = "Inside Range"
        status_line = "Neutral range"
        action_read = "Avoid mid-range entries"
    else:
        location_summary = "BETWEEN LEVELS"
        zone_label = "Between Levels"
        status_line = "Mixed location"
        action_read = "Wait for cleaner location"

    macro_local_conflict = bool(
        (macro_state == "positive" and local_state == "below_local")
        or (macro_state == "negative" and local_state == "above_local")
    )
    net_gamma_sign_conflict = bool(
        raw_net_gamma is not None
        and macro_state in {"positive", "negative"}
        and (
            (raw_net_gamma >= 0 and macro_state == "negative")
            or (raw_net_gamma < 0 and macro_state == "positive")
        )
    )

    score = 50
    if macro_state in {"positive", "negative"}:
        score += 20
    elif macro_state == "neutral":
        score -= 25
    else:
        score -= 15

    if local_state in {"above_local", "below_local"}:
        score += 25
    elif local_state == "at_local":
        score -= 18
    else:
        score -= 18

    if macro_local_conflict:
        score -= 22
    elif macro_state == "positive" and local_state == "above_local":
        score += 12
    elif macro_state == "negative" and local_state == "below_local":
        score += 12

    if midrange:
        score -= 20
    if near_main_flip:
        score -= 12
    if near_local_flip:
        score += 8
    if near_call_wall or near_put_wall:
        score += 12
    if macro_local_conflict and inside_walls:
        score -= 10
    if nearest_distance is not None and nearest_distance > 140:
        score -= 10
    if structure_range_state in {"knife_edge_structure", "compressed_trap_zone"}:
        score -= 10
    if execution_chart.get("mode") == "unavailable":
        score -= 6
    if local_state == "unknown":
        score = min(score, 55)

    score = max(0, min(100, int(round(score))))
    readiness_status = _market_pulse_score_status(score)
    readiness_tone = _market_pulse_tone_for_status(readiness_status)
    grade = _market_pulse_score_grade(score)

    if readiness_status == "NO TRADE":
        if local_state == "unknown":
            best_look = "Wait for local flip to print"
            avoid = "Trading without a tactical pivot"
            need = "Live local flip or usable intraday anchor"
            why = "Macro context exists, but the intraday local flip is unavailable."
        elif local_state == "at_local" or macro_state == "neutral":
            best_look = "Wait for local flip resolution"
            avoid = "Do not trade the flip zone"
            need = "Clear break or reclaim"
            why = "Macro and tactical context are not resolved."
        elif midrange:
            best_look = "Wait for wall interaction"
            avoid = "Midrange guessing inside structure"
            need = "Touch or rejection at a real level"
            why = "Price is too far from a clean trigger level."
        else:
            best_look = "Wait for better alignment"
            avoid = "Forcing entries without alignment"
            need = "Macro and local bias to agree"
            why = "Context is conflicted or not close enough to a usable level."
    elif readiness_status == "CAUTION":
        if local_state == "unknown":
            best_look = "Wait for local pivot or wall test"
            avoid = "Committing before tactical structure appears"
            need = "Local flip or clear wall reaction"
            why = "Macro context is present, but tactical intraday bias is still unknown."
        elif macro_local_conflict:
            best_look = "Wait for local flip reclaim or rejection"
            avoid = "Trading against the local pivot"
            need = "Tactical bias to align with regime"
            why = "Macro and tactical reads still conflict."
        else:
            best_look = action_read
            avoid = "Late entries away from levels"
            need = "Tighter proximity to key structure"
            why = "There is context, but not enough location quality yet."
    elif readiness_status == "WATCH":
        if macro_state == "positive" and local_state == "above_local":
            best_look = "Buy dip above local flip"
            avoid = "Chasing extension into resistance"
            need = "Hold above local flip or support"
            why = "Macro and local structure are supportive, but trigger quality still matters."
        elif macro_state == "negative" and local_state == "below_local":
            best_look = "Sell rip below local flip"
            avoid = "Shorting directly into put wall"
            need = "Failed reclaim or rejection"
            why = "Macro and local weakness align, but location still matters."
        elif macro_state == "positive" and local_state == "below_local":
            best_look = "Wait for local flip reclaim"
            avoid = "Buying weak structure below local pivot"
            need = "Reclaim and hold above local flip"
            why = "Macro is supportive, but tactical bias remains weak."
        else:
            best_look = action_read
            avoid = "Low-quality midrange entries"
            need = "Confirmation at the nearest level"
            why = "Usable context exists, but it is not a clean go yet."
    else:
        if macro_state == "negative" and local_state == "below_local":
            best_look = "Sell rip into failed local reclaim"
            avoid = "Shorting into put wall support"
            need = "Rejection candle or lower high"
            why = "Negative macro and bearish tactical bias are aligned."
        elif macro_state == "positive" and local_state == "above_local":
            best_look = "Buy dip above local flip"
            avoid = "Buying straight into call wall"
            need = "Pullback hold or reclaim"
            why = "Positive macro and bullish tactical bias are aligned."
        else:
            best_look = action_read
            avoid = "Trading before confirmation"
            need = "Execution trigger at the nearest level"
            why = "The board is aligned enough to act once the trigger prints."

    posture_parts = []
    if macro_state == "positive":
        posture_parts.append("Macro positive")
    elif macro_state == "negative":
        posture_parts.append("Macro negative")
    elif macro_state == "neutral":
        posture_parts.append("Flip zone")
    if local_state == "above_local":
        posture_parts.append("local bullish")
    elif local_state == "below_local":
        posture_parts.append("local bearish")
    elif local_state == "at_local":
        posture_parts.append("local undecided")
    elif local_state == "unknown":
        posture_parts.append("local pivot unavailable")
    if inside_walls:
        posture_parts.append("inside range")
    if readiness_status == "NO TRADE":
        posture_parts.append("no clean trigger")
    elif readiness_status == "WATCH":
        posture_parts.append(best_look.lower())

    ladder_rows = [
        {
            "key": "call_wall",
            "label": "Call Wall",
            "short_label": "CW",
            "value": call_wall,
            "distance_points": distances["to_call_wall"],
            "tone": "call",
        },
        {
            "key": "main_flip",
            "label": "Main Flip",
            "short_label": "Main Flip",
            "value": main_flip,
            "distance_points": distances["to_main_flip"],
            "tone": "flip",
        },
        {
            "key": "local_flip",
            "label": "Local Flip",
            "short_label": "Local Flip",
            "value": local_flip,
            "distance_points": distances["to_local_flip"],
            "tone": "local",
        },
        {
            "key": "price",
            "label": "Price",
            "short_label": "Price",
            "value": spot,
            "distance_points": None,
            "tone": "price",
        },
        {
            "key": "put_wall",
            "label": "Put Wall",
            "short_label": "PW",
            "value": put_wall,
            "distance_points": distances["to_put_wall"],
            "tone": "put",
        },
    ]
    ladder_rows = [row for row in ladder_rows if isinstance(row.get("value"), (int, float))]
    ladder_rows.sort(key=lambda row: float(row.get("value") or 0.0), reverse=True)

    structure_bar_rows = [
        row
        for row in (
            {"key": "put_wall", "label": "PW", "value": put_wall, "tone": "put"},
            {"key": "main_flip", "label": "Main Flip", "value": main_flip, "tone": "flip"},
            {"key": "local_flip", "label": "Local Flip", "value": local_flip, "tone": "local"},
            {"key": "call_wall", "label": "CW", "value": call_wall, "tone": "call"},
            {"key": "price", "label": "Price", "value": spot, "tone": "price"},
        )
        if isinstance(row.get("value"), (int, float))
    ]

    return {
        "levels": {
            "spot": spot,
            "main_flip": main_flip,
            "local_flip": local_flip,
            "call_wall": call_wall,
            "put_wall": put_wall,
        },
        "neutral_band_main": neutral_band_main,
        "neutral_band_local": neutral_band_local,
        "raw_net_gamma": raw_net_gamma,
        "structure_range_state": structure_range_state,
        "macro_regime": {
            "state": macro_state,
            "title": macro_title,
            "subtitle": macro_subtitle,
            "band_state": macro_band_state,
        },
        "local_bias": {
            "state": local_state,
            "title": local_title,
            "action": local_action,
            "label": local_short,
            "context": local_context,
        },
        "location": {
            "summary": location_summary,
            "zone": zone_label,
            "nearest_level_name": nearest_name,
            "nearest_level_value": nearest_value,
            "distance_points": nearest_distance,
            "inside_range": inside_walls,
            "midrange": midrange,
            "status": status_line,
            "read": action_read,
            "bar_rows": structure_bar_rows,
        },
        "playbook": {
            "status": readiness_status,
            "tone": readiness_tone,
            "grade": grade,
            "score": score,
            "score_pct": score,
            "best_look": best_look,
            "avoid": avoid,
            "need": need,
            "why": why,
        },
        "distances": {
            "to_main_flip": distances["to_main_flip"],
            "to_local_flip": distances["to_local_flip"],
            "to_call_wall": distances["to_call_wall"],
            "to_put_wall": distances["to_put_wall"],
        },
        "distance_rows": [
            {
                "key": "main_flip",
                "label": "Main Flip",
                "value": (
                    abs(float(distances["to_main_flip"]))
                    if isinstance(distances["to_main_flip"], (int, float))
                    else None
                ),
                "signed_value": distances["to_main_flip"],
                "pct": (
                    min(
                        100.0,
                        (abs(float(distances["to_main_flip"])) / max(20.0, wall_span)) * 100.0,
                    )
                    if isinstance(distances["to_main_flip"], (int, float))
                    else 0.0
                ),
                "direction": (
                    "up"
                    if isinstance(distances["to_main_flip"], (int, float))
                    and float(distances["to_main_flip"]) > 0
                    else (
                        "down"
                        if isinstance(distances["to_main_flip"], (int, float))
                        and float(distances["to_main_flip"]) < 0
                        else "flat"
                    )
                ),
            },
            {
                "key": "local_flip",
                "label": "Local Flip",
                "value": (
                    abs(float(distances["to_local_flip"]))
                    if isinstance(distances["to_local_flip"], (int, float))
                    else None
                ),
                "signed_value": distances["to_local_flip"],
                "pct": (
                    min(
                        100.0,
                        (abs(float(distances["to_local_flip"])) / max(20.0, wall_span)) * 100.0,
                    )
                    if isinstance(distances["to_local_flip"], (int, float))
                    else 0.0
                ),
                "direction": (
                    "up"
                    if isinstance(distances["to_local_flip"], (int, float))
                    and float(distances["to_local_flip"]) > 0
                    else (
                        "down"
                        if isinstance(distances["to_local_flip"], (int, float))
                        and float(distances["to_local_flip"]) < 0
                        else "flat"
                    )
                ),
            },
            {
                "key": "call_wall",
                "label": "Call Wall",
                "value": (
                    abs(float(distances["to_call_wall"]))
                    if isinstance(distances["to_call_wall"], (int, float))
                    else None
                ),
                "signed_value": distances["to_call_wall"],
                "pct": (
                    min(
                        100.0,
                        (abs(float(distances["to_call_wall"])) / max(20.0, wall_span)) * 100.0,
                    )
                    if isinstance(distances["to_call_wall"], (int, float))
                    else 0.0
                ),
                "direction": (
                    "up"
                    if isinstance(distances["to_call_wall"], (int, float))
                    and float(distances["to_call_wall"]) > 0
                    else (
                        "down"
                        if isinstance(distances["to_call_wall"], (int, float))
                        and float(distances["to_call_wall"]) < 0
                        else "flat"
                    )
                ),
            },
            {
                "key": "put_wall",
                "label": "Put Wall",
                "value": (
                    abs(float(distances["to_put_wall"]))
                    if isinstance(distances["to_put_wall"], (int, float))
                    else None
                ),
                "signed_value": distances["to_put_wall"],
                "pct": (
                    min(
                        100.0, (abs(float(distances["to_put_wall"])) / max(20.0, wall_span)) * 100.0
                    )
                    if isinstance(distances["to_put_wall"], (int, float))
                    else 0.0
                ),
                "direction": (
                    "up"
                    if isinstance(distances["to_put_wall"], (int, float))
                    and float(distances["to_put_wall"]) > 0
                    else (
                        "down"
                        if isinstance(distances["to_put_wall"], (int, float))
                        and float(distances["to_put_wall"]) < 0
                        else "flat"
                    )
                ),
            },
        ],
        "conflicts": {
            "net_gamma_sign_conflict": net_gamma_sign_conflict,
            "macro_local_conflict": macro_local_conflict,
        },
        "posture_summary": " — ".join([part for part in posture_parts if part]),
        "ladder_rows": ladder_rows,
    }


def _market_pulse_snapshot_session_mode(now_et: datetime) -> str:
    phase = _market_pulse_session_phase(now_et)
    if phase == "open":
        return "regular"
    if phase == "afterhours":
        return "after_hours"
    if phase == "premarket":
        return "premarket"
    return "closed"


def _market_pulse_gamma_data_status(
    gamma_snapshot: Dict[str, Any],
    *,
    level_count: int,
) -> str:
    snapshot_status = str(gamma_snapshot.get("snapshot_status") or "").strip().lower()
    build_status = str(gamma_snapshot.get("build_status") or "").strip().lower()
    regime_text = str(gamma_snapshot.get("regime") or "").strip().lower()
    has_regime = regime_text not in {"", "unavailable", "invalid", "unknown"}

    if build_status == "fallback_valid":
        return "fallback_valid" if level_count >= 4 and has_regime else "partial"

    # Treat snapshot health first, then downgrade based on structural completeness.
    if snapshot_status == "healthy":
        return "fresh_valid" if level_count >= 4 and has_regime else "partial"
    if snapshot_status in {"degraded", "stale"}:
        return "stale_but_usable" if level_count >= 3 else "partial"
    if level_count >= 2 or has_regime:
        return "partial"
    return "invalid"


def _market_pulse_gamma_regime_state(
    gamma_snapshot: Dict[str, Any],
    *,
    gamma_data_status: str,
    session_mode: str,
    levels: Dict[str, Any],
    next_call_wall: Any,
    next_put_wall: Any,
) -> Dict[str, Any]:
    regime_text = str(gamma_snapshot.get("regime") or "").strip().lower()
    has_regime = regime_text not in {"", "unavailable", "invalid", "unknown", "unconfirmed"}
    effective_session = "regular" if session_mode == "stale" else session_mode
    spot = _market_pulse_positive_float(
        levels.get("spot")
        if isinstance(levels.get("spot"), (int, float))
        else (
            gamma_snapshot.get("spot_price_used")
            if isinstance(gamma_snapshot.get("spot_price_used"), (int, float))
            else gamma_snapshot.get("spot")
        )
    )
    main_flip = _market_pulse_positive_float(levels.get("main_flip"))
    local_flip = _market_pulse_positive_float(levels.get("local_flip"))
    call_wall = _market_pulse_positive_float(levels.get("call_wall"))
    put_wall = _market_pulse_positive_float(levels.get("put_wall"))
    next_call = _market_pulse_positive_float(next_call_wall)
    next_put = _market_pulse_positive_float(next_put_wall)
    level_count = sum(
        1
        for value in (main_flip, local_flip, call_wall, put_wall, next_call, next_put)
        if value is not None
    )
    has_primary_wall = call_wall is not None or put_wall is not None
    intraday_session = effective_session in {"premarket", "regular"}

    if gamma_data_status == "invalid":
        state = {
            "regime_status": "unavailable",
            "board_status": "Unavailable",
            "confidence": "none",
            "missing_reason": "no_usable_snapshot",
            "level_count": level_count,
        }
    elif gamma_data_status in {"fresh_valid", "fallback_valid", "stale_but_usable"} and has_regime:
        state = {
            "regime_status": "confirmed",
            "board_status": "Complete",
            "confidence": "high",
            "missing_reason": "",
            "level_count": level_count,
        }
    elif not intraday_session:
        state = {
            "regime_status": "unconfirmed",
            "board_status": "Partial",
            "confidence": "low",
            "missing_reason": "waiting_on_intraday_session",
            "level_count": level_count,
        }
    elif not has_regime:
        state = {
            "regime_status": "unconfirmed",
            "board_status": "Partial",
            "confidence": "low",
            "missing_reason": "waiting_on_regime",
            "level_count": level_count,
        }
    elif spot is None:
        state = {
            "regime_status": "unconfirmed",
            "board_status": "Partial",
            "confidence": "low",
            "missing_reason": "waiting_on_spot_anchor",
            "level_count": level_count,
        }
    elif main_flip is None:
        state = {
            "regime_status": "unconfirmed",
            "board_status": "Partial",
            "confidence": "low",
            "missing_reason": "waiting_on_main_flip",
            "level_count": level_count,
        }
    elif not has_primary_wall:
        state = {
            "regime_status": "unconfirmed",
            "board_status": "Partial",
            "confidence": "low",
            "missing_reason": "waiting_on_wall_structure",
            "level_count": level_count,
        }
    elif level_count < 3:
        state = {
            "regime_status": "unconfirmed",
            "board_status": "Partial",
            "confidence": "low",
            "missing_reason": "waiting_on_core_levels",
            "level_count": level_count,
        }
    else:
        state = {
            "regime_status": "provisional",
            "board_status": "Partial",
            "confidence": "medium",
            "missing_reason": "",
            "level_count": level_count,
        }

    _market_pulse_log(
        "gamma_regime_state",
        session_mode=session_mode,
        effective_session=effective_session,
        gamma_data_status=gamma_data_status,
        spot=spot,
        regime=regime_text,
        main_flip=main_flip,
        local_flip=local_flip,
        call_wall=call_wall,
        put_wall=put_wall,
        validated_level_count=level_count,
        gamma_regime_state=state.get("regime_status"),
        gamma_regime_confidence=state.get("confidence"),
        gamma_board_status=state.get("board_status"),
        gamma_regime_missing_reason=state.get("missing_reason"),
    )
    return state


def _market_pulse_gamma_regime_viewmodel(
    gamma_snapshot: Dict[str, Any],
    *,
    gamma_data_status: str,
    regime_status: str = "",
    missing_reason: str = "",
) -> Dict[str, str]:
    regime_text = str(gamma_snapshot.get("regime") or "").strip()
    regime_lower = regime_text.lower().replace("-", "_").replace(" ", "_")
    normalized_regime_status = str(regime_status or "").strip().lower()
    if not normalized_regime_status:
        normalized_regime_status = (
            "unavailable"
            if gamma_data_status == "invalid"
            else "unconfirmed" if gamma_data_status == "partial" else "confirmed"
        )

    if gamma_data_status == "invalid" or normalized_regime_status == "unavailable":
        return {
            "gamma_regime": "unavailable",
            "gamma_regime_label": "Regime Unavailable",
            "gamma_regime_subtitle": "No usable gamma snapshot",
        }

    if normalized_regime_status == "unconfirmed":
        subtitle = {
            "waiting_on_spot_anchor": "Waiting on spot anchor",
            "waiting_on_regime": "Waiting on regime",
            "waiting_on_main_flip": "Waiting on main flip",
            "waiting_on_wall_structure": "Waiting on wall structure",
            "waiting_on_core_levels": "Waiting on core levels",
            "waiting_on_intraday_session": "Gamma board incomplete",
            "using_last_valid_session_structure": "Using last valid session structure",
            "using_degraded_session_structure": "Using degraded session structure",
        }.get(missing_reason, "Gamma board incomplete")
        return {
            "gamma_regime": "unconfirmed",
            "gamma_regime_label": "Unconfirmed",
            "gamma_regime_subtitle": subtitle,
        }

    provisional_prefix = "Provisional " if normalized_regime_status == "provisional" else ""
    provisional_subtitle = (
        "Core levels present · medium confidence"
        if normalized_regime_status == "provisional"
        else None
    )
    if "strong_negative" in regime_lower:
        return {
            "gamma_regime": "negative",
            "gamma_regime_label": f"{provisional_prefix}Negative Gamma",
            "gamma_regime_subtitle": provisional_subtitle or "Strong trend / momentum active",
        }
    if "negative" in regime_lower:
        return {
            "gamma_regime": "negative",
            "gamma_regime_label": f"{provisional_prefix}Negative Gamma",
            "gamma_regime_subtitle": provisional_subtitle or "Trend / momentum active",
        }
    if "strong_positive" in regime_lower:
        return {
            "gamma_regime": "positive",
            "gamma_regime_label": f"{provisional_prefix}Positive Gamma",
            "gamma_regime_subtitle": provisional_subtitle
            or "Strong mean reversion / pinning active",
        }
    if "positive" in regime_lower:
        return {
            "gamma_regime": "positive",
            "gamma_regime_label": f"{provisional_prefix}Positive Gamma",
            "gamma_regime_subtitle": provisional_subtitle or "Mean reversion active",
        }
    if "neutral" in regime_lower or "flip" in regime_lower:
        return {
            "gamma_regime": "neutral",
            "gamma_regime_label": f"{provisional_prefix}Neutral Gamma",
            "gamma_regime_subtitle": provisional_subtitle or "Mixed dealer structure",
        }
    return {
        "gamma_regime": "unconfirmed",
        "gamma_regime_label": "Unconfirmed",
        "gamma_regime_subtitle": "Gamma board incomplete",
    }


def _market_pulse_gamma_reason_label(
    *,
    gamma_regime: str,
    gamma_data_status: str,
    levels_source: str,
    structure_invariant_status: str,
    structure_invariant_issues: Optional[List[str]] = None,
    regime_status: str = "",
    missing_reason: str = "",
) -> str:
    issues = [
        str(issue or "").strip().lower()
        for issue in (structure_invariant_issues or [])
        if str(issue or "").strip()
    ]
    primary_issue = issues[0] if issues else ""

    if levels_source == "last_valid_snapshot":
        return "Last valid structure"
    if levels_source == "prior_close_fallback":
        return "Prior close anchor"
    if levels_source == "last_valid_snapshot_fallback":
        return "Last valid snapshot anchor"
    if levels_source == "last_valid_quote_fallback":
        return "Last valid session quote"
    if levels_source == "fallback_snapshot":
        return "Fallback spot anchor"
    if gamma_data_status == "invalid":
        if primary_issue == "wall_order_invalid":
            return "Wall order invalid"
        if primary_issue == "local_flip_outside_wall_span":
            return "Local Flip above Call Wall"
        return "Gamma unavailable"
    if gamma_data_status == "fallback_valid":
        return "Reduced confidence"
    if gamma_data_status == "partial":
        if structure_invariant_status == "soft_invalid":
            if set(issues) >= {
                "next_call_wall_not_above_call_wall",
                "next_put_wall_not_below_put_wall",
            }:
                return "Next walls sanitized"
            if primary_issue == "next_call_wall_not_above_call_wall":
                return "Next call wall invalid"
            if primary_issue == "next_put_wall_not_below_put_wall":
                return "Next put wall invalid"
            return "Soft downgrade"
        if regime_status == "provisional":
            return "Intraday usable"
        if missing_reason == "using_last_valid_session_structure":
            return "Last valid structure"
        if missing_reason == "using_degraded_session_structure":
            return "Degraded structure"
        if missing_reason == "waiting_on_main_flip":
            return "Waiting on main flip"
        if missing_reason == "waiting_on_wall_structure":
            return "Waiting on wall structure"
        if missing_reason == "waiting_on_core_levels":
            return "Waiting on core levels"
        if missing_reason == "waiting_on_spot_anchor":
            return "Waiting on spot anchor"
        return "Partial board" if gamma_regime == "unconfirmed" else "Reduced confidence"
    if gamma_data_status == "stale_but_usable" and levels_source == "stale_snapshot":
        return "Stale but usable"
    return ""


def _market_pulse_regime_confidence(
    *,
    session_mode: str,
    gamma_data_status: str,
    regime_status: str = "",
) -> str:
    if regime_status == "confirmed":
        return "high"
    if regime_status == "provisional":
        return "medium"
    if regime_status == "unconfirmed":
        return "low"
    if regime_status == "unavailable":
        return "none"
    if gamma_data_status == "fresh_valid":
        return "high" if session_mode == "regular" else "medium"
    if gamma_data_status == "fallback_valid":
        return "low"
    if gamma_data_status == "stale_but_usable":
        return "medium" if session_mode == "regular" else "low"
    if gamma_data_status == "partial":
        return "low"
    return "none"


def _market_pulse_levels_source(
    *,
    session_mode: str,
    execution_chart: Dict[str, Any],
    gamma_data_status: str,
    level_count: int,
    spot_source: str = "",
) -> str:
    if level_count <= 0:
        return "unavailable"
    if gamma_data_status == "fallback_valid":
        return {
            "prior_close": "prior_close_fallback",
            "last_valid_snapshot": "last_valid_snapshot_fallback",
            "last_valid_quote": "last_valid_quote_fallback",
        }.get(str(spot_source or "").strip().lower(), "fallback_snapshot")
    if gamma_data_status == "stale_but_usable":
        return "stale_snapshot"
    if (
        session_mode == "regular"
        and str(execution_chart.get("mode") or "") == "live_session"
        and gamma_data_status == "fresh_valid"
    ):
        return "live_session_snapshot"
    if str(execution_chart.get("mode") or "") == "last_session_replay" or session_mode in {
        "after_hours",
        "premarket",
        "closed",
    }:
        return "last_valid_snapshot"
    if gamma_data_status in {"fresh_valid", "partial"}:
        return "live_session_snapshot"
    return "unavailable"


def _market_pulse_planning_bias_viewmodel(
    *,
    spot: Any,
    local_flip: Any,
    call_wall: Any,
    put_wall: Any,
    next_call_wall: Any,
    next_put_wall: Any,
) -> Dict[str, str]:
    spot_num = float(spot) if isinstance(spot, (int, float)) else None
    local_num = float(local_flip) if isinstance(local_flip, (int, float)) else None
    call_num = float(call_wall) if isinstance(call_wall, (int, float)) else None
    put_num = float(put_wall) if isinstance(put_wall, (int, float)) else None
    next_call_num = float(next_call_wall) if isinstance(next_call_wall, (int, float)) else None
    next_put_num = float(next_put_wall) if isinstance(next_put_wall, (int, float)) else None

    if spot_num is None:
        return {"planning_bias": "unavailable", "planning_bias_label": "Unavailable"}
    if call_num is not None and spot_num > call_num:
        target = next_call_num if next_call_num is not None else call_num
        return {
            "planning_bias": "above_call_wall_extension_risk",
            "planning_bias_label": f"Above Call Wall · extension risk toward {target:.0f}",
        }
    if put_num is not None and spot_num < put_num:
        target = next_put_num if next_put_num is not None else put_num
        return {
            "planning_bias": "below_put_wall_breakdown_risk",
            "planning_bias_label": f"Below Put Wall · breakdown risk toward {target:.0f}",
        }
    if local_num is not None and spot_num >= local_num:
        return {
            "planning_bias": "bullish_above_local_flip",
            "planning_bias_label": f"Bullish above Local Flip {local_num:.0f}",
        }
    if local_num is not None and spot_num < local_num:
        return {
            "planning_bias": "bearish_below_local_flip",
            "planning_bias_label": f"Bearish below Local Flip {local_num:.0f}",
        }
    if any(isinstance(value, (int, float)) for value in (call_num, put_num, local_num)):
        return {
            "planning_bias": "neutral_between_levels",
            "planning_bias_label": "Neutral between levels",
        }
    return {"planning_bias": "unavailable", "planning_bias_label": "Unavailable"}


def _market_pulse_compact_level_label(prefix: str, value: Any, fallback: str) -> str:
    numeric = _market_pulse_positive_float(value)
    if numeric is None:
        return fallback
    return f"{prefix} {numeric:.0f}"


def _market_pulse_display_context_viewmodel(
    *,
    session_mode: str,
    levels_source: str,
    planning_bias: Dict[str, str],
    local_bias: Dict[str, Any],
    location: Dict[str, Any],
    playbook: Dict[str, Any],
    execution_model: Dict[str, Any],
    local_flip: Any,
    call_wall: Any,
    put_wall: Any,
    next_call_wall: Any,
    next_put_wall: Any,
) -> Dict[str, str]:
    """Return UI-facing labels from the unified backend snapshot.

    Regular-session flow can still use the richer execution model language when
    tactical structure is available. Outside regular hours, or when tactical
    context is missing, we switch to planning-oriented language derived from
    the resolved spot/levels snapshot rather than surfacing vague "unknown"
    labels from missing intraday fields.
    """

    local_state = str(local_bias.get("state") or "").strip().lower()
    location_zone = str(location.get("zone") or "").strip()
    playbook_why = str(playbook.get("why") or execution_model.get("posture_summary") or "").strip()

    if (
        session_mode == "regular"
        and levels_source != "unavailable"
        and local_state not in {"", "unknown"}
    ):
        return {
            "bias_state": local_bias.get("state") or "neutral",
            "bias_context": local_bias.get("title")
            or local_bias.get("context")
            or planning_bias.get("planning_bias_label")
            or "Awaiting structure",
            "bias_label": local_bias.get("label") or "WAIT",
            "bias": planning_bias.get("planning_bias_label")
            or local_bias.get("title")
            or "Unavailable",
            "current_read": location_zone or "Awaiting structure",
            "pullback_level": playbook.get("pullback_level")
            or _market_pulse_compact_level_label("LF", local_flip, "Awaiting level"),
            "next_destination": location.get("nearest_level_name") or "Awaiting next test",
            "plan_note": playbook_why or "Awaiting market posture.",
            "best_look": playbook.get("best_look") or "Wait for cleaner structure",
            "required_trigger": playbook.get("need") or "Confirmation required",
            "invalidation": playbook.get("need") or "Wait for live structure",
        }

    if levels_source == "unavailable":
        return {
            "bias_state": "neutral",
            "bias_context": "STRUCTURE UNAVAILABLE",
            "bias_label": "WAIT",
            "bias": "Unavailable",
            "current_read": "Unavailable",
            "pullback_level": "Awaiting level",
            "next_destination": "Awaiting next test",
            "plan_note": "No validated session structure is available.",
            "best_look": "Wait for validated structure",
            "required_trigger": "Validated session structure",
            "invalidation": "Structure unavailable",
        }

    session_prefix = {
        "after_hours": "After-hours planning",
        "premarket": "Premarket planning",
        "closed": "Closed-session planning",
    }.get(session_mode, "Planning")

    local_label = _market_pulse_compact_level_label("LF", local_flip, "Awaiting level")
    call_label = _market_pulse_compact_level_label("CW", call_wall, "Awaiting level")
    put_label = _market_pulse_compact_level_label("PW", put_wall, "Awaiting level")
    next_call_label = _market_pulse_compact_level_label("NCW", next_call_wall, call_label)
    next_put_label = _market_pulse_compact_level_label("NPW", next_put_wall, put_label)
    planning_state = planning_bias.get("planning_bias") or "unavailable"

    if planning_state == "above_call_wall_extension_risk":
        return {
            "bias_state": "above_local",
            "bias_context": "ABOVE CALL WALL",
            "bias_label": "EXTENSION RISK",
            "bias": planning_bias.get("planning_bias_label") or "Above Call Wall",
            "current_read": "Above Call Wall",
            "pullback_level": call_label,
            "next_destination": next_call_label,
            "plan_note": f"{session_prefix}: above call wall. Respect extension risk and plan for pullback into call wall.",
            "best_look": "Wait for pullback into Call Wall",
            "required_trigger": "Next live session retest and hold at Call Wall",
            "invalidation": call_label,
        }
    if planning_state == "below_put_wall_breakdown_risk":
        return {
            "bias_state": "below_local",
            "bias_context": "BELOW PUT WALL",
            "bias_label": "BREAKDOWN RISK",
            "bias": planning_bias.get("planning_bias_label") or "Below Put Wall",
            "current_read": "Below Put Wall",
            "pullback_level": put_label,
            "next_destination": next_put_label,
            "plan_note": f"{session_prefix}: below put wall. Respect breakdown risk and plan from put-wall reclaim or continuation.",
            "best_look": "Wait for reclaim or continuation below Put Wall",
            "required_trigger": "Next live session reclaim or clean continuation below Put Wall",
            "invalidation": put_label,
        }
    if planning_state == "bullish_above_local_flip":
        return {
            "bias_state": "above_local",
            "bias_context": "ABOVE LOCAL FLIP",
            "bias_label": "BUY DIPS",
            "bias": planning_bias.get("planning_bias_label") or "Bullish above Local Flip",
            "current_read": "Above Local Flip",
            "pullback_level": local_label,
            "next_destination": call_label if call_label != "Awaiting level" else next_call_label,
            "plan_note": f"{session_prefix}: macro context is supportive above local flip. Plan for dip-buying only if the next live session holds above the local flip.",
            "best_look": "Buy dip above Local Flip",
            "required_trigger": "Next live session dip-hold above Local Flip",
            "invalidation": local_label,
        }
    if planning_state == "bearish_below_local_flip":
        return {
            "bias_state": "below_local",
            "bias_context": "BELOW LOCAL FLIP",
            "bias_label": "SELL RIPS",
            "bias": planning_bias.get("planning_bias_label") or "Bearish below Local Flip",
            "current_read": "Below Local Flip",
            "pullback_level": local_label,
            "next_destination": put_label if put_label != "Awaiting level" else next_put_label,
            "plan_note": f"{session_prefix}: macro context is weak below local flip. Plan for rip-selling only if the next live session fails back under the local flip.",
            "best_look": "Sell rip below Local Flip",
            "required_trigger": "Next live session failed reclaim at Local Flip",
            "invalidation": local_label,
        }
    if planning_state == "neutral_between_levels":
        return {
            "bias_state": "neutral",
            "bias_context": "BETWEEN LEVELS",
            "bias_label": "WAIT",
            "bias": planning_bias.get("planning_bias_label") or "Neutral between levels",
            "current_read": "Between Levels",
            "pullback_level": local_label,
            "next_destination": call_label if call_label != "Awaiting level" else put_label,
            "plan_note": f"{session_prefix}: structure is coherent, but price is between actionable levels. Wait for a cleaner live-session test.",
            "best_look": "Wait for cleaner structure",
            "required_trigger": "Next live session test at Local Flip or wall",
            "invalidation": "No live trigger yet",
        }

    return {
        "bias_state": "neutral",
        "bias_context": "MACRO CONTEXT VALID",
        "bias_label": "LIVE TRIGGER PENDING",
        "bias": planning_bias.get("planning_bias_label")
        or "Macro context valid / live trigger pending",
        "current_read": "Macro Context Valid",
        "pullback_level": local_label,
        "next_destination": call_label if call_label != "Awaiting level" else put_label,
        "plan_note": f"{session_prefix}: structure is usable, but the next live trigger has not printed yet.",
        "best_look": "Wait for next live trigger",
        "required_trigger": "Next live session confirmation at a key level",
        "invalidation": "Live trigger pending",
    }


def _market_pulse_trigger_validation_viewmodel(
    *,
    session_mode: str,
    trade_state: str,
    gamma_regime: str,
    planning_bias: Dict[str, str],
    local_bias: Dict[str, Any],
    playbook: Dict[str, Any],
    display_context: Dict[str, str],
) -> Dict[str, Any]:
    """Return trigger checklist copy from the same backend playbook contract.

    This keeps Action / Trade Context and Trigger Validation on one source of
    truth instead of letting the frontend improvise different setup language.
    """

    planning_mode = (
        session_mode in {"after_hours", "premarket", "closed"} or trade_state == "PLANNING_ONLY"
    )
    local_state = str(local_bias.get("state") or "").strip().lower()
    planning_state = str(planning_bias.get("planning_bias") or "").strip().lower()
    playbook_status = str(playbook.get("status") or "").strip().upper()
    plan_note = str(
        display_context.get("plan_note") or playbook.get("why") or "Awaiting market posture."
    ).strip()
    required_trigger = str(
        display_context.get("required_trigger") or playbook.get("need") or "Confirmation required"
    ).strip()

    if planning_mode:
        manual_label = "Manual confirmation required"
        header_line = (
            "After-hours context is valid, but entry confirmation belongs to the next live session."
        )
        status_line = "PLANNING ONLY — NEXT LIVE TRIGGER REQUIRED"
        status_badge = "PLANNING"
    elif playbook_status == "READY":
        manual_label = "Context ready — trigger not confirmed"
        header_line = "Location is usable, but the trader still has to confirm the trigger."
        status_line = "NO TRIGGER YET — WAIT FOR CONFIRMATION"
        status_badge = "WAITING"
    elif playbook_status == "CAUTION":
        manual_label = "Waiting for manual confirmation"
        header_line = "The context exists, but the setup still needs trader confirmation."
        status_line = "CAUTION — WAIT FOR CLEANER EDGE"
        status_badge = "CAUTION"
    elif playbook_status == "WATCH":
        manual_label = "Waiting for manual confirmation"
        header_line = "The idea is valid on watch, but the trigger is still trader-verified."
        status_line = "WATCHLIST — TRIGGER STILL REQUIRED"
        status_badge = "WATCH"
    else:
        manual_label = "No trigger yet"
        header_line = "No trigger = no trade. The system has context, but the trader still needs the setup to print."
        status_line = "NO TRIGGER — NO TRADE"
        status_badge = "NO TRIGGER"

    if planning_state == "above_call_wall_extension_risk":
        sweep_line = "Need pullback into Call Wall before any continuation long is valid."
        reclaim_line = "Need Call Wall to hold on retest after the pullback."
        reversal_line = "Need a 5m reversal or continuation only after Call Wall confirms."
    elif planning_state == "below_put_wall_breakdown_risk":
        sweep_line = (
            "Need reclaim failure or continuation under Put Wall before any short is valid."
        )
        reclaim_line = "Need Put Wall to reject or fail cleanly on retest."
        reversal_line = "Need a 5m continuation trigger after Put Wall confirms."
    elif local_state == "above_local" or planning_state == "bullish_above_local_flip":
        sweep_line = "Need sweep into support or Local Flip before the long exists."
        reclaim_line = "Need reclaim back above the working level after the sweep."
        reversal_line = "Need 5m 2-2 up / 3-1-2 continuation before the long is valid."
    elif local_state == "below_local" or planning_state == "bearish_below_local_flip":
        sweep_line = "Need pop into resistance or Local Flip before the short exists."
        reclaim_line = "Need failed reclaim back under the working level after the pop."
        reversal_line = "Need 5m 2-2 down / 3-1-2 continuation before the short is valid."
    else:
        sweep_line = "Need interaction into the working level before entry is considered."
        reclaim_line = "Wait for the level to prove itself after the interaction."
        reversal_line = "A clean 5m reversal or continuation trigger still has to print."

    if planning_mode:
        sweep_line = f"Next live session: {sweep_line[0].lower() + sweep_line[1:]}"
        reclaim_line = f"Next live session: {reclaim_line[0].lower() + reclaim_line[1:]}"
        reversal_line = f"Next live session: {reversal_line[0].lower() + reversal_line[1:]}"

    volume_line = (
        "Fast tape is active. Cash-session participation is mandatory."
        if gamma_regime == "negative"
        else "Require real participation before calling the move valid."
    )

    return {
        "manual_label": manual_label,
        "status_badge": status_badge,
        "header_line": header_line,
        "status_line": status_line,
        "footer_line": plan_note,
        "items": {
            "sweep": {
                "line": sweep_line,
                "active": False,
            },
            "reclaim": {
                "line": reclaim_line,
                "active": False,
            },
            "reversal": {
                "line": reversal_line,
                "active": False,
            },
            "volume": {
                "line": volume_line,
                "active": False,
            },
        },
        "required_trigger": required_trigger,
    }


def _market_pulse_execution_regime_viewmodel(
    *,
    session_mode: str,
    gamma_regime: str,
    gamma_data_status: str,
    planning_bias: str,
    levels_source: str,
) -> Dict[str, str]:
    if session_mode in {"after_hours", "premarket"}:
        if levels_source in {"last_valid_snapshot", "stale_snapshot", "live_session_snapshot"}:
            return {
                "execution_regime": "planning_mode_only",
                "execution_regime_label": "PLANNING_ONLY",
            }
        return {
            "execution_regime": "planning_mode_only",
            "execution_regime_label": "PLANNING_ONLY",
        }
    if session_mode == "closed":
        if levels_source != "unavailable":
            return {
                "execution_regime": "planning_mode_only",
                "execution_regime_label": "PLANNING_ONLY",
            }
        return {
            "execution_regime": "planning_mode_only",
            "execution_regime_label": "PLANNING_ONLY",
        }
    if gamma_data_status in {"invalid", "partial"} or levels_source == "unavailable":
        return {
            "execution_regime": "planning_mode_only",
            "execution_regime_label": "PLANNING_ONLY",
        }
    if planning_bias == "above_call_wall_extension_risk":
        return {
            "execution_regime": "full_mode",
            "execution_regime_label": "FULL_MODE",
        }
    if planning_bias == "below_put_wall_breakdown_risk":
        return {
            "execution_regime": "full_mode",
            "execution_regime_label": "FULL_MODE",
        }
    if gamma_regime == "negative":
        return {
            "execution_regime": "full_mode",
            "execution_regime_label": "FULL_MODE",
        }
    if gamma_regime == "positive":
        return {
            "execution_regime": "scalp_mode",
            "execution_regime_label": "SCALP_MODE",
        }
    return {
        "execution_regime": "planning_mode_only",
        "execution_regime_label": "PLANNING_ONLY",
    }


def _market_pulse_trade_state(
    *,
    session_mode: str,
    levels_source: str,
    gamma_data_status: str,
    playbook_status: str,
) -> str:
    if session_mode in {"after_hours", "premarket"}:
        return "PLANNING_ONLY" if levels_source != "unavailable" else "UNAVAILABLE"
    if session_mode == "closed":
        return "PLANNING_ONLY" if levels_source != "unavailable" else "UNAVAILABLE"
    if levels_source == "unavailable" or gamma_data_status == "invalid":
        return "NO_TRADE"
    normalized = str(playbook_status or "").strip().upper().replace(" ", "_")
    if normalized == "GO":
        return "READY"
    if normalized in {"WATCH", "CAUTION"}:
        return "WAIT"
    if normalized == "NO_TRADE":
        return "NO_TRADE"
    return "WAIT"


def _market_pulse_last_valid_snapshot_usable(
    *,
    now_et: datetime,
    gamma_snapshot: Dict[str, Any],
    execution_chart: Dict[str, Any],
    levels: Dict[str, Any],
    next_call_wall: Any,
    next_put_wall: Any,
) -> Dict[str, Any]:
    """Decide whether the last valid structure board is coherent enough for planning.

    This is intentionally stricter than "some numbers exist". For outside-session
    planning we require a recent last-valid timestamp plus the core structure
    levels that matter most for next-session framing: local flip, call wall,
    and put wall.
    """

    max_age_seconds = max(
        60,
        int(
            float(
                os.environ.get("MARKET_PULSE_LAST_VALID_MAX_AGE_SECONDS")
                or MARKET_PULSE_LAST_VALID_MAX_AGE_SECONDS
            )
        ),
    )
    last_valid_text = (
        gamma_snapshot.get("last_successful_compute")
        or gamma_snapshot.get("computed_at")
        or gamma_snapshot.get("asof")
        or ""
    )
    last_valid_dt = _parse_iso_et(last_valid_text)
    if last_valid_dt is None:
        return {"usable": False, "reason": "missing_last_valid_time", "age_seconds": None}

    age_seconds = max(0.0, (now_et - last_valid_dt).total_seconds())
    if age_seconds > max_age_seconds:
        return {"usable": False, "reason": "last_valid_too_old", "age_seconds": age_seconds}

    local_flip = levels.get("local_flip")
    call_wall = levels.get("call_wall")
    put_wall = levels.get("put_wall")
    spot = levels.get("spot")
    required_present = all(
        isinstance(value, (int, float)) for value in (local_flip, call_wall, put_wall)
    )
    if not required_present:
        return {"usable": False, "reason": "required_levels_missing", "age_seconds": age_seconds}

    local_num = float(local_flip)
    call_num = float(call_wall)
    put_num = float(put_wall)
    spot_num = float(spot) if isinstance(spot, (int, float)) else None
    coherent = call_num > put_num and put_num <= local_num <= call_num
    if spot_num is not None:
        coherent = coherent and (put_num - 80.0) <= spot_num <= (call_num + 80.0)
    if not coherent:
        return {"usable": False, "reason": "coherence_failed", "age_seconds": age_seconds}

    has_next_levels = any(
        isinstance(value, (int, float)) for value in (next_call_wall, next_put_wall)
    )
    source_label = str(
        execution_chart.get("last_valid_session_label")
        or execution_chart.get("session_label")
        or ""
    ).strip()
    return {
        "usable": True,
        "reason": "usable_last_valid_snapshot",
        "age_seconds": age_seconds,
        "has_next_levels": has_next_levels,
        "source_label": source_label,
    }


def _market_pulse_structure_snapshot(
    *,
    spx_quote: Dict[str, Any],
    gamma_snapshot: Dict[str, Any],
    execution_chart: Dict[str, Any],
    execution_model: Dict[str, Any],
    now_et: datetime,
) -> Dict[str, Any]:
    levels = dict(execution_model.get("levels") or {})
    canonical_structure = _market_pulse_resolve_canonical_market_structure(
        levels={
            "spot": (
                levels.get("spot") if levels.get("spot") is not None else spx_quote.get("price")
            ),
            "main_flip": levels.get("main_flip"),
            "local_flip": levels.get("local_flip"),
            "call_wall": levels.get("call_wall"),
            "put_wall": levels.get("put_wall"),
            "next_call_wall": gamma_snapshot.get("next_call_wall_above"),
            "next_put_wall": gamma_snapshot.get("next_put_wall_below"),
        },
        gamma_snapshot=gamma_snapshot,
    )
    levels.update(
        {
            "main_flip": canonical_structure.get("main_flip"),
            "local_flip": canonical_structure.get("local_flip"),
            "call_wall": canonical_structure.get("call_wall"),
            "put_wall": canonical_structure.get("put_wall"),
            "next_call_wall": canonical_structure.get("next_call_wall"),
            "next_put_wall": canonical_structure.get("next_put_wall"),
        }
    )
    local_bias = dict(execution_model.get("local_bias") or {})
    playbook = dict(execution_model.get("playbook") or {})
    location = dict(execution_model.get("location") or {})
    raw_snapshot_status = str(gamma_snapshot.get("snapshot_status") or "").strip().lower()
    session_mode = _market_pulse_snapshot_session_mode(now_et)
    next_call_wall = canonical_structure.get("next_call_wall")
    next_put_wall = canonical_structure.get("next_put_wall")
    level_count = sum(
        1
        for value in (
            levels.get("main_flip"),
            levels.get("local_flip"),
            levels.get("call_wall"),
            levels.get("put_wall"),
            next_call_wall,
            next_put_wall,
        )
        if isinstance(value, (int, float))
    )
    gamma_data_status = _market_pulse_gamma_data_status(gamma_snapshot, level_count=level_count)
    last_valid_snapshot_check = _market_pulse_last_valid_snapshot_usable(
        now_et=now_et,
        gamma_snapshot=gamma_snapshot,
        execution_chart=execution_chart,
        levels=levels,
        next_call_wall=next_call_wall,
        next_put_wall=next_put_wall,
    )
    raw_snapshot_invalid = raw_snapshot_status in {"", "invalid"}
    planning_fallback_active = (
        session_mode in {"after_hours", "premarket", "closed"}
        and raw_snapshot_invalid
        and bool(last_valid_snapshot_check.get("usable"))
    )
    degraded_snapshot_active = (
        session_mode in {"after_hours", "premarket", "closed"}
        and raw_snapshot_invalid
        and not planning_fallback_active
        and level_count >= 4
    )
    effective_gamma_data_status = gamma_data_status
    if planning_fallback_active or degraded_snapshot_active:
        effective_gamma_data_status = "partial"
    elif session_mode in {"after_hours", "premarket", "closed"} and raw_snapshot_invalid:
        effective_gamma_data_status = "invalid"
    _market_pulse_log(
        "gamma_regime_input",
        session_mode=session_mode,
        gamma_data_status=effective_gamma_data_status,
        snapshot_status=str(gamma_snapshot.get("snapshot_status") or ""),
        build_status=str(gamma_snapshot.get("build_status") or ""),
        spot=levels.get("spot"),
        regime=str(gamma_snapshot.get("regime") or ""),
        main_flip=levels.get("main_flip"),
        local_flip=levels.get("local_flip"),
        call_wall=levels.get("call_wall"),
        put_wall=levels.get("put_wall"),
        next_call_wall=next_call_wall,
        next_put_wall=next_put_wall,
        validated_level_count=level_count,
    )
    regime_state = _market_pulse_gamma_regime_state(
        gamma_snapshot,
        gamma_data_status=effective_gamma_data_status,
        session_mode=session_mode,
        levels=levels,
        next_call_wall=next_call_wall,
        next_put_wall=next_put_wall,
    )
    regime = _market_pulse_gamma_regime_viewmodel(
        gamma_snapshot,
        gamma_data_status=effective_gamma_data_status,
        regime_status=str(regime_state.get("regime_status") or ""),
        missing_reason=str(regime_state.get("missing_reason") or ""),
    )
    if planning_fallback_active:
        regime = {
            "gamma_regime": "unconfirmed",
            "gamma_regime_label": "Unconfirmed",
            "gamma_regime_subtitle": "Using last valid session structure",
        }
        regime_state = {
            "regime_status": "unconfirmed",
            "board_status": "Partial",
            "confidence": "low",
            "missing_reason": "using_last_valid_session_structure",
            "level_count": level_count,
        }
    elif degraded_snapshot_active:
        regime = {
            "gamma_regime": "unconfirmed",
            "gamma_regime_label": "Degraded",
            "gamma_regime_subtitle": "Using degraded session structure",
        }
        regime_state = {
            "regime_status": "unconfirmed",
            "board_status": "Partial",
            "confidence": "low",
            "missing_reason": "using_degraded_session_structure",
            "level_count": level_count,
        }
    levels_source = _market_pulse_levels_source(
        session_mode=session_mode,
        execution_chart=execution_chart,
        gamma_data_status=effective_gamma_data_status,
        level_count=level_count,
        spot_source=str(gamma_snapshot.get("spot_source") or ""),
    )
    if planning_fallback_active or degraded_snapshot_active:
        levels_source = "last_valid_snapshot"
    elif session_mode in {"after_hours", "premarket", "closed"} and raw_snapshot_invalid:
        levels_source = "unavailable"
    regime_confidence = _market_pulse_regime_confidence(
        session_mode=session_mode,
        gamma_data_status=effective_gamma_data_status,
        regime_status=str(regime_state.get("regime_status") or ""),
    )
    planning_bias = _market_pulse_planning_bias_viewmodel(
        spot=levels.get("spot"),
        local_flip=levels.get("local_flip"),
        call_wall=levels.get("call_wall"),
        put_wall=levels.get("put_wall"),
        next_call_wall=next_call_wall,
        next_put_wall=next_put_wall,
    )
    execution_regime = _market_pulse_execution_regime_viewmodel(
        session_mode=session_mode,
        gamma_regime=regime["gamma_regime"],
        gamma_data_status=effective_gamma_data_status,
        planning_bias=planning_bias["planning_bias"],
        levels_source=levels_source,
    )
    trade_state = _market_pulse_trade_state(
        session_mode=session_mode,
        levels_source=levels_source,
        gamma_data_status=effective_gamma_data_status,
        playbook_status=str(playbook.get("status") or "WAIT"),
    )
    trade_state_label = str(trade_state).replace("_", " ")
    display_context = _market_pulse_display_context_viewmodel(
        session_mode=session_mode,
        levels_source=levels_source,
        planning_bias=planning_bias,
        local_bias=local_bias,
        location=location,
        playbook=playbook,
        execution_model=execution_model,
        local_flip=levels.get("local_flip"),
        call_wall=levels.get("call_wall"),
        put_wall=levels.get("put_wall"),
        next_call_wall=next_call_wall,
        next_put_wall=next_put_wall,
    )

    confidence_label = {
        "high": "High confidence",
        "medium": "Medium confidence",
        "low": "Low confidence",
        "none": "No confidence",
    }.get(regime_confidence, "No confidence")
    session_mode_label = {
        "regular": "Regular",
        "after_hours": "After Hours",
        "premarket": "Premarket",
        "closed": "Closed",
    }.get(session_mode, "Closed")
    levels_source_label = {
        "live_session_snapshot": "Live session snapshot",
        "last_valid_snapshot": "Last valid snapshot",
        "stale_snapshot": "Stale snapshot",
        "fallback_snapshot": "Fallback spot anchor",
        "prior_close_fallback": "Prior close fallback",
        "last_valid_snapshot_fallback": "Last valid snapshot fallback",
        "last_valid_quote_fallback": "Last valid quote fallback",
        "unavailable": "Unavailable",
    }.get(levels_source, "Unavailable")
    gamma_data_status_label = {
        "fresh_valid": "Fresh valid",
        "fallback_valid": "Fallback Build",
        "stale_but_usable": "Stale but usable",
        "partial": "Partial",
        "invalid": "Invalid",
    }.get(effective_gamma_data_status, "Invalid")
    gamma_regime_state = str(regime_state.get("regime_status") or "unavailable")
    gamma_board_status = str(regime_state.get("board_status") or "Unavailable")
    gamma_regime_confidence = {
        "high": "High",
        "medium": "Medium",
        "low": "Low",
        "none": "Invalid",
    }.get(regime_confidence, "Invalid")

    gamma_reason_label = _market_pulse_gamma_reason_label(
        gamma_regime=regime["gamma_regime"],
        gamma_data_status=effective_gamma_data_status,
        levels_source=levels_source,
        structure_invariant_status=str(gamma_snapshot.get("structure_invariant_status") or ""),
        structure_invariant_issues=list(gamma_snapshot.get("structure_invariant_issues") or []),
        regime_status=gamma_regime_state,
        missing_reason=str(regime_state.get("missing_reason") or ""),
    )

    snapshot_timestamp = (
        gamma_snapshot.get("last_successful_compute")
        or gamma_snapshot.get("computed_at")
        or gamma_snapshot.get("asof")
        or spx_quote.get("asof")
        or spx_quote.get("as_of")
        or ""
    )
    last_valid_snapshot_time = (
        gamma_snapshot.get("last_successful_compute")
        or gamma_snapshot.get("computed_at")
        or gamma_snapshot.get("asof")
        or ""
    )
    trigger_validation = _market_pulse_trigger_validation_viewmodel(
        session_mode=session_mode,
        trade_state=trade_state,
        gamma_regime=regime["gamma_regime"],
        planning_bias=planning_bias,
        local_bias=execution_model.get("local_bias") or {},
        playbook=playbook,
        display_context=display_context,
    )

    return {
        "spot": levels.get("spot"),
        "session_mode": session_mode,
        "session_mode_label": session_mode_label,
        "levels_source": levels_source,
        "levels_source_label": levels_source_label,
        "gamma_data_status": effective_gamma_data_status,
        "gamma_data_status_label": gamma_data_status_label,
        "gamma_regime_state": gamma_regime_state,
        "gamma_board_status": gamma_board_status,
        "gamma_regime_confidence": gamma_regime_confidence,
        "gamma_regime_missing_reason": str(regime_state.get("missing_reason") or ""),
        "build_status": gamma_snapshot.get("build_status"),
        "build_confidence": gamma_snapshot.get("build_confidence"),
        "gamma_regime": regime["gamma_regime"],
        "gamma_regime_label": regime["gamma_regime_label"],
        "gamma_regime_subtitle": regime["gamma_regime_subtitle"],
        "gamma_regime_reason_label": gamma_reason_label,
        "regime_confidence": regime_confidence,
        "regime_confidence_label": confidence_label,
        "execution_regime": execution_regime["execution_regime"],
        "execution_regime_label": execution_regime["execution_regime_label"],
        "planning_bias": planning_bias["planning_bias"],
        "planning_bias_label": planning_bias["planning_bias_label"],
        "main_flip": levels.get("main_flip"),
        "local_flip": levels.get("local_flip"),
        "call_wall": levels.get("call_wall"),
        "put_wall": levels.get("put_wall"),
        "next_call_wall": next_call_wall,
        "next_put_wall": next_put_wall,
        "next_call_wall_source": canonical_structure.get("next_call_wall_source"),
        "next_put_wall_source": canonical_structure.get("next_put_wall_source"),
        "next_put_wall_confidence": canonical_structure.get("next_put_wall_confidence"),
        "secondary_structure_complete": bool(
            canonical_structure.get("secondary_structure_complete")
        ),
        "secondary_structure_displayable": bool(
            canonical_structure.get("secondary_structure_displayable")
        ),
        "resolution_source": canonical_structure.get("resolution_source") or "Unavailable",
        "degraded_reason": canonical_structure.get("degraded_reason") or "",
        "secondary_structure_resolution_source": canonical_structure.get("resolution_source")
        or "Unavailable",
        "secondary_structure_degraded_reason": canonical_structure.get("degraded_reason") or "",
        "bias_state": display_context["bias_state"],
        "bias_context": display_context["bias_context"],
        "bias_label": display_context["bias_label"],
        "bias": display_context["bias"],
        "trade_state": trade_state,
        "trade_state_label": trade_state_label,
        "trade_state_tone": playbook.get("tone") or "warn",
        "context_grade": playbook.get("grade"),
        "context_score": playbook.get("score"),
        "context_score_pct": playbook.get("score_pct"),
        "context_tone": playbook.get("tone") or "warn",
        "context_status": playbook.get("status") or "WATCH",
        # Preserve current UI fields while feeding them from the unified snapshot.
        "tradeability": execution_regime["execution_regime_label"],
        "session": f"{session_mode_label} · {confidence_label}",
        "current_read": display_context["current_read"],
        "pullback_level": display_context["pullback_level"],
        "next_destination": display_context["next_destination"],
        "plan_note": display_context["plan_note"],
        "best_look": display_context["best_look"],
        "required_trigger": display_context["required_trigger"],
        "invalidation": display_context["invalidation"],
        "trigger_validation": trigger_validation,
        "snapshot_timestamp": snapshot_timestamp,
        "snapshot_timestamp_label": _format_iso_et_label(snapshot_timestamp) or "—",
        "last_valid_snapshot_time": last_valid_snapshot_time,
        "last_valid_snapshot_time_label": _format_iso_et_label(last_valid_snapshot_time) or "—",
        "last_valid_snapshot_usable": bool(last_valid_snapshot_check.get("usable")),
        "last_valid_snapshot_reason": str(last_valid_snapshot_check.get("reason") or ""),
        "last_valid_snapshot_age_seconds": last_valid_snapshot_check.get("age_seconds"),
    }


def _build_playbook_view_model(
    *,
    market_structure_snapshot: Dict[str, Any],
    execution_model: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Derive one canonical playbook object for dashboard + market pulse.

    Gamma math, snapshot validation, and fallback selection stay upstream.
    This function is only responsible for consistent presentation language
    after the snapshot has already been resolved.
    """

    structure = dict(market_structure_snapshot or {})
    model = dict(execution_model or {})
    location = dict(model.get("location") or {})
    legacy_playbook = dict(model.get("playbook") or {})
    distances = dict(model.get("distances") or {})
    distance_rows = list(model.get("distance_rows") or [])

    def _num(value: Any) -> Optional[float]:
        return _market_pulse_positive_float(value)

    def _fmt_number(value: Any, *, decimals: int = 0, fallback: str = "—") -> str:
        numeric = _num(value)
        if numeric is None:
            return fallback
        return f"{numeric:,.{decimals}f}"

    def _normalize_key(value: Any) -> str:
        return str(value or "").strip().lower().replace(" ", "_")

    def _clip_text(value: str, *, limit: int = 88) -> str:
        text = str(value or "").strip()
        if len(text) <= limit:
            return text
        return text[: limit - 1].rstrip() + "…"

    spot = _num(structure.get("spot"))
    main_flip = _num(structure.get("main_flip"))
    local_flip = _num(structure.get("local_flip"))
    call_wall = _num(structure.get("call_wall"))
    put_wall = _num(structure.get("put_wall"))
    next_call_wall = _num(structure.get("next_call_wall"))
    next_put_wall = _num(structure.get("next_put_wall"))

    session_mode = _normalize_key(structure.get("session_mode") or "closed")
    session_mode_label = str(structure.get("session_mode_label") or "Closed").strip()
    gamma_regime = _normalize_key(structure.get("gamma_regime") or "unavailable")
    gamma_regime_label = str(
        structure.get("gamma_regime_label")
        or (
            "Regime Unavailable"
            if gamma_regime == "unavailable"
            else gamma_regime.replace("_", " ").title()
        )
    ).strip()
    gamma_regime_subtitle = str(structure.get("gamma_regime_subtitle") or "").strip()
    gamma_reason_label = str(
        structure.get("gamma_regime_reason_label")
        or gamma_regime_subtitle
        or "Gamma snapshot unavailable"
    ).strip()
    levels_source = _normalize_key(structure.get("levels_source") or "unavailable")
    gamma_data_status = _normalize_key(structure.get("gamma_data_status") or "invalid")
    trade_state = _normalize_key(structure.get("trade_state") or "unavailable")
    trade_state_label = str(
        structure.get("trade_state_label") or trade_state.replace("_", " ").upper() or "UNAVAILABLE"
    ).strip()
    execution_regime = _normalize_key(
        structure.get("execution_regime") or "reduced_confidence_structure_first"
    )
    execution_regime_label = str(structure.get("execution_regime_label") or "PLANNING_ONLY").strip()
    regime_confidence = _normalize_key(structure.get("regime_confidence") or "none")
    regime_confidence_label = str(
        structure.get("regime_confidence_label") or "No confidence"
    ).strip()
    planning_bias = _normalize_key(structure.get("planning_bias") or "unavailable")
    planning_bias_label = str(structure.get("planning_bias_label") or "Unavailable").strip()
    context_score = int(structure.get("context_score") or legacy_playbook.get("score") or 0)
    context_grade = (
        str(structure.get("context_grade") or legacy_playbook.get("grade") or "—").strip() or "—"
    )
    context_tone = (
        str(structure.get("context_tone") or legacy_playbook.get("tone") or "warn").strip()
        or "warn"
    )

    if trade_state == "planning_only":
        snapshot_mode = "planning_only"
    elif levels_source == "unavailable":
        snapshot_mode = "degraded"
    elif gamma_data_status in {"fallback_valid", "partial", "invalid"} or levels_source in {
        "stale_snapshot",
        "last_valid_snapshot",
        "fallback_snapshot",
        "prior_close_fallback",
        "last_valid_snapshot_fallback",
        "last_valid_quote_fallback",
    }:
        snapshot_mode = "stale" if levels_source != "unavailable" else "degraded"
    else:
        snapshot_mode = "live"

    snapshot_mode_label = {
        "live": "LIVE",
        "planning_only": "PLANNING ONLY",
        "stale": "STALE",
        "degraded": "DEGRADED",
    }.get(snapshot_mode, "DEGRADED")
    risk_label = {
        "ready": "Normal size",
        "wait": "Reduced size",
        "no_trade": "No trade / stand down",
        "planning_only": "Planning only",
        "unavailable": "No trade / stand down",
    }.get(trade_state, "Reduced size")

    neutral_band_local = _num(model.get("neutral_band_local")) or 2.5
    if spot is not None and local_flip is not None:
        if spot > (local_flip + neutral_band_local):
            location_state = "above_local_flip"
            location_label = "Above Local Flip"
        elif spot < (local_flip - neutral_band_local):
            location_state = "below_local_flip"
            location_label = "Below Local Flip"
        else:
            location_state = "at_local_flip"
            location_label = "At Local Flip"
    else:
        location_state = "between_key_levels"
        location_label = (
            str(structure.get("current_read") or "Between Levels").strip() or "Between Levels"
        )

    zone_text = str(location.get("zone") or "").strip().lower()
    if "above call wall" in zone_text:
        structure_zone = "above_call_wall"
        structure_zone_label = "Above Call Wall"
    elif "near call wall" in zone_text:
        structure_zone = "near_call_wall"
        structure_zone_label = "Near Call Wall"
    elif "below put wall" in zone_text:
        structure_zone = "below_put_wall"
        structure_zone_label = "Below Put Wall"
    elif "near put wall" in zone_text:
        structure_zone = "near_put_wall"
        structure_zone_label = "Near Put Wall"
    elif "inside range" in zone_text or "between" in zone_text:
        structure_zone = "between_key_levels"
        structure_zone_label = str(location.get("zone") or "Inside Range").strip() or "Inside Range"
    else:
        structure_zone = location_state
        structure_zone_label = location_label

    is_planning_only = snapshot_mode == "planning_only"
    is_unconfirmed_gamma = gamma_regime == "unconfirmed"
    suppress_aggressive_copy = (
        is_planning_only
        or is_unconfirmed_gamma
        or gamma_regime in {"neutral", "unavailable"}
        or regime_confidence in {"low", "none"}
    )

    if location_state == "above_local_flip":
        bias_state = (
            "conditional_bullish" if suppress_aggressive_copy else "bullish_above_local_flip"
        )
        bias_label = (
            f"Conditional bullish above Local Flip {local_flip:.0f}"
            if suppress_aggressive_copy and local_flip is not None
            else (
                f"Bullish above Local Flip {local_flip:.0f}"
                if local_flip is not None
                else (
                    "Conditional bullish above Local Flip"
                    if suppress_aggressive_copy
                    else "Bullish above Local Flip"
                )
            )
        )
        bias_short_label = "CONDITIONAL BULLISH" if suppress_aggressive_copy else "BUY DIPS"
    elif location_state == "below_local_flip":
        bias_state = (
            "conditional_bearish" if suppress_aggressive_copy else "bearish_below_local_flip"
        )
        bias_label = (
            f"Conditional bearish below Local Flip {local_flip:.0f}"
            if suppress_aggressive_copy and local_flip is not None
            else (
                f"Bearish below Local Flip {local_flip:.0f}"
                if local_flip is not None
                else (
                    "Conditional bearish below Local Flip"
                    if suppress_aggressive_copy
                    else "Bearish below Local Flip"
                )
            )
        )
        bias_short_label = "CONDITIONAL BEARISH" if suppress_aggressive_copy else "SELL RIPS"
    elif location_state == "at_local_flip":
        bias_state = "neutral_at_local_flip"
        bias_label = (
            f"Wait at Local Flip {local_flip:.0f}"
            if local_flip is not None
            else "Wait at Local Flip"
        )
        bias_short_label = "WAIT"
    else:
        bias_state = _normalize_key(structure.get("bias_state") or "neutral_between_levels")
        bias_label = (
            str(structure.get("bias") or planning_bias_label or location_label).strip()
            or "Unavailable"
        )
        bias_short_label = (
            "EXTENSION RISK"
            if planning_bias == "above_call_wall_extension_risk"
            else "BREAKDOWN RISK" if planning_bias == "below_put_wall_breakdown_risk" else "WAIT"
        )

    setup_bias_label = str(structure.get("best_look") or "").strip()
    if not setup_bias_label:
        if structure_zone == "above_call_wall":
            setup_bias_label = "Wait for pullback into Call Wall"
        elif structure_zone == "near_call_wall":
            setup_bias_label = "Sell rips near resistance"
        elif structure_zone == "below_put_wall":
            setup_bias_label = "Wait for reclaim or continuation below Put Wall"
        elif structure_zone == "near_put_wall":
            setup_bias_label = "Buy dips near support"
        elif location_state == "above_local_flip":
            setup_bias_label = "Buy dips above Local Flip"
        elif location_state == "below_local_flip":
            setup_bias_label = "Sell rips below Local Flip"
        elif location_state == "at_local_flip":
            setup_bias_label = "Wait for Local Flip resolution"
        else:
            setup_bias_label = "Wait for cleaner structure"

    plan_label = setup_bias_label
    if suppress_aggressive_copy:
        if setup_bias_label == "Buy dips above Local Flip":
            plan_label = "Buy dips above Local Flip only if next session confirms"
        elif setup_bias_label == "Sell rips below Local Flip":
            plan_label = "Sell rips below Local Flip only if next session confirms"
        elif setup_bias_label == "Buy dips near support":
            plan_label = "Buy dips near support only if live structure confirms"
        elif setup_bias_label == "Sell rips near resistance":
            plan_label = "Sell rips near resistance only if rejection confirms"

    trade_gate_label = str(structure.get("required_trigger") or "").strip()
    if not trade_gate_label:
        if location_state == "above_local_flip":
            trade_gate_label = "Next live session dip-hold above Local Flip"
        elif location_state == "below_local_flip":
            trade_gate_label = "Next live session failed reclaim below Local Flip"
        elif structure_zone in {"near_call_wall", "above_call_wall"}:
            trade_gate_label = "Next live session rejection or retest at Call Wall"
        elif structure_zone in {"near_put_wall", "below_put_wall"}:
            trade_gate_label = "Next live session reclaim or bounce at Put Wall"
        else:
            trade_gate_label = "Next live session confirmation required"

    invalidation_label = str(structure.get("invalidation") or "").strip()
    if not invalidation_label:
        invalidation_label = (
            f"Lose Local Flip {local_flip:.0f}" if local_flip is not None else "Lose working level"
        )

    execution_status, execution_status_label, execution_status_badge = (
        ("waiting_for_confirmation", "Waiting for confirmation", "WAITING")
        if trade_state == "wait"
        else (
            ("trigger_required", "Context ready — trigger not confirmed", "READY")
            if trade_state == "ready"
            else (
                ("stand_down", "Stand down until cleaner structure", "NO TRADE")
                if trade_state in {"no_trade", "unavailable"}
                else ("planning_only", "Waiting for live-session confirmation", "PLANNING")
            )
        )
    )
    if is_planning_only:
        execution_status = "planning_only"
        execution_status_label = "Waiting for live-session confirmation"
        execution_status_badge = "PLANNING"

    current_read_label = (
        str(structure.get("current_read") or location_label).strip() or location_label
    )
    pullback_level_label = str(structure.get("pullback_level") or "").strip()
    if not pullback_level_label:
        if local_flip is not None:
            pullback_level_label = f"LF {local_flip:.0f}"
        elif call_wall is not None:
            pullback_level_label = f"CW {call_wall:.0f}"
        else:
            pullback_level_label = "Awaiting level"
    next_destination_label = str(structure.get("next_destination") or "").strip()
    if not next_destination_label:
        if call_wall is not None and spot is not None and spot <= call_wall:
            next_destination_label = f"CW {call_wall:.0f}"
        elif next_call_wall is not None:
            next_destination_label = f"NCW {next_call_wall:.0f}"
        elif put_wall is not None:
            next_destination_label = f"PW {put_wall:.0f}"
        else:
            next_destination_label = "Awaiting next test"

    context_lead_label = str(structure.get("plan_note") or legacy_playbook.get("why") or "").strip()
    if not context_lead_label:
        if structure_zone == "near_call_wall":
            context_lead_label = (
                "There is context, but location quality is reduced near resistance."
            )
        elif is_planning_only:
            context_lead_label = (
                "Planning posture is valid, but live confirmation is still required."
            )
        else:
            context_lead_label = "Execution context is waiting on a clean trigger."

    context_footer_label = " · ".join(
        part
        for part in (
            structure_zone_label,
            (
                gamma_reason_label
                if gamma_regime in {"unconfirmed", "unavailable"}
                else regime_confidence_label
            ),
        )
        if part
    )

    nearest_level_name = str(location.get("nearest_level_name") or "").strip()
    nearest_level_distance = _num(location.get("distance_points"))
    structure_status_label = str(location.get("status") or "").strip() or "Awaiting structure"
    action_context_label = _clip_text(setup_bias_label, limit=48)
    hero_title_label = f"{snapshot_mode_label} - {current_read_label.upper()}"
    session_summary_label = str(
        structure.get("session") or f"{session_mode_label} · {regime_confidence_label}"
    ).strip()

    ui_flags = {
        "is_planning_only": is_planning_only,
        "is_unconfirmed_gamma": is_unconfirmed_gamma,
        "is_actionable_now": trade_state == "ready" and not suppress_aggressive_copy,
        "show_unconfirmed_badge": is_unconfirmed_gamma,
        "suppress_aggressive_copy": suppress_aggressive_copy,
        "dim_unconfirmed_levels": is_unconfirmed_gamma
        or levels_source in {"last_valid_snapshot", "stale_snapshot"},
        "use_conditional_bias_tone": suppress_aggressive_copy,
    }

    return {
        "snapshot_mode": snapshot_mode,
        "snapshot_mode_label": snapshot_mode_label,
        "session_mode": session_mode,
        "session_mode_label": session_mode_label,
        "trade_state": trade_state,
        "trade_state_label": trade_state_label,
        "risk_label": risk_label,
        "gamma_regime": gamma_regime,
        "gamma_regime_label": gamma_regime_label,
        "gamma_regime_subtitle": gamma_regime_subtitle,
        "regime_reason": _normalize_key(gamma_reason_label),
        "regime_reason_label": gamma_reason_label,
        "regime_confidence": regime_confidence,
        "regime_confidence_label": regime_confidence_label,
        "spot": spot,
        "spot_label": _fmt_number(spot, decimals=2),
        "main_flip": main_flip,
        "local_flip": local_flip,
        "call_wall": call_wall,
        "put_wall": put_wall,
        "next_call_wall": next_call_wall,
        "next_put_wall": next_put_wall,
        "main_flip_label": _fmt_number(main_flip),
        "local_flip_label": _fmt_number(local_flip),
        "call_wall_label": _fmt_number(call_wall),
        "put_wall_label": _fmt_number(put_wall),
        "next_call_wall_label": _fmt_number(next_call_wall, fallback="--"),
        "next_put_wall_label": _fmt_number(next_put_wall, fallback="--"),
        "next_call_wall_source": str(structure.get("next_call_wall_source") or "unresolved"),
        "next_put_wall_source": str(structure.get("next_put_wall_source") or "unresolved"),
        "next_put_wall_confidence": str(structure.get("next_put_wall_confidence") or "none"),
        "secondary_structure_complete": bool(structure.get("secondary_structure_complete")),
        "secondary_structure_displayable": bool(structure.get("secondary_structure_displayable")),
        "resolution_source": str(structure.get("resolution_source") or "Unavailable"),
        "degraded_reason": str(structure.get("degraded_reason") or ""),
        "secondary_structure_resolution_source": str(
            structure.get("secondary_structure_resolution_source") or "Unavailable"
        ),
        "secondary_structure_degraded_reason": str(
            structure.get("secondary_structure_degraded_reason") or ""
        ),
        "levels_source": levels_source,
        "gamma_data_status": gamma_data_status,
        "location_state": location_state,
        "location_label": location_label,
        "structure_zone": structure_zone,
        "structure_zone_label": structure_zone_label,
        "structure_status_label": structure_status_label,
        "tradeability": execution_regime,
        "tradeability_label": execution_regime_label,
        "context_score": context_score,
        "context_grade": context_grade,
        "context_tone": context_tone,
        "bias_state": bias_state,
        "bias_label": bias_label,
        "bias_short_label": bias_short_label,
        "setup_bias_label": setup_bias_label,
        "action_context_label": action_context_label,
        "plan_label": plan_label,
        "trade_gate_label": trade_gate_label,
        "execution_status": execution_status,
        "execution_status_label": execution_status_label,
        "execution_status_badge": execution_status_badge,
        "invalidation_label": invalidation_label,
        "context_lead_label": context_lead_label,
        "context_footer_label": context_footer_label,
        "hero_title_label": hero_title_label,
        "state_strip_label": current_read_label.upper(),
        "mode_strip_label": snapshot_mode_label,
        "session_summary_label": session_summary_label,
        "nearest_level_name": nearest_level_name,
        "nearest_level_distance": nearest_level_distance,
        "structure_read": {
            "current_read_label": current_read_label,
            "pullback_level_label": pullback_level_label,
            "next_destination_label": next_destination_label,
        },
        "distance_metrics": {
            "to_main_flip": _num(distances.get("to_main_flip")),
            "to_local_flip": _num(distances.get("to_local_flip")),
            "to_call_wall": _num(distances.get("to_call_wall")),
            "to_put_wall": _num(distances.get("to_put_wall")),
        },
        "distance_rows": distance_rows,
        "ui_flags": ui_flags,
    }


def _market_pulse_regime_strip_viewmodel(
    *,
    spx_quote: Dict[str, Any],
    gamma_snapshot: Dict[str, Any],
    execution_chart: Dict[str, Any],
    spx_priority_context: Dict[str, Any],
) -> Dict[str, Any]:
    spot = spx_quote.get("price")
    spot_label = f"{float(spot):.2f}" if isinstance(spot, (int, float)) else "—"
    metrics = dict(spx_priority_context.get("metrics") or {})
    trap_state = str(metrics.get("trap_zone_state") or "unavailable").replace("_", " ")
    distance_to_flip = metrics.get("distance_to_flip")
    if not isinstance(distance_to_flip, (int, float)):
        distance_to_flip = None
    if trap_state in {"knife edge structure", "compressed trap zone"}:
        bias = "WAIT INSIDE RANGE"
    elif distance_to_flip is not None and distance_to_flip > 12:
        bias = "BUY DIPS ABOVE FLIP"
    elif distance_to_flip is not None and distance_to_flip < -12:
        bias = "SELL RIPS BELOW FLIP"
    else:
        bias = "WAIT FOR LEVEL TEST"
    return {
        "gamma_regime": str(execution_chart.get("regime") or gamma_snapshot.get("regime") or "—"),
        "behavior": str(execution_chart.get("environment") or "—"),
        "last_valid_session": str(execution_chart.get("last_valid_session_label") or "—"),
        "spot": spot_label,
        "bias": bias,
    }


def get_or_build_market_pulse_snapshot(
    *,
    ticker: str = DEFAULT_PLAYBOOK_TICKER,
    force_refresh: bool = False,
    now_et: Optional[datetime] = None,
    preloaded_snapshot: Optional[Dict[str, Any]] = None,
    preloaded_gamma_snapshot: Optional[Dict[str, Any]] = None,
    preloaded_quotes: Optional[List[Dict[str, Any]]] = None,
    preloaded_macro_events: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Build the canonical Market Pulse snapshot consumed by the page and hero API.

    The snapshot keeps spot resolution, gamma board selection, local flip
    validity, chart source, and planning/execution labels separate so a single
    weak field does not collapse the entire page into an ambiguous state.
    """

    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import market_pulse_runtime
    from mccain_capital.services import market_worker

    now_et = now_et or app_runtime.now_et()
    selected_ticker = get_supported_playbook_ticker(ticker)
    raw_snapshot = dict(
        preloaded_snapshot or _market_pulse_snapshot(force_refresh=force_refresh) or {}
    )
    gamma_snapshot = dict(preloaded_gamma_snapshot or gamma_map_service.get_gamma_snapshot() or {})

    if (force_refresh or not gamma_snapshot.get("asof")) and not current_app.config.get("TESTING"):
        runtime_payload = market_pulse_runtime.refresh_market_pulse_runtime(force_gamma=True)
        gamma_snapshot = dict(runtime_payload.get("gamma_snapshot") or gamma_snapshot)

    quotes = list(preloaded_quotes or [])
    if not quotes:
        quotes = _market_pulse_enrich_quotes(list(raw_snapshot.get("quotes") or []), now_et)
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

    spx_quote = next(
        (q for q in quotes if str(q.get("label") or q.get("symbol") or "").upper() == "SPX"), {}
    )
    macro_events = list(preloaded_macro_events or [])
    session_mode = _market_pulse_snapshot_session_mode(now_et)
    cached_playbook = _market_pulse_cached_playbook_snapshot(now_et)

    spot_meta = _market_pulse_resolve_spot_snapshot(
        session_mode=session_mode,
        spx_quote=spx_quote,
        gamma_snapshot=gamma_snapshot,
        now_et=now_et,
        last_good_snapshot=cached_playbook,
    )
    gamma_resolution = _market_pulse_resolve_gamma_payload(
        gamma_snapshot=gamma_snapshot,
        last_good_snapshot=cached_playbook,
        session_mode=session_mode,
        now_et=now_et,
    )
    resolved_levels = {
        key: dict(value or {}).get("value")
        for key, value in dict(gamma_resolution.get("level_meta") or {}).items()
    }
    resolved_spx_quote = _market_pulse_apply_resolved_spot(spx_quote, spot_meta)
    resolved_gamma_snapshot = _market_pulse_apply_resolved_levels(gamma_snapshot, gamma_resolution)
    active_quote = dict(resolved_spx_quote)
    active_gamma_snapshot = dict(resolved_gamma_snapshot)
    active_spot_meta = dict(spot_meta)
    active_level_meta = copy.deepcopy(dict(gamma_resolution.get("level_meta") or {}))
    active_local_flip_meta = copy.deepcopy(dict(gamma_resolution.get("local_flip_meta") or {}))
    active_gamma_regime_meta = copy.deepcopy(
        dict(gamma_resolution.get("gamma_regime_meta") or {})
    )
    active_levels_source = gamma_resolution.get("levels_source")
    active_gamma_data_status = gamma_resolution.get("gamma_data_status")
    active_structure_invariant_status = gamma_resolution.get("structure_invariant_status")
    active_structure_invariant_issues = list(gamma_resolution.get("structure_invariant_issues") or [])
    active_canonical_structure = dict(gamma_resolution.get("canonical_structure") or {})

    if selected_ticker != "SPX":
        instrument_quote = _market_pulse_quote_for_ticker(
            quotes,
            selected_ticker,
            fallback={"symbol": selected_ticker, "label": selected_ticker},
        )
        active_quote = instrument_quote
        active_gamma_snapshot = _market_pulse_scaled_gamma_snapshot(
            ticker=selected_ticker,
            base_gamma_snapshot=resolved_gamma_snapshot,
            base_quote=resolved_spx_quote,
            target_quote=instrument_quote,
        )
        active_spot_meta = _market_pulse_spot_meta_for_ticker(
            ticker=selected_ticker,
            quote=instrument_quote,
            session_mode=session_mode,
        )
        ratio = _market_pulse_proxy_ratio(resolved_spx_quote, instrument_quote)
        for payload in active_level_meta.values():
            if not isinstance(payload, dict):
                continue
            payload["value"] = _market_pulse_scale_price(payload.get("value"), ratio)
            if payload.get("label"):
                payload["label"] = _market_pulse_localize_label(payload.get("label"), selected_ticker)
        active_local_flip_meta = _market_pulse_localize_payload(active_local_flip_meta, selected_ticker)
        active_gamma_regime_meta = _market_pulse_localize_payload(
            active_gamma_regime_meta, selected_ticker
        )
        active_canonical_structure = _market_pulse_localize_payload(
            active_canonical_structure, selected_ticker
        )

    active_levels = {
        "main_flip": active_gamma_snapshot.get("gamma_flip_combined_basket"),
        "local_flip": active_gamma_snapshot.get("local_flip_aggregated_gamma"),
        "call_wall": active_gamma_snapshot.get("call_wall_aggregated_gamma"),
        "put_wall": active_gamma_snapshot.get("put_wall_aggregated_gamma"),
        "next_call_wall": active_gamma_snapshot.get("next_call_wall_above"),
        "next_put_wall": active_gamma_snapshot.get("next_put_wall_below"),
    }
    execution_chart = _market_pulse_execution_chart_viewmodel(
        spx_quote=active_quote,
        gamma_snapshot=active_gamma_snapshot,
        macro_events=macro_events,
        now_et=now_et,
        resolved_spot=active_spot_meta.get("value"),
        resolved_levels=active_levels,
    )
    chart_meta = _market_pulse_chart_source_viewmodel(
        execution_chart=execution_chart,
        session_mode=session_mode,
    )
    spx_priority_context = build_spx_priority_context(
        spx_quote=active_quote,
        gamma_snapshot=active_gamma_snapshot,
    )
    execution_model = _market_pulse_execution_model(
        spx_quote=active_quote,
        gamma_snapshot=active_gamma_snapshot,
        execution_chart=execution_chart,
        spx_priority_context=spx_priority_context,
    )
    market_structure_snapshot = _market_pulse_structure_snapshot(
        spx_quote=active_quote,
        gamma_snapshot=active_gamma_snapshot,
        execution_chart=execution_chart,
        execution_model=execution_model,
        now_et=now_et,
    )

    # Backend remains the source of truth for UI labels and provenance.
    market_structure_snapshot["spot"] = active_spot_meta.get("value")
    market_structure_snapshot["spot_meta"] = active_spot_meta
    market_structure_snapshot["level_meta"] = active_level_meta
    market_structure_snapshot["local_flip_meta"] = active_local_flip_meta
    market_structure_snapshot["gamma_regime_meta"] = active_gamma_regime_meta
    market_structure_snapshot["chart_meta"] = chart_meta
    market_structure_snapshot["levels_source"] = active_levels_source or market_structure_snapshot.get(
        "levels_source"
    )
    market_structure_snapshot["gamma_data_status"] = active_gamma_data_status or market_structure_snapshot.get(
        "gamma_data_status"
    )
    market_structure_snapshot["structure_invariant_status"] = (
        active_structure_invariant_status or market_structure_snapshot.get("structure_invariant_status")
    )
    market_structure_snapshot["structure_invariant_issues"] = list(
        active_structure_invariant_issues or market_structure_snapshot.get("structure_invariant_issues") or []
    )
    market_structure_snapshot["main_flip"] = active_levels.get("main_flip")
    market_structure_snapshot["local_flip"] = active_levels.get("local_flip")
    market_structure_snapshot["call_wall"] = active_levels.get("call_wall")
    market_structure_snapshot["put_wall"] = active_levels.get("put_wall")
    market_structure_snapshot["next_call_wall"] = active_levels.get("next_call_wall")
    market_structure_snapshot["next_put_wall"] = active_levels.get("next_put_wall")
    canonical_structure = active_canonical_structure
    market_structure_snapshot["next_call_wall_source"] = str(
        canonical_structure.get("next_call_wall_source") or "unresolved"
    )
    market_structure_snapshot["next_put_wall_source"] = str(
        canonical_structure.get("next_put_wall_source") or "unresolved"
    )
    market_structure_snapshot["next_put_wall_confidence"] = str(
        canonical_structure.get("next_put_wall_confidence") or "none"
    )
    market_structure_snapshot["secondary_structure_complete"] = bool(
        canonical_structure.get("secondary_structure_complete")
    )
    market_structure_snapshot["secondary_structure_displayable"] = bool(
        canonical_structure.get("secondary_structure_displayable")
    )
    market_structure_snapshot["resolution_source"] = str(
        canonical_structure.get("resolution_source") or "Unavailable"
    )
    market_structure_snapshot["degraded_reason"] = str(
        canonical_structure.get("degraded_reason") or ""
    )
    market_structure_snapshot["secondary_structure_resolution_source"] = str(
        canonical_structure.get("resolution_source") or "Unavailable"
    )
    market_structure_snapshot["secondary_structure_degraded_reason"] = str(
        canonical_structure.get("degraded_reason") or ""
    )
    market_structure_snapshot["session_mode"] = session_mode
    market_structure_snapshot["app_state"] = (
        spot_meta.get("state") if spot_meta.get("value") is not None else "UNAVAILABLE"
    )
    market_structure_snapshot["app_state_label"] = {
        "LIVE_SESSION": "Live Session",
        "AFTER_HOURS_VALID": "After-Hours Planning",
        "UNAVAILABLE": "Unavailable",
    }.get(str(market_structure_snapshot.get("app_state") or "UNAVAILABLE"), "Unavailable")
    market_structure_snapshot["session"] = (
        f"{market_structure_snapshot.get('session_mode_label') or session_mode.title()} · "
        f"{market_structure_snapshot.get('regime_confidence_label') or 'No confidence'}"
    )
    market_structure_snapshot["spot_source_label"] = active_spot_meta.get("source_label") or "Unavailable"
    market_structure_snapshot["spot_source"] = active_spot_meta.get("source") or ""
    market_structure_snapshot["spot_is_fallback"] = bool(active_spot_meta.get("spot_is_fallback"))
    market_structure_snapshot["fallback_reason"] = active_spot_meta.get("fallback_reason") or ""
    market_structure_snapshot["spot_source_short_label"] = (
        f"{selected_ticker} Spot"
        if active_spot_meta.get("source") == "live"
        else f"Spot · {active_spot_meta.get('source_label') or 'Unavailable'}"
    )
    merged_regime_state = _market_pulse_gamma_regime_state(
        active_gamma_snapshot,
        gamma_data_status=str(market_structure_snapshot.get("gamma_data_status") or ""),
        session_mode=session_mode,
        levels={
            "spot": market_structure_snapshot.get("spot"),
            "main_flip": market_structure_snapshot.get("main_flip"),
            "local_flip": market_structure_snapshot.get("local_flip"),
            "call_wall": market_structure_snapshot.get("call_wall"),
            "put_wall": market_structure_snapshot.get("put_wall"),
        },
        next_call_wall=market_structure_snapshot.get("next_call_wall"),
        next_put_wall=market_structure_snapshot.get("next_put_wall"),
    )
    merged_regime = _market_pulse_gamma_regime_viewmodel(
        active_gamma_snapshot,
        gamma_data_status=str(market_structure_snapshot.get("gamma_data_status") or ""),
        regime_status=str(merged_regime_state.get("regime_status") or ""),
        missing_reason=str(merged_regime_state.get("missing_reason") or ""),
    )
    merged_confidence = _market_pulse_regime_confidence(
        session_mode=session_mode,
        gamma_data_status=str(market_structure_snapshot.get("gamma_data_status") or ""),
        regime_status=str(merged_regime_state.get("regime_status") or ""),
    )
    market_structure_snapshot["gamma_regime_state"] = str(
        merged_regime_state.get("regime_status") or "unavailable"
    )
    market_structure_snapshot["gamma_board_status"] = str(
        merged_regime_state.get("board_status") or "Unavailable"
    )
    market_structure_snapshot["gamma_regime_confidence"] = {
        "high": "High",
        "medium": "Medium",
        "low": "Low",
        "none": "Invalid",
    }.get(merged_confidence, "Invalid")
    market_structure_snapshot["gamma_regime_missing_reason"] = str(
        merged_regime_state.get("missing_reason") or ""
    )
    market_structure_snapshot["gamma_regime"] = merged_regime.get(
        "gamma_regime"
    ) or market_structure_snapshot.get("gamma_regime")
    market_structure_snapshot["gamma_regime_label"] = merged_regime.get(
        "gamma_regime_label"
    ) or market_structure_snapshot.get("gamma_regime_label")
    market_structure_snapshot["gamma_regime_subtitle"] = merged_regime.get(
        "gamma_regime_subtitle"
    ) or market_structure_snapshot.get("gamma_regime_subtitle")
    market_structure_snapshot["regime_confidence"] = merged_confidence
    market_structure_snapshot["regime_confidence_label"] = {
        "high": "High confidence",
        "medium": "Medium confidence",
        "low": "Low confidence",
        "none": "No confidence",
    }.get(merged_confidence, "No confidence")
    market_structure_snapshot["gamma_regime_reason_label"] = _market_pulse_gamma_reason_label(
        gamma_regime=str(market_structure_snapshot.get("gamma_regime") or ""),
        gamma_data_status=str(market_structure_snapshot.get("gamma_data_status") or ""),
        levels_source=str(market_structure_snapshot.get("levels_source") or ""),
        structure_invariant_status=str(
            market_structure_snapshot.get("structure_invariant_status") or ""
        ),
        structure_invariant_issues=list(
            market_structure_snapshot.get("structure_invariant_issues") or []
        ),
        regime_status=str(market_structure_snapshot.get("gamma_regime_state") or ""),
        missing_reason=str(market_structure_snapshot.get("gamma_regime_missing_reason") or ""),
    )
    market_structure_snapshot["chart_state"] = chart_meta.get("chart_state")
    market_structure_snapshot["bars_source"] = chart_meta.get("bars_source")
    market_structure_snapshot["bars_as_of"] = chart_meta.get("bars_as_of")
    market_structure_snapshot["last_valid_snapshot_time"] = dict(
        active_gamma_regime_meta or {}
    ).get("as_of") or market_structure_snapshot.get("last_valid_snapshot_time")
    market_structure_snapshot["last_valid_snapshot_time_label"] = (
        _format_iso_et_label(market_structure_snapshot.get("last_valid_snapshot_time")) or "—"
    )
    market_structure_snapshot["snapshot_timestamp"] = (
        active_spot_meta.get("as_of")
        or chart_meta.get("bars_as_of")
        or market_structure_snapshot.get("snapshot_timestamp")
        or ""
    )
    market_structure_snapshot["snapshot_timestamp_label"] = (
        _format_iso_et_label(market_structure_snapshot.get("snapshot_timestamp")) or "—"
    )

    if (
        market_structure_snapshot.get("app_state") == "AFTER_HOURS_VALID"
        and market_structure_snapshot.get("levels_source") != "unavailable"
    ):
        market_structure_snapshot["trade_state"] = "PLANNING_ONLY"
        market_structure_snapshot["trade_state_label"] = "PLANNING ONLY"
        market_structure_snapshot["execution_regime"] = "planning_mode_only"
        market_structure_snapshot["execution_regime_label"] = "PLANNING_ONLY"
        market_structure_snapshot["tradeability"] = "PLANNING_ONLY"
        execution_model.setdefault("playbook", {})
        execution_model["playbook"]["status"] = "PLANNING_ONLY"

    if market_structure_snapshot.get("levels_source") == "unavailable":
        market_structure_snapshot["planning_bias"] = "unavailable"
        market_structure_snapshot["planning_bias_label"] = "Unavailable"
        market_structure_snapshot["bias"] = "Unavailable"
        if market_structure_snapshot.get("app_state") == "UNAVAILABLE":
            market_structure_snapshot["trade_state"] = "UNAVAILABLE"
            market_structure_snapshot["trade_state_label"] = "UNAVAILABLE"

    playbook_view = _build_playbook_view_model(
        market_structure_snapshot=market_structure_snapshot,
        execution_model=execution_model,
    )
    market_structure_snapshot["bias_state"] = playbook_view.get("bias_state")
    market_structure_snapshot["bias_context"] = playbook_view.get("location_label")
    market_structure_snapshot["bias_label"] = playbook_view.get("bias_short_label")
    market_structure_snapshot["bias"] = playbook_view.get("bias_label")
    market_structure_snapshot["current_read"] = playbook_view.get("structure_read", {}).get(
        "current_read_label"
    )
    market_structure_snapshot["pullback_level"] = playbook_view.get("structure_read", {}).get(
        "pullback_level_label"
    )
    market_structure_snapshot["next_destination"] = playbook_view.get("structure_read", {}).get(
        "next_destination_label"
    )
    market_structure_snapshot["plan_note"] = playbook_view.get("context_lead_label")
    market_structure_snapshot["best_look"] = playbook_view.get("plan_label")
    market_structure_snapshot["required_trigger"] = playbook_view.get("trade_gate_label")
    market_structure_snapshot["invalidation"] = playbook_view.get("invalidation_label")
    market_structure_snapshot["tradeability"] = playbook_view.get("tradeability_label")
    market_structure_snapshot["trade_state_label"] = playbook_view.get("trade_state_label")
    market_structure_snapshot["context_grade"] = playbook_view.get("context_grade")
    market_structure_snapshot["context_score"] = playbook_view.get("context_score")
    market_structure_snapshot["context_score_pct"] = playbook_view.get("context_score")
    market_structure_snapshot["context_tone"] = playbook_view.get("context_tone")

    if selected_ticker != "SPX":
        active_gamma_snapshot = _market_pulse_localize_payload(active_gamma_snapshot, selected_ticker)
        execution_chart = _market_pulse_localize_payload(execution_chart, selected_ticker)
        execution_model = _market_pulse_localize_payload(execution_model, selected_ticker)
        market_structure_snapshot = _market_pulse_localize_payload(
            market_structure_snapshot, selected_ticker
        )
        playbook_view = _market_pulse_localize_payload(playbook_view, selected_ticker)
        spx_priority_context = _market_pulse_localize_payload(spx_priority_context, selected_ticker)

    payload = {
        "snapshot": raw_snapshot,
        "ticker": selected_ticker,
        "gamma_snapshot": active_gamma_snapshot,
        "quotes": quotes,
        "spx_quote": active_quote,
        "playbook_quote": active_quote,
        "spx_priority_context": spx_priority_context,
        "playbook_priority_context": spx_priority_context,
        "execution_chart": execution_chart,
        "execution_model": execution_model,
        "market_structure_snapshot": market_structure_snapshot,
        "playbook_view": playbook_view,
    }
    if _market_pulse_playbook_snapshot_valid(payload):
        _market_pulse_store_playbook_snapshot(now_et, payload)
        _market_pulse_log(
            "playbook_snapshot_cached",
            app_state=market_structure_snapshot.get("app_state"),
            levels_source=market_structure_snapshot.get("levels_source"),
            snapshot_timestamp=market_structure_snapshot.get("snapshot_timestamp"),
        )
    else:
        _market_pulse_log(
            "playbook_snapshot_not_cached",
            app_state=market_structure_snapshot.get("app_state"),
            levels_source=market_structure_snapshot.get("levels_source"),
        )
    return payload


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
    impact_score = _market_news_score(row)
    category = _market_news_category(headline, summary, row, forced_tag=forced_tag)
    impact = _market_news_impact_label(category, impact_score)
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
        "impact": impact,
        "category": category,
        "impact_score": _market_news_score_bucket(impact_score),
        "datetime": int(row.get("datetime") or 0) if row.get("datetime") else 0,
    }


def _market_news_score_bucket(score: int) -> str:
    if score >= 12:
        return "High"
    if score >= 6:
        return "Medium"
    return "Low"


def _market_news_category(
    headline: str, summary: str, row: Dict[str, Any], *, forced_tag: str = ""
) -> str:
    text = f"{headline} {summary} {row.get('related') or ''} {forced_tag}".lower()
    if any(
        term in text
        for term in (
            "fed",
            "powell",
            "cpi",
            "pce",
            "fomc",
            "rates",
            "payroll",
            "inflation",
            "jobs",
            "treasury",
            "yield",
            "macro",
            "ism",
            "ppi",
            "nfp",
        )
    ):
        return "Macro"
    if any(
        term in text
        for term in (
            "earnings",
            "guidance",
            "results",
            "eps",
            "revenue",
            "sector",
            "semiconductor",
            "chip",
            "software",
            "energy",
            "oil",
            "bank",
            "financial",
            "healthcare",
            "biotech",
            "retail",
            "consumer",
            "industrial",
            "utilities",
            "materials",
            "real estate",
            "xlf",
            "xle",
            "xlk",
            "xli",
            "xlv",
            "xly",
            "xlp",
            "xlu",
            "xlb",
            "xlre",
            "soxx",
            "smh",
            "nvidia",
            "apple",
            "microsoft",
            "amazon",
            "meta",
            "tesla",
        )
    ):
        return "Sector"
    return "General"


def _market_news_impact_label(category: str, score: int) -> str:
    if category == "Macro" or score >= 12:
        return "High"
    if category == "Sector" or score >= 6:
        return "Medium"
    return "Low"


def _market_news_rss_snapshot(
    now_et: datetime,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    rss_rows: List[Dict[str, Any]] = []
    watchlist_items: List[Dict[str, Any]] = []
    for symbol in MARKET_PULSE_WATCHLIST_NEWS_SYMBOLS[:MARKET_NEWS_RSS_SYMBOL_LIMIT]:
        rows = _market_news_rss_rows(symbol, limit=4)
        if not rows:
            continue
        fresh_rows = [
            row
            for row in rows
            if _market_news_is_recent(row.get("datetime"), now_et, WATCHLIST_NEWS_MAX_AGE_SECONDS)
        ]
        if not fresh_rows:
            continue
        rss_rows.extend(fresh_rows)
        fresh_rows.sort(key=lambda row: _market_news_row_priority(row, now_et))
        watchlist_items.append(
            _market_news_item(fresh_rows[0], now_et=now_et, symbol=symbol, forced_tag=symbol)
        )
    investing_rows = _market_news_investing_rows(limit=6)
    rss_rows.extend(
        [
            row
            for row in investing_rows
            if _market_news_is_recent(row.get("datetime"), now_et, MARKET_NEWS_MAX_AGE_SECONDS)
        ]
    )
    return _dedupe_market_news_rows(rss_rows), watchlist_items


def _market_news_rss_rows_from_url(
    url: str, *, source_label: str, related: str = "", limit: int = 8
) -> List[Dict[str, Any]]:
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
                "source": source_label,
                "url": link,
                "datetime": stamp,
                "related": related,
            }
        )
    return rows


def _market_news_rss_rows(symbol: str, *, limit: int = 8) -> List[Dict[str, Any]]:
    params = {
        "s": symbol,
        "region": "US",
        "lang": "en-US",
    }
    url = YAHOO_RSS_SYMBOL_URL + "?" + urllib.parse.urlencode(params)
    return _market_news_rss_rows_from_url(
        url,
        source_label="Yahoo Finance RSS",
        related=symbol,
        limit=limit,
    )


def _market_news_investing_rows(*, limit: int = 8) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for url in INVESTING_RSS_URLS:
        rows.extend(
            _market_news_rss_rows_from_url(
                url,
                source_label="Investing.com RSS",
                related="SPX",
                limit=max(3, limit),
            )
        )
    deduped: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("headline") or "").strip().lower(), str(row.get("url") or "").strip())
        if not key[0]:
            continue
        prior = deduped.get(key)
        if prior is None or int(row.get("datetime") or 0) > int(prior.get("datetime") or 0):
            deduped[key] = row
    return list(deduped.values())[: max(1, int(limit))]


def _market_pulse_feed_impact(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"HIGH", "MED", "FLOW", "LOW"}:
        return raw
    if raw == "HIGH":
        return "HIGH"
    if raw == "MEDIUM":
        return "MED"
    if raw == "LOW":
        return "LOW"
    return "LOW"


def _market_pulse_feed_priority(item: Dict[str, Any]) -> Tuple[int, int, int]:
    impact = _market_pulse_feed_impact(item.get("impact"))
    impact_order = {"HIGH": 0, "MED": 1, "FLOW": 2, "LOW": 3}.get(impact, 4)
    score_bucket = str(item.get("impact_score") or "").strip().lower()
    score_order = {"high": 0, "medium": 1, "low": 2}.get(score_bucket, 3)
    return (
        impact_order,
        score_order,
        -int(item.get("datetime") or 0),
    )


def _market_pulse_feed_item(
    row: Dict[str, Any], *, fallback_source: str = "Market"
) -> Dict[str, Any]:
    headline = str(row.get("headline") or "Market headline").strip() or "Market headline"
    summary = str(row.get("summary") or row.get("why") or "").strip()
    source = (
        str(row.get("source_label") or row.get("source") or fallback_source).strip()
        or fallback_source
    )
    source_handle = str(row.get("source_handle") or source).strip() or source
    category = str(row.get("category") or "General").strip() or "General"
    impact = _market_pulse_feed_impact(row.get("impact"))
    return {
        "headline": headline,
        "summary": summary,
        "source": str(row.get("source") or fallback_source).strip() or fallback_source,
        "source_label": source,
        "source_handle": source_handle,
        "url": str(row.get("url") or "").strip(),
        "published_label": str(row.get("published_label") or "Just now").strip() or "Just now",
        "absolute_label": str(row.get("absolute_label") or "").strip(),
        "impact": impact,
        "category": category,
        "impact_score": str(row.get("impact_score") or "Low").strip() or "Low",
        "datetime": int(row.get("datetime") or 0),
        "tag": str(row.get("tag") or category).strip() or category,
        "why": str(row.get("why") or category).strip() or category,
    }


def _market_news_compose_feed(
    *,
    market_items: List[Dict[str, Any]],
    macro_events: List[Dict[str, Any]],
    watchlist_items: List[Dict[str, Any]],
    x_feed_items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in list(market_items or [])[:MARKET_NEWS_FEED_LIMIT]:
        if isinstance(item, dict):
            rows.append(_market_pulse_feed_item(item, fallback_source="Market"))
    for item in list(watchlist_items or [])[:4]:
        if isinstance(item, dict):
            rows.append(_market_pulse_feed_item(item, fallback_source="Watchlist"))
    for item in list(macro_events or [])[:4]:
        if isinstance(item, dict):
            enriched = dict(item)
            enriched.setdefault("impact", "HIGH")
            enriched.setdefault("category", "Macro")
            enriched.setdefault("impact_score", "High")
            rows.append(_market_pulse_feed_item(enriched, fallback_source="Macro"))
    for item in list(x_feed_items or [])[:4]:
        if isinstance(item, dict):
            rows.append(_market_pulse_feed_item(item, fallback_source="X"))
    deduped = _dedupe_market_news_rows(rows)
    deduped.sort(key=_market_pulse_feed_priority)
    return deduped[:MARKET_NEWS_FEED_LIMIT]


def get_supported_playbook_ticker(value: Any) -> str:
    ticker = str(value or "").strip().upper()
    return ticker if ticker in SUPPORTED_PLAYBOOK_TICKERS else DEFAULT_PLAYBOOK_TICKER


def get_playbook_ticker_context(value: Any) -> Dict[str, Any]:
    ticker = get_supported_playbook_ticker(value)
    alternate = next(
        (item for item in SUPPORTED_PLAYBOOK_TICKERS if item != ticker),
        DEFAULT_PLAYBOOK_TICKER,
    )
    return {
        "ticker": ticker,
        "alternate_ticker": alternate,
        "supported_tickers": SUPPORTED_PLAYBOOK_TICKERS,
        "storage_key": PLAYBOOK_TICKER_STORAGE_KEY,
    }


def _dashboard_tape_symbols(selected_ticker: str) -> List[str]:
    return ["SPY", "QQQ", "VIX", "IWM"]


def _market_pulse_quote_for_ticker(
    quotes: List[Dict[str, Any]],
    ticker: str,
    *,
    fallback: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    resolved_ticker = str(ticker or "").strip().upper()
    for row in list(quotes or []):
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or row.get("label") or "").strip().upper()
        if symbol == resolved_ticker:
            return dict(row)
    return dict(fallback or {})


def _market_pulse_round_price(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return round(numeric, 4)


def _market_pulse_scale_price(value: Any, ratio: Optional[float]) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if ratio is None:
        return None
    return _market_pulse_round_price(numeric * ratio)


def _market_pulse_localize_label(value: Any, ticker: str) -> Any:
    if not isinstance(value, str):
        return value
    localized = re.sub(r"\bSPX\b", ticker, value)
    localized = re.sub(r"\bS&P 500\b", ticker, localized, flags=re.IGNORECASE)
    return localized


def _market_pulse_localize_payload(value: Any, ticker: str) -> Any:
    if isinstance(value, dict):
        return {
            key: _market_pulse_localize_payload(item, ticker)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_market_pulse_localize_payload(item, ticker) for item in value]
    return _market_pulse_localize_label(value, ticker)


def _market_pulse_proxy_ratio(
    base_quote: Dict[str, Any],
    target_quote: Dict[str, Any],
) -> Optional[float]:
    try:
        base_price = float(base_quote.get("price"))
        target_price = float(target_quote.get("price"))
    except (TypeError, ValueError):
        return None
    if base_price <= 0 or target_price <= 0:
        return None
    return target_price / base_price


def _market_pulse_scaled_gamma_snapshot(
    *,
    ticker: str,
    base_gamma_snapshot: Dict[str, Any],
    base_quote: Dict[str, Any],
    target_quote: Dict[str, Any],
) -> Dict[str, Any]:
    scaled = copy.deepcopy(dict(base_gamma_snapshot or {}))
    ratio = _market_pulse_proxy_ratio(base_quote, target_quote)
    if ratio is None:
        scaled["spot"] = _market_pulse_round_price(target_quote.get("price"))
        scaled["spot_price_used"] = _market_pulse_round_price(target_quote.get("price"))
        scaled["symbol"] = ticker
        scaled["proxy_scale_ratio"] = None
        scaled["proxy_source"] = "unavailable"
        return _market_pulse_localize_payload(scaled, ticker)

    scalar_fields = (
        "spot",
        "spot_price_used",
        "gamma_flip_combined_basket",
        "local_flip_aggregated_gamma",
        "call_wall_aggregated_gamma",
        "put_wall_aggregated_gamma",
        "local_flip_distance_from_spot",
        "nearest_positive_gamma_above_spot",
        "nearest_negative_gamma_below_spot",
        "gamma_range_estimate",
        "gamma_range_high",
        "gamma_range_low",
        "next_call_wall_above",
        "next_put_wall_below",
    )
    for field in scalar_fields:
        if field in scaled:
            scaled[field] = _market_pulse_scale_price(scaled.get(field), ratio)

    for field in (
        "gamma_walls_top3",
        "top_positive_strikes",
        "top_negative_strikes",
        "top_3_positive_gamma_strikes",
        "top_3_negative_gamma_strikes",
    ):
        values = scaled.get(field)
        if isinstance(values, list):
            scaled[field] = [
                scaled_value
                for scaled_value in (_market_pulse_scale_price(item, ratio) for item in values)
                if scaled_value is not None
            ]

    void_zone = dict(scaled.get("void_zone") or {})
    if void_zone:
        void_zone["start"] = _market_pulse_scale_price(void_zone.get("start"), ratio)
        void_zone["end"] = _market_pulse_scale_price(void_zone.get("end"), ratio)
        scaled["void_zone"] = void_zone

    grouped_rows = []
    for row in list(scaled.get("grouped_strike_rows") or []):
        if not isinstance(row, dict):
            continue
        next_row = dict(row)
        next_row["strike"] = _market_pulse_scale_price(row.get("strike"), ratio)
        grouped_rows.append(next_row)
    if grouped_rows:
        scaled["grouped_strike_rows"] = grouped_rows

    scaled["spot"] = _market_pulse_round_price(target_quote.get("price"))
    scaled["spot_price_used"] = _market_pulse_round_price(target_quote.get("price"))
    scaled["spot_source"] = str(target_quote.get("provider") or target_quote.get("reason") or "market_snapshot")
    scaled["spot_source_label"] = f"Live {ticker} Quote"
    scaled["spot_source_name"] = ticker
    scaled["spot_source_timestamp"] = str(
        target_quote.get("asof") or target_quote.get("as_of") or scaled.get("spot_source_timestamp") or ""
    )
    scaled["symbol"] = ticker
    scaled["proxy_scale_ratio"] = ratio
    scaled["proxy_source"] = "scaled_from_spx_gamma_snapshot"
    scaled["warning_state"] = dict(scaled.get("warning_state") or {})
    warning_notes = list(scaled["warning_state"].get("warnings") or [])
    proxy_warning = (
        f"{ticker} playbook levels are currently scaled from the shared SPX gamma snapshot."
    )
    if proxy_warning not in warning_notes:
        warning_notes.append(proxy_warning)
    scaled["warning_state"]["warnings"] = warning_notes
    warnings = list(scaled.get("warnings") or [])
    if proxy_warning not in warnings:
        warnings.append(proxy_warning)
    scaled["warnings"] = warnings
    return _market_pulse_localize_payload(scaled, ticker)


def _market_pulse_spot_meta_for_ticker(
    *,
    ticker: str,
    quote: Dict[str, Any],
    session_mode: str,
) -> Dict[str, Any]:
    price = _market_pulse_round_price(quote.get("price"))
    as_of = str(quote.get("asof") or quote.get("as_of") or "")
    state = "LIVE_SESSION" if price is not None and session_mode == "regular" else "AFTER_HOURS_VALID"
    if price is None:
        state = "UNAVAILABLE"
    provider = str(quote.get("provider") or quote.get("reason") or "market_snapshot")
    source_label = f"Live {ticker} Quote" if price is not None else f"{ticker} quote unavailable"
    return {
        "value": price,
        "source": provider,
        "source_label": source_label,
        "as_of": as_of,
        "state": state,
        "spot_is_fallback": price is None,
        "fallback_reason": (
            f"{ticker} quote unavailable; instrument playbook levels cannot be refreshed."
            if price is None
            else ""
        ),
    }


def _market_pulse_x_clean_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _x_api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    if not X_BEARER_TOKEN:
        return None
    query = urllib.parse.urlencode(params or {})
    url = f"{X_API_BASE_URL}{path}"
    if query:
        url = f"{url}?{query}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {X_BEARER_TOKEN}",
            "User-Agent": "McCainCapital/1.0",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=X_API_TIMEOUT_SECONDS) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (
        urllib.error.URLError,
        urllib.error.HTTPError,
        TimeoutError,
        OSError,
        json.JSONDecodeError,
    ):
        return None


def _market_pulse_x_user_ids(handles: List[str]) -> Dict[str, str]:
    normalized = [
        str(handle or "").lstrip("@").strip() for handle in handles if str(handle or "").strip()
    ]
    if not normalized or not X_BEARER_TOKEN:
        return {}
    now_et = app_runtime.now_et()
    cached_at = _market_pulse_x_user_cache.get("fetched_at")
    cached_payload = _market_pulse_x_user_cache.get("payload") or {}
    if (
        isinstance(cached_at, datetime)
        and isinstance(cached_payload, dict)
        and (now_et - cached_at).total_seconds() < 12 * 60 * 60
        and all(str(handle).lower() in cached_payload for handle in normalized)
    ):
        return {
            str(handle).lower(): str(cached_payload.get(str(handle).lower()) or "")
            for handle in normalized
            if str(cached_payload.get(str(handle).lower()) or "").strip()
        }

    payload = _x_api_get(
        "/users/by",
        {
            "usernames": ",".join(normalized),
            "user.fields": "id,username,name",
        },
    )
    rows = payload.get("data") if isinstance(payload, dict) else []
    resolved = dict(cached_payload) if isinstance(cached_payload, dict) else {}
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        username = str(row.get("username") or "").strip().lower()
        user_id = str(row.get("id") or "").strip()
        if username and user_id:
            resolved[username] = user_id
    _market_pulse_x_user_cache["fetched_at"] = now_et
    _market_pulse_x_user_cache["payload"] = resolved
    return {
        str(handle).lower(): str(resolved.get(str(handle).lower()) or "")
        for handle in normalized
        if str(resolved.get(str(handle).lower()) or "").strip()
    }


def _market_pulse_x_rows_from_api(
    account: Dict[str, str], *, user_id: str, limit: int = 4
) -> List[Dict[str, Any]]:
    handle = str(account.get("handle") or "").lstrip("@").strip()
    if not handle or not user_id or not X_BEARER_TOKEN:
        return []
    payload = _x_api_get(
        f"/users/{urllib.parse.quote(user_id, safe='')}/tweets",
        {
            "exclude": "retweets,replies",
            "max_results": max(3, int(limit)),
            "tweet.fields": "created_at,lang",
        },
    )
    rows = payload.get("data") if isinstance(payload, dict) else []
    items: List[Dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        created_at_raw = str(row.get("created_at") or "").strip()
        stamp = 0
        if created_at_raw:
            try:
                stamp = int(
                    datetime.fromisoformat(created_at_raw.replace("Z", "+00:00")).timestamp()
                )
            except Exception:
                stamp = 0
        text = _market_pulse_x_clean_text(row.get("text") or "")
        if not text:
            continue
        tweet_id = str(row.get("id") or "").strip()
        items.append(
            {
                "headline": text,
                "summary": text,
                "source": "X API",
                "url": (
                    f"https://x.com/{handle}/status/{tweet_id}"
                    if tweet_id
                    else f"https://x.com/{handle}"
                ),
                "datetime": stamp,
                "related": f"@{handle}",
                "account_handle": f"@{handle}",
                "account_label": str(account.get("label") or handle),
                "account_lane": str(account.get("lane") or ""),
            }
        )
    return items


def _market_pulse_x_relevance_score(text: str, handle: str = "") -> int:
    raw = f"{text} {handle}".lower()
    weighted = {
        "fed": 6,
        "powell": 6,
        "fomc": 6,
        "cpi": 6,
        "pce": 6,
        "jobs": 5,
        "payroll": 5,
        "nfp": 5,
        "inflation": 5,
        "rates": 5,
        "yield": 5,
        "treasury": 5,
        "recession": 4,
        "tariff": 5,
        "sanction": 5,
        "geopolit": 4,
        "spx": 6,
        "spy": 5,
        "es ": 4,
        "s&p": 6,
        "options": 4,
        "gamma": 5,
        "vix": 5,
        "put wall": 5,
        "call wall": 5,
        "dealer": 4,
        "flow": 3,
        "0dte": 4,
        "white house": 3,
        "potus": 3,
        "trump": 3,
        "breaking": 3,
    }
    score = 0
    for term, value in weighted.items():
        if term in raw:
            score += value
    if any(
        name in raw
        for name in ("reuters", "bloomberg", "wsj", "kobeissi", "unusual whales", "deitaone")
    ):
        score += 1
    return score


def _market_pulse_x_category(text: str, lane: str = "") -> str:
    raw = f"{text} {lane}".lower()
    if any(term in raw for term in ("fed", "powell", "fomc")):
        return "Fed"
    if any(term in raw for term in ("yield", "treasury", "rates", "2y", "10y")):
        return "Rates"
    if any(
        term in raw
        for term in (
            "cpi",
            "pce",
            "inflation",
            "jobs",
            "payroll",
            "nfp",
            "ism",
            "pmi",
            "gdp",
            "macro",
        )
    ):
        return "Macro"
    if any(
        term in raw
        for term in (
            "tariff",
            "sanction",
            "executive order",
            "white house",
            "potus",
            "trump",
            "administration",
            "policy",
        )
    ):
        return "Policy"
    if any(
        term in raw
        for term in ("war", "geopolit", "iran", "china", "russia", "taiwan", "israel", "opec")
    ):
        return "Geopolitics"
    if any(
        term in raw
        for term in (
            "options",
            "gamma",
            "vix",
            "put wall",
            "call wall",
            "dealer",
            "flow",
            "0dte",
            "unusual whales",
        )
    ):
        return "Options Flow"
    if any(term in raw for term in ("earnings", "guidance", "eps", "revenue")):
        return "Earnings"
    return "Breaking"


def _market_pulse_x_impact(category: str, score: int) -> str:
    if category in {"Fed", "Macro", "Rates", "Policy", "Geopolitics"} or score >= 12:
        return "HIGH"
    if category in {"Earnings", "Breaking"} or score >= 8:
        return "MED"
    if category == "Options Flow" or score >= 6:
        return "FLOW"
    return "LOW"


def _market_pulse_x_priority(impact: str) -> int:
    order = {"HIGH": 0, "MED": 1, "FLOW": 2, "LOW": 3}
    return order.get(str(impact or "").upper(), 4)


def _market_pulse_x_canonical_url(handle: str, link: str) -> str:
    text = str(link or "").strip()
    match = re.search(r"/status/(\d+)", text)
    if match:
        return f"https://x.com/{handle}/status/{match.group(1)}"
    if "x.com/" in text or "twitter.com/" in text:
        return text
    return f"https://x.com/{handle}"


def _market_pulse_x_summary(text: str, *, limit: int = 168) -> str:
    cleaned = _market_pulse_x_clean_text(text)
    if len(cleaned) <= limit:
        return cleaned
    trimmed = cleaned[:limit].rsplit(" ", 1)[0].strip()
    return f"{trimmed}…"


def _market_pulse_x_rows_from_account(
    account: Dict[str, str], *, limit: int = 2
) -> List[Dict[str, Any]]:
    handle = str(account.get("handle") or "").lstrip("@").strip()
    if not handle:
        return []
    for template in MARKET_PULSE_X_RSS_URLS:
        url = template.format(handle=urllib.parse.quote(handle, safe=""))
        rows = _market_news_rss_rows_from_url(
            url,
            source_label="X",
            related=f"@{handle}",
            limit=max(1, int(limit)),
        )
        if rows:
            for row in rows:
                row["account_handle"] = f"@{handle}"
                row["account_label"] = str(account.get("label") or handle)
                row["account_lane"] = str(account.get("lane") or "")
                row["url"] = _market_pulse_x_canonical_url(handle, str(row.get("url") or ""))
            return rows
    return []


def _market_pulse_x_item(row: Dict[str, Any], *, now_et: datetime) -> Dict[str, Any]:
    handle = str(row.get("account_handle") or "@source").strip() or "@source"
    lane = str(row.get("account_lane") or "").strip()
    headline = _market_pulse_x_clean_text(row.get("headline") or row.get("summary") or "")
    summary = _market_pulse_x_summary(row.get("summary") or headline)
    score = _market_pulse_x_relevance_score(f"{headline} {summary}", handle=handle)
    category = _market_pulse_x_category(f"{headline} {summary}", lane=lane)
    impact = _market_pulse_x_impact(category, score)
    return {
        "headline": headline or "Market Pulse post",
        "summary": summary,
        "source": "X",
        "source_handle": handle,
        "source_label": str(row.get("account_label") or handle.lstrip("@")),
        "source_lane": lane or category,
        "url": str(row.get("url") or f"https://x.com/{handle.lstrip('@')}"),
        "published_label": _market_news_age_label(row.get("datetime"), now_et),
        "absolute_label": _market_news_timestamp_label(row.get("datetime")),
        "impact": impact,
        "category": category,
        "impact_score": _market_news_score_bucket(score),
        "relevance_score": score,
        "datetime": int(row.get("datetime") or 0),
        "tag": handle,
        "why": lane or category,
    }


def _market_pulse_x_feed(now_et: datetime) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if X_BEARER_TOKEN:
        user_ids = _market_pulse_x_user_ids(
            [str(account.get("handle") or "") for account in MARKET_PULSE_X_ACCOUNTS]
        )
        for account in MARKET_PULSE_X_ACCOUNTS:
            handle = str(account.get("handle") or "").lstrip("@").strip().lower()
            user_id = str(user_ids.get(handle) or "").strip()
            if not user_id:
                continue
            rows.extend(
                _market_pulse_x_rows_from_api(
                    account,
                    user_id=user_id,
                    limit=MARKET_PULSE_X_API_PER_ACCOUNT_LIMIT,
                )
            )
    if not rows:
        for account in MARKET_PULSE_X_ACCOUNTS:
            rows.extend(
                _market_pulse_x_rows_from_account(
                    account,
                    limit=MARKET_PULSE_X_PER_ACCOUNT_LIMIT,
                )
            )
    deduped = _dedupe_market_news_rows(rows)
    items = [
        _market_pulse_x_item(row, now_et=now_et)
        for row in deduped
        if _market_news_is_recent(row.get("datetime"), now_et, WATCHLIST_NEWS_MAX_AGE_SECONDS)
    ]
    relevant = [
        item
        for item in items
        if int(item.get("relevance_score") or 0) >= MARKET_PULSE_X_MIN_RELEVANCE
    ]
    relevant.sort(
        key=lambda item: (
            _market_pulse_x_priority(str(item.get("impact") or "")),
            -int(item.get("relevance_score") or 0),
            -int(item.get("datetime") or 0),
        )
    )
    return relevant[:MARKET_PULSE_X_FEED_LIMIT]


def _dedupe_market_news_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in list(rows or []):
        if not isinstance(row, dict):
            continue
        headline = str(row.get("headline") or "").strip().lower()
        url = str(row.get("url") or "").strip()
        if not headline:
            continue
        key = (headline, url)
        prior = deduped.get(key)
        if prior is None or int(row.get("datetime") or 0) > int(prior.get("datetime") or 0):
            deduped[key] = row
    return list(deduped.values())


def _market_news_macro_events(now_et: datetime) -> List[Dict[str, Any]]:
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
                "starts_at": str(event.get("starts_at") or ""),
                "time_label": str(event.get("time_label") or ""),
                "iso": str(event.get("iso") or ""),
                "tag": "Macro",
                "why": str(event.get("tooltip") or "Calendar event"),
            }
        )
    return macro_events


def _market_news_snapshot(
    *,
    now_et: Optional[datetime] = None,
    quotes: Optional[List[Dict[str, Any]]] = None,
    context: Optional[Dict[str, Any]] = None,
    market_structure_snapshot: Optional[Dict[str, Any]] = None,
    macro_events: Optional[List[Dict[str, Any]]] = None,
    force_refresh_feed: bool = False,
    page_type: str = "",
    feed_limit: int = 0,
) -> Dict[str, Any]:
    now_et = now_et or app_runtime.now_et()
    macro_events = list(macro_events or _market_news_macro_events(now_et))

    feed_snapshot = build_twitter_feed_snapshot(
        limit=feed_limit,
        now_et=now_et,
        market_structure_snapshot=market_structure_snapshot,
        force_refresh=force_refresh_feed,
        page_type=page_type,
    )
    result = {
        "available": bool(feed_snapshot.get("top_items") or macro_events),
        "source_note": str(feed_snapshot.get("source_note") or ""),
        "macro_events": macro_events,
        "market_items": list(feed_snapshot.get("market_items") or []),
        "watchlist_items": [],
        "pulse_feed_available": bool(feed_snapshot.get("top_items")),
        "pulse_feed_source_note": str(feed_snapshot.get("source_note") or ""),
        "pulse_feed_accounts": list(feed_snapshot.get("sources_monitored") or []),
        "pulse_feed_items": list(feed_snapshot.get("top_items") or []),
        "fetched_at": str(feed_snapshot.get("updated_at") or now_et.isoformat()),
        "market_feed_snapshot": feed_snapshot,
    }
    _market_news_cache["fetched_at"] = now_et
    _market_news_cache["payload"] = result
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


def _load_dashboard_pace_settings() -> Dict[str, Any]:
    custom_daily = float(app_runtime.get_setting_float("dashboard_pace_daily", 0.0) or 0.0)
    pass_buffer = float(app_runtime.get_setting_float("dashboard_pace_buffer", 0.0) or 0.0)
    start_date_raw = str(app_runtime.get_setting_value("dashboard_projection_start_date", "") or "")
    target_date_raw = str(
        app_runtime.get_setting_value("dashboard_projection_target_date", "") or ""
    )
    start_date = ""
    if start_date_raw:
        try:
            start_date = date.fromisoformat(start_date_raw).isoformat()
        except ValueError:
            start_date = ""
    target_date = ""
    if target_date_raw:
        try:
            target_date = date.fromisoformat(target_date_raw).isoformat()
        except ValueError:
            target_date = ""
    return {
        "custom_daily": max(0.0, custom_daily),
        "custom_enabled": custom_daily > 0.0,
        "pass_buffer": max(0.0, pass_buffer),
        "pass_buffer_enabled": pass_buffer > 0.0,
        "start_date": start_date,
        "target_date": target_date,
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


def _dashboard_reflection_setting_key(day: str) -> str:
    return f"dashboard_reflection::{str(day or '').strip()}"


def _load_dashboard_reflection_settings(day: str) -> Dict[str, str]:
    raw = str(app_runtime.get_setting_value(_dashboard_reflection_setting_key(day), "") or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _save_dashboard_reflection_settings(day: str, payload: Dict[str, Any]) -> None:
    answer = str(payload.get("answer") or "").strip().lower()
    if answer not in {"yes", "no"}:
        answer = ""
    clean = {
        "answer": answer,
        "break_alignment": str(payload.get("break_alignment") or "").strip()[:220],
        "urgency_trigger": str(payload.get("urgency_trigger") or "").strip()[:220],
        "obey_tomorrow": str(payload.get("obey_tomorrow") or "").strip()[:220],
    }
    if answer != "no":
        clean["break_alignment"] = ""
        clean["urgency_trigger"] = ""
        clean["obey_tomorrow"] = ""
    app_runtime.set_setting_value(_dashboard_reflection_setting_key(day), json.dumps(clean))


def _dashboard_reflection_viewmodel(day: str) -> Dict[str, Any]:
    saved = _load_dashboard_reflection_settings(day)
    answer = str(saved.get("answer") or "").strip().lower()
    if answer not in {"yes", "no"}:
        answer = ""
    has_entry = bool(
        answer
        or saved.get("break_alignment")
        or saved.get("urgency_trigger")
        or saved.get("obey_tomorrow")
    )
    return {
        "day": day,
        "answer": answer,
        "break_alignment": str(saved.get("break_alignment") or "").strip(),
        "urgency_trigger": str(saved.get("urgency_trigger") or "").strip(),
        "obey_tomorrow": str(saved.get("obey_tomorrow") or "").strip(),
        "has_entry": has_entry,
        "status_label": "Saved to day" if has_entry else "Not saved",
    }


def _dashboard_behavior_setting_key(day: str) -> str:
    return f"dashboard_behavior::{str(day or '').strip()}"


def _load_dashboard_behavior_settings(day: str) -> Dict[str, Any]:
    raw = str(app_runtime.get_setting_value(_dashboard_behavior_setting_key(day), "") or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _save_dashboard_behavior_settings(day: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    current = _load_dashboard_behavior_settings(day)
    urgency_count = int(current.get("urgency_count") or 0)
    urgency_increment_count = 0
    try:
        urgency_increment_count = max(0, int(payload.get("urgency_increment_count") or 0))
    except (TypeError, ValueError):
        urgency_increment_count = 0
    if urgency_increment_count > 0:
        urgency_count += urgency_increment_count
    elif bool(payload.get("increment_urgency")):
        urgency_count += 1
    elif "urgency_count" in payload:
        try:
            urgency_count = max(0, int(payload.get("urgency_count") or 0))
        except (TypeError, ValueError):
            urgency_count = max(0, urgency_count)

    discipline_state = str(payload.get("discipline_state") or current.get("discipline_state") or "").strip().lower()
    if discipline_state not in {"locked-in", "neutral", "urgent", "frustrated"}:
        discipline_state = str(current.get("discipline_state") or "locked-in").strip().lower() or "locked-in"

    discipline_mode = str(payload.get("discipline_mode") or current.get("discipline_mode") or "").strip().lower()
    if discipline_mode not in {"a-plus-only", "manage-winner", "done-for-day"}:
        discipline_mode = str(current.get("discipline_mode") or "a-plus-only").strip().lower() or "a-plus-only"

    def _int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    clean = {
        "day": day,
        "discipline_state": discipline_state,
        "discipline_mode": discipline_mode,
        "gate_count": max(0, min(3, _int(payload.get("gate_count"), _int(current.get("gate_count"), 0)))),
        "routine_done": max(0, _int(payload.get("routine_done"), _int(current.get("routine_done"), 0))),
        "routine_total": max(0, _int(payload.get("routine_total"), _int(current.get("routine_total"), 16))),
        "alignment_pct": max(0, min(100, _int(payload.get("alignment_pct"), _int(current.get("alignment_pct"), 0)))),
        "intention": str(payload.get("intention") or current.get("intention") or "").strip()[:140],
        "reflection_answer": str(payload.get("reflection_answer") or current.get("reflection_answer") or "").strip().lower()[:8],
        "urgency_count": urgency_count,
        "updated_at": str(payload.get("updated_at") or app_runtime.now_iso()).strip() or app_runtime.now_iso(),
    }
    if clean["reflection_answer"] not in {"yes", "no"}:
        clean["reflection_answer"] = ""
    app_runtime.set_setting_value(_dashboard_behavior_setting_key(day), json.dumps(clean, separators=(",", ":")))
    return clean


def _dashboard_behavior_status(saved: Dict[str, Any]) -> Dict[str, str]:
    alignment_pct = 0
    try:
        alignment_pct = int(saved.get("alignment_pct") or 0)
    except (TypeError, ValueError):
        alignment_pct = 0
    discipline_state = str(saved.get("discipline_state") or "").strip().lower()
    discipline_mode = str(saved.get("discipline_mode") or "").strip().lower()
    reflection_answer = str(saved.get("reflection_answer") or "").strip().lower()
    urgency_count = 0
    try:
        urgency_count = int(saved.get("urgency_count") or 0)
    except (TypeError, ValueError):
        urgency_count = 0

    if discipline_mode == "done-for-day":
        return {"label": "Closed", "tone": "neutral"}
    if reflection_answer == "no" or discipline_state in {"urgent", "frustrated"} or alignment_pct < 40:
        return {"label": "Drift", "tone": "negative"}
    if urgency_count > 0 or discipline_mode == "manage-winner" or alignment_pct < 80:
        return {"label": "Guarded", "tone": "warning"}
    return {"label": "Aligned", "tone": "positive"}


def _dashboard_behavior_trend_viewmodel(day: str, window: int = 5) -> Dict[str, Any]:
    try:
        anchor = date.fromisoformat(str(day or "").strip())
    except ValueError:
        anchor = app_runtime.now_et().date()

    entries: List[Dict[str, Any]] = []
    doer_streak = 0
    total_urgency = 0
    aligned_days = 0
    for idx in range(window):
        current_day = anchor - timedelta(days=(window - 1 - idx))
        current_key = current_day.isoformat()
        saved = _load_dashboard_behavior_settings(current_key)
        reflection = _dashboard_reflection_viewmodel(current_key)
        reflection_answer = str(reflection.get("answer") or saved.get("reflection_answer") or "").strip().lower()
        status = _dashboard_behavior_status({**saved, "reflection_answer": reflection_answer})
        alignment_pct = 0
        try:
            alignment_pct = int(saved.get("alignment_pct") or 0)
        except (TypeError, ValueError):
            alignment_pct = 0
        urgency_count = 0
        try:
            urgency_count = int(saved.get("urgency_count") or 0)
        except (TypeError, ValueError):
            urgency_count = 0
        is_doer = reflection_answer == "yes"
        if is_doer:
            doer_streak += 1
        else:
            doer_streak = 0
        if status["label"] == "Aligned":
            aligned_days += 1
        total_urgency += urgency_count
        entries.append(
            {
                "day": current_key,
                "dow": current_day.strftime("%a").upper(),
                "daynum": current_day.strftime("%d").lstrip("0") or "0",
                "alignment_pct": alignment_pct,
                "status_label": status["label"],
                "status_tone": status["tone"],
                "urgency_count": urgency_count,
                "doer_answer": reflection_answer,
                "is_today": current_key == anchor.isoformat(),
                "has_data": bool(saved),
            }
        )

    recent_streak = 0
    for entry in reversed(entries):
        if entry["doer_answer"] == "yes":
            recent_streak += 1
        else:
            break

    return {
        "entries": entries,
        "doer_streak": recent_streak,
        "aligned_days": aligned_days,
        "window": window,
        "urgency_total": total_urgency,
    }


def _dashboard_daily_brief_viewmodel(
    *,
    now_et: datetime,
    dashboard_spx: Dict[str, Any],
    dashboard_vix: Dict[str, Any],
    ticker: str = DEFAULT_PLAYBOOK_TICKER,
    dashboard_instrument: Optional[Dict[str, Any]] = None,
    gamma_snapshot: Dict[str, Any],
    market_structure_snapshot: Optional[Dict[str, Any]] = None,
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

    structure = dict(market_structure_snapshot or {})
    selected_ticker = get_supported_playbook_ticker(ticker)
    instrument_quote = dict(dashboard_instrument or dashboard_spx or {})
    spot = _num(structure.get("spot"))
    if spot is None:
        spot = _num(instrument_quote.get("price"))
    change_pct = _num(instrument_quote.get("pct_change"))
    vix = _num(dashboard_vix.get("price"))
    gamma_flip = _num(structure.get("main_flip"))
    if gamma_flip is None:
        gamma_flip = _num(gamma_snapshot.get("gamma_flip")) or _num(
            gamma_snapshot.get("gamma_flip_combined_basket")
        )
    local_flip = _num(structure.get("local_flip"))
    if local_flip is None:
        local_flip = _num(gamma_snapshot.get("local_flip")) or _num(
            gamma_snapshot.get("local_flip_aggregated_gamma")
        )
    call_wall = _num(structure.get("call_wall"))
    if call_wall is None:
        call_wall = _num(gamma_snapshot.get("call_wall")) or _num(
            gamma_snapshot.get("call_wall_aggregated_gamma")
        )
    put_wall = _num(structure.get("put_wall"))
    if put_wall is None:
        put_wall = _num(gamma_snapshot.get("put_wall")) or _num(
            gamma_snapshot.get("put_wall_aggregated_gamma")
        )
    next_call_wall = _num(structure.get("next_call_wall"))
    next_put_wall = _num(structure.get("next_put_wall"))
    day_open = _num(instrument_quote.get("day_open"))

    def _macro_event_dt(row: Dict[str, Any]) -> Optional[datetime]:
        raw = str(row.get("starts_at") or "").strip()
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=app_runtime.TZ)
            return parsed.astimezone(app_runtime.TZ)
        except Exception:
            return None

    if (
        spot is not None
        and gamma_flip is not None
        and spot > gamma_flip
        and (change_pct or 0.0) >= 0
    ):
        bias_label = "Bullish above flip"
        bias_tone = "positive"
        bias_summary = (
            f"{selected_ticker} is trading above gamma flip {gamma_flip:.0f}, so continuation "
            "longs have cleaner structure than reactive fades."
        )
    elif (
        spot is not None
        and gamma_flip is not None
        and spot < gamma_flip
        and (change_pct or 0.0) <= 0
    ):
        bias_label = "Defensive below flip"
        bias_tone = "negative"
        bias_summary = (
            f"{selected_ticker} is below gamma flip {gamma_flip:.0f}, so failed bounces and "
            "risk-off structure deserve more respect than impulsive longs."
        )
    else:
        bias_label = "Two-way / responsive"
        bias_tone = ""
        bias_summary = "Price is inside a mixed zone. Stay selective, shorten hold times, and avoid forcing trend conviction before levels confirm."

    def _nearest_actionable_level() -> Dict[str, Any]:
        candidates = []
        for key, label, value, role in (
            ("local_flip", "Local Flip", local_flip, "pivot"),
            ("call_wall", "Call Wall", call_wall, "resistance"),
            ("put_wall", "Put Wall", put_wall, "support"),
            ("main_flip", "Main Flip", gamma_flip, "pivot"),
        ):
            if spot is None or value is None:
                continue
            candidates.append(
                {
                    "key": key,
                    "label": label,
                    "value": float(value),
                    "role": role,
                    "distance": abs(float(spot) - float(value)),
                }
            )
        if not candidates:
            return {
                "key": "",
                "label": "Unavailable",
                "value": None,
                "role": "unavailable",
                "distance": None,
            }
        candidates.sort(
            key=lambda row: (
                row["distance"],
                (
                    0
                    if row["key"] == "local_flip"
                    else 1 if row["key"] in {"call_wall", "put_wall"} else 2
                ),
            )
        )
        return candidates[0]

    active_level = _nearest_actionable_level()

    def _market_location_text() -> str:
        if spot is None:
            return "Price unavailable."
        if call_wall is not None and spot > call_wall:
            return f"Price is above Call Wall {call_wall:.0f} and extended."
        if put_wall is not None and spot < put_wall:
            return f"Price is below Put Wall {put_wall:.0f} and breaking down."
        if local_flip is not None and call_wall is not None and local_flip < spot < call_wall:
            return f"Price is between Local Flip {local_flip:.0f} and Call Wall {call_wall:.0f}."
        if put_wall is not None and local_flip is not None and put_wall < spot < local_flip:
            return f"Price is between Put Wall {put_wall:.0f} and Local Flip {local_flip:.0f}."
        if gamma_flip is not None and local_flip is not None:
            low, high = sorted((gamma_flip, local_flip))
            if low < spot < high:
                return (
                    f"Price is between Main Flip {gamma_flip:.0f} and Local Flip {local_flip:.0f}."
                )
        if active_level.get("value") is not None:
            level_value = float(active_level["value"])
            relation = (
                "at"
                if abs(float(spot) - level_value) <= 2.0
                else "above" if float(spot) > level_value else "below"
            )
            return f"Price is {relation} {active_level['label']} {level_value:.0f}."
        return "Price is between levels."

    def _market_condition_text() -> str:
        active_label = str(active_level.get("label") or "structure")
        active_value = active_level.get("value")
        if active_value is not None:
            if active_level.get("role") == "resistance":
                return f"Resistance test near {active_label} {float(active_value):.0f}."
            if active_level.get("role") == "support":
                return f"Support test near {active_label} {float(active_value):.0f}."
            return f"Pivot decision near {active_label} {float(active_value):.0f}."
        return bias_summary

    def _execution_triggers_text() -> str:
        value = active_level.get("value")
        label = str(active_level.get("label") or "level")
        role = str(active_level.get("role") or "")
        if value is None:
            return "Wait for a nearby actionable level before defining execution."
        value_num = float(value)
        if role == "resistance":
            cont_target = (
                f"{next_call_wall:.0f}"
                if next_call_wall is not None
                else "the next upside extension"
            )
            return (
                f"Rejection at {label} {value_num:.0f} can trigger a short. "
                f"Acceptance above {value_num:.0f} opens continuation long toward {cont_target}."
            )
        if role == "support":
            break_target = (
                f"{next_put_wall:.0f}"
                if next_put_wall is not None
                else "the next downside extension"
            )
            return (
                f"Bounce at {label} {value_num:.0f} can trigger a long. "
                f"Breakdown below {value_num:.0f} opens continuation short toward {break_target}."
            )
        return (
            f"Hold above {label} {value_num:.0f} supports longs. "
            f"Failure back below {value_num:.0f} supports shorts."
        )

    def _do_not_do_text() -> str:
        value = active_level.get("value")
        label = str(active_level.get("label") or "the nearest level")
        role = str(active_level.get("role") or "")
        if value is None:
            return "Do not trade without a nearby validated structure anchor."
        value_num = float(value)
        if role == "resistance":
            return f"Do not buy directly into {label} {value_num:.0f} without clean acceptance."
        if role == "support":
            return f"Do not short directly into {label} {value_num:.0f} without clean breakdown."
        return f"Do not force entries in the middle of the range away from {label} {value_num:.0f}."

    market_condition_default = _market_condition_text()
    market_location_default = _market_location_text()
    execution_triggers_default = _execution_triggers_text()
    do_not_do_default = _do_not_do_text()

    active_level_display = (
        f"{active_level.get('label')} {float(active_level['value']):.0f} · {str(active_level.get('role') or '').title()}"
        if active_level.get("value") is not None
        else "Unavailable"
    )

    if vix is not None and vix >= 22:
        volatility_label = "Elevated vol"
    elif vix is not None and vix >= 18:
        volatility_label = "Active vol"
    elif vix is not None:
        volatility_label = "Contained vol"
    else:
        volatility_label = "Vol unknown"

    relevant_macro_rows: List[Dict[str, Any]] = []
    stale_cutoff = now_et - timedelta(minutes=30)
    for row in list(news_snapshot.get("macro_events") or []):
        if not isinstance(row, dict):
            continue
        event_dt = _macro_event_dt(row)
        if event_dt is None:
            continue
        if event_dt < stale_cutoff:
            continue
        relevant_macro_rows.append(dict(row))
    relevant_macro_rows.sort(key=lambda row: _macro_event_dt(row) or now_et)
    macro_events = [
        {
            "headline": str(row.get("headline") or "Macro event"),
            "published_label": str(row.get("published_label") or ""),
            "summary": str(row.get("summary") or ""),
            "starts_at": str(row.get("starts_at") or ""),
        }
        for row in relevant_macro_rows[:3]
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
        (
            str(active_level.get("label") or "Active Level"),
            active_level.get("value"),
            (
                "positive"
                if active_level.get("role") == "support"
                else "negative" if active_level.get("role") == "resistance" else ""
            ),
        ),
        ("Local Flip", local_flip, ""),
        ("Put Wall", put_wall, "positive"),
        ("Call Wall", call_wall, "negative"),
        ("Main Flip", gamma_flip, ""),
        ("Day Open", day_open, ""),
    ):
        if value is None:
            continue
        if any(item.get("label") == label for item in key_levels):
            continue
        key_levels.append({"label": label, "value": f"{value:.0f}", "tone": tone})
    if not key_levels and spot is not None:
        key_levels.append({"label": "Spot", "value": f"{spot:.2f}", "tone": ""})

    default_focus = market_condition_default
    plan_a = execution_triggers_default
    plan_b = market_location_default
    if macro_events:
        no_trade = f"{do_not_do_default} Stand down during or immediately into {macro_events[0]['headline']} unless structure is already resolved and risk is smaller than usual."
    else:
        no_trade = do_not_do_default

    market_condition_value = str(saved.get("focus") or default_focus).strip() or default_focus
    execution_triggers_value = str(saved.get("plan_a") or plan_a).strip() or plan_a
    market_location_value = str(saved.get("plan_b") or plan_b).strip() or plan_b
    do_not_do_value = str(saved.get("no_trade") or no_trade).strip() or no_trade

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
        "focus": market_condition_value,
        "plan_a": execution_triggers_value,
        "plan_b": market_location_value,
        "no_trade": do_not_do_value,
        "market_condition": market_condition_value,
        "market_location": market_location_value,
        "active_level_name": str(active_level.get("label") or "Unavailable"),
        "active_level_value": active_level.get("value"),
        "active_level_role": str(active_level.get("role") or "unavailable"),
        "active_level_display": active_level_display,
        "execution_triggers": execution_triggers_value,
        "do_not_do": do_not_do_value,
        "risk_posture": f"{bias_label} · {volatility_label}",
        "key_levels": key_levels,
        "macro_events": macro_events,
        "headline": (
            f"{market_condition_default} · {volatility_label}"
            + (f" · Today {app_runtime.money(today_net)}" if today_count else "")
        ),
        "summary": market_location_default,
        "cta_label": "Trade Gate" if today_count == 0 else "Add Trade",
        "source_label": "Manually tuned" if is_tuned else "Auto-generated",
        "source_detail": (
            "Using your saved brief edits for this day."
            if is_tuned
            else (
                f"Generated from live {selected_ticker}, nearest actionable gamma level, "
                "and macro context."
            )
        ),
        "is_tuned": is_tuned,
    }


def _dashboard_snapshot_viewmodel(
    *,
    today_net: float,
    today_count: int,
    today_wins: int,
    today_losses: int,
    scope_label: str,
    data_trust: Dict[str, Any],
    balance_integrity: Dict[str, Any],
    sync_badges: List[Dict[str, Any]],
) -> Dict[str, Any]:
    def _badge_value(badge: Any, field: str) -> Any:
        if isinstance(badge, dict):
            return badge.get(field)
        return getattr(badge, field, None)

    def _trust_value(field: str, default: Any = "") -> Any:
        if isinstance(data_trust, dict):
            return data_trust.get(field, default)
        return getattr(data_trust, field, default)

    sync_value = "Unknown"
    updated_value = "—"
    for badge in sync_badges:
        label = str(_badge_value(badge, "label") or "").strip().lower()
        if label == "sync":
            sync_value = str(_badge_value(badge, "value") or "Unknown")
        elif label == "updated":
            updated_value = str(_badge_value(badge, "value") or "—")
    source_label = str(
        balance_integrity.get("source_label")
        or balance_integrity.get("source_short")
        or "Derived ledger"
    )
    drift_value = float(balance_integrity.get("delta") or 0.0)
    has_drift = bool(balance_integrity.get("has_drift"))
    items = [
        {
            "label": "Today P/L",
            "value": app_runtime.money(today_net),
            "detail": "Closed session result",
            "tone": "positive" if today_net > 0 else "negative" if today_net < 0 else "neutral",
        },
        {
            "label": "Trades",
            "value": str(today_count),
            "detail": "In current session",
            "tone": "neutral",
        },
        {
            "label": "Record",
            "value": f"{today_wins}W / {today_losses}L",
            "detail": "Closed trades only",
            "tone": "neutral",
        },
        {
            "label": "Account",
            "value": scope_label,
            "detail": source_label,
            "tone": "neutral",
        },
        {
            "label": "Sync Health",
            "value": sync_value,
            "detail": updated_value,
            "tone": (
                "positive"
                if str(sync_value).lower() == "success"
                else (
                    "negative"
                    if str(sync_value).lower() in {"stalled", "failed", "error"}
                    else "neutral"
                )
            ),
        },
        {
            "label": "Ledger State",
            "value": "Drift flagged" if has_drift else "Aligned",
            "detail": (
                app_runtime.money(drift_value)
                if has_drift
                else str(_trust_value("message") or "Data aligned")
            ),
            "tone": "negative" if has_drift else "positive",
        },
    ]
    return {"entries": items}


def _dashboard_readiness_viewmodel(
    checklist: List[Dict[str, Any]],
    *,
    brief_ready: bool,
    today_count: int,
    data_trust: Any,
) -> Dict[str, Any]:
    def _trust_value(field: str, default: Any = "") -> Any:
        if isinstance(data_trust, dict):
            return data_trust.get(field, default)
        return getattr(data_trust, field, default)

    total = len(checklist)
    done = sum(1 for item in checklist if bool(item.get("done")))
    pct = (100.0 * done / total) if total else 0.0
    blockers = [
        str(item.get("label") or "").strip() for item in checklist if not bool(item.get("done"))
    ]
    if not blockers and str(_trust_value("tone") or "").strip() == "critical":
        blockers.append("Data trust")
    if pct >= 100.0 and brief_ready and str(_trust_value("tone") or "").strip() != "critical":
        state_label = "Ready to trade"
        state_tone = "positive"
    elif done >= max(1, total - 1):
        state_label = "Almost ready"
        state_tone = "warning"
    else:
        state_label = "Needs attention"
        state_tone = "negative"
    return {
        "done": done,
        "total": total,
        "pct": pct,
        "state_label": state_label,
        "state_tone": state_tone,
        "blockers": blockers[:3],
        "summary": (
            "All core checks are locked."
            if not blockers
            else "Clear the missing blockers before adding risk."
        ),
        "session_loaded": today_count > 0,
    }


def _dashboard_decision_viewmodel(
    *,
    daily_brief: Dict[str, Any],
    risk_posture_title: str,
    risk_posture_detail: str,
    data_trust: Dict[str, Any],
    readiness: Dict[str, Any],
    dashboard_vix: Dict[str, Any],
    playbook_view: Optional[Dict[str, Any]] = None,
    gamma_strip: Optional[Dict[str, Any]] = None,
    execution_model: Optional[Dict[str, Any]] = None,
    market_structure_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    def _trust_value(field: str, default: Any = "") -> Any:
        if isinstance(data_trust, dict):
            return data_trust.get(field, default)
        return getattr(data_trust, field, default)

    playbook = dict(playbook_view or {})
    if playbook:
        trade_state = (
            str(playbook.get("trade_state_label") or "UNAVAILABLE").strip() or "UNAVAILABLE"
        )
        tone_map = {
            "ready": "positive",
            "wait": "warning",
            "no_trade": "negative",
            "planning_only": "warning",
            "unavailable": "negative",
        }
        return {
            "bias": str(playbook.get("bias_label") or "Unavailable").strip() or "Unavailable",
            "plan": str(playbook.get("plan_label") or daily_brief.get("plan_a") or "Wait").strip(),
            "risk_size": str(playbook.get("risk_label") or "No trade / stand down").strip(),
            "status": trade_state,
            "status_tone": tone_map.get(str(playbook.get("trade_state") or ""), "warning"),
            "trade_gate": str(
                playbook.get("trade_gate_label") or daily_brief.get("no_trade") or "Wait"
            ).strip(),
            "risk_posture_title": risk_posture_title,
            "risk_posture_detail": risk_posture_detail,
            "gamma_strip": gamma_strip or {"entries": [], "headline": "Structure unavailable"},
            "posture_summary": str(
                playbook.get("context_lead_label") or daily_brief.get("headline") or ""
            ).strip(),
            "playbook_status": trade_state,
            "playbook_score": playbook.get("context_score"),
            "playbook_grade": playbook.get("context_grade"),
            "local_bias_state": str(playbook.get("bias_state") or "unknown"),
        }

    structure = dict(market_structure_snapshot or {})
    if structure:
        trade_state = str(structure.get("trade_state") or "UNAVAILABLE").strip().upper()
        bias = (
            str(
                structure.get("bias")
                or structure.get("planning_bias_label")
                or structure.get("bias_context")
                or "Unavailable"
            ).strip()
            or "Unavailable"
        )
        posture_summary = str(
            structure.get("plan_note")
            or structure.get("execution_regime_label")
            or structure.get("gamma_regime_subtitle")
            or daily_brief.get("headline")
            or ""
        ).strip()
        tone_map = {
            "READY": "positive",
            "WAIT": "warning",
            "NO_TRADE": "negative",
            "PLANNING_ONLY": "warning",
            "UNAVAILABLE": "negative",
        }
        risk_map = {
            "READY": "Normal size",
            "WAIT": "Reduced size",
            "NO_TRADE": "No trade / stand down",
            "PLANNING_ONLY": "Planning only",
            "UNAVAILABLE": "No trade / stand down",
        }
        return {
            "bias": bias,
            "plan": str(structure.get("best_look") or daily_brief.get("plan_a") or "Wait").strip(),
            "risk_size": risk_map.get(trade_state, "No trade / stand down"),
            "status": str(
                structure.get("trade_state_label") or trade_state.replace("_", " ")
            ).strip(),
            "status_tone": tone_map.get(trade_state, "negative"),
            "trade_gate": str(
                structure.get("required_trigger")
                or structure.get("invalidation")
                or daily_brief.get("no_trade")
                or "Wait"
            ).strip(),
            "risk_posture_title": risk_posture_title,
            "risk_posture_detail": risk_posture_detail,
            "gamma_strip": gamma_strip or {"entries": [], "headline": "Structure unavailable"},
            "posture_summary": posture_summary,
            "playbook_status": str(
                structure.get("trade_state_label") or trade_state.replace("_", " ")
            ).strip(),
            "playbook_score": None,
            "playbook_grade": structure.get("regime_confidence_label"),
            "local_bias_state": str(structure.get("bias_state") or "unknown"),
        }

    model = dict(execution_model or {})
    macro = dict(model.get("macro_regime") or {})
    local = dict(model.get("local_bias") or {})
    location = dict(model.get("location") or {})
    playbook = dict(model.get("playbook") or {})
    conflicts = dict(model.get("conflicts") or {})
    posture_summary = str(model.get("posture_summary") or "").strip()
    levels = dict(model.get("levels") or {})
    gamma_state = str(macro.get("state") or "").strip()
    local_state = str(local.get("state") or "").strip()
    playbook_status = str(playbook.get("status") or "").strip()
    snapshot_unavailable = gamma_state in {"", "unknown"} or not any(
        isinstance(levels.get(key), (int, float)) for key in ("main_flip", "call_wall", "put_wall")
    )

    if snapshot_unavailable:
        bias = "Unavailable"
    elif gamma_state == "positive" and local_state == "above_local":
        bias = "Buy dips bias"
    elif gamma_state == "negative" and local_state == "below_local":
        bias = "Sell rips bias"
    elif gamma_state == "positive" and local_state == "unknown":
        bias = "Positive"
    elif gamma_state == "negative" and local_state == "unknown":
        bias = "Negative"
    elif (
        gamma_state == "neutral"
        or local_state == "at_local"
        or bool(location.get("midrange"))
        or bool(conflicts.get("macro_local_conflict"))
    ):
        bias = "Two-way / responsive"
    else:
        bias = "Two-way / responsive"

    if str(_trust_value("tone") or "").strip() == "critical" or snapshot_unavailable:
        risk_size = "No trade / stand down"
        status = "Stand down until structure is valid"
        tone = "negative"
    elif playbook_status == "GO":
        risk_size = "Normal size"
        status = "Aligned if trigger confirms"
        tone = "positive"
    elif playbook_status in {"WATCH", "CAUTION"}:
        risk_size = "Reduced size"
        status = "Ready only if structure confirms cleanly"
        tone = "warning"
    else:
        risk_size = "No trade / stand down"
        status = "No clean trade right now"
        tone = "negative"

    plan = str(playbook.get("best_look") or "").strip() or str(daily_brief.get("plan_a") or "Wait")
    trade_gate = (
        str(playbook.get("need") or "").strip()
        or str(playbook.get("avoid") or "").strip()
        or str(daily_brief.get("no_trade") or "").strip()
        or "Wait"
    )
    return {
        "bias": bias,
        "plan": plan,
        "risk_size": risk_size,
        "status": status,
        "status_tone": tone,
        "trade_gate": trade_gate,
        "risk_posture_title": risk_posture_title,
        "risk_posture_detail": risk_posture_detail,
        "gamma_strip": gamma_strip or {"entries": [], "headline": "Structure unavailable"},
        "posture_summary": posture_summary or str(daily_brief.get("headline") or ""),
        "playbook_status": playbook_status or "NO TRADE",
        "playbook_score": playbook.get("score"),
        "playbook_grade": playbook.get("grade"),
        "local_bias_state": local_state or "unknown",
    }


def _dashboard_gamma_strip_viewmodel(
    *,
    playbook_view: Optional[Dict[str, Any]] = None,
    execution_model: Optional[Dict[str, Any]] = None,
    gamma_snapshot: Optional[Dict[str, Any]] = None,
    market_structure_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    def _dashboard_gamma_regime_short_label(value: str) -> str:
        normalized = str(value or "").strip().lower()
        if normalized in {"positive", "positive gamma"}:
            return "POSITIVE"
        if normalized in {"negative", "negative gamma"}:
            return "NEGATIVE"
        if normalized in {"neutral", "neutral gamma"}:
            return "NEUTRAL"
        if normalized in {"unconfirmed"}:
            return "UNCONFIRMED"
        if normalized in {"unavailable", "regime unavailable"}:
            return "UNAVAILABLE"
        return str(value or "UNAVAILABLE").strip().upper() or "UNAVAILABLE"

    def _num(value: Any) -> Optional[float]:
        try:
            if value is None or str(value).strip() == "":
                return None
            return float(value)
        except Exception:
            return None

    playbook = dict(playbook_view or {})
    if playbook:
        structure = {
            "spot": playbook.get("spot"),
            "gamma_data_status": playbook.get("gamma_data_status"),
            "levels_source": playbook.get("levels_source"),
            "gamma_regime": playbook.get("gamma_regime"),
            "gamma_regime_label": playbook.get("gamma_regime_label"),
            "gamma_regime_reason_label": playbook.get("regime_reason_label"),
            "session_mode_label": playbook.get("session_mode_label"),
            "levels_source_label": {
                "live_session_snapshot": "Live session snapshot",
                "last_valid_snapshot": "Last valid snapshot",
                "stale_snapshot": "Stale snapshot",
                "unavailable": "Unavailable",
            }.get(str(playbook.get("levels_source") or ""), "Unavailable"),
            "last_valid_snapshot_time_label": playbook.get("session_summary_label"),
            "snapshot_timestamp_label": playbook.get("session_summary_label"),
            "main_flip": playbook.get("main_flip"),
            "local_flip": playbook.get("local_flip"),
            "call_wall": playbook.get("call_wall"),
            "put_wall": playbook.get("put_wall"),
            "next_call_wall": playbook.get("next_call_wall"),
            "next_put_wall": playbook.get("next_put_wall"),
        }
    else:
        structure = dict(market_structure_snapshot or {})
    if structure:
        structure = {
            **structure,
            **_market_pulse_resolve_canonical_market_structure(
                levels=structure,
                gamma_snapshot=gamma_snapshot or {},
            ),
        }
    if structure:
        spot = _num(structure.get("spot"))

        def _fmt_level(value: Any) -> str:
            if isinstance(value, (int, float)):
                return f"{float(value):.0f}"
            return "--"

        def _fmt_level_with_distance(value: Any) -> str:
            numeric = _num(value)
            if numeric is None:
                return "--"
            base = f"{numeric:.0f}"
            if spot is None:
                return base
            delta = numeric - spot
            if abs(delta) < 0.5:
                return f"{base} (0)"
            return f"{base} ({delta:+.0f})"

        gamma_status = str(structure.get("gamma_data_status") or "").strip().lower()
        levels_source = str(structure.get("levels_source") or "").strip().lower()
        regime_value = _dashboard_gamma_regime_short_label(
            str(
                structure.get("gamma_regime_label")
                or structure.get("gamma_regime")
                or "Regime Unavailable"
            )
        )
        regime_reason = str(structure.get("gamma_regime_reason_label") or "").strip()
        regime_state = str(structure.get("gamma_regime") or "").strip().lower()
        regime_tone = (
            "positive"
            if regime_state == "positive"
            else (
                "negative"
                if regime_state == "negative"
                else "warning" if regime_state in {"neutral", "unconfirmed"} else "info"
            )
        )

        if levels_source == "unavailable":
            state = "unavailable"
            status_text = (
                str(structure.get("gamma_regime_subtitle") or "").strip()
                or str(structure.get("levels_source_label") or "Gamma unavailable").strip()
                or "Gamma context unavailable"
            )
        elif gamma_status in {"fallback_valid", "stale_but_usable", "partial"} or levels_source in {
            "last_valid_snapshot",
            "stale_snapshot",
            "fallback_snapshot",
            "prior_close_fallback",
            "last_valid_snapshot_fallback",
            "last_valid_quote_fallback",
        }:
            state = "stale"
            session_label = str(structure.get("session_mode_label") or "").strip()
            source_label = str(structure.get("levels_source_label") or "").strip()
            ts_label = (
                str(structure.get("last_valid_snapshot_time_label") or "").strip()
                or str(structure.get("snapshot_timestamp_label") or "").strip()
            )
            parts = [
                part for part in (session_label, source_label, ts_label) if part and part != "—"
            ]
            status_text = " · ".join(parts) or "Using last valid structure"
        elif gamma_status in {"fresh_valid"} and levels_source == "live_session_snapshot":
            state = "live"
            session_label = str(structure.get("session_mode_label") or "").strip()
            regime_subtitle = str(structure.get("gamma_regime_subtitle") or "").strip()
            parts = [part for part in (session_label, regime_subtitle) if part]
            status_text = " · ".join(parts) or "Live gamma snapshot"
        else:
            state = "loading"
            status_text = "Loading gamma context..."

        items = [
            {
                "key": "regime",
                "label": "Gamma Regime",
                "value": regime_value,
                "detail": regime_reason if regime_state in {"unconfirmed", "unavailable"} else "",
                "emphasis": "strong",
                "tone": regime_tone,
                "glow": regime_tone in {"positive", "negative"},
            },
            {
                "key": "local_flip",
                "label": "Local Flip",
                "value": _fmt_level(structure.get("local_flip")),
                "emphasis": "strong",
                "tone": "",
                "glow": True,
            },
            {
                "key": "next_call_wall",
                "label": "NCW",
                "value": _fmt_level(structure.get("next_call_wall")),
                "emphasis": "strong",
                "tone": "negative",
                "glow": True,
            },
            {
                "key": "next_put_wall",
                "label": "NPW",
                "value": _fmt_level(structure.get("next_put_wall")),
                "emphasis": "strong",
                "tone": "positive",
                "glow": True,
            },
            {
                "key": "main_flip",
                "label": "Main Flip",
                "value": _fmt_level(structure.get("main_flip")),
                "emphasis": "quiet",
                "tone": "info",
                "glow": False,
            },
            {
                "key": "call_wall",
                "label": "CW",
                "value": _fmt_level(structure.get("call_wall")),
                "emphasis": "quiet",
                "tone": "negative",
                "glow": False,
            },
            {
                "key": "put_wall",
                "label": "PW",
                "value": _fmt_level(structure.get("put_wall")),
                "emphasis": "quiet",
                "tone": "positive",
                "glow": False,
            },
        ]
        # The dashboard only renders display-safe structure. Upstream provider
        # asymmetry is allowed, but one-sided NCW/NPW is forbidden here.
        return {
            "headline": "Gamma structure",
            "entries": items,
            "state": state,
            "status_text": status_text or "Gamma context unavailable",
            "updated_label": str(structure.get("snapshot_timestamp_label") or "").strip(),
        }

    gamma_snapshot = dict(gamma_snapshot or {})
    spot = _num(gamma_snapshot.get("spot"))

    def _fmt_level(value: Any, *, key: str = "") -> str:
        if isinstance(value, (int, float)):
            return f"{float(value):.0f}"
        if (
            key == "local_flip"
            and snapshot_status in {"healthy", "degraded", "stale"}
            and not bool(gamma_snapshot.get("local_flip_found"))
        ):
            return "No Local Flip between Put Wall and Call Wall"
        return "--"

    def _fmt_level_with_distance(value: Any, *, key: str = "") -> str:
        if not isinstance(value, (int, float)):
            return _fmt_level(value, key=key)
        numeric = float(value)
        base = f"{numeric:.0f}"
        if spot is None:
            return base
        delta = numeric - spot
        if abs(delta) < 0.5:
            return f"{base} (0)"
        return f"{base} ({delta:+.0f})"

    model = dict(execution_model or {})
    macro = dict(model.get("macro_regime") or {})
    levels = dict(model.get("levels") or {})
    levels = {
        **levels,
        **_market_pulse_resolve_canonical_market_structure(
            levels={
                **levels,
                "spot": levels.get("spot") if levels.get("spot") is not None else spot,
            },
            gamma_snapshot=gamma_snapshot,
        ),
    }
    if spot is None:
        spot = _num(levels.get("spot")) or _num(levels.get("price"))
    snapshot_status = str(gamma_snapshot.get("snapshot_status") or "").strip().lower()
    snapshot_label = str(gamma_snapshot.get("snapshot_status_label") or "").strip()
    snapshot_detail = str(gamma_snapshot.get("snapshot_status_detail") or "").strip()
    updated_raw = (
        gamma_snapshot.get("last_successful_compute")
        or gamma_snapshot.get("computed_at")
        or gamma_snapshot.get("asof")
    )
    updated_label = _format_iso_et_label(updated_raw)
    regime_state_raw = str(macro.get("state") or "").strip().lower()
    if regime_state_raw == "positive":
        regime_label = "POSITIVE"
    elif regime_state_raw == "negative":
        regime_label = "NEGATIVE"
    elif regime_state_raw == "neutral":
        regime_label = "NEUTRAL"
    else:
        regime_label = "UNAVAILABLE"
    regime_state = regime_state_raw
    regime_tone = (
        "positive"
        if regime_state == "positive"
        else (
            "negative"
            if regime_state == "negative"
            else "warning" if regime_state == "neutral" else "info"
        )
    )
    has_levels = any(
        isinstance(levels.get(key), (int, float))
        for key in ("main_flip", "local_flip", "call_wall", "put_wall")
    ) or regime_label not in {"", "--", "Unavailable", "REGIME UNKNOWN"}

    state = "live"
    status_text = snapshot_label or "Live gamma snapshot"
    if not has_levels and not str(updated_raw or "").strip():
        state = "loading"
        status_text = "Loading gamma context..."
    elif snapshot_status in {"stale", "degraded"}:
        state = "stale"
        status_text = snapshot_label or "Stale gamma snapshot"
    elif snapshot_status == "invalid" and not has_levels:
        state = "unavailable"
        status_text = snapshot_label or "Gamma unavailable"

    if updated_label and state in {"live", "stale"}:
        status_text = f"{status_text} · {updated_label}"
    elif snapshot_detail and state in {"loading", "unavailable"}:
        status_text = snapshot_detail

    items = [
        {
            "key": "regime",
            "label": "Gamma Regime",
            "value": regime_label,
            "emphasis": "strong",
            "tone": regime_tone,
            "glow": regime_tone in {"positive", "negative"},
        },
        {
            "key": "local_flip",
            "label": "Local Flip",
            "value": _fmt_level(levels.get("local_flip"), key="local_flip"),
            "emphasis": "strong",
            "tone": "",
            "glow": False,
        },
        {
            "key": "next_call_wall",
            "label": "NCW",
            "value": _fmt_level(levels.get("next_call_wall"), key="next_call_wall"),
            "emphasis": "strong",
            "tone": "negative",
            "glow": False,
        },
        {
            "key": "next_put_wall",
            "label": "NPW",
            "value": _fmt_level(levels.get("next_put_wall"), key="next_put_wall"),
            "emphasis": "strong",
            "tone": "positive",
            "glow": False,
        },
        {
            "key": "main_flip",
            "label": "Main Flip",
            "value": _fmt_level(levels.get("main_flip"), key="main_flip"),
            "emphasis": "quiet",
            "tone": "info",
            "glow": False,
        },
        {
            "key": "call_wall",
            "label": "CW",
            "value": _fmt_level(levels.get("call_wall"), key="call_wall"),
            "emphasis": "quiet",
            "tone": "negative",
            "glow": False,
        },
        {
            "key": "put_wall",
            "label": "PW",
            "value": _fmt_level(levels.get("put_wall"), key="put_wall"),
            "emphasis": "quiet",
            "tone": "positive",
            "glow": False,
        },
    ]
    return {
        "headline": "Gamma structure",
        "entries": items,
        "state": state,
        "status_text": status_text or "Gamma context unavailable",
        "updated_label": updated_label,
    }


def _advance_market_sessions(start_day: date, sessions: int) -> date:
    cursor = start_day
    remaining = max(0, int(sessions))
    if remaining <= 0:
        while not _is_market_session(cursor):
            cursor += timedelta(days=1)
        return cursor
    while remaining > 0:
        cursor += timedelta(days=1)
        if _is_market_session(cursor):
            remaining -= 1
    return cursor


def _market_sessions_between(start_day: date, target_day: date) -> int:
    """Count future market sessions after start_day through target_day."""
    if target_day <= start_day:
        return 0
    cursor = start_day
    sessions = 0
    while cursor < target_day:
        cursor += timedelta(days=1)
        if _is_market_session(cursor):
            sessions += 1
    return sessions


def _dashboard_pace_viewmodel(
    proj: Dict[str, Any],
    milestone: Dict[str, Any],
    pace_settings: Dict[str, Any],
    *,
    anchor_day: date,
) -> Dict[str, Any]:
    live_avg = float(proj.get("avg") or 0.0)
    custom_daily = float(pace_settings.get("custom_daily") or 0.0)
    custom_enabled = bool(pace_settings.get("custom_enabled")) and custom_daily > 0.0
    pass_buffer = max(0.0, float(pace_settings.get("pass_buffer") or 0.0))
    pass_buffer_enabled = bool(pace_settings.get("pass_buffer_enabled")) and pass_buffer > 0.0
    applied_avg = custom_daily if custom_enabled else live_avg
    base_balance = float(proj.get("base_balance") or 0.0)
    start_date_input = str(pace_settings.get("start_date") or "").strip()
    target_date_input = str(pace_settings.get("target_date") or "").strip()
    projection_anchor = anchor_day
    start_date_label = anchor_day.strftime("%b %d")
    start_date_full = anchor_day.strftime("%a, %b %d, %Y")
    start_date_configured = False
    if start_date_input:
        try:
            projection_anchor = date.fromisoformat(start_date_input)
            start_date_label = projection_anchor.strftime("%b %d")
            start_date_full = projection_anchor.strftime("%a, %b %d, %Y")
            start_date_configured = True
        except ValueError:
            start_date_input = ""
    nodes: List[Dict[str, Any]] = []
    for key, label in (("p5", "5D"), ("p10", "10D"), ("p20", "20D")):
        row = dict(proj.get(key) or {})
        sessions = int(row.get("days") or 0)
        gross_pnl = float(applied_avg * sessions)
        net_pnl = gross_pnl - pass_buffer
        est_balance = base_balance + gross_pnl
        target_day = _advance_market_sessions(projection_anchor, sessions)
        nodes.append(
            {
                "label": label,
                "sessions": sessions,
                "est_pnl": app_runtime.money(net_pnl),
                "gross_pnl": app_runtime.money(gross_pnl),
                "est_balance": app_runtime.money(est_balance),
                "target_date_label": target_day.strftime("%b %d"),
                "target_date_full": target_day.strftime("%a, %b %d, %Y"),
                "tone": "positive" if net_pnl > 0 else "negative" if net_pnl < 0 else "neutral",
            }
        )
    target_projection: Dict[str, Any] = {
        "configured": False,
        "input": target_date_input,
        "label": "Choose a date",
        "sessions": 0,
        "est_pnl": app_runtime.money(0.0),
        "est_balance": app_runtime.money(base_balance),
        "target_date_full": "",
        "tone": "neutral",
        "detail": "Set a target date to see the projected profit by that session.",
    }
    if target_date_input:
        try:
            target_day = date.fromisoformat(target_date_input)
            target_sessions = _market_sessions_between(projection_anchor, target_day)
            target_est_pnl = applied_avg * target_sessions
            target_est_pnl_net = target_est_pnl - pass_buffer
            target_est_balance = base_balance + target_est_pnl
            target_projection = {
                "configured": True,
                "input": target_date_input,
                "label": target_day.strftime("%b %d"),
                "sessions": target_sessions,
                "est_pnl": app_runtime.money(target_est_pnl_net),
                "gross_pnl": app_runtime.money(target_est_pnl),
                "est_balance": app_runtime.money(target_est_balance),
                "target_date_full": target_day.strftime("%a, %b %d, %Y"),
                "tone": (
                    "positive"
                    if target_est_pnl_net > 0
                    else "negative" if target_est_pnl_net < 0 else "neutral"
                ),
                "detail": (
                    f"{target_sessions} trading sessions at "
                    f"{app_runtime.money(applied_avg)}/day from {start_date_label}."
                ),
            }
            if pass_buffer_enabled:
                target_projection["detail"] += f" Buffer {app_runtime.money(pass_buffer)} applied."
        except ValueError:
            target_projection["input"] = ""
    note = "Use for pacing, not certainty."
    milestone_eta = ""
    if applied_avg > 0.0:
        projected_days_profit = None
        projected_days_balance = None
        profit_goal = float(milestone.get("profit_goal") or 0.0)
        profit_current = float(milestone.get("profit_current") or 0.0)
        target_balance = float(milestone.get("target_balance") or 0.0)
        current_balance = base_balance
        if profit_goal > 0.0:
            projected_days_profit = int(
                (max(0.0, profit_goal - profit_current) / applied_avg) + 0.9999
            )
        if target_balance > 0.0:
            projected_days_balance = int(
                (max(0.0, target_balance - current_balance) / applied_avg) + 0.9999
            )
        projected_days_overall: Optional[int] = None
        if projected_days_profit is not None and projected_days_balance is not None:
            projected_days_overall = max(projected_days_profit, projected_days_balance)
        elif projected_days_profit is not None:
            projected_days_overall = projected_days_profit
        elif projected_days_balance is not None:
            projected_days_overall = projected_days_balance
        if projected_days_overall is not None:
            eta_day = _advance_market_sessions(projection_anchor, projected_days_overall)
            milestone_eta = eta_day.strftime("%a, %b %d, %Y")
    if milestone_eta:
        note = f"Milestone tracks to {milestone_eta} at this trading-day pace."
    elif custom_enabled:
        note = "Custom pace is active. Dates use trading days only."
    elif live_avg > 0.0:
        note = "Live pace is based on recent trading sessions only."
    if pass_buffer_enabled:
        note += f" Pass buffer {app_runtime.money(pass_buffer)} is subtracted from projected profit."
    return {
        "headline": app_runtime.money(applied_avg),
        "headline_suffix": "/day",
        "base_balance": app_runtime.money(base_balance),
        "live_headline": app_runtime.money(live_avg),
        "custom_enabled": custom_enabled,
        "custom_daily": custom_daily,
        "custom_input": f"{custom_daily:.2f}" if custom_enabled else "",
        "pass_buffer": pass_buffer,
        "pass_buffer_enabled": pass_buffer_enabled,
        "pass_buffer_input": f"{pass_buffer:.2f}" if pass_buffer_enabled else "",
        "pass_buffer_label": app_runtime.money(pass_buffer),
        "mode_label": "Custom pace" if custom_enabled else "Live pace",
        "mode_detail": (
            f"Using your manual pace of {app_runtime.money(custom_daily)}/day."
            if custom_enabled
            else "Using your recent trading-day average."
        ),
        "start": {
            "configured": start_date_configured,
            "input": start_date_input,
            "label": start_date_label,
            "full": start_date_full,
        },
        "trading_day_label": "Trading-day projections only",
        "note": note,
        "nodes": nodes,
        "target": target_projection,
    }


def _dashboard_calendar_state_viewmodel(heat: Dict[str, Any], scope_label: str) -> Dict[str, Any]:
    traded_days = 0
    green_days = 0
    red_days = 0
    flat_days = 0
    for week in list(heat.get("weeks") or []):
        for day in list((week or {}).get("days") or []):
            if day.get("daynum") is None or day.get("wd") is None or int(day.get("wd")) >= 5:
                continue
            traded_days += 1
            net = float(day.get("net") or 0.0)
            if net > 0:
                green_days += 1
            elif net < 0:
                red_days += 1
            else:
                flat_days += 1
    return {
        "scope_label": scope_label,
        "traded_days": traded_days,
        "green_days": green_days,
        "red_days": red_days,
        "flat_days": flat_days,
    }


def _dashboard_calendar_payload(
    *,
    year: int,
    month: int,
    scope_active: bool,
    scope_start: str,
    scope_starting_balance: float,
    scope_label: str,
) -> Dict[str, Any]:
    from mccain_capital.repositories import trades as trades_repo

    heat = trades_repo.month_heatmap(
        year,
        month,
        start_date=scope_start if scope_active else "",
        starting_balance=scope_starting_balance if scope_active else None,
    )
    month_name = date(year, month, 1).strftime("%B %Y")
    calendar_state = _dashboard_calendar_state_viewmodel(heat, scope_label)
    return {
        "heat": heat,
        "month_name": month_name,
        "calendar_state": calendar_state,
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
    ticker = get_supported_playbook_ticker(request.form.get("ticker"))
    params: Dict[str, str] = {}
    if y:
        params["y"] = y
    if m:
        params["m"] = m
    if scope in {"active", "all"}:
        params["scope"] = scope
    params["ticker"] = ticker
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
    ticker = get_supported_playbook_ticker(request.form.get("ticker"))
    params: Dict[str, str] = {}
    if y:
        params["y"] = y
    if m:
        params["m"] = m
    if scope in {"active", "all"}:
        params["scope"] = scope
    params["ticker"] = ticker
    return redirect(url_for("dashboard", **params))


def dashboard_pace_update():
    pace_reset = str(request.form.get("pace_reset") or "").strip() == "1"
    custom_daily = app_runtime.parse_float(request.form.get("dashboard_pace_daily") or "") or 0.0
    pass_buffer = app_runtime.parse_float(request.form.get("dashboard_pace_buffer") or "") or 0.0
    start_date = str(request.form.get("dashboard_projection_start_date") or "").strip()
    target_date = str(request.form.get("dashboard_projection_target_date") or "").strip()
    if pace_reset or custom_daily <= 0.0:
        app_runtime.set_setting_value("dashboard_pace_daily", "")
        flash("Forward pace reset to live trading pace.", "success")
    else:
        app_runtime.set_setting_value("dashboard_pace_daily", f"{max(0.0, custom_daily):.2f}")
        flash("Forward pace updated.", "success")
    if pace_reset or pass_buffer <= 0.0:
        app_runtime.set_setting_value("dashboard_pace_buffer", "")
    else:
        app_runtime.set_setting_value("dashboard_pace_buffer", f"{max(0.0, pass_buffer):.2f}")
    if start_date:
        try:
            start_iso = date.fromisoformat(start_date).isoformat()
        except ValueError:
            start_iso = ""
            flash("Projection start date was ignored because it was not a valid date.", "warning")
        app_runtime.set_setting_value("dashboard_projection_start_date", start_iso)
    else:
        app_runtime.set_setting_value("dashboard_projection_start_date", "")
    if target_date:
        try:
            target_iso = date.fromisoformat(target_date).isoformat()
        except ValueError:
            target_iso = ""
            flash("Projection target date was ignored because it was not a valid date.", "warning")
        app_runtime.set_setting_value("dashboard_projection_target_date", target_iso)
    else:
        app_runtime.set_setting_value("dashboard_projection_target_date", "")

    y = str(request.form.get("y") or "").strip()
    m = str(request.form.get("m") or "").strip()
    scope = str(request.form.get("scope") or "").strip().lower()
    ticker = get_supported_playbook_ticker(request.form.get("ticker"))
    params: Dict[str, str] = {}
    if y:
        params["y"] = y
    if m:
        params["m"] = m
    if scope in {"active", "all"}:
        params["scope"] = scope
    params["ticker"] = ticker
    return redirect(url_for("dashboard", **params))


def dashboard_reflection_update():
    if auth_enabled() and not is_authenticated():
        return jsonify({"ok": False, "error": "auth_required"}), 401
    day = (
        str(request.form.get("reflection_day") or app_runtime.today_iso()).strip()
        or app_runtime.today_iso()
    )
    try:
        day = date.fromisoformat(day).isoformat()
    except ValueError:
        day = app_runtime.today_iso()
    _save_dashboard_reflection_settings(
        day,
        {
            "answer": request.form.get("reflection_answer") or "",
            "break_alignment": request.form.get("reflection_break_alignment") or "",
            "urgency_trigger": request.form.get("reflection_urgency_trigger") or "",
            "obey_tomorrow": request.form.get("reflection_obey_tomorrow") or "",
        },
    )
    return jsonify({"ok": True, "reflection": _dashboard_reflection_viewmodel(day)})


def dashboard_behavior_update():
    if auth_enabled() and not is_authenticated():
        return jsonify({"ok": False, "error": "auth_required"}), 401
    day = (
        str(request.form.get("behavior_day") or app_runtime.today_iso()).strip()
        or app_runtime.today_iso()
    )
    try:
        day = date.fromisoformat(day).isoformat()
    except ValueError:
        day = app_runtime.today_iso()
    saved = _save_dashboard_behavior_settings(
        day,
        {
            "discipline_state": request.form.get("discipline_state") or "",
            "discipline_mode": request.form.get("discipline_mode") or "",
            "gate_count": request.form.get("gate_count") or "",
            "routine_done": request.form.get("routine_done") or "",
            "routine_total": request.form.get("routine_total") or "",
            "alignment_pct": request.form.get("alignment_pct") or "",
            "intention": request.form.get("intention") or "",
            "reflection_answer": request.form.get("reflection_answer") or "",
            "urgency_count": request.form.get("urgency_count") or "",
            "increment_urgency": str(request.form.get("increment_urgency") or "").strip() in {"1", "true", "yes"},
            "urgency_increment_count": request.form.get("urgency_increment_count") or "",
            "updated_at": request.form.get("updated_at") or app_runtime.now_iso(),
        },
    )
    return jsonify(
        {
            "ok": True,
            "behavior": saved,
            "trend": _dashboard_behavior_trend_viewmodel(day),
        }
    )


def dashboard():
    from mccain_capital.services import market_data_service
    from mccain_capital.services import market_worker
    from mccain_capital.repositories import analytics as analytics_repo
    from mccain_capital.repositories import journal as journal_repo
    from mccain_capital.repositories import trades as trades_repo

    ticker_context = get_playbook_ticker_context(request.args.get("ticker"))
    selected_ticker = str(ticker_context["ticker"])
    alternate_ticker = str(ticker_context["alternate_ticker"])
    scope = trades_repo.account_scope_snapshot()
    scope_enabled = bool(scope.get("enabled"))
    scope_mode_raw = (request.args.get("scope") or "").strip().lower()
    scope_active = scope_enabled and scope_mode_raw != "all"
    scope_start = str(scope.get("start_date") or "")
    scope_starting_balance = float(scope.get("starting_balance") or 50000.0)
    anchor = trades_repo.latest_trade_day() or app_runtime.now_et().date()
    year = int(request.args.get("y") or anchor.year)
    month = max(1, min(12, int(request.args.get("m") or anchor.month)))
    calendar_scope_label = "Active Account" if scope_active else "All History"
    calendar_payload = _dashboard_calendar_payload(
        year=year,
        month=month,
        scope_active=scope_active,
        scope_start=scope_start,
        scope_starting_balance=scope_starting_balance,
        scope_label=calendar_scope_label,
    )
    heat = calendar_payload["heat"]
    prev_y, prev_m = (year, month - 1)
    next_y, next_m = (year, month + 1)
    if prev_m == 0:
        prev_m = 12
        prev_y -= 1
    if next_m == 13:
        next_m = 1
        next_y += 1

    month_name = calendar_payload["month_name"]
    balance_integrity = trades_repo.balance_integrity_snapshot(
        start_date=scope_start if scope_active else None,
        starting_balance=scope_starting_balance if scope_active else None,
    )
    overall_balance = float(balance_integrity.get("canonical_balance") or 0.0)
    trajectory_title = "Active Account Balance" if scope_active else "Capital Trajectory"
    trajectory_caption = (
        "Scoped account balance with calendar context."
        if scope_active
        else "Live account balance with calendar context."
    )
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
    today_key = app_runtime.today_iso()
    today_rows = [dict(r) for r in trades_repo.fetch_trades(d=today_key, q="")]
    if scope_active and scope_start and today_key < scope_start:
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
    pace_settings = _load_dashboard_pace_settings()
    selected_pace = (
        float(pace_settings.get("custom_daily") or 0.0)
        if bool(pace_settings.get("custom_enabled"))
        else float(proj.get("avg") or 0.0)
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
        avg_daily_profit=selected_pace,
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
    dashboard_instrument = dict(tape_prices.get(selected_ticker) or {})
    dashboard_peer = dict(tape_prices.get(alternate_ticker) or {})
    dashboard_vix = dict(tape_prices.get("VIX") or {})
    if current_app.config.get("TESTING"):
        watch_symbols = [selected_ticker, alternate_ticker, "VIX"]
        if (
            dashboard_instrument.get("price") is None
            or dashboard_peer.get("price") is None
            or dashboard_vix.get("price") is None
        ):
            try:
                tradier_quotes = market_data_service.get_watchlist_tradier(watch_symbols)
            except Exception:
                tradier_quotes = {}
            if dashboard_instrument.get("price") is None:
                dashboard_instrument = dict(
                    tradier_quotes.get(selected_ticker) or dashboard_instrument
                )
            if dashboard_peer.get("price") is None:
                dashboard_peer = dict(tradier_quotes.get(alternate_ticker) or dashboard_peer)
            if dashboard_vix.get("price") is None:
                dashboard_vix = dict(tradier_quotes.get("VIX") or dashboard_vix)
        if (
            dashboard_instrument.get("price") is None
            or dashboard_peer.get("price") is None
            or dashboard_vix.get("price") is None
        ):
            try:
                fallback = market_data_service.get_watchlist(
                    watch_symbols,
                    allow_yf_fallback=False,
                )
            except Exception:
                fallback = {}
            if dashboard_instrument.get("price") is None:
                dashboard_instrument = dict(fallback.get(selected_ticker) or dashboard_instrument)
            if dashboard_peer.get("price") is None:
                dashboard_peer = dict(fallback.get(alternate_ticker) or dashboard_peer)
            if dashboard_vix.get("price") is None:
                dashboard_vix = dict(fallback.get("VIX") or dashboard_vix)
    now_et = app_runtime.now_et()
    now_epoch = int(now_et.timestamp())

    intraday_rows_cache: Dict[str, List[Dict[str, Any]]] = {}

    def _dashboard_intraday_rows(symbol: str) -> List[Dict[str, Any]]:
        key = str(symbol or "").strip().upper()
        if not key:
            return []
        cached = intraday_rows_cache.get(key)
        if cached is not None:
            return cached
        if current_app.config.get("TESTING"):
            try:
                rows = market_data_service.get_intraday(key)
            except Exception:
                rows = []
            cleaned = [dict(row) for row in rows if isinstance(row, dict)]
        else:
            # Keep dashboard requests fast on mobile: render only from the worker
            # snapshot and avoid blocking the page on direct intraday fetches.
            cleaned = []
        intraday_rows_cache[key] = cleaned
        return cleaned

    def _dashboard_mini_series(symbol: str) -> List[float]:
        key = str(symbol or "").strip().upper()
        if not key:
            return []

        # Match Market Pulse first: its tape cards are driven by quote-level mini series.
        # Worker tape points can be sparse or too smooth after a cold start.
        try:
            pulse_snapshot = _market_pulse_snapshot(force_refresh=False)
        except Exception:
            pulse_snapshot = {}
        for quote in list((pulse_snapshot or {}).get("quotes") or []):
            if not isinstance(quote, dict):
                continue
            quote_symbol = str(quote.get("label") or quote.get("symbol") or "").strip().upper()
            if quote_symbol != key:
                continue
            pulse_src = quote.get("mini_series")
            if not isinstance(pulse_src, list):
                pulse_src = quote.get("series") if isinstance(quote.get("series"), list) else []
            pulse_points: List[float] = []
            for item in pulse_src[-120:]:
                if isinstance(item, dict):
                    value = item.get("v")
                else:
                    value = item
                if isinstance(value, (int, float)):
                    pulse_points.append(float(value))
            if len(pulse_points) >= 4:
                return pulse_points[-40:]

        points_src = (tape_snapshot.get("series_points") or {}).get(key) or []
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
            if len(deduped) >= 4:
                return deduped[-40:]
        raw_src = (tape_snapshot.get("series") or {}).get(key) or []
        if isinstance(raw_src, list):
            raw = [float(v) for v in raw_src if isinstance(v, (int, float))]
            deduped_raw: List[float] = []
            for value in raw[-120:]:
                if not deduped_raw or abs(deduped_raw[-1] - value) > 0.01:
                    deduped_raw.append(value)
            if len(deduped_raw) >= 4:
                return deduped_raw[-40:]

        # If cached tape points are too sparse/flat, pull a clean intraday curve directly.
        rows = _dashboard_intraday_rows(key)
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

        replay_points, _replay_day = _market_pulse_cached_replay_series(key)
        replay_values = [
            float(point.get("v"))
            for point in replay_points[-120:]
            if isinstance(point, dict) and isinstance(point.get("v"), (int, float))
        ]
        if len(replay_values) >= 8:
            return replay_values[-40:]
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
        full_intraday_rows = _dashboard_intraday_rows(symbol)
        replay_points: List[Dict[str, Any]] = []
        replay_day: Optional[str] = None
        if full_intraday_rows:
            first_open = next(
                (
                    float(r.get("open"))
                    for r in full_intraday_rows
                    if isinstance(r.get("open"), (int, float))
                ),
                None,
            )
            highs = [
                float(r.get("high"))
                for r in full_intraday_rows
                if isinstance(r.get("high"), (int, float))
            ]
            lows = [
                float(r.get("low"))
                for r in full_intraday_rows
                if isinstance(r.get("low"), (int, float))
            ]
            closes = [
                float(r.get("close"))
                for r in full_intraday_rows
                if isinstance(r.get("close"), (int, float))
            ]
            if first_open is not None:
                enriched["day_open"] = first_open
            if highs and lows:
                day_low = min(lows)
                day_high = max(highs)
                enriched["day_range"] = f"{day_low:.2f} to {day_high:.2f}"
                enriched["day_range_compact"] = (
                    f"{day_high:.2f}"
                    if abs(day_high - day_low) < 0.01
                    else f"{day_low:.2f}-{day_high:.2f}"
                )
            elif closes:
                day_low = min(closes)
                day_high = max(closes)
                enriched["day_range"] = f"{day_low:.2f} to {day_high:.2f}"
                enriched["day_range_compact"] = (
                    f"{day_high:.2f}"
                    if abs(day_high - day_low) < 0.01
                    else f"{day_low:.2f}-{day_high:.2f}"
                )
            vwap_num = 0.0
            vwap_den = 0.0
            for r in full_intraday_rows:
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
                enriched["vwap"] = vwap_num / vwap_den
        elif intraday_series:
            day_low = min(intraday_series)
            day_high = max(intraday_series)
            enriched["day_range"] = f"{day_low:.2f} to {day_high:.2f}"
            if abs(day_high - day_low) < 0.01:
                enriched["day_range_compact"] = f"{day_high:.2f}"
            else:
                enriched["day_range_compact"] = f"{day_low:.2f}-{day_high:.2f}"
        else:
            enriched["day_range"] = "—"
            enriched["day_range_compact"] = "—"
            if enriched.get("day_open") is None:
                enriched["day_open"] = None
        replay_points, replay_day = _market_pulse_cached_replay_series(symbol)
        if len(replay_points) >= 2:
            if not list(enriched.get("prior_session_series") or []):
                enriched["prior_session_series"] = replay_points
            if replay_day and not str(enriched.get("prior_session_day") or "").strip():
                enriched["prior_session_day"] = replay_day
            if not isinstance(enriched.get("vwap"), (int, float)):
                replay_vwap = _market_pulse_series_vwap(replay_points)
                if replay_vwap is not None:
                    enriched["vwap"] = replay_vwap
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
        prev_close = (
            _float_or_none(enriched.get("prior_close"))
            or _float_or_none(enriched.get("prev_close"))
            or _float_or_none(enriched.get("previous_close"))
        )
        if prev_close is None and price is not None and pct is not None and abs(100.0 + pct) > 1e-9:
            prev_close = price / (1.0 + (pct / 100.0))
        if pct is None and price is not None and prev_close is not None and prev_close > 0:
            pct = ((price - prev_close) / prev_close) * 100.0
            enriched["pct_change"] = pct
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
        enriched["sparkline_svg"] = _market_pulse_sparkline_svg(chart_series[-40:], tone)

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
                enriched["freshness_label"] = f"{state} · {_market_data_age_label(age_s)}"
            except Exception:
                enriched["freshness_label"] = f"{state} · 0s old"
        else:
            enriched["freshness_label"] = f"{state} · 0s old"
        enriched["market_state_display"] = state
        enriched["freshness_age_seconds"] = age_s
        enriched["freshness_band_display"] = "Critical" if age_s >= 900 else state
        if age_s >= 3600:
            age_compact = f"{age_s // 3600}h"
        elif age_s >= 60:
            age_compact = f"{age_s // 60}m"
        else:
            age_compact = f"{age_s}s"
        enriched["freshness_label_compact"] = f"{state} · {age_compact}"
        return enriched

    def _dashboard_tape_row_viewmodel(
        symbol: str, quote: Dict[str, Any], descriptor: str = ""
    ) -> Dict[str, Any]:
        enriched = dict(quote or {})
        change_points = (
            enriched.get("change_points")
            if enriched.get("change_points") is not None
            else enriched.get("change")
        )
        row_tone = (
            "up"
            if isinstance(change_points, (int, float)) and float(change_points) > 0
            else (
                "down"
                if isinstance(change_points, (int, float)) and float(change_points) < 0
                else "flat"
            )
        )
        pct_change = (
            enriched.get("pct_change")
            if enriched.get("pct_change") is not None
            else enriched.get("change_pct")
        )
        if pct_change is None and enriched.get("price") is not None:
            prev_close = (
                enriched.get("prior_close")
                if enriched.get("prior_close") is not None
                else enriched.get("prev_close") or enriched.get("previous_close")
            )
            if isinstance(prev_close, (int, float)) and float(prev_close) > 0:
                pct_change = ((float(enriched["price"]) - float(prev_close)) / float(prev_close)) * 100.0
        tape_state = _market_pulse_tape_state(symbol, pct_change)
        return {
            "symbol": symbol,
            "descriptor": descriptor,
            "row_tone": row_tone,
            "state_label": tape_state["label"],
            "state_title": tape_state["title"],
            "tape_tone": tape_state["tone"],
            "sparkline_svg": str(enriched.get("sparkline_svg") or ""),
            "price_display": (
                f"{float(enriched['price']):.2f}"
                if enriched.get("price") is not None
                else "loading..."
            ),
            "pct_change_display": (
                f"{float(pct_change):+.2f}%"
                if pct_change is not None
                else "loading..."
            ),
            "change_display": (
                f"{float(change_points):+.2f}"
                if isinstance(change_points, (int, float))
                else "loading..."
            ),
            "change_arrow": "▲" if row_tone == "up" else "▼" if row_tone == "down" else "",
            "open_display": (
                f"{float(enriched['day_open']):.2f}"
                if enriched.get("day_open") is not None
                else "—"
            ),
            "prev_display": (
                f"{float(enriched['prior_close']):.2f}"
                if enriched.get("prior_close") is not None
                else "—"
            ),
            "live_display": str(enriched.get("freshness_label_compact") or "—")
            .replace("Live · ", "")
            .replace("Delayed · ", "")
            .replace("Unavailable · ", ""),
            "market_state_display": str(enriched.get("market_state_display") or "Live"),
            "freshness_display": (
                f"{str(enriched.get('freshness_band_display') or 'Critical')} · "
                f"{_market_data_age_label(enriched.get('freshness_age_seconds') or 0)}"
            ),
            "gap_display": str(enriched.get("overnight_gap_compact") or "—"),
            "range_display": str(enriched.get("day_range_compact") or "—"),
            "source_display": str(enriched.get("source_badge_label") or "—"),
        }

    dashboard_instrument = _enrich_dashboard_quote(selected_ticker, dashboard_instrument)
    dashboard_peer = _enrich_dashboard_quote(alternate_ticker, dashboard_peer)
    dashboard_vix = _enrich_dashboard_quote("VIX", dashboard_vix)
    dashboard_tape_symbols = _dashboard_tape_symbols(selected_ticker)
    dashboard_tape_quotes: Dict[str, Dict[str, Any]] = {
        selected_ticker: dict(dashboard_instrument),
        alternate_ticker: dict(dashboard_peer),
        "VIX": dict(dashboard_vix),
    }
    extra_tape_quotes: Dict[str, Dict[str, Any]] = {}
    if current_app.config.get("TESTING"):
        try:
            extra_tape_quotes = market_data_service.get_watchlist_with_fallback(
                [symbol for symbol in dashboard_tape_symbols if symbol not in dashboard_tape_quotes]
            )
        except Exception:
            extra_tape_quotes = {}
    for symbol in dashboard_tape_symbols:
        if symbol in dashboard_tape_quotes:
            continue
        worker_quote = dict(tape_prices.get(symbol) or {})
        if current_app.config.get("TESTING"):
            fallback_quote = dict(extra_tape_quotes.get(symbol) or {})
            source_quote = dict(fallback_quote)
            for key, value in worker_quote.items():
                if value is None and key in source_quote:
                    continue
                source_quote[key] = value
        else:
            source_quote = worker_quote
        dashboard_tape_quotes[symbol] = _enrich_dashboard_quote(
            symbol,
            source_quote,
        )
    dashboard_tape_rows = [
        _dashboard_tape_row_viewmodel("SPY", dashboard_tape_quotes.get("SPY") or {}, "market proxy"),
        _dashboard_tape_row_viewmodel("QQQ", dashboard_tape_quotes.get("QQQ") or {}, "tech proxy"),
        _dashboard_tape_row_viewmodel("VIX", dashboard_tape_quotes.get("VIX") or {}, "volatility"),
        _dashboard_tape_row_viewmodel("IWM", dashboard_tape_quotes.get("IWM") or {}, "breadth proxy"),
    ]
    journal_today_rows = [dict(r) for r in journal_repo.fetch_entries(d=today_key)]
    journal_capture_count_today = 0
    for row in journal_today_rows:
        try:
            payload = json.loads(row.get("template_payload") or "{}")
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        if str(payload.get("capture_screenshot_path") or "").strip():
            journal_capture_count_today += 1
    journal_count_today = len(journal_today_rows)
    dashboard_checklist = [
        {
            "label": "Mindset anchored",
            "status": "Loading",
            "detail": "Planning, gamma, and brief context load after the page paints.",
            "done": False,
            "href": "#dashboardPlanningSection",
            "action": "Load",
        },
        {
            "label": "Post-session import",
            "status": "Loaded" if today_count else "Pending",
            "detail": (
                f"{today_count} trade{'s' if today_count != 1 else ''} loaded for today."
                if today_count
                else "No trades loaded yet. Import after the session if needed."
            ),
            "done": today_count > 0,
            "href": "/trades" if today_count else "/trades/upload/statement",
            "action": "Open" if today_count else "Import",
        },
        {
            "label": "Debrief before re-risk",
            "status": "Logged" if journal_count_today else "Missing",
            "detail": (
                f"{journal_count_today} entr{'y' if journal_count_today == 1 else 'ies'} logged"
                + (
                    f" · {journal_capture_count_today} capture{'s' if journal_capture_count_today != 1 else ''} attached."
                    if journal_capture_count_today
                    else "."
                )
                if journal_count_today
                else "No debrief or quick capture logged for today yet."
            ),
            "done": journal_count_today > 0,
            "href": (
                f"/journal?d={today_key}"
                if journal_count_today
                else f"/journal/new?d={today_key}&entry_type=trade_debrief&link_all_day=1&auto_draft=1"
            ),
            "action": "Open" if journal_count_today else "Log",
        },
    ]
    dashboard_planning_ready = False
    daily_brief = {
        "day_key": today_key,
        "cta_label": "Open Trades",
        "status_label": "Planning loading",
    }
    dashboard_market_structure_snapshot = {}
    placeholder_gamma_entries = [
        {
            "key": key,
            "label": label,
            "value": "--",
            "detail": "",
            "tone": "",
            "glow": False,
            "emphasis": "primary" if key in ("regime", "local_flip") else "secondary",
        }
        for key, label in (
            ("regime", "Regime"),
            ("local_flip", "Local Flip"),
            ("next_call_wall", "Next Call Wall"),
            ("next_put_wall", "Next Put Wall"),
            ("main_flip", "Main Flip"),
            ("call_wall", "Call Wall"),
            ("put_wall", "Put Wall"),
        )
    ]
    decision_panel = {
        "status": "Loading",
        "status_tone": "info",
        "posture_summary": "Loading planning context.",
        "plan": "Loading plan...",
        "bias": "Loading",
        "risk_size": "Loading",
        "trade_gate": "Loading trade gate...",
        "gamma_strip": {
            "state": "loading",
            "status_text": "Loading gamma structure and live brief.",
            "entries": placeholder_gamma_entries,
        },
    }

    scope_label = (
        str(scope.get("label") or "").strip()
        if scope_enabled and scope_active and str(scope.get("label") or "").strip()
        else "Active Account" if scope_enabled and scope_active else "All History"
    )
    snapshot_bar = _dashboard_snapshot_viewmodel(
        today_net=today_net,
        today_count=today_count,
        today_wins=today_wins,
        today_losses=today_losses,
        scope_label=scope_label,
        data_trust=data_trust,
        balance_integrity=balance_integrity,
        sync_badges=sync_badges,
    )
    readiness = _dashboard_readiness_viewmodel(
        dashboard_checklist,
        brief_ready=False,
        today_count=today_count,
        data_trust=data_trust,
    )
    dashboard_reflection = _dashboard_reflection_viewmodel(today_key)
    dashboard_behavior_trend = _dashboard_behavior_trend_viewmodel(today_key)
    pace_card = _dashboard_pace_viewmodel(
        proj,
        milestone,
        pace_settings,
        anchor_day=app_runtime.now_et().date(),
    )
    calendar_state = calendar_payload["calendar_state"]

    dashboard_tape_updated_raw = str(tape_snapshot.get("updated_at") or "")
    dashboard_tape_updated_label = _format_iso_et_label(dashboard_tape_updated_raw)
    if dashboard_tape_updated_label:
        # Keep dashboard tape timestamp compact for quick scanning.
        parts = dashboard_tape_updated_label.split(" ", 3)
        if len(parts) >= 4:
            dashboard_tape_updated_label = parts[3]

    content = render_template(
        "dashboard.html",
        selected_ticker=selected_ticker,
        alternate_ticker=alternate_ticker,
        supported_tickers=ticker_context["supported_tickers"],
        playbook_ticker_storage_key=ticker_context["storage_key"],
        heat=heat,
        prev_y=prev_y,
        prev_m=prev_m,
        next_y=next_y,
        next_m=next_m,
        month_name=month_name,
        overall_balance=overall_balance,
        trajectory_title=trajectory_title,
        trajectory_caption=trajectory_caption,
        calendar_scope_label=calendar_scope_label,
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
        dashboard_instrument=dashboard_instrument,
        dashboard_vix=dashboard_vix,
        dashboard_tape_quotes=dashboard_tape_quotes,
        dashboard_tape_rows=dashboard_tape_rows,
        dashboard_tape_updated=dashboard_tape_updated_raw,
        dashboard_tape_updated_label=dashboard_tape_updated_label,
        dashboard_market_structure_snapshot=dashboard_market_structure_snapshot,
        daily_brief=daily_brief,
        dashboard_checklist=dashboard_checklist,
        dashboard_reflection=dashboard_reflection,
        dashboard_behavior_trend=dashboard_behavior_trend,
        snapshot_bar=snapshot_bar,
        readiness=readiness,
        decision_panel=decision_panel,
        pace_card=pace_card,
        calendar_state=calendar_state,
        proj=proj,
        account_scope=scope,
        scope_mode=("active" if scope_active else "all"),
        scope_active_href=f"/dashboard?y={year}&m={month}&scope=active&ticker={selected_ticker}",
        scope_all_href=f"/dashboard?y={year}&m={month}&scope=all&ticker={selected_ticker}",
        market_pulse_href=url_for("market_pulse_page", ticker=selected_ticker, refresh=1),
        dashboard_year=year,
        dashboard_month=month,
        milestone=milestone,
        vanquish_lock=vanquish_lock,
        dashboard_planning_ready=dashboard_planning_ready,
        today_key=today_key,
        money=app_runtime.money,
        money_compact=_money_compact,
    )
    return render_page(content, active="dashboard", vanquish_lock=vanquish_lock)


def dashboard_calendar_fragment():
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
    calendar_scope_label = "Active Account" if scope_active else "All History"
    calendar_payload = _dashboard_calendar_payload(
        year=year,
        month=month,
        scope_active=scope_active,
        scope_start=scope_start,
        scope_starting_balance=scope_starting_balance,
        scope_label=calendar_scope_label,
    )
    return render_template(
        "dashboard/_calendar_panel.html",
        heat=calendar_payload["heat"],
        month_name=calendar_payload["month_name"],
        calendar_state=calendar_payload["calendar_state"],
        money=app_runtime.money,
        money_compact=_money_compact,
    )


def dashboard_planning_refresh_api():
    from mccain_capital.repositories import analytics as analytics_repo
    from mccain_capital.repositories import trades as trades_repo
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import market_worker

    if auth_enabled() and not is_authenticated():
        return jsonify({"ok": False, "error": "auth_required"}), 401

    ticker_context = get_playbook_ticker_context(request.args.get("ticker"))
    selected_ticker = str(ticker_context["ticker"])
    force_refresh = (request.args.get("force") or "").strip() == "1"
    scope_mode = (request.args.get("scope") or "all").strip().lower()

    try:
        market_worker.start_market_worker_once()
    except Exception:
        pass

    now_et = app_runtime.now_et()
    today_key = app_runtime.today_iso()
    anchor = trades_repo.latest_trade_day() or now_et.date()
    year = int(request.args.get("y") or anchor.year)
    month = max(1, min(12, int(request.args.get("m") or anchor.month)))

    today_rows = [dict(r) for r in trades_repo.fetch_trades(d=today_key, q="")]
    today_stats = trades_repo.trade_day_stats(today_rows)
    today_net = float(today_stats.get("total", 0.0))
    today_wins = int(today_stats.get("wins", 0) or 0)
    today_losses = int(today_stats.get("losses", 0) or 0)
    today_count = len(today_rows)

    ytd_trades_list = [
        dict(r)
        for r in trades_repo.fetch_trades_range(
            date(year, 1, 1).isoformat(), date(year + 1, 1, 1).isoformat()
        )
    ]
    ytd_cons = trades_repo.calc_consistency(ytd_trades_list)
    recent_start = max(date(year, month, 1), anchor - timedelta(days=45))
    recent_rows = analytics_repo.fetch_analytics_rows(recent_start.isoformat(), anchor.isoformat())
    recent_rule_breaks = analytics_repo.rule_break_counts(recent_rows)
    top_rule_break = recent_rule_breaks[0] if recent_rule_breaks else None

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

    tape_snapshot = market_worker.get_market_snapshot()
    tape_prices = dict(tape_snapshot.get("prices") or {})
    dashboard_instrument = dict(tape_prices.get(selected_ticker) or {})
    dashboard_vix = dict(tape_prices.get("VIX") or {})

    try:
        gamma_snapshot = gamma_map_service.get_gamma_snapshot()
    except Exception:
        gamma_snapshot = {}

    try:
        news_snapshot = _market_news_snapshot(page_type="dashboard")
    except Exception:
        news_snapshot = {"macro_events": []}

    playbook_snapshot = get_or_build_market_pulse_snapshot(
        ticker=selected_ticker,
        force_refresh=force_refresh,
        now_et=now_et,
        preloaded_gamma_snapshot=gamma_snapshot,
        preloaded_macro_events=list(news_snapshot.get("macro_events") or []),
    )
    dashboard_playbook_view = dict(playbook_snapshot.get("playbook_view") or {})
    dashboard_market_structure_snapshot = dict(
        playbook_snapshot.get("market_structure_snapshot") or {}
    )
    refreshed_gamma_snapshot = dict(playbook_snapshot.get("gamma_snapshot") or gamma_snapshot or {})
    daily_brief = _dashboard_daily_brief_viewmodel(
        now_et=now_et,
        dashboard_spx=dashboard_instrument,
        dashboard_vix=dashboard_vix,
        ticker=selected_ticker,
        dashboard_instrument=dict(playbook_snapshot.get("playbook_quote") or dashboard_instrument),
        gamma_snapshot=refreshed_gamma_snapshot,
        market_structure_snapshot=dashboard_market_structure_snapshot,
        news_snapshot=news_snapshot,
        today_count=today_count,
        today_net=today_net,
    )
    readiness = _dashboard_readiness_viewmodel(
        [],
        brief_ready=True,
        today_count=today_count,
        data_trust={},
    )
    gamma_strip = _dashboard_gamma_strip_viewmodel(
        playbook_view=dashboard_playbook_view,
        market_structure_snapshot=dashboard_market_structure_snapshot,
    )
    decision_panel = _dashboard_decision_viewmodel(
        daily_brief=daily_brief,
        risk_posture_title=risk_posture_title,
        risk_posture_detail=risk_posture_detail,
        data_trust={},
        readiness=readiness,
        dashboard_vix=dashboard_vix,
        playbook_view=dashboard_playbook_view,
        gamma_strip=gamma_strip,
        market_structure_snapshot=dashboard_market_structure_snapshot,
    )
    brief_html = render_template(
        "dashboard/_brief_card.html",
        selected_ticker=selected_ticker,
        daily_brief=daily_brief,
        decision_panel=decision_panel,
        dashboard_year=year,
        dashboard_month=month,
        scope_mode=("active" if scope_mode == "active" else "all"),
        market_pulse_href=url_for("market_pulse_page", ticker=selected_ticker, refresh=1),
    )
    return jsonify(
        {
            "ok": True,
            "decision_panel": decision_panel,
            "dashboard_gamma": gamma_strip,
            "market_structure_snapshot": dashboard_market_structure_snapshot,
            "pattern_watch": pattern_watch,
            "brief_html": brief_html,
            "refreshed_at": app_runtime.now_iso(),
        }
    )


def dashboard_tape_refresh_api():
    from mccain_capital.services import market_data_service
    from mccain_capital.services import market_worker

    if auth_enabled() and not is_authenticated():
        return jsonify({"ok": False, "error": "auth_required"}), 401

    ticker_context = get_playbook_ticker_context(request.args.get("ticker"))
    symbols = _dashboard_tape_symbols(str(ticker_context["ticker"]))
    try:
        market_worker.start_market_worker_once()
    except Exception:
        pass

    snapshot = market_worker.get_market_snapshot()
    prices = dict(snapshot.get("prices") or {})
    needs_fill = [sym for sym in symbols if not isinstance((prices.get(sym) or {}).get("price"), (int, float))]

    if needs_fill:
        try:
            direct_quotes = market_data_service.get_watchlist_tradier(needs_fill)
        except Exception:
            direct_quotes = {}
        for sym in needs_fill:
            quote = dict((direct_quotes or {}).get(sym) or {})
            if isinstance(quote.get("price"), (int, float)):
                prices[sym] = quote

    remaining = [sym for sym in symbols if not isinstance((prices.get(sym) or {}).get("price"), (int, float))]
    if remaining:
        try:
            fallback_quotes = market_data_service.get_watchlist_with_fallback(remaining)
        except Exception:
            fallback_quotes = {}
        for sym in remaining:
            quote = dict((fallback_quotes or {}).get(sym) or {})
            if isinstance(quote.get("price"), (int, float)):
                prices[sym] = quote

    series_points: Dict[str, List[Dict[str, float]]] = {}
    for sym in symbols:
        quote = dict(prices.get(sym) or {})
        quote.setdefault("symbol", sym)
        quote.setdefault("label", sym)
        spark_values = _market_pulse_resolve_sparkline_values(quote)
        if len(spark_values) >= 4:
            series_points[sym] = [
                {"v": float(value), "close": float(value)}
                for value in spark_values[-40:]
                if isinstance(value, (int, float))
            ]

    updated_raw = str(snapshot.get("updated_at") or app_runtime.now_iso())
    updated_label = _format_iso_et_label(updated_raw)
    if updated_label:
        parts = updated_label.split(" ", 3)
        if len(parts) >= 4:
            updated_label = parts[3]

    return jsonify(
        {
            "ok": True,
            "quotes": {sym: dict(prices.get(sym) or {}) for sym in symbols},
            "series_points": series_points,
            "updated_at": updated_raw,
            "updated_label": updated_label or "—",
        }
    )


def stream_market():
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import market_data_service
    from mccain_capital.services import market_pulse_runtime
    from mccain_capital.services import market_worker
    from mccain_capital.services import options_panel_service

    is_testing = bool(current_app.config.get("TESTING"))
    ticker_context = get_playbook_ticker_context(request.args.get("ticker"))
    selected_ticker = str(ticker_context["ticker"])
    gamma_snapshot = gamma_map_service.get_gamma_snapshot()
    if not is_testing:
        market_pulse_runtime.ensure_market_pulse_runtime_started()
        if not gamma_snapshot.get("asof"):
            gamma_snapshot = dict(
                (market_pulse_runtime.refresh_market_pulse_runtime(force_gamma=True) or {}).get(
                    "gamma_snapshot"
                )
                or gamma_map_service.get_gamma_snapshot()
            )

    @stream_with_context
    def generate():
        started_at = time.time()
        yield f"data: {json.dumps({'server_ts': app_runtime.now_iso(), 'stream_ready': True})}\n\n"
        while True:
            payload = market_worker.get_market_snapshot()
            prices = dict(payload.get("prices") or {})
            vix_tick = prices.get("VIX") or prices.get("^VIX") or {}
            if not isinstance(vix_tick, dict) or vix_tick.get("price") in {None, ""}:
                try:
                    tradier_vix = market_data_service.get_watchlist_tradier(["VIX"])
                    if isinstance(tradier_vix, dict) and tradier_vix.get("VIX"):
                        prices["VIX"] = tradier_vix["VIX"]
                except Exception:
                    pass
            payload["prices"] = prices
            payload["options"] = options_panel_service.get_options_snapshot()
            current_gamma_snapshot = (
                gamma_snapshot if is_testing else gamma_map_service.get_gamma_snapshot()
            )
            now_et = app_runtime.now_et()
            current_spx_quote = dict(((payload.get("prices") or {}).get("SPX") or {}))
            current_execution_chart = {
                "mode": (
                    "live_session"
                    if _market_pulse_session_phase(now_et) == "open"
                    else "unavailable"
                ),
                "latest_price": current_spx_quote.get("price"),
                "levels": [
                    {
                        "key": "gamma_flip",
                        "value": current_gamma_snapshot.get("gamma_flip_combined_basket"),
                    },
                    {
                        "key": "local_flip",
                        "value": current_gamma_snapshot.get("local_flip_aggregated_gamma"),
                    },
                    {
                        "key": "call_wall",
                        "value": current_gamma_snapshot.get("call_wall_aggregated_gamma"),
                    },
                    {
                        "key": "put_wall",
                        "value": current_gamma_snapshot.get("put_wall_aggregated_gamma"),
                    },
                ],
            }
            cached_playbook = _market_pulse_cached_playbook_snapshot(now_et)
            if (
                not isinstance(cached_playbook, dict)
                or get_supported_playbook_ticker(cached_playbook.get("ticker")) != selected_ticker
            ):
                cached_playbook = get_or_build_market_pulse_snapshot(
                    ticker=selected_ticker,
                    force_refresh=False,
                    now_et=now_et,
                    preloaded_gamma_snapshot=current_gamma_snapshot,
                )
            current_execution_model = dict((cached_playbook or {}).get("execution_model") or {})
            if not current_execution_model:
                current_execution_model = _market_pulse_execution_model(
                    spx_quote=current_spx_quote,
                    gamma_snapshot=current_gamma_snapshot,
                    execution_chart=current_execution_chart,
                    spx_priority_context={"metrics": {}},
                )
            current_structure_snapshot = dict(
                (cached_playbook or {}).get("market_structure_snapshot") or {}
            )
            current_playbook_view = dict((cached_playbook or {}).get("playbook_view") or {})
            payload["gamma_map"] = current_gamma_snapshot
            payload["ticker"] = selected_ticker
            payload["execution_model"] = current_execution_model
            payload["market_structure_snapshot"] = current_structure_snapshot
            payload["playbook_view"] = current_playbook_view
            payload["dashboard_gamma"] = _dashboard_gamma_strip_viewmodel(
                market_structure_snapshot=current_structure_snapshot,
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
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Connection"] = "keep-alive"
    response.headers["X-Accel-Buffering"] = "no"
    return response


def stream_market_ws():
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import market_pulse_runtime
    from mccain_capital.services import market_worker
    from mccain_capital.services import options_panel_service

    try:
        from simple_websocket import ConnectionClosed
        from simple_websocket import Server
    except Exception:
        return Response("websocket dependency unavailable", status=501)

    is_testing = bool(current_app.config.get("TESTING"))
    if not is_testing:
        market_pulse_runtime.ensure_market_pulse_runtime_started()

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
    from mccain_capital.services import market_pulse_runtime
    from mccain_capital.services import options_panel_service

    is_testing = bool(current_app.config.get("TESTING"))
    if not is_testing:
        market_pulse_runtime.ensure_market_pulse_runtime_started()

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
    from mccain_capital.services import market_pulse_runtime
    from mccain_capital.services import options_panel_service

    if auth_enabled() and not is_authenticated():
        return redirect(url_for("login_page", next="/market-pulse"))
    ticker_context = get_playbook_ticker_context(request.args.get("ticker"))
    selected_ticker = str(ticker_context["ticker"])
    force_refresh = (request.args.get("refresh") or "").strip().lower() in {"1", "true", "yes"}
    now_et = app_runtime.now_et()
    if not current_app.config.get("TESTING"):
        market_pulse_runtime.ensure_market_pulse_runtime_started()
    snapshot = _market_pulse_snapshot(force_refresh=force_refresh)
    cached_playbook_snapshot = dict(_market_pulse_cached_playbook_snapshot(now_et) or {})
    gamma_snapshot = dict(cached_playbook_snapshot.get("gamma_snapshot") or {})
    if force_refresh:
        gamma_snapshot = gamma_map_service.get_gamma_snapshot()
    options_snapshot = options_panel_service.get_options_snapshot()
    if force_refresh and not current_app.config.get("TESTING"):
        runtime_payload = market_pulse_runtime.refresh_market_pulse_runtime(force_gamma=True)
        gamma_snapshot = dict(runtime_payload.get("gamma_snapshot") or gamma_snapshot)
        options_snapshot = dict(runtime_payload.get("options_snapshot") or options_snapshot)
    options_spx = dict((options_snapshot.get("symbols") or {}).get("SPX") or {})
    options_contracts = list(options_spx.get("contracts") or [])
    gamma_updated_label = (
        _format_iso_et_label(
            gamma_snapshot.get("last_successful_compute")
            or gamma_snapshot.get("computed_at")
            or gamma_snapshot.get("asof")
        )
        or "—"
    )
    raw_quotes = list(snapshot.get("quotes") or [])
    vix_row = next(
        (
            q
            for q in raw_quotes
            if str(q.get("label") or q.get("symbol") or "").strip().upper() in {"VIX", "^VIX"}
        ),
        None,
    )
    vix_debug_reason = ""
    if not isinstance(vix_row, dict) or vix_row.get("price") in {None, ""}:
        vix_fallback = None
        try:
            tradier_vix = market_data_service.get_watchlist_tradier(["VIX"])
            if isinstance(tradier_vix, dict) and tradier_vix.get("VIX"):
                vix_fallback = dict(tradier_vix["VIX"])
                vix_debug_reason = "tradier"
        except Exception:
            vix_fallback = None
        if not vix_fallback:
            try:
                vix_alt = market_data_service.get_watchlist(["^VIX"], allow_yf_fallback=True)
                if isinstance(vix_alt, dict) and vix_alt.get("^VIX"):
                    vix_fallback = dict(vix_alt["^VIX"])
                    vix_debug_reason = "yahoo-^VIX"
            except Exception:
                vix_fallback = None
        if not vix_fallback:
            try:
                vix_alt = market_data_service.get_watchlist(["VIX"], allow_yf_fallback=True)
                if isinstance(vix_alt, dict) and vix_alt.get("VIX"):
                    vix_fallback = dict(vix_alt["VIX"])
                    vix_debug_reason = "yahoo-VIX"
            except Exception:
                vix_fallback = None
        if vix_fallback:
            vix_fallback["label"] = "VIX"
            vix_fallback["symbol"] = "VIX"
            if vix_debug_reason:
                vix_fallback["data_reason"] = f"Market Pulse fallback: {vix_debug_reason}"
                vix_fallback["provider"] = vix_debug_reason
            if vix_row and isinstance(vix_row, dict):
                raw_quotes = [q for q in raw_quotes if q is not vix_row]
            raw_quotes.append(vix_fallback)
    quotes = _market_pulse_enrich_quotes(raw_quotes, now_et)
    spx_quote = next((q for q in quotes if str(q.get("label") or "") == "SPX"), {})
    vix_quote = next((q for q in quotes if str(q.get("label") or "") == "VIX"), {})
    try:
        page_spot = float(spx_quote.get("price")) if spx_quote.get("price") is not None else None
        gamma_spot = (
            float(gamma_snapshot.get("spot_price_used"))
            if gamma_snapshot.get("spot_price_used") is not None
            else None
        )
    except Exception:
        page_spot = None
        gamma_spot = None
    page_spot_ts = _parse_iso_et(spx_quote.get("asof") or spx_quote.get("as_of"))
    gamma_spot_ts = _parse_iso_et(gamma_snapshot.get("spot_source_timestamp"))
    if (
        page_spot is not None
        and gamma_spot is not None
        and abs(page_spot - gamma_spot) > GAMMA_SPOT_MISMATCH_POINTS_THRESHOLD
    ):
        stale_flags = list(gamma_snapshot.get("stale_flags") or [])
        if "spot_source_mismatch" not in stale_flags:
            stale_flags.append("spot_source_mismatch")
        gamma_snapshot["stale_flags"] = stale_flags
        warnings = list(gamma_snapshot.get("warnings") or [])
        warning_text = "SPX quote and gamma compute spot are materially different."
        if warning_text not in warnings:
            warnings.append(warning_text)
        gamma_snapshot["warnings"] = warnings
    if (
        page_spot_ts is not None
        and gamma_spot_ts is not None
        and abs(int((page_spot_ts - gamma_spot_ts).total_seconds()))
        > GAMMA_SPOT_TIMESTAMP_DRIFT_SECONDS
    ):
        stale_flags = list(gamma_snapshot.get("stale_flags") or [])
        if "spot_timestamp_drift" not in stale_flags:
            stale_flags.append("spot_timestamp_drift")
        gamma_snapshot["stale_flags"] = stale_flags
        warnings = list(gamma_snapshot.get("warnings") or [])
        warning_text = "SPX quote and gamma compute spot timestamps are materially different."
        if warning_text not in warnings:
            warnings.append(warning_text)
        gamma_snapshot["warnings"] = warnings
    quotes_map = {str(q.get("label") or ""): q for q in quotes if isinstance(q, dict)}
    series_points = {
        str(q.get("label") or q.get("symbol") or ""): list(q.get("series") or [])
        for q in quotes
        if isinstance(q, dict) and str(q.get("label") or q.get("symbol") or "").strip()
    }
    for q in quotes:
        if not isinstance(q, dict):
            continue
        label = str(q.get("label") or q.get("symbol") or "").strip()
        if not label or label in series_points:
            continue
        fallback_values = [
            float(value)
            for value in list(q.get("mini_series") or [])[-40:]
            if isinstance(value, (int, float))
        ]
        if len(fallback_values) >= 4:
            series_points[label] = [{"v": value, "close": value} for value in fallback_values]
    context = _market_pulse_context(quotes)
    macro_events: List[Dict[str, Any]] = []
    playbook_snapshot = get_or_build_market_pulse_snapshot(
        ticker=selected_ticker,
        force_refresh=False,
        now_et=now_et,
        preloaded_snapshot=snapshot,
        preloaded_gamma_snapshot=gamma_snapshot,
        preloaded_quotes=quotes,
        preloaded_macro_events=list(macro_events or []),
    )
    gamma_snapshot = dict(playbook_snapshot.get("gamma_snapshot") or gamma_snapshot)
    playbook_quote = dict(
        playbook_snapshot.get("playbook_quote") or playbook_snapshot.get("spx_quote") or spx_quote
    )
    spx_quote = dict(playbook_snapshot.get("spx_quote") or playbook_quote or spx_quote)
    spx_priority_context = dict(
        playbook_snapshot.get("playbook_priority_context")
        or playbook_snapshot.get("spx_priority_context")
        or {}
    )
    execution_chart = dict(playbook_snapshot.get("execution_chart") or {})
    execution_model = dict(playbook_snapshot.get("execution_model") or {})
    market_structure_snapshot = dict(playbook_snapshot.get("market_structure_snapshot") or {})
    playbook_view = dict(playbook_snapshot.get("playbook_view") or {})
    news_snapshot = {
        "macro_events": list(macro_events or []),
        "market_items": [],
        "pulse_feed_items": [],
        "pulse_feed_accounts": list(MARKET_PULSE_X_ACCOUNTS),
        "watchlist_items": [],
        "market_feed_snapshot": {
            "status": "idle",
            "source_note": "Feed loads on demand. Use refresh when you want the latest posts.",
            "raw_items": [],
        },
        "pulse_feed_available": False,
        "pulse_feed_source_note": "Feed loads on demand. Use refresh when you want the latest posts.",
        "fetched_at": "",
    }
    execution_chart_payload = {**execution_chart, "execution_model": execution_model}
    alert = _market_pulse_alert(quotes)
    guardrail = _market_pulse_guardrail(quotes)
    gamma_quality = _gamma_data_quality(gamma_snapshot, quotes, now_et)
    source_health = build_market_source_health(snapshot, news_snapshot, gamma_snapshot, now_et)
    integrity = dict(snapshot.get("integrity") or {})
    stats = _market_pulse_stats(quotes)
    core_quotes = [q for q in quotes if str(q.get("label") or "") != "SPX"]
    core_quotes.extend([q for q in quotes if str(q.get("label") or "") == "SPX"])
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
        ticker=selected_ticker,
        supported_tickers=ticker_context["supported_tickers"],
        playbook_ticker_storage_key=ticker_context["storage_key"],
        playbook_quote=playbook_quote,
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
        execution_chart=execution_chart,
        execution_chart_payload=execution_chart_payload,
        execution_model=execution_model,
        playbook_view=playbook_view,
        market_structure_snapshot=market_structure_snapshot,
        gamma_csv_href=gamma_csv_href,
        gamma_png_href=gamma_png_href,
        options_contracts=options_contracts,
        news_available=bool(news_snapshot.get("pulse_feed_available")),
        news_source_note=str(
            news_snapshot.get("pulse_feed_source_note") or news_snapshot.get("source_note") or ""
        ),
        macro_events=list(news_snapshot.get("macro_events") or []),
        market_items=list(news_snapshot.get("market_items") or []),
        pulse_feed_items=list(news_snapshot.get("pulse_feed_items") or []),
        pulse_feed_accounts=list(news_snapshot.get("pulse_feed_accounts") or []),
        watchlist_items=list(news_snapshot.get("watchlist_items") or []),
        market_feed_snapshot=dict(news_snapshot.get("market_feed_snapshot") or {}),
        market_pulse_context_api_url=url_for("market_pulse_context_api", ticker=selected_ticker),
        market_pulse_tape_api_url=url_for("market_pulse_tape_api", ticker=selected_ticker),
        money=app_runtime.money,
        money_compact=_money_compact,
    )
    resp = make_response(
        render_page(
            content,
            active="market-pulse",
            title=f"McCain Capital · {selected_ticker} Playbook",
        )
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


def market_pulse_context_api():
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import market_pulse_runtime

    if auth_enabled() and not is_authenticated():
        return jsonify({"ok": False, "error": "auth_required"}), 401
    ticker_context = get_playbook_ticker_context(request.args.get("ticker"))
    selected_ticker = str(ticker_context["ticker"])
    force_refresh = (request.args.get("refresh") or "").strip().lower() in {"1", "true", "yes"}
    now_et = app_runtime.now_et()
    if not current_app.config.get("TESTING"):
        market_pulse_runtime.ensure_market_pulse_runtime_started()
    snapshot = _market_pulse_snapshot(force_refresh=force_refresh)
    cached_playbook_snapshot = dict(_market_pulse_cached_playbook_snapshot(now_et) or {})
    gamma_snapshot = dict(cached_playbook_snapshot.get("gamma_snapshot") or {})
    if force_refresh:
        gamma_snapshot = gamma_map_service.get_gamma_snapshot()
    if force_refresh and not current_app.config.get("TESTING"):
        runtime_payload = market_pulse_runtime.refresh_market_pulse_runtime(force_gamma=True)
        gamma_snapshot = dict(runtime_payload.get("gamma_snapshot") or gamma_snapshot)

    raw_quotes = list(snapshot.get("quotes") or [])
    quotes = _market_pulse_enrich_quotes(raw_quotes, now_et)
    spx_quote = next((q for q in quotes if str(q.get("label") or "") == "SPX"), {})
    vix_quote = next((q for q in quotes if str(q.get("label") or "") == "VIX"), {})
    quotes_map = {str(q.get("label") or ""): q for q in quotes if isinstance(q, dict)}
    series_points = {
        str(q.get("label") or q.get("symbol") or ""): list(q.get("series") or [])
        for q in quotes
        if isinstance(q, dict) and str(q.get("label") or q.get("symbol") or "").strip()
    }
    context = _market_pulse_context(quotes)
    macro_events = _market_news_macro_events(now_et)
    playbook_snapshot = get_or_build_market_pulse_snapshot(
        ticker=selected_ticker,
        force_refresh=force_refresh,
        now_et=now_et,
        preloaded_snapshot=snapshot,
        preloaded_gamma_snapshot=gamma_snapshot,
        preloaded_quotes=quotes,
        preloaded_macro_events=list(macro_events or []),
    )
    gamma_snapshot = dict(playbook_snapshot.get("gamma_snapshot") or gamma_snapshot)
    playbook_quote = dict(
        playbook_snapshot.get("playbook_quote") or playbook_snapshot.get("spx_quote") or spx_quote
    )
    spx_quote = dict(playbook_snapshot.get("spx_quote") or playbook_quote or spx_quote)
    execution_model = dict(playbook_snapshot.get("execution_model") or {})
    market_structure_snapshot = dict(playbook_snapshot.get("market_structure_snapshot") or {})
    playbook_view = dict(playbook_snapshot.get("playbook_view") or {})
    execution_chart = dict(playbook_snapshot.get("execution_chart") or {})
    spx_priority_context = dict(
        playbook_snapshot.get("playbook_priority_context")
        or playbook_snapshot.get("spx_priority_context")
        or {}
    )
    return jsonify(
        {
            "ok": True,
            "payload": {
                "ticker": selected_ticker,
                "playbook_quote": playbook_quote,
                "spx_quote": spx_quote,
                "vix_quote": vix_quote,
                "gamma_snapshot": gamma_snapshot,
                "execution_model": execution_model,
                "market_structure_snapshot": market_structure_snapshot,
                "playbook_view": playbook_view,
                "execution_chart": execution_chart,
                "playbook_priority_context": spx_priority_context,
                "spx_priority_context": spx_priority_context,
                "market_now_iso": now_et.isoformat(),
                "server_ts": now_et.isoformat(),
                "series_points": series_points,
                "quotes_map": quotes_map,
                "vix_direction": context.get("vix_direction") if context else "",
            },
        }
    )


def market_pulse_tape_api():
    if auth_enabled() and not is_authenticated():
        return jsonify({"ok": False, "error": "auth_required"}), 401
    started = time.perf_counter()
    now_et = app_runtime.now_et()
    include_series = (request.args.get("include_series") or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    force_refresh = (request.args.get("refresh") or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    symbols = [str(spec.get("symbol") or "").strip().upper() for spec in MARKET_PULSE_SYMBOLS]
    quotes_by_symbol = market_data_service.get_watchlist(
        symbols,
        allow_yf_fallback=False,
        force_refresh=force_refresh,
    )
    if not quotes_by_symbol:
        snapshot = _market_pulse_snapshot(force_refresh=False)
        raw_quotes = list(snapshot.get("quotes") or [])
        source_label = str(snapshot.get("source_label") or "Market feed cached snapshot")
        fetched_at = str(snapshot.get("fetched_at") or now_et.strftime("%b %d, %Y %I:%M:%S %p ET"))
    else:
        source_label = "Tradier market feed"
        fetched_at = now_et.strftime("%b %d, %Y %I:%M:%S %p ET")
        vix_symbol = "^VIX"
        vix_quote = dict(quotes_by_symbol.get(vix_symbol) or {})
        if not vix_quote or vix_quote.get("price") in {None, ""}:
            try:
                tradier_vix = market_data_service.get_watchlist_tradier(["VIX"])
                if isinstance(tradier_vix, dict) and tradier_vix.get("VIX"):
                    quotes_by_symbol[vix_symbol] = tradier_vix["VIX"]
            except Exception:
                pass
        raw_quotes = []
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
                (price_num - prev_close)
                if (price_num is not None and prev_close is not None)
                else 0.0
            )
            state = "live" if price_num is not None and provider == "tradier" else "delayed"
            if price_num is None:
                state = "missing"
            raw_quotes.append(
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
                    "market_state": (
                        "Live"
                        if state == "live"
                        else "Provider fallback" if state == "delayed" else "Unavailable"
                    ),
                    "day_range": "—",
                    "as_of": as_of or fetched_at,
                    "asof": as_of or fetched_at,
                    "asof_epoch": asof_epoch,
                    "data_state": state,
                    "data_status_label": (
                        "Live" if state == "live" else "Delayed" if state == "delayed" else "Missing"
                    ),
                    "data_reason": reason,
                    "mini_series": [],
                    "series": [],
                }
            )
    quotes = _market_pulse_enrich_quotes(raw_quotes, now_et)
    series_points: Dict[str, List[Dict[str, Any]]] = {}

    def _load_tape_series(symbol: str) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
        try:
            rows = market_data_service.get_intraday(symbol)
        except Exception:
            rows = []
        return symbol, rows, _market_pulse_rows_to_points(rows)

    quote_by_symbol: Dict[str, Dict[str, Any]] = {}
    label_by_symbol: Dict[str, str] = {}
    for q in quotes:
        if not isinstance(q, dict):
            continue
        symbol = str(q.get("symbol") or "").strip().upper()
        label = str(q.get("label") or symbol).strip().upper()
        if not symbol or not label:
            continue
        quote_by_symbol[symbol] = q
        label_by_symbol[symbol] = label

    if include_series:
        max_workers = min(8, max(1, len(quote_by_symbol)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_load_tape_series, symbol): symbol for symbol in quote_by_symbol
            }
            for future in as_completed(futures):
                symbol, rows, points = future.result()
                q = quote_by_symbol.get(symbol)
                label = label_by_symbol.get(symbol, symbol)
                if not q:
                    continue
                if len(points) >= 4:
                    series_points[label] = points
                    q["series"] = points
                    q["mini_series"] = [
                        float(p["v"]) for p in points if isinstance(p.get("v"), (int, float))
                    ]
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
                        q["day_range"] = f"{min(lows):,.2f} to {max(highs):,.2f}"
    quotes_map = {str(q.get("label") or ""): q for q in quotes if isinstance(q, dict)}
    stats = _market_pulse_stats(quotes)
    context = _market_pulse_context(quotes)
    core_quotes = [q for q in quotes if str(q.get("label") or "") != "SPX"]
    leaders = [
        str(q.get("label") or q.get("symbol") or "")
        for q in core_quotes
        if isinstance(q.get("change_pct"), (int, float)) and float(q.get("change_pct")) > 0
    ][:3]
    laggard_label = "—"
    laggard_move = None
    for q in core_quotes:
        q_pct = q.get("change_pct")
        if isinstance(q_pct, (int, float)) and (
            laggard_move is None or float(q_pct) < float(laggard_move)
        ):
            laggard_label = str(q.get("label") or q.get("symbol") or "—")
            laggard_move = float(q_pct)
    drag_label = (
        f"{laggard_label} {laggard_move:+.2f}%"
        if laggard_move is not None
        else "—"
    )
    return jsonify(
        {
            "ok": True,
            "payload": {
                "server_ts": now_et.isoformat(),
                "latency_ms": int((time.perf_counter() - started) * 1000),
                "source_label": source_label,
                "fetched_at": fetched_at,
                "quotes_map": quotes_map,
                "series_points": series_points,
                "stats": stats,
                "tape_meta": {
                    "risk": str(context.get("leadership") or "—"),
                    "leaders": leaders,
                    "drag": drag_label,
                    "missing": int(stats.get("missing") or 0),
                    "advancers": int(stats.get("advancers") or 0),
                    "decliners": int(stats.get("decliners") or 0),
                },
            },
        }
    )


def market_pulse_news_feed_api():
    if auth_enabled() and not is_authenticated():
        return jsonify({"ok": False, "error": "auth_required"}), 401
    force_refresh = (request.args.get("refresh") or "").strip().lower() in {"1", "true", "yes"}
    mode = (request.args.get("mode") or "").strip().lower()
    page_type = (request.args.get("page") or "market-pulse").strip().lower()
    try:
        requested_limit = int(request.args.get("limit") or 0)
    except (TypeError, ValueError):
        requested_limit = 0
    limit = max(
        1,
        min(100, requested_limit or (15 if page_type == "market-pulse" else 5 if page_type == "dashboard" else 15)),
    )
    now_et = app_runtime.now_et()
    playbook_snapshot = get_or_build_market_pulse_snapshot(
        force_refresh=force_refresh, now_et=now_et
    )
    market_structure_snapshot = dict(playbook_snapshot.get("market_structure_snapshot") or {})
    news_snapshot = _market_news_snapshot(
        now_et=now_et,
        market_structure_snapshot=market_structure_snapshot,
        macro_events=_market_news_macro_events(now_et),
        force_refresh_feed=force_refresh,
        page_type=page_type,
        feed_limit=limit,
    )
    feed_snapshot = dict(news_snapshot.get("market_feed_snapshot") or {})
    items = list(news_snapshot.get("pulse_feed_items") or [])
    raw_items = list(feed_snapshot.get("raw_items") or [])
    return jsonify(
        {
            "ok": True,
            "items": (raw_items if mode == "raw" else items)[:limit],
            "status": str(
                feed_snapshot.get("status")
                or ("live" if news_snapshot.get("pulse_feed_available") else "quiet")
            ),
            "source_note": str(
                news_snapshot.get("pulse_feed_source_note")
                or news_snapshot.get("source_note")
                or ""
            ),
            "available": bool(news_snapshot.get("pulse_feed_available")),
            "fetched_at": str(news_snapshot.get("fetched_at") or ""),
            "tracked_accounts": list(news_snapshot.get("pulse_feed_accounts") or []),
            "sources_monitored": list(
                feed_snapshot.get("sources_monitored")
                or news_snapshot.get("pulse_feed_accounts")
                or []
            ),
            "now_summary": dict(feed_snapshot.get("now_summary") or {}),
            "flow_summary": dict(feed_snapshot.get("flow_summary") or {}),
            "limit": limit,
            "returned_count": len((raw_items if mode == "raw" else items)[:limit]),
        }
    )


def market_pulse_feed_page():
    if auth_enabled() and not is_authenticated():
        return redirect(url_for("login_page"))
    now_et = app_runtime.now_et()
    playbook_snapshot = get_or_build_market_pulse_snapshot(force_refresh=False, now_et=now_et)
    market_structure_snapshot = dict(playbook_snapshot.get("market_structure_snapshot") or {})
    news_snapshot = _market_news_snapshot(
        now_et=now_et,
        market_structure_snapshot=market_structure_snapshot,
        macro_events=_market_news_macro_events(now_et),
        force_refresh_feed=False,
        page_type="market-pulse",
    )
    feed_snapshot = dict(news_snapshot.get("market_feed_snapshot") or {})
    feed_items = list(news_snapshot.get("pulse_feed_items") or [])[:MARKET_NEWS_FEED_LIMIT]
    content = render_template(
        "core/market_pulse_feed.html",
        feed_items=feed_items,
        feed_snapshot=feed_snapshot,
        feed_limit=MARKET_NEWS_FEED_LIMIT,
        feed_source_note=str(
            news_snapshot.get("pulse_feed_source_note") or news_snapshot.get("source_note") or ""
        ),
        tracked_accounts=list(news_snapshot.get("pulse_feed_accounts") or []),
        feed_fetched_at=str(news_snapshot.get("fetched_at") or ""),
    )
    resp = make_response(
        render_page(content, active="market-pulse", title="McCain Capital · X Feed Inbox")
    )
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


def hero_bars_api():
    from mccain_capital.services import tradier_hero_chart_service as hero_service

    if auth_enabled() and not is_authenticated():
        return jsonify({"ok": False, "error": "auth_required"}), 401
    symbol = get_supported_playbook_ticker(request.args.get("symbol") or request.args.get("ticker"))
    interval = str(request.args.get("interval") or hero_service.DEFAULT_INTERVAL).strip().lower()
    return jsonify(hero_service.get_intraday_bars(symbol=symbol, interval=interval))


def hero_levels_api():
    from mccain_capital.services import tradier_hero_chart_service as hero_service

    if auth_enabled() and not is_authenticated():
        return jsonify({"ok": False, "error": "auth_required"}), 401
    symbol = get_supported_playbook_ticker(request.args.get("symbol") or request.args.get("ticker"))
    now_et = app_runtime.now_et()
    playbook_snapshot = get_or_build_market_pulse_snapshot(
        ticker=symbol,
        force_refresh=False,
        now_et=now_et,
    )
    return jsonify(
        hero_service.get_hero_levels(
            symbol=symbol,
            playbook_snapshot=playbook_snapshot,
            now_et=now_et,
        )
    )


def hero_quote_api():
    from mccain_capital.services import tradier_hero_chart_service as hero_service

    if auth_enabled() and not is_authenticated():
        return jsonify({"ok": False, "error": "auth_required"}), 401
    symbol = get_supported_playbook_ticker(request.args.get("symbol") or request.args.get("ticker"))
    return jsonify(hero_service.get_live_quote(symbol=symbol, force_refresh=False))


def hero_stream_session_api():
    from mccain_capital.services import tradier_hero_chart_service as hero_service

    if auth_enabled() and not is_authenticated():
        return jsonify({"ok": False, "error": "auth_required"}), 401
    return jsonify(hero_service.get_stream_session_payload())


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
    review_needed_days = 0
    full_review_days = 0
    partial_review_days = 0
    state_rollup: Dict[str, int] = {}
    mistake_rollup: Dict[str, int] = {}
    setup_rollup: Dict[str, float] = {}
    session_rollup: Dict[str, float] = {}

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
            day["quality_key"], day["quality_label"], day["visual_tone"] = _calendar_day_quality(
                day, journal, goals, day_analytics
            )
            review_model = _calendar_review_state(day, journal, day_analytics)
            day.update(review_model)
            day["dominant_setup"] = _calendar_dominant_value(day_analytics, "setup_display")
            day["dominant_session"] = _calendar_dominant_value(day_analytics, "session_tag")
            summary_signal = day["dominant_setup"] or day["dominant_session"]
            if not summary_signal and day["journal_summary"]:
                summary_signal = day["journal_summary"][0]
            if not summary_signal and day["project_summary"]:
                summary_signal = day["project_summary"][0]
            if not summary_signal and not day.get("has_trades"):
                summary_signal = day["focus_label"]
            day["summary_signal"] = summary_signal
            state_rollup[day["day_state"]] = int(state_rollup.get(day["day_state"], 0)) + 1
            if day["mistake_summary"]:
                mistake_rollup[day["mistake_summary"]] = (
                    int(mistake_rollup.get(day["mistake_summary"], 0)) + 1
                )
            if day.get("has_trades") and day["review_state"] != "fully_reviewed":
                review_needed_days += 1
            if day["review_state"] == "fully_reviewed":
                full_review_days += 1
            elif day["review_state"] == "partially_reviewed":
                partial_review_days += 1
            for row in day_analytics:
                net_pl = float(row.get("net_pl") or 0.0)
                setup_name = str(row.get("setup_display") or "").strip()
                session_name = str(row.get("session_tag") or "").strip()
                if setup_name and setup_name.lower() != "unknown":
                    setup_rollup[setup_name] = float(setup_rollup.get(setup_name, 0.0)) + net_pl
                if session_name:
                    session_rollup[session_name] = (
                        float(session_rollup.get(session_name, 0.0)) + net_pl
                    )

        week_days = [day for day in week["days"] if day.get("daynum") is not None]
        week_rows = [
            row for day in week_days for row in (analytics_map.get(str(day.get("iso") or "")) or [])
        ]
        checklist_scores = [
            float(row.get("checklist_score"))
            for row in week_rows
            if row.get("checklist_score") not in (None, "")
        ]
        avg_checklist = (
            round(sum(checklist_scores) / len(checklist_scores)) if checklist_scores else None
        )
        week["wins"] = sum(int(day.get("wins") or 0) for day in week_days)
        week["losses"] = sum(int(day.get("losses") or 0) for day in week_days)
        week["review_needed"] = sum(
            1
            for day in week_days
            if day.get("has_trades") and day.get("review_state") != "fully_reviewed"
        )
        week["fully_reviewed"] = sum(
            1
            for day in week_days
            if day.get("has_trades") and day.get("review_state") == "fully_reviewed"
        )
        week["avg_review_pct"] = (
            round(
                sum(
                    int(day.get("review_completion_pct") or 0)
                    for day in week_days
                    if day.get("has_trades")
                )
                / max(1, sum(1 for day in week_days if day.get("has_trades")))
            )
            if any(day.get("has_trades") for day in week_days)
            else None
        )
        week["avg_grade_score"] = avg_checklist
        week["avg_grade_letter"] = _calendar_grade_letter(avg_checklist)

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

    month_insights = [
        {
            "label": "Best setup",
            "value": (
                max(setup_rollup.items(), key=lambda kv: kv[1])[0]
                if setup_rollup
                else "No setup edge"
            ),
            "meta": (
                _money_compact(max(setup_rollup.values())) if setup_rollup else "Month still thin"
            ),
        },
        {
            "label": "Best session",
            "value": (
                max(session_rollup.items(), key=lambda kv: kv[1])[0]
                if session_rollup
                else "No session edge"
            ),
            "meta": (
                _money_compact(max(session_rollup.values()))
                if session_rollup
                else "No session bias yet"
            ),
        },
        {
            "label": "Worst pattern",
            "value": (
                top_mistake := (
                    max(mistake_rollup.items(), key=lambda kv: kv[1])[0] if mistake_rollup else ""
                )
            )
            and top_mistake.replace("-", " ").title()
            or "No repeated mistake",
            "meta": (
                f"{mistake_rollup.get(top_mistake, 0)} tagged trades"
                if top_mistake
                else "Keep tagging mistakes"
            ),
        },
        {
            "label": "Needs review",
            "value": str(review_needed_days),
            "meta": f"{partial_review_days} partial · {full_review_days} full",
        },
    ]

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
        review_needed_days=review_needed_days,
        full_review_days=full_review_days,
        partial_review_days=partial_review_days,
        state_rollup=state_rollup,
        top_mistake=max(mistake_rollup.items(), key=lambda kv: kv[1])[0] if mistake_rollup else "",
        month_insights=month_insights,
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
        top_notice=model["today_top_notice"],
    )


def analytics_page():
    from mccain_capital.services import analytics as analytics_svc

    return analytics_svc.analytics_page()


def analytics_dashboard_api():
    from mccain_capital.services import analytics as analytics_svc

    return analytics_svc.analytics_dashboard_api()


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


def notifications_test_page():
    content = render_template("ops/notifications_test.html")
    return render_page(content, active="ops", title="McCain Capital · Notifications Lab")


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


def _remove_temp_file(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


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
    response = send_file(
        out_path,
        as_attachment=True,
        download_name=f"mccain_capital_backup_{stamp}.zip",
        mimetype="application/zip",
    )
    response.call_on_close(lambda: _remove_temp_file(out_path))
    return response


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
        max_upload_mb = int((current_app.config.get("MAX_CONTENT_LENGTH") or 0) / (1024 * 1024))
        content = render_template(
            "core/restore_backup.html",
            db_path=str(app_runtime.DB_PATH),
            upload_dir=str(app_runtime.UPLOAD_DIR),
            max_upload_mb=max_upload_mb,
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
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "missing_backup_zip",
                        "message": "Please choose a backup zip file.",
                    }
                ),
                400,
            )
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
    anchor = now_et.date()
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
            importance = _candle_day_importance(day_labels, week_labels, month_labels, day_news)
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
                    "is_dense_day": bool(
                        in_month
                        and is_trading
                        and ((len(day_labels) + len(week_labels) + len(month_labels)) >= 4)
                    ),
                    "is_quiet_day": bool(
                        in_month
                        and is_trading
                        and not day_labels
                        and not week_labels
                        and not month_labels
                        and not day_news
                    ),
                    "reset_count": len(day_labels) + len(week_labels) + len(month_labels),
                    "macro_count": len(day_news),
                    "day_detail": ", ".join(day_labels) or "None",
                    "week_detail": ", ".join(week_labels) or "None",
                    "month_detail": ", ".join(month_labels) or "None",
                    "macro_detail": _candle_macro_detail(day_news),
                    "importance_score": importance["score"],
                    "importance_label": importance["label"],
                    "timing_tags": importance["timing_tags"],
                    "timing_tags_label": " · ".join(importance["timing_tags"]) or "Quiet Day",
                    "hidden_reset_detail": _candle_grouped_reset_detail(
                        day_labels,
                        week_labels,
                        month_labels,
                    ),
                    "reset_detail": ", ".join(day_labels + week_labels + month_labels)
                    or "No reset markers",
                    "labels": day_labels + week_labels + month_labels,
                }
            )
        weeks.append(cells)

    month_name = date(year, month, 1).strftime("%B %Y")
    cluster_summaries = _build_candle_cluster_summaries(
        anchor=anchor,
        year=year,
        month=month,
        weeks=weeks,
    )
    cluster_dates = {item.get("kind"): item.get("iso") for item in cluster_summaries}
    focus_cards = _build_candle_focus_cards(
        anchor=anchor,
        year=year,
        month=month,
        weeks=weeks,
        news_top_days=list(news_overlay["top_days"] or []),
    )
    today_cell = next(
        (
            day
            for week in weeks
            for day in week
            if day.get("in_month") and str(day.get("iso") or "") == anchor.isoformat()
        ),
        None,
    )
    if today_cell is None:
        today_session_index = session_index
        today_week_index = week_index
        today_week_open_dates = week_open_dates
        today_month_index = month_index
        today_month_open_dates = month_open_dates
        if anchor.year != year:
            today_session_index = _trading_day_index_map(anchor.year)
            today_week_index, today_week_open_dates = _trading_week_index_map(anchor.year)
            today_month_index, today_month_open_dates = _trading_month_index_map(anchor.year)
        today_holiday_name = _market_holiday_name(anchor)
        today_is_trading = anchor.weekday() < 5 and not today_holiday_name
        today_day_labels: List[str] = []
        today_week_labels: List[str] = []
        today_month_labels: List[str] = []
        if today_is_trading:
            today_idx = today_session_index.get(anchor)
            if today_idx is not None:
                today_day_labels = [
                    f"{span}D" for span in DAY_OPEN_INTERVALS if today_idx % span == 1
                ]
            if anchor in today_week_open_dates:
                today_widx = today_week_index.get(anchor)
                if today_widx is not None:
                    today_week_labels = [
                        f"{span}W" for span in WEEK_OPEN_INTERVALS if today_widx % span == 1
                    ]
            if anchor in today_month_open_dates:
                today_midx = today_month_index.get(anchor)
                if today_midx is not None:
                    today_month_labels = [
                        f"{span}M" for span in MONTH_OPEN_INTERVALS if today_midx % span == 1
                    ]
        today_events = list(news_overlay["events_by_day"].get(anchor.isoformat(), []))
        if not today_events and (anchor.year, anchor.month) != (year, month):
            today_overlay = _forex_factory_usd_window_events(anchor, anchor)
            today_events = list(today_overlay["events_by_day"].get(anchor.isoformat(), []))
        today_importance = _candle_day_importance(
            today_day_labels,
            today_week_labels,
            today_month_labels,
            today_events,
        )
        today_cell = {
            "iso": anchor.isoformat(),
            "weekday_label": anchor.strftime("%a"),
            "reset_count": len(today_day_labels) + len(today_week_labels) + len(today_month_labels),
            "macro_count": len(today_events),
            "importance_score": today_importance["score"],
            "importance_label": today_importance["label"],
            "timing_tags": today_importance["timing_tags"],
            "timing_tags_label": " · ".join(today_importance["timing_tags"]) or "Quiet Day",
            "reset_detail": ", ".join(today_day_labels + today_week_labels + today_month_labels)
            or "No reset markers",
            "hidden_reset_detail": _candle_grouped_reset_detail(
                today_day_labels,
                today_week_labels,
                today_month_labels,
            ),
        }
    today_snapshot = {
        "iso": anchor.isoformat(),
        "date_label": f"{anchor.strftime('%a %b')} {anchor.day}, {anchor.year}",
        "importance_label": str(today_cell.get("importance_label") or "Quiet"),
        "importance_class": str(today_cell.get("importance_label") or "Quiet").lower(),
        "reset_count": int(today_cell.get("reset_count") or 0),
        "macro_count": int(today_cell.get("macro_count") or 0),
        "reset_detail": str(today_cell.get("reset_detail") or "No reset markers"),
        "hidden_reset_detail": str(today_cell.get("hidden_reset_detail") or "No hidden resets"),
        "timing_tags": list(today_cell.get("timing_tags") or []),
        "timing_tags_label": str(today_cell.get("timing_tags_label") or "Quiet Day"),
        "in_display_month": (anchor.year, anchor.month) == (year, month),
        "session_label": _market_pulse_session_phase(now_et).replace("afterhours", "after hours").title(),
    }
    top_notice = _candle_page_top_notice(now_et, news_overlay["events"])
    today_top_notice = {
        "label": "Today",
        "text": f"{today_snapshot['date_label']} · {today_snapshot['importance_label']}",
        "date_text": today_snapshot["date_label"],
        "density_label": today_snapshot["importance_label"],
        "detail": (
            f"{today_snapshot['reset_count']} resets · "
            f"{today_snapshot['macro_count']} macro · {today_snapshot['timing_tags_label']}"
        ),
        "href": f"#day-{today_snapshot['iso']}" if today_snapshot["in_display_month"] else "",
        "level": f"today-{today_snapshot['importance_class']}",
    }
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
        "focus_cards": focus_cards,
        "cluster_summaries": cluster_summaries,
        "cluster_dates": cluster_dates,
        "today_snapshot": today_snapshot,
        "today_top_notice": today_top_notice,
        "next_macro_notice": top_notice,
    }


def _candle_day_importance(
    day_labels: List[str],
    week_labels: List[str],
    month_labels: List[str],
    news_events: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Score timing density only: reset cycles, macro severity, and reset/macro overlap."""
    reset_count = len(day_labels) + len(week_labels) + len(month_labels)
    score = len(day_labels) + (2 * len(week_labels)) + (3 if month_labels else 0)
    high_macros = sum(1 for event in news_events if str(event.get("impact_class") or "") == "high")
    medium_macros = sum(1 for event in news_events if str(event.get("impact_class") or "") == "medium")
    score += (4 * high_macros) + (2 * medium_macros)
    if reset_count and news_events:
        score += 3
    if score >= 6:
        label = "High"
    elif score >= 3:
        label = "Medium"
    elif score >= 1:
        label = "Light"
    else:
        label = "Quiet"

    timing_tags = []
    if reset_count >= 3:
        timing_tags.append("Reset Cluster")
    if day_labels:
        timing_tags.append("Day Open")
    if week_labels:
        timing_tags.append("Week Open")
    if month_labels:
        timing_tags.append("Month Open")
    if news_events and reset_count:
        timing_tags.append("Macro Collision")
    elif news_events:
        timing_tags.append("Macro Event")
    if not timing_tags:
        timing_tags.append("Quiet Day")
    return {
        "score": score,
        "label": label,
        "timing_tags": timing_tags,
    }


def _candle_grouped_reset_detail(
    day_labels: List[str],
    week_labels: List[str],
    month_labels: List[str],
) -> str:
    groups = [
        f"Day: {', '.join(day_labels)}" if day_labels else "",
        f"Week: {', '.join(week_labels)}" if week_labels else "",
        f"Month: {', '.join(month_labels)}" if month_labels else "",
    ]
    return " · ".join(group for group in groups if group) or "No hidden resets"


def _candle_macro_detail(events: List[Dict[str, Any]]) -> str:
    if not events:
        return "No macro markers"
    parts = []
    for event in events[:4]:
        time_label = str(event.get("time_label") or "").strip()
        title = str(event.get("title") or "Macro event").strip()
        parts.append(f"{time_label} {title}".strip())
    remaining = len(events) - len(parts)
    if remaining > 0:
        parts.append(f"+{remaining} more")
    return " · ".join(parts)


def _candle_cluster_summary(day: Optional[Dict[str, Any]], fallback_title: str) -> Dict[str, str]:
    if not day:
        return {
            "kind": "empty",
            "iso": "",
            "title": fallback_title,
            "date": "None scheduled",
            "headline": "No qualifying cluster in this month.",
            "meta": "Calendar is clear for this cluster type.",
            "href": "#",
            "tone": "quiet",
        }
    reset_count = int(day.get("reset_count") or 0)
    macro_count = int(day.get("macro_count") or 0)
    total = reset_count + macro_count
    return {
        "kind": "cluster",
        "iso": str(day["iso"]),
        "title": fallback_title,
        "date": datetime.strptime(str(day["iso"]), "%Y-%m-%d").strftime("%a, %b %d"),
        "headline": f"{total} timing marker{'s' if total != 1 else ''}",
        "meta": " · ".join(
            part
            for part in [
                f"{reset_count} reset{'s' if reset_count != 1 else ''}" if reset_count else "",
                f"{macro_count} macro" if macro_count else "",
                str(day.get("reset_detail") or ""),
            ]
            if part
        ),
        "href": f"#day-{day['iso']}",
        "tone": "overlap" if reset_count and macro_count else ("reset" if reset_count else "macro"),
    }


def _build_candle_cluster_summaries(
    *,
    anchor: date,
    year: int,
    month: int,
    weeks: List[List[Dict[str, Any]]],
) -> List[Dict[str, str]]:
    month_days = [
        day
        for week in weeks
        for day in week
        if day.get("in_month") and day.get("is_trading")
    ]
    if not month_days:
        return []

    active_days = [
        day
        for day in month_days
        if date.fromisoformat(str(day.get("iso"))) >= anchor
    ]
    horizon_days = active_days or month_days

    def cluster_score(day: Dict[str, Any]) -> tuple[int, int, int]:
        reset_count = int(day.get("reset_count") or 0)
        macro_count = int(day.get("macro_count") or 0)
        return (reset_count + macro_count, reset_count, macro_count)

    next_reset = next((day for day in horizon_days if int(day.get("reset_count") or 0)), None)
    largest = max(month_days, key=cluster_score, default=None)
    if largest and cluster_score(largest)[0] == 0:
        largest = None
    overlap = next(
        (
            day
            for day in horizon_days
            if int(day.get("reset_count") or 0) and int(day.get("macro_count") or 0)
        ),
        None,
    )
    if overlap is None:
        overlap = max(
            (
                day
                for day in month_days
                if int(day.get("reset_count") or 0) and int(day.get("macro_count") or 0)
            ),
            key=cluster_score,
            default=None,
        )

    next_summary = _candle_cluster_summary(next_reset, "Next reset cluster")
    next_summary["kind"] = "next"
    largest_summary = _candle_cluster_summary(largest, "Largest cluster this month")
    largest_summary["kind"] = "largest"
    overlap_summary = _candle_cluster_summary(overlap, "Macro / reset overlap")
    overlap_summary["kind"] = "overlap"
    return [next_summary, largest_summary, overlap_summary]


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


def _build_candle_focus_cards(
    *,
    anchor: date,
    year: int,
    month: int,
    weeks: List[List[Dict[str, Any]]],
    news_top_days: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    month_days = [
        day
        for week in weeks
        for day in week
        if day.get("in_month") and not day.get("is_weekend") and not day.get("is_holiday")
    ]
    if not month_days:
        return []

    active_days = [day for day in month_days if date.fromisoformat(day["iso"]) >= anchor]
    horizon_days = active_days or month_days

    def _day_signal_score(day: Dict[str, Any]) -> tuple[int, int, int]:
        news_score = int(day.get("news_priority_score") or 0)
        label_count = len(day.get("labels") or [])
        event_count = len(day.get("news_events") or [])
        return (news_score, label_count, event_count)

    upcoming_reset = next((day for day in horizon_days if day.get("labels")), None)
    peak_reset = max((day for day in month_days if day.get("labels")), key=_day_signal_score, default=None)
    next_macro = None
    for top_day in list(news_top_days or []):
        try:
            top_day_date = date.fromisoformat(str(top_day.get("iso") or ""))
        except ValueError:
            continue
        if top_day_date >= anchor and top_day_date.month == month and top_day_date.year == year:
            next_macro = top_day
            break
    if next_macro is None:
        next_macro = next((top for top in list(news_top_days or []) if top.get("iso")), None)

    cards: List[Dict[str, Any]] = []
    if upcoming_reset:
        cards.append(
            {
                "kind": "reset",
                "tone": (
                    "dense"
                    if len(upcoming_reset.get("labels") or []) >= 4
                    else "standard"
                ),
                "eyebrow": "Next Reset Cluster",
                "title": datetime.strptime(upcoming_reset["iso"], "%Y-%m-%d").strftime("%a, %b %d"),
                "headline": (
                    f"{len(upcoming_reset.get('labels') or [])} reset markers"
                    if len(upcoming_reset.get("labels") or []) > 1
                    else f"{(upcoming_reset.get('labels') or ['Reset'])[0]} opens"
                ),
                "meta": " / ".join(list(upcoming_reset.get("labels") or [])[:3])
                or "No reset markers",
                "href": f"#day-{upcoming_reset['iso']}",
            }
        )
    if next_macro:
        cards.append(
            {
                "kind": "macro",
                "tone": str(next_macro.get("focus_key") or "macro"),
                "eyebrow": "Next Macro Collision",
                "title": str(next_macro.get("date_label") or ""),
                "headline": str(next_macro.get("headline") or "Macro day"),
                "meta": " · ".join(
                    [
                        part
                        for part in [
                            (
                                f"{int(next_macro.get('high_count') or 0)} red"
                                if int(next_macro.get("high_count") or 0)
                                else ""
                            ),
                            (
                                f"{int(next_macro.get('medium_count') or 0)} orange"
                                if int(next_macro.get("medium_count") or 0)
                                else ""
                            ),
                            (f"{next_macro.get('focus_label')} priority" if next_macro.get("focus_label") else ""),
                        ]
                        if part
                    ]
                ),
                "href": f"#news-day-{next_macro['iso']}",
            }
        )
    if peak_reset and (not upcoming_reset or peak_reset.get("iso") != upcoming_reset.get("iso")):
        cards.append(
            {
                "kind": "peak",
                "tone": "standard",
                "eyebrow": "Peak Signal Day",
                "title": datetime.strptime(peak_reset["iso"], "%Y-%m-%d").strftime("%a, %b %d"),
                "headline": f"{len(peak_reset.get('labels') or [])} reset markers in one session",
                "meta": " / ".join(list(peak_reset.get("labels") or [])[:4]),
                "href": f"#day-{peak_reset['iso']}",
            }
        )
    return cards[:3]


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


def _calendar_grade_letter(score: Optional[float]) -> str:
    if score is None:
        return ""
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def _calendar_dominant_value(rows: List[Dict[str, Any]], key: str) -> str:
    counts: Dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "").strip()
        if not value or value.lower() in {"unknown", "n/a", "none"}:
            continue
        counts[value] = int(counts.get(value, 0)) + 1
    if not counts:
        return ""
    return max(counts.items(), key=lambda kv: (kv[1], kv[0]))[0]


def _calendar_review_state(
    day_row: Dict[str, Any], journal_row: Dict[str, Any], analytics_rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    if not day_row.get("has_trades"):
        return {
            "review_state": "none",
            "review_state_label": "",
            "review_completion_pct": 0,
            "review_marker_label": "",
        }

    row_scores = [
        int(row.get("review_completion_pct") or 0)
        for row in analytics_rows
        if row.get("review_completion_pct") is not None
    ]
    base_pct = round(sum(row_scores) / len(row_scores)) if row_scores else 0
    if journal_row:
        base_pct = max(base_pct, 45 if not row_scores else min(100, base_pct + 15))
    if base_pct >= 85:
        state = "fully_reviewed"
        label = "Full"
    elif base_pct >= 35:
        state = "partially_reviewed"
        label = "Partial"
    else:
        state = "not_reviewed"
        label = "Open"
    return {
        "review_state": state,
        "review_state_label": label,
        "review_completion_pct": max(0, min(100, base_pct)),
        "review_marker_label": f"{label} · {max(0, min(100, base_pct))}%",
    }


def _calendar_day_quality(
    day_row: Dict[str, Any],
    journal_row: Dict[str, Any],
    goal_row: Dict[str, Any],
    analytics_rows: List[Dict[str, Any]],
) -> Tuple[str, str, str]:
    if day_row.get("has_trades"):
        net = float(day_row.get("net") or 0.0)
        has_mistake = bool(_day_mistake_summary(analytics_rows))
        if net > 0:
            return (
                ("bad_win", "Bad Win", "warn")
                if has_mistake
                else ("clean_win", "Clean Win", "gain")
            )
        if net < 0:
            return (
                ("bad_loss", "Bad Loss", "loss")
                if has_mistake
                else ("good_loss", "Good Loss", "info")
            )
        return ("scratch", "Scratch", "flat")
    if journal_row and goal_row:
        return ("review_build", "Review + Build", "project")
    if journal_row:
        return ("debrief_day", "Debrief", "neutral")
    if goal_row:
        return ("project_day", "Project", "project")
    if day_row.get("is_weekend"):
        return ("closed", "Weekend", "muted")
    return ("quiet_day", "Quiet", "neutral")


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
