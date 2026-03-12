"""In-app polling worker for live market cache + alert evaluation."""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any, Dict, List

from mccain_capital import runtime as app_runtime
from mccain_capital.services import alerts_service
from mccain_capital.services import market_data_service

WATCHLIST = [
    "SPX",
    "SPY",
    "QQQ",
    "IWM",
    "VIX",
    "NVDA",
    "MSFT",
    "AAPL",
    "AMZN",
    "META",
    "TSLA",
    "CSCO",
    "INTC",
]
POLL_SECONDS = 2
FAST_TICK_SECONDS = 0.25
FAST_TICK_SYMBOLS = ["SPX", "VIX"]
MAX_ALERT_MESSAGES = 100
MAX_SERIES_POINTS = 240

_LOCK = threading.Lock()
_STARTED = False
_FAST_STARTED = False
_ALERT_STATE: Dict[int, bool] = {}
_MARKET_CACHE: Dict[str, Any] = {
    "prices": {s: {"price": None, "pct_change": None, "as_of": ""} for s in WATCHLIST},
    "series": {s: [] for s in WATCHLIST},
    "series_points": {s: [] for s in WATCHLIST},
    "alerts": [],
    "updated_at": "",
}
_SERIES: Dict[str, deque] = {s: deque(maxlen=MAX_SERIES_POINTS) for s in WATCHLIST}
_SERIES_POINTS: Dict[str, deque] = {s: deque(maxlen=MAX_SERIES_POINTS) for s in WATCHLIST}
_LAST_INTRADAY_SEED_AT: Dict[str, float] = {s: 0.0 for s in WATCHLIST}


def get_market_snapshot() -> Dict[str, Any]:
    with _LOCK:
        return {
            "prices": dict(_MARKET_CACHE.get("prices") or {}),
            "series": {k: list(v) for k, v in (_MARKET_CACHE.get("series") or {}).items()},
            "series_points": {
                k: list(v) for k, v in (_MARKET_CACHE.get("series_points") or {}).items()
            },
            "alerts": list(_MARKET_CACHE.get("alerts") or []),
            "updated_at": str(_MARKET_CACHE.get("updated_at") or ""),
        }


def _update_cache(prices: Dict[str, Dict[str, Any]], alerts: List[str]) -> None:
    now_ts = time.time()
    # Seed full intraday curves so cards/charts don't collapse to 2-point diagonals.
    for symbol in WATCHLIST:
        if symbol not in _SERIES:
            _SERIES[symbol] = deque(maxlen=MAX_SERIES_POINTS)
        if symbol not in _SERIES_POINTS:
            _SERIES_POINTS[symbol] = deque(maxlen=MAX_SERIES_POINTS)
        last_seed = float(_LAST_INTRADAY_SEED_AT.get(symbol) or 0.0)
        if _SERIES[symbol] and (now_ts - last_seed) < 900:
            continue
        rows = market_data_service.get_intraday(symbol)
        points = [
            {"ts": str(r.get("ts") or ""), "v": float(r.get("close"))}
            for r in rows[-MAX_SERIES_POINTS:]
            if isinstance(r, dict) and r.get("close") is not None
        ]
        closes = [float(p["v"]) for p in points]
        if len(closes) >= 20:
            _SERIES[symbol] = deque(closes, maxlen=MAX_SERIES_POINTS)
            _SERIES_POINTS[symbol] = deque(points, maxlen=MAX_SERIES_POINTS)
            _LAST_INTRADAY_SEED_AT[symbol] = now_ts

    for symbol, quote in prices.items():
        if symbol not in _SERIES:
            _SERIES[symbol] = deque(maxlen=MAX_SERIES_POINTS)
        if symbol not in _SERIES_POINTS:
            _SERIES_POINTS[symbol] = deque(maxlen=MAX_SERIES_POINTS)
        price = quote.get("price") if isinstance(quote, dict) else None
        if price is None:
            continue
        try:
            val = float(price)
            _SERIES[symbol].append(val)
            _SERIES_POINTS[symbol].append(
                {"ts": str(quote.get("as_of") or app_runtime.now_iso()), "v": val}
            )
        except Exception:
            continue

    with _LOCK:
        _MARKET_CACHE["prices"] = prices
        _MARKET_CACHE["series"] = {k: list(v) for k, v in _SERIES.items()}
        _MARKET_CACHE["series_points"] = {k: list(v) for k, v in _SERIES_POINTS.items()}
        existing = list(_MARKET_CACHE.get("alerts") or [])
        merged = (alerts + existing)[:MAX_ALERT_MESSAGES]
        _MARKET_CACHE["alerts"] = merged
        _MARKET_CACHE["updated_at"] = app_runtime.now_iso()


def _merge_fast_prices(prices: Dict[str, Dict[str, Any]]) -> None:
    if not prices:
        return
    now_iso = app_runtime.now_iso()
    with _LOCK:
        cache_prices = dict(_MARKET_CACHE.get("prices") or {})
        for symbol, quote in prices.items():
            if symbol not in WATCHLIST or not isinstance(quote, dict):
                continue
            prior = dict(cache_prices.get(symbol) or {})
            merged = {**prior, **quote}
            cache_prices[symbol] = merged
            price = merged.get("price")
            if price is None:
                continue
            try:
                val = float(price)
            except Exception:
                continue
            _SERIES.setdefault(symbol, deque(maxlen=MAX_SERIES_POINTS)).append(val)
            _SERIES_POINTS.setdefault(symbol, deque(maxlen=MAX_SERIES_POINTS)).append(
                {"ts": str(merged.get("as_of") or now_iso), "v": val}
            )
        _MARKET_CACHE["prices"] = cache_prices
        _MARKET_CACHE["series"] = {k: list(v) for k, v in _SERIES.items()}
        _MARKET_CACHE["series_points"] = {k: list(v) for k, v in _SERIES_POINTS.items()}
        _MARKET_CACHE["updated_at"] = now_iso


def _poll_once() -> None:
    alerts_service.ensure_alert_tables()
    # Keep the live cache moving even when primary providers are rate-limited/forbidden.
    prices = market_data_service.get_watchlist(WATCHLIST, allow_yf_fallback=True)
    fired: List[str] = []
    for symbol, quote in prices.items():
        price = quote.get("price") if isinstance(quote, dict) else None
        if price is None:
            continue
        fired.extend(alerts_service.evaluate_alerts(symbol, float(price), _ALERT_STATE))
    _update_cache(prices, fired)


def _worker_loop() -> None:
    while True:
        try:
            _poll_once()
        except Exception:
            # Keep worker alive on transient provider/db errors.
            pass
        try:
            sleep_s = float(
                app_runtime.get_setting_value("market_poll_seconds", POLL_SECONDS) or POLL_SECONDS
            )
        except Exception:
            sleep_s = float(POLL_SECONDS)
        if sleep_s < 1.0:
            sleep_s = 1.0
        time.sleep(sleep_s)


def _fast_tick_loop() -> None:
    while True:
        try:
            # Keep fast loop strict to primary providers; avoid yfinance for intrasecond cadence.
            fast_prices = market_data_service.get_watchlist(
                FAST_TICK_SYMBOLS, allow_yf_fallback=False
            )
            _merge_fast_prices(fast_prices)
        except Exception:
            pass
        try:
            sleep_s = float(
                app_runtime.get_setting_value("market_fast_tick_seconds", FAST_TICK_SECONDS)
                or FAST_TICK_SECONDS
            )
        except Exception:
            sleep_s = float(FAST_TICK_SECONDS)
        if sleep_s < 0.20:
            sleep_s = 0.20
        time.sleep(sleep_s)


def start_market_worker_once() -> None:
    global _STARTED, _FAST_STARTED
    with _LOCK:
        start_main = not _STARTED
        start_fast = not _FAST_STARTED
        if start_main:
            _STARTED = True
        if start_fast:
            _FAST_STARTED = True
    if start_main:
        t = threading.Thread(target=_worker_loop, name="market-worker", daemon=True)
        t.start()
    if start_fast:
        tf = threading.Thread(target=_fast_tick_loop, name="market-fast-tick-worker", daemon=True)
        tf.start()
