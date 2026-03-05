"""In-app polling worker for live market cache + alert evaluation."""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List

from mccain_capital import runtime as app_runtime
from mccain_capital.services import alerts_service
from mccain_capital.services import market_data_service

WATCHLIST = ["SPX", "QQQ", "NVDA", "TSLA"]
POLL_SECONDS = 10
MAX_ALERT_MESSAGES = 100

_LOCK = threading.Lock()
_STARTED = False
_ALERT_STATE: Dict[int, bool] = {}
_MARKET_CACHE: Dict[str, Any] = {
    "prices": {s: {"price": None, "pct_change": None, "as_of": ""} for s in WATCHLIST},
    "alerts": [],
    "updated_at": "",
}


def get_market_snapshot() -> Dict[str, Any]:
    with _LOCK:
        return {
            "prices": dict(_MARKET_CACHE.get("prices") or {}),
            "alerts": list(_MARKET_CACHE.get("alerts") or []),
            "updated_at": str(_MARKET_CACHE.get("updated_at") or ""),
        }


def _update_cache(prices: Dict[str, Dict[str, Any]], alerts: List[str]) -> None:
    with _LOCK:
        _MARKET_CACHE["prices"] = prices
        existing = list(_MARKET_CACHE.get("alerts") or [])
        merged = (alerts + existing)[:MAX_ALERT_MESSAGES]
        _MARKET_CACHE["alerts"] = merged
        _MARKET_CACHE["updated_at"] = app_runtime.now_iso()


def _poll_once() -> None:
    alerts_service.ensure_alert_tables()
    prices = market_data_service.get_watchlist(WATCHLIST)
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
        time.sleep(POLL_SECONDS)


def start_market_worker_once() -> None:
    global _STARTED
    with _LOCK:
        if _STARTED:
            return
        _STARTED = True
    t = threading.Thread(target=_worker_loop, name="market-worker", daemon=True)
    t.start()
