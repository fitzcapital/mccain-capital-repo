"""Yahoo Finance market data adapter used by live dashboard pulse."""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any, Dict, List, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _load_yfinance():
    try:
        import yfinance as yf  # type: ignore

        return yf
    except Exception:
        return None


def _history(symbol: str):
    yf = _load_yfinance()
    if yf is None:
        return None
    try:
        ticker = yf.Ticker(str(symbol or "").strip().upper())
        return ticker.history(period="1d", interval="1m")
    except Exception:
        return None


def get_price(symbol: str) -> Optional[float]:
    """Return latest 1m close for symbol, or None when unavailable."""
    hist = _history(symbol)
    if hist is None:
        return None
    try:
        if hist.empty:
            return None
        close = hist["Close"].dropna()
        if close.empty:
            return None
        return float(close.iloc[-1])
    except Exception:
        return None


def get_intraday(symbol: str) -> List[Dict[str, Any]]:
    """Return today's 1m candles as normalized list."""
    hist = _history(symbol)
    if hist is None:
        return []
    out: List[Dict[str, Any]] = []
    try:
        if hist.empty:
            return out
        for idx, row in hist.iterrows():
            ts = getattr(idx, "to_pydatetime", lambda: idx)()
            if hasattr(ts, "isoformat"):
                ts_iso = ts.isoformat(timespec="seconds")
            else:
                ts_iso = str(ts)
            out.append(
                {
                    "ts": ts_iso,
                    "open": float(row.get("Open") or 0.0),
                    "high": float(row.get("High") or 0.0),
                    "low": float(row.get("Low") or 0.0),
                    "close": float(row.get("Close") or 0.0),
                    "volume": float(row.get("Volume") or 0.0),
                }
            )
    except Exception:
        return []
    return out


def _pct_change_from_history(hist) -> Optional[float]:
    try:
        if hist is None or hist.empty:
            return None
        close = hist["Close"].dropna()
        open_ = hist["Open"].dropna()
        if close.empty:
            return None
        last_close = float(close.iloc[-1])
        base = None
        if not open_.empty:
            base = float(open_.iloc[0])
        if (not base or base <= 0.0) and len(close) >= 2:
            base = float(close.iloc[0])
        if not base or base <= 0.0:
            return None
        return ((last_close - base) / base) * 100.0
    except Exception:
        return None


def get_watchlist(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Return latest price and percent change for a symbol list."""
    snapshot: Dict[str, Dict[str, Any]] = {}
    as_of = _now_iso()
    for raw in symbols:
        symbol = str(raw or "").strip().upper()
        if not symbol:
            continue
        hist = _history(symbol)
        price = None
        pct = None
        try:
            if hist is not None and not hist.empty:
                close = hist["Close"].dropna()
                if not close.empty:
                    price = float(close.iloc[-1])
                pct = _pct_change_from_history(hist)
        except Exception:
            price = None
            pct = None
        snapshot[symbol] = {
            "price": price,
            "pct_change": pct,
            "as_of": as_of,
        }
    return snapshot
