"""Market data adapter for dashboard pulse.

Primary provider: Massive/Polygon API.
Fallback provider: yfinance.
"""

from __future__ import annotations

from datetime import date
from datetime import datetime
from datetime import timezone
import json
import os
from typing import Any, Dict, List, Optional
import urllib.parse
import urllib.request

from mccain_capital import runtime as app_runtime


YF_SYMBOL_ALIASES = {
    "SPX": "^GSPC",
}

MASSIVE_SYMBOL_ALIASES = {
    "SPX": "I:SPX",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _massive_api_key() -> str:
    return (
        (os.environ.get("MASSIVE_API_KEY") or "").strip()
        or (os.environ.get("POLYGON_API_KEY") or "").strip()
        or str(app_runtime.get_setting_value("massive_api_key", "") or "").strip()
        or str(app_runtime.get_setting_value("polygon_api_key", "") or "").strip()
    )


def _massive_symbol(symbol: str) -> str:
    sym = str(symbol or "").strip().upper()
    return MASSIVE_SYMBOL_ALIASES.get(sym, sym)


def _massive_json(path: str, params: Dict[str, Any]) -> Dict[str, Any] | None:
    key = _massive_api_key()
    if not key:
        return None
    q = dict(params)
    q["apiKey"] = key
    url = "https://api.polygon.io" + path + "?" + urllib.parse.urlencode(q)
    req = urllib.request.Request(url, headers={"User-Agent": "mccain-capital/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            parsed = json.loads(body)
            if isinstance(parsed, dict):
                return parsed
            return None
    except Exception:
        return None


def _massive_intraday_rows(symbol: str) -> List[Dict[str, Any]]:
    ticker = urllib.parse.quote(_massive_symbol(symbol), safe="")
    d = date.today().isoformat()
    payload = _massive_json(
        f"/v2/aggs/ticker/{ticker}/range/1/minute/{d}/{d}",
        {"adjusted": "true", "sort": "asc", "limit": 50000},
    )
    if not payload:
        return []
    results = payload.get("results")
    if not isinstance(results, list):
        return []
    out: List[Dict[str, Any]] = []
    for r in results:
        if not isinstance(r, dict):
            continue
        ts_ms = r.get("t")
        try:
            dt = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=timezone.utc)
            ts_iso = dt.isoformat(timespec="seconds")
        except Exception:
            ts_iso = ""
        out.append(
            {
                "ts": ts_iso,
                "open": float(r.get("o") or 0.0),
                "high": float(r.get("h") or 0.0),
                "low": float(r.get("l") or 0.0),
                "close": float(r.get("c") or 0.0),
                "volume": float(r.get("v") or 0.0),
            }
        )
    return out


def _massive_prev_close(symbol: str) -> Optional[float]:
    ticker = urllib.parse.quote(_massive_symbol(symbol), safe="")
    payload = _massive_json(f"/v2/aggs/ticker/{ticker}/prev", {"adjusted": "true"})
    if not payload:
        return None
    results = payload.get("results")
    if isinstance(results, list) and results:
        return _safe_float(results[0].get("c"))
    return None


def _massive_watch_quote(symbol: str) -> Dict[str, Any]:
    rows = _massive_intraday_rows(symbol)
    prev_close = _massive_prev_close(symbol)

    price = None
    if rows:
        price = _safe_float(rows[-1].get("close"))
    if price is None:
        price = prev_close

    pct = None
    if price is not None and prev_close is not None and prev_close > 0:
        pct = ((float(price) - float(prev_close)) / float(prev_close)) * 100.0

    return {"price": price, "pct_change": pct, "as_of": _now_iso()}


def _load_yfinance():
    try:
        import yfinance as yf  # type: ignore

        return yf
    except Exception:
        return None


def _yf_symbol(symbol: str) -> str:
    sym = str(symbol or "").strip().upper()
    return YF_SYMBOL_ALIASES.get(sym, sym)


def _yf_ticker(symbol: str):
    yf = _load_yfinance()
    if yf is None:
        return None
    try:
        return yf.Ticker(_yf_symbol(symbol))
    except Exception:
        return None


def _yf_history(symbol: str):
    ticker = _yf_ticker(symbol)
    if ticker is None:
        return None
    try:
        return ticker.history(period="1d", interval="1m", prepost=True)
    except Exception:
        return None


def _yf_previous_close(ticker) -> Optional[float]:
    if ticker is None:
        return None
    try:
        fi = getattr(ticker, "fast_info", None)
        if fi:
            for key in ("previous_close", "regular_market_previous_close", "last_price"):
                val = _safe_float(fi.get(key))
                if val is not None and val > 0:
                    return val
    except Exception:
        pass
    try:
        info = getattr(ticker, "info", None) or {}
        for key in ("previousClose", "regularMarketPreviousClose", "regularMarketPrice"):
            val = _safe_float(info.get(key))
            if val is not None and val > 0:
                return val
    except Exception:
        pass
    return None


def _yf_last_price(ticker, hist) -> Optional[float]:
    try:
        if hist is not None and not hist.empty:
            close = hist["Close"].dropna()
            if not close.empty:
                return float(close.iloc[-1])
    except Exception:
        pass
    return _yf_previous_close(ticker)


def _yf_pct_change(hist, previous_close: Optional[float]) -> Optional[float]:
    try:
        last_close = None
        if hist is not None and not hist.empty:
            close = hist["Close"].dropna()
            if not close.empty:
                last_close = float(close.iloc[-1])
        if last_close is None:
            return None

        base = previous_close if previous_close and previous_close > 0 else None
        if base is None and hist is not None and not hist.empty:
            open_ = hist["Open"].dropna()
            if not open_.empty:
                base = float(open_.iloc[0])
        if base is None or base <= 0:
            return None
        return ((last_close - base) / base) * 100.0
    except Exception:
        return None


def _yf_watch_quote(symbol: str) -> Dict[str, Any]:
    ticker = _yf_ticker(symbol)
    hist = _yf_history(symbol)
    prev = _yf_previous_close(ticker)
    price = _yf_last_price(ticker, hist)
    pct = _yf_pct_change(hist, prev)
    return {"price": price, "pct_change": pct, "as_of": _now_iso()}


def get_price(symbol: str) -> Optional[float]:
    if _massive_api_key():
        q = _massive_watch_quote(symbol)
        if q.get("price") is not None:
            return _safe_float(q.get("price"))
    q = _yf_watch_quote(symbol)
    return _safe_float(q.get("price"))


def get_intraday(symbol: str) -> List[Dict[str, Any]]:
    if _massive_api_key():
        rows = _massive_intraday_rows(symbol)
        if rows:
            return rows

    hist = _yf_history(symbol)
    if hist is None:
        return []
    out: List[Dict[str, Any]] = []
    try:
        if hist.empty:
            return out
        for idx, row in hist.iterrows():
            ts = getattr(idx, "to_pydatetime", lambda: idx)()
            ts_iso = ts.isoformat(timespec="seconds") if hasattr(ts, "isoformat") else str(ts)
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


def get_watchlist(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    snapshot: Dict[str, Dict[str, Any]] = {}
    use_massive = bool(_massive_api_key())
    for raw in symbols:
        symbol = str(raw or "").strip().upper()
        if not symbol:
            continue
        quote = _massive_watch_quote(symbol) if use_massive else _yf_watch_quote(symbol)
        # If primary provider fails for this symbol, fallback per-symbol.
        if quote.get("price") is None and use_massive:
            quote = _yf_watch_quote(symbol)
        snapshot[symbol] = quote
    return snapshot
