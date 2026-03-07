"""Market data adapter for dashboard pulse.

Provider priority:
1) Tradier
2) Massive/Polygon
3) yfinance (optional fallback)
"""

from __future__ import annotations

from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import json
import os
from typing import Any, Dict, List, Optional
import urllib.error
import urllib.parse
import urllib.request

from mccain_capital import runtime as app_runtime


YF_SYMBOL_ALIASES = {
    "SPX": "^GSPC",
}

MASSIVE_SYMBOL_ALIASES = {
    "SPX": "I:SPX",
    "^GSPC": "I:SPX",
    "^VIX": "I:VIX",
    "VIX": "I:VIX",
}
TRADIER_SYMBOL_ALIASES = {
    "^GSPC": "SPX",
    "^VIX": "VIX",
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


def _tradier_api_key() -> str:
    return (
        (os.environ.get("TRADIER_API_KEY") or "").strip()
        or str(app_runtime.get_setting_value("tradier_api_key", "") or "").strip()
    )


def _tradier_symbol(symbol: str) -> str:
    sym = str(symbol or "").strip().upper()
    return TRADIER_SYMBOL_ALIASES.get(sym, sym)


def _tradier_json(path: str, params: Dict[str, Any]) -> Dict[str, Any] | None:
    key = _tradier_api_key()
    if not key:
        return None
    base = (os.environ.get("TRADIER_BASE_URL") or "https://api.tradier.com").strip().rstrip("/")
    url = base + path + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
            "User-Agent": "mccain-capital/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            parsed = json.loads(body)
            return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _massive_symbol(symbol: str) -> str:
    sym = str(symbol or "").strip().upper()
    return MASSIVE_SYMBOL_ALIASES.get(sym, sym)


def _tradier_quote_map(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not _tradier_api_key():
        return out
    clean = [str(s or "").strip().upper() for s in symbols if str(s or "").strip()]
    if not clean:
        return out
    mapped = [_tradier_symbol(s) for s in clean]
    payload = _tradier_json("/v1/markets/quotes", {"symbols": ",".join(mapped), "greeks": "false"})
    quotes = ((payload or {}).get("quotes") or {}).get("quote")
    rows: List[Dict[str, Any]]
    if isinstance(quotes, list):
        rows = [q for q in quotes if isinstance(q, dict)]
    elif isinstance(quotes, dict):
        rows = [quotes]
    else:
        rows = []

    reverse = {_tradier_symbol(s): s for s in clean}
    now_iso = _now_iso()
    for row in rows:
        tsym = str(row.get("symbol") or "").strip().upper()
        original = reverse.get(tsym, tsym)
        last = _safe_float(row.get("last"))
        close = _safe_float(row.get("close"))
        price = last if last is not None else close
        change_pct = _safe_float(row.get("change_percentage"))
        if change_pct is None:
            change = _safe_float(row.get("change"))
            if change is not None and price is not None and (price - change) != 0:
                try:
                    change_pct = (change / (price - change)) * 100.0
                except Exception:
                    change_pct = None
        reason = "tradier_live" if last is not None else "tradier_close"
        if price is None:
            reason = "tradier_no_data"
        out[original] = {
            "price": price,
            "pct_change": change_pct,
            "as_of": now_iso,
            "provider": "tradier",
            "reason": reason,
        }
    return out


def _tradier_intraday_rows(symbol: str) -> List[Dict[str, Any]]:
    key = _tradier_api_key()
    if not key:
        return []
    now_et = app_runtime.now_et()
    start_et = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    if now_et < start_et:
        start_et = (start_et - timedelta(days=1)).replace(hour=9, minute=30)
    params = {
        "symbol": _tradier_symbol(symbol),
        "interval": "1min",
        "start": start_et.strftime("%Y-%m-%d %H:%M"),
        "end": now_et.strftime("%Y-%m-%d %H:%M"),
        "session_filter": "all",
    }
    payload = _tradier_json("/v1/markets/timesales", params) or {}
    series = ((payload.get("series") or {}).get("data")) if isinstance(payload, dict) else None
    rows: List[Dict[str, Any]]
    if isinstance(series, list):
        rows = [r for r in series if isinstance(r, dict)]
    elif isinstance(series, dict):
        rows = [series]
    else:
        rows = []
    out: List[Dict[str, Any]] = []
    for r in rows:
        close = _safe_float(r.get("close"))
        open_ = _safe_float(r.get("open"))
        high = _safe_float(r.get("high"))
        low = _safe_float(r.get("low"))
        vol = _safe_float(r.get("volume"))
        ts_raw = str(r.get("time") or r.get("timestamp") or "").strip()
        ts_iso = ts_raw
        if ts_raw:
            try:
                # Tradier returns ET-local timestamps.
                dt = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M")
                dt = dt.replace(tzinfo=app_runtime.TZ)
                ts_iso = dt.astimezone(timezone.utc).isoformat(timespec="seconds")
            except Exception:
                ts_iso = ts_raw
        out.append(
            {
                "ts": ts_iso,
                "open": float(open_ or 0.0),
                "high": float(high or 0.0),
                "low": float(low or 0.0),
                "close": float(close or 0.0),
                "volume": float(vol or 0.0),
            }
        )
    return out


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


def _massive_error_code(path: str, params: Dict[str, Any]) -> str:
    key = _massive_api_key()
    if not key:
        return "massive_key_missing"
    q = dict(params)
    q["apiKey"] = key
    url = "https://api.polygon.io" + path + "?" + urllib.parse.urlencode(q)
    req = urllib.request.Request(url, headers={"User-Agent": "mccain-capital/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=8):
            return ""
    except urllib.error.HTTPError as exc:
        return f"massive_http_{int(exc.code)}"
    except urllib.error.URLError:
        return "massive_network_error"
    except TimeoutError:
        return "massive_timeout"
    except Exception:
        return "massive_error"


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
    if not _massive_api_key():
        return {
            "price": None,
            "pct_change": None,
            "as_of": _now_iso(),
            "provider": "massive",
            "reason": "massive_key_missing",
        }
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

    reason = "massive_intraday"
    if price is None and prev_close is None:
        ticker = urllib.parse.quote(_massive_symbol(symbol), safe="")
        reason = _massive_error_code(f"/v2/aggs/ticker/{ticker}/prev", {"adjusted": "true"})
        if not reason:
            reason = "massive_no_data"
    elif rows and price is not None:
        reason = "massive_intraday"
    elif price is not None and prev_close is not None:
        reason = "massive_prev_close_only"
    return {
        "price": price,
        "pct_change": pct,
        "as_of": _now_iso(),
        "provider": "massive",
        "reason": reason,
    }


def _massive_bulk_stock_quotes(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    clean = [str(s or "").strip().upper() for s in symbols if str(s or "").strip()]
    if not clean:
        return out
    payload = _massive_json(
        "/v2/snapshot/locale/us/markets/stocks/tickers",
        {"tickers": ",".join(clean)},
    )
    rows = payload.get("tickers") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return out
    now_iso = _now_iso()
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        day = row.get("day") if isinstance(row.get("day"), dict) else {}
        prev_day = row.get("prevDay") if isinstance(row.get("prevDay"), dict) else {}
        last_trade = row.get("lastTrade") if isinstance(row.get("lastTrade"), dict) else {}
        price = _safe_float(last_trade.get("p"))
        if price is None:
            price = _safe_float(day.get("c"))
        prev_close = _safe_float(prev_day.get("c"))
        pct = _safe_float(row.get("todaysChangePerc"))
        if pct is None and price is not None and prev_close is not None and prev_close > 0:
            pct = ((float(price) - float(prev_close)) / float(prev_close)) * 100.0
        reason = "massive_snapshot"
        if price is None and prev_close is None:
            reason = "massive_no_data"
        elif price is None and prev_close is not None:
            reason = "massive_prev_close_only"
        out[ticker] = {
            "price": price if price is not None else prev_close,
            "pct_change": pct,
            "as_of": now_iso,
            "provider": "massive",
            "reason": reason,
        }
    return out


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
    return {
        "price": price,
        "pct_change": pct,
        "as_of": _now_iso(),
        "provider": "yfinance",
        "reason": "yfinance_fallback",
    }


def get_price(symbol: str) -> Optional[float]:
    tq = _tradier_quote_map([symbol]).get(str(symbol or "").strip().upper(), {})
    if tq.get("price") is not None:
        return _safe_float(tq.get("price"))
    if _massive_api_key():
        q = _massive_watch_quote(symbol)
        if q.get("price") is not None:
            return _safe_float(q.get("price"))
    q = _yf_watch_quote(symbol)
    return _safe_float(q.get("price"))


def get_intraday(symbol: str) -> List[Dict[str, Any]]:
    tradier_rows = _tradier_intraday_rows(symbol)
    if len(tradier_rows) >= 20:
        return tradier_rows

    if _massive_api_key():
        rows = _massive_intraday_rows(symbol)
        if len(rows) >= 20:
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
    # If provider intraday is sparse/unavailable, yfinance offers a fuller curve for charting.
    if out:
        return out
    if tradier_rows:
        return tradier_rows
    return []


def get_watchlist(symbols: List[str], *, allow_yf_fallback: bool = True) -> Dict[str, Dict[str, Any]]:
    snapshot: Dict[str, Dict[str, Any]] = {}
    tradier_map = _tradier_quote_map(symbols)
    use_massive = bool(_massive_api_key())
    if use_massive and not allow_yf_fallback:
        return get_watchlist_massive(symbols)
    for raw in symbols:
        symbol = str(raw or "").strip().upper()
        if not symbol:
            continue
        quote = dict(tradier_map.get(symbol) or {})
        if not quote:
            quote = (
                _massive_watch_quote(symbol)
                if use_massive
                else {
                    "price": None,
                    "pct_change": None,
                    "as_of": _now_iso(),
                    "provider": "massive",
                    "reason": "massive_key_missing",
                }
            )
        # If primary provider fails for this symbol, fallback per-symbol.
        if quote.get("price") is None and use_massive and allow_yf_fallback:
            quote = _yf_watch_quote(symbol)
            quote["provider"] = "yfinance"
        snapshot[symbol] = quote
    return snapshot


def get_watchlist_massive(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    snapshot: Dict[str, Dict[str, Any]] = {}
    tradier_map = _tradier_quote_map(symbols)
    stock_symbols: List[str] = []
    index_symbols: List[str] = []
    for raw in symbols:
        symbol = str(raw or "").strip().upper()
        if not symbol:
            continue
        if symbol in tradier_map and tradier_map[symbol].get("price") is not None:
            snapshot[symbol] = tradier_map[symbol]
            continue
        mapped = _massive_symbol(symbol)
        if ":" in mapped:
            index_symbols.append(symbol)
        else:
            stock_symbols.append(symbol)

    if stock_symbols:
        bulk = _massive_bulk_stock_quotes(stock_symbols)
        for symbol in stock_symbols:
            if symbol in bulk:
                snapshot[symbol] = bulk[symbol]
            else:
                snapshot[symbol] = {
                    "price": None,
                    "pct_change": None,
                    "as_of": _now_iso(),
                    "provider": "massive",
                    "reason": "massive_no_data",
                }

    for symbol in index_symbols:
        snapshot[symbol] = _massive_watch_quote(symbol)
    return snapshot
