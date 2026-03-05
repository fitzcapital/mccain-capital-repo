"""Yahoo Finance market data adapter used by live dashboard pulse."""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any, Dict, List, Optional


SYMBOL_ALIASES = {
    "SPX": "^GSPC",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _resolve_symbol(symbol: str) -> str:
    sym = str(symbol or "").strip().upper()
    return SYMBOL_ALIASES.get(sym, sym)


def _load_yfinance():
    try:
        import yfinance as yf  # type: ignore

        return yf
    except Exception:
        return None


def _ticker(symbol: str):
    yf = _load_yfinance()
    if yf is None:
        return None
    try:
        return yf.Ticker(_resolve_symbol(symbol))
    except Exception:
        return None


def _history(symbol: str):
    ticker = _ticker(symbol)
    if ticker is None:
        return None
    try:
        # prepost=True captures after-hours and pre-market for supported tickers.
        return ticker.history(period="1d", interval="1m", prepost=True)
    except Exception:
        return None


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _previous_close_from_ticker(ticker) -> Optional[float]:
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


def _last_price_from_ticker_or_history(ticker, hist) -> Optional[float]:
    try:
        if hist is not None and not hist.empty:
            close = hist["Close"].dropna()
            if not close.empty:
                return float(close.iloc[-1])
    except Exception:
        pass
    return _previous_close_from_ticker(ticker)


def get_price(symbol: str) -> Optional[float]:
    """Return latest trade/close for symbol, with close fallback when market is shut."""
    ticker = _ticker(symbol)
    if ticker is None:
        return None
    hist = _history(symbol)
    return _last_price_from_ticker_or_history(ticker, hist)


def get_intraday(symbol: str) -> List[Dict[str, Any]]:
    """Return today's 1m candles as normalized list (includes pre/post when available)."""
    hist = _history(symbol)
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


def _pct_change_from_history_or_close(hist, previous_close: Optional[float]) -> Optional[float]:
    try:
        last_close = None
        if hist is not None and not hist.empty:
            close = hist["Close"].dropna()
            if not close.empty:
                last_close = float(close.iloc[-1])
        if last_close is None:
            return None

        base = None
        if previous_close is not None and previous_close > 0.0:
            base = previous_close
        elif hist is not None and not hist.empty:
            open_ = hist["Open"].dropna()
            if not open_.empty:
                base = float(open_.iloc[0])
            if base is None or base <= 0.0:
                close = hist["Close"].dropna()
                if len(close) >= 2:
                    base = float(close.iloc[0])
        if base is None or base <= 0.0:
            return None
        return ((last_close - base) / base) * 100.0
    except Exception:
        return None


def get_watchlist(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Return latest price and percent change for symbols.

    Uses after-hours/pre-market when available, otherwise falls back to previous close.
    """
    snapshot: Dict[str, Dict[str, Any]] = {}
    as_of = _now_iso()
    for raw in symbols:
        symbol = str(raw or "").strip().upper()
        if not symbol:
            continue

        ticker = _ticker(symbol)
        hist = _history(symbol)
        previous_close = _previous_close_from_ticker(ticker)
        price = _last_price_from_ticker_or_history(ticker, hist)
        pct = _pct_change_from_history_or_close(hist, previous_close)

        snapshot[symbol] = {
            "price": price,
            "pct_change": pct,
            "as_of": as_of,
        }
    return snapshot
