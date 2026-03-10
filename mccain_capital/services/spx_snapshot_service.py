"""Index live snapshot adapter shared by dashboard and market pulse."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from mccain_capital import runtime as app_runtime


def _num(v: Any) -> float | None:
    if isinstance(v, (int, float)):
        return float(v)
    text = str(v or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


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


def get_index_live_snapshot(symbol: str) -> Dict[str, Any]:
    from mccain_capital.services import market_data_service
    from mccain_capital.services import market_worker

    key = str(symbol or "").strip().upper()
    if not key:
        return {
            "price": None,
            "gap_pct": None,
            "gap_label": "—",
            "as_of": "Awaiting tick",
            "live": False,
        }

    quote: Dict[str, Any] = {}
    try:
        cached = market_worker.get_market_snapshot()
        quote = dict((cached.get("prices") or {}).get(key) or {})
    except Exception:
        quote = {}
    if quote.get("price") is None:
        try:
            quote = dict(
                (market_data_service.get_watchlist([key], allow_yf_fallback=True)).get(key) or {}
            )
        except Exception:
            quote = {}

    price = _num(quote.get("price"))
    gap_pct = _num(quote.get("pct_change"))
    as_of_raw = str(quote.get("as_of") or "")
    return {
        "price": price,
        "gap_pct": gap_pct,
        "gap_label": (f"{gap_pct:+.2f}%" if gap_pct is not None else "—"),
        "as_of": _format_iso_et_label(as_of_raw) or as_of_raw or "Awaiting tick",
        "live": bool(price is not None),
    }


def get_spx_live_snapshot() -> Dict[str, Any]:
    return get_index_live_snapshot("SPX")
