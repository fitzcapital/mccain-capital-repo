"""Market Pulse source health helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from mccain_capital import runtime as app_runtime


def _status_label(status: str) -> str:
    if status == "healthy":
        return "Healthy"
    if status == "degraded":
        return "Degraded"
    return "Offline"


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=app_runtime.TZ)
    return dt.astimezone(app_runtime.TZ)


def build_market_source_health(
    snapshot: Dict[str, Any],
    news_snapshot: Dict[str, Any],
    gamma_snapshot: Dict[str, Any],
    now_et: datetime,
) -> Dict[str, Any]:
    integrity = dict(snapshot.get("integrity") or {})
    missing = int(integrity.get("missing_count") or 0)
    delayed = int(integrity.get("delayed_count") or 0)
    live = int(integrity.get("live_count") or 0)
    source_note = str(snapshot.get("source_note") or "").lower()
    market_status = "healthy"
    if live <= 0 or missing > 0:
        market_status = "degraded"
    if "fallback" in source_note or "cached" in source_note:
        market_status = "degraded"

    news_note = str(news_snapshot.get("source_note") or "").lower()
    news_available = bool(news_snapshot.get("available"))
    news_status = "healthy" if news_available else "offline"
    if news_available and ("cached" in news_note or "fallback" in news_note):
        news_status = "degraded"

    gamma_asof = _parse_iso(gamma_snapshot.get("asof"))
    gamma_age = int((now_et - gamma_asof).total_seconds()) if gamma_asof else 0
    gamma_status = "healthy" if gamma_asof else "offline"
    if gamma_asof and gamma_age > 1800:
        gamma_status = "degraded"

    rows = [
        {
            "name": "Quotes",
            "status": market_status,
            "status_label": _status_label(market_status),
            "detail": f"{live} live · {delayed} delayed · {missing} missing",
        },
        {
            "name": "News",
            "status": news_status,
            "status_label": _status_label(news_status),
            "detail": (
                "Live feed"
                if news_status == "healthy"
                else "Fallback/cached feed"
                if news_status == "degraded"
                else "No feed"
            ),
        },
        {
            "name": "Gamma",
            "status": gamma_status,
            "status_label": _status_label(gamma_status),
            "detail": (
                f"Updated {gamma_asof.strftime('%I:%M %p ET')}" if gamma_asof else "No gamma snapshot"
            ),
        },
    ]

    overall = "healthy"
    if any(r["status"] == "offline" for r in rows):
        overall = "offline"
    elif any(r["status"] == "degraded" for r in rows):
        overall = "degraded"

    return {
        "rows": rows,
        "overall_status": overall,
        "overall_label": _status_label(overall),
        "is_degraded": overall != "healthy",
        "banner": (
            "Degraded data mode active. Use caution before execution."
            if overall != "healthy"
            else ""
        ),
    }

