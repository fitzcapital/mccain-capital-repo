"""Canonical Market Pulse generation and component freshness metadata."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Mapping


ACTIVE_THRESHOLDS = {
    "spot": 15,
    # Five-minute candles are only actionable after close. Allow one full
    # completed-candle cycle plus a one-minute provider delivery grace period.
    "bars": 360,
    # The live options-chain worker refreshes on a five-minute cadence.
    # One additional minute absorbs provider completion and promotion delay.
    "gamma": 360,
    "options": 30,
    "strategy": 120,
}
IDLE_THRESHOLDS = {
    "spot": 900,
    "bars": 900,
    "gamma": 1800,
    "options": 900,
    "strategy": 900,
}
REQUIRED_COMPONENTS = ("spot", "bars", "gamma")
MAX_FUTURE_SKEW_SECONDS = 30


def _parse_timestamp(value: Any, now: datetime) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=now.tzinfo)
    return parsed.astimezone(now.tzinfo)


def build_canonical_freshness(
    *,
    now: datetime,
    market_open: bool,
    timestamps: Mapping[str, Any],
    fingerprint: Mapping[str, Any],
    symbol: str = "SPX",
    session_id: str = "",
) -> dict[str, Any]:
    """Return deterministic generation metadata without masking component ages."""

    thresholds = ACTIVE_THRESHOLDS if market_open else IDLE_THRESHOLDS
    components: dict[str, dict[str, Any]] = {}
    for name in ("spot", "bars", "gamma", "options", "strategy"):
        parsed = _parse_timestamp(timestamps.get(name), now)
        signed_age = int((now - parsed).total_seconds()) if parsed else None
        future_dated = signed_age is not None and signed_age < -MAX_FUTURE_SKEW_SECONDS
        age = max(0, signed_age) if signed_age is not None else None
        threshold = int(thresholds[name])
        status = (
            "unavailable"
            if age is None
            else ("future" if future_dated else ("stale" if age > threshold else "current"))
        )
        components[name] = {
            "as_of": parsed.isoformat() if parsed else "",
            "age_seconds": age,
            "threshold_seconds": threshold,
            "status": status,
            "required": name in REQUIRED_COMPONENTS,
            "future_dated": future_dated,
        }

    required = [components[name] for name in REQUIRED_COMPONENTS]
    stale_required = [
        name for name in REQUIRED_COMPONENTS if components[name]["status"] != "current"
    ]
    ages = [row["age_seconds"] for row in required if row["age_seconds"] is not None]
    canonical = {
        "symbol": str(symbol or "SPX").upper(),
        "session_id": str(session_id or now.date().isoformat()),
        "fingerprint": fingerprint,
        "timestamps": {name: components[name]["as_of"] for name in components},
    }
    generation_id = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()[:20]
    return {
        "generation_id": generation_id,
        "generated_at": now.isoformat(),
        "symbol": canonical["symbol"],
        "session_id": canonical["session_id"],
        "components": components,
        "oldest_required_age_seconds": max(ages) if ages else None,
        "stale_required_components": stale_required,
        "execution_locked": bool(stale_required),
        "sync_interval_seconds": 15,
    }
