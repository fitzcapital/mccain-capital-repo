"""Point-in-time Gamma context shared by Replay and Setup Analytics."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
NAMED_GAMMA_REGIMES = ("positive", "negative", "transition", "unconfirmed")
VALID_GAMMA_REGIMES = {*NAMED_GAMMA_REGIMES, "unavailable"}


def _timestamp(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ET)
    return parsed.astimezone(ET)


def normalize_gamma_regime(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if "positive" in raw:
        return "positive"
    if "negative" in raw:
        return "negative"
    if raw in {"mixed", "mixed_gamma", "neutral", "transition", "transition_gamma"}:
        return "transition"
    if raw == "unconfirmed":
        return "unconfirmed"
    return "unavailable"


def signal_gamma_context(
    source: Mapping[str, Any] | None,
    signal_time: Any,
    *,
    default_source: str = "",
) -> dict[str, str]:
    """Return valid Gamma evidence only when it existed no later than the signal."""

    row = dict(source or {})
    signal_stamp = _timestamp(signal_time)
    gamma_stamp = _timestamp(row.get("gamma_as_of") or row.get("as_of") or row.get("timestamp"))
    regime = normalize_gamma_regime(row.get("gamma_regime") or row.get("regime"))
    if signal_stamp is None or gamma_stamp is None or gamma_stamp > signal_stamp:
        reason = (
            "future_observation"
            if gamma_stamp and signal_stamp and gamma_stamp > signal_stamp
            else "unavailable"
        )
        return {
            "gamma_regime": "unavailable",
            "gamma_as_of": "",
            "gamma_source": "",
            "gamma_status": reason,
        }
    if regime == "unavailable":
        return {
            "gamma_regime": "unavailable",
            "gamma_as_of": "",
            "gamma_source": "",
            "gamma_status": "invalid_regime",
        }
    return {
        "gamma_regime": regime,
        "gamma_as_of": gamma_stamp.isoformat(),
        "gamma_source": str(
            row.get("gamma_source")
            or row.get("source")
            or row.get("generation_id")
            or default_source
        ).strip(),
        "gamma_status": "captured",
    }
