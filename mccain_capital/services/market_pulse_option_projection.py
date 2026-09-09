"""Current Tradier option premium anchors for hypothetical Setup Analytics projections."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from mccain_capital.services.options_panel_service import get_options_snapshot

FALLBACK_COST = 750.0
FALLBACK_DELTA = 0.40
MAX_SNAPSHOT_AGE_SECONDS = 45


def _float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


def _timestamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _contract_parts(label: Any) -> tuple[str, str, str, float | None]:
    parts = str(label or "").strip().upper().split()
    if len(parts) < 3:
        return "", "", "", None
    cp = parts[-1][-1:] if parts[-1][-1:] in {"C", "P"} else ""
    return parts[0], parts[1], cp, _float(parts[-1][:-1]) if cp else None


def fallback_anchor(direction: str, reason: str) -> dict[str, Any]:
    return {
        "pricing_mode": "fallback_estimate",
        "direction": direction,
        "source": "Documented planning assumption",
        "contract_label": "",
        "mid": None,
        "contract_cost": FALLBACK_COST,
        "absolute_delta": FALLBACK_DELTA,
        "spread": None,
        "dte": None,
        "strike": None,
        "spot": None,
        "spot_distance": None,
        "as_of": "",
        "fallback_reason": reason,
    }


def premium_anchor(
    direction: str,
    *,
    snapshot: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Select a current illustrative call/put without making a provider request."""

    normalized = str(direction or "").strip().lower()
    if normalized not in {"bullish", "bearish"}:
        return fallback_anchor(normalized or "mixed", "mixed_direction_selection")
    data = dict(snapshot or get_options_snapshot() or {})
    as_of = _timestamp(data.get("asof"))
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if as_of is None:
        return fallback_anchor(normalized, "quote_timestamp_unavailable")
    if (current - as_of).total_seconds() > MAX_SNAPSHOT_AGE_SECONDS:
        return fallback_anchor(normalized, "quote_stale")
    spx = (data.get("symbols") or {}).get("SPX") or {}
    contracts = list(spx.get("contracts") or [])
    spot = _float((spx.get("underlying") or {}).get("price"))
    if spot is None or spot <= 0:
        return fallback_anchor(normalized, "underlying_spot_unavailable")
    desired_cp = "C" if normalized == "bullish" else "P"
    eligible: list[dict[str, Any]] = []
    for raw in contracts:
        if not isinstance(raw, Mapping):
            continue
        item = dict(raw)
        root, expiry, cp, label_strike = _contract_parts(item.get("label"))
        strike = _float(item.get("strike")) or label_strike
        mid, delta, spread = (
            _float(item.get("mid")),
            _float(item.get("delta")),
            _float(item.get("spread")),
        )
        if cp != desired_cp or not root or not expiry or strike is None:
            continue
        if mid is None or mid <= 0 or delta is None or not 0.05 <= abs(delta) <= 0.95:
            continue
        if spread is None or spread < 0 or spread > mid:
            continue
        try:
            expiry_date = datetime.fromisoformat(expiry).date()
        except ValueError:
            continue
        dte = (expiry_date - current.date()).days
        if dte < 0:
            continue
        item.update(
            {
                "root": root,
                "expiry": expiry,
                "cp": cp,
                "mid": mid,
                "delta": delta,
                "spread": spread,
                "dte": dte,
                "strike": strike,
                "spot": spot,
                "spot_distance": abs(strike - spot),
            }
        )
        eligible.append(item)
    if not eligible:
        return fallback_anchor(normalized, "eligible_tradier_contract_unavailable")
    eligible.sort(
        key=lambda item: (
            item["spot_distance"],
            0 if item["root"] == "SPXW" else 1,
            item["dte"],
            0 if str(item.get("liq") or "") in {"Tight", "OK"} else 1,
            abs(item["mid"] - (FALLBACK_COST / 100)),
            abs(abs(item["delta"]) - FALLBACK_DELTA),
            -int(_float(item.get("vol")) or 0),
            -int(_float(item.get("oi")) or 0),
            item["spread"],
        )
    )
    chosen = eligible[0]
    return {
        "pricing_mode": "tradier_current_quote",
        "direction": normalized,
        "source": "Tradier current options snapshot",
        "contract_label": str(chosen.get("label") or ""),
        "mid": round(chosen["mid"], 2),
        "contract_cost": round(chosen["mid"] * 100, 2),
        "absolute_delta": round(abs(chosen["delta"]), 4),
        "spread": round(chosen["spread"], 2),
        "dte": chosen["dte"],
        "strike": round(chosen["strike"], 2),
        "spot": round(chosen["spot"], 2),
        "spot_distance": round(chosen["spot_distance"], 2),
        "as_of": as_of.isoformat(),
        "fallback_reason": "",
    }


def premium_anchors(
    *, snapshot: Mapping[str, Any] | None = None, now: datetime | None = None
) -> dict[str, Any]:
    data = snapshot if snapshot is not None else get_options_snapshot(recover_if_empty=True)
    return {
        direction: premium_anchor(direction, snapshot=data, now=now)
        for direction in ("bullish", "bearish")
    }
