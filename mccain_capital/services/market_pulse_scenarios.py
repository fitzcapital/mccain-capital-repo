"""Deterministic multi-level Market Pulse scenarios."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import math
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from mccain_capital.services.spx_strat_patterns import detect_latest_key_level_pattern


ET = ZoneInfo("America/New_York")


class ScenarioFamily(StrEnum):
    FAILED_HIGH = "failed_high"
    FAILED_LOW = "failed_low"
    BREAKDOWN = "breakdown"
    BREAKOUT = "breakout"
    LOCAL_FLIP_LOSS = "local_flip_loss"
    LOCAL_FLIP_RECLAIM = "local_flip_reclaim"


class ScenarioLane(StrEnum):
    ACTIVE_NOW = "active_now"
    ALTERNATIVE = "alternative"
    DORMANT = "dormant"


SCENARIO_FAMILY_LABELS = {
    ScenarioFamily.FAILED_HIGH: "Sweep and Reject High",
    ScenarioFamily.FAILED_LOW: "Sweep and Recover Low",
    ScenarioFamily.BREAKDOWN: "Acceptance Below",
    ScenarioFamily.BREAKOUT: "Reclaim Above",
    ScenarioFamily.LOCAL_FLIP_LOSS: "Failed Reclaim Below Local Flip",
    ScenarioFamily.LOCAL_FLIP_RECLAIM: "Reclaim Above Local Flip",
}


class ScenarioState(StrEnum):
    TRIGGERED = "triggered"
    CONFIRMED = "confirmed"  # Backward-compatible alias for older consumers.
    TRIGGER_ARMED = "trigger_armed"
    ARMED = "armed"
    WATCHING = "watching"
    LOCKED = "locked"


CONFLUENCE_WEIGHTS: dict[str, int] = {
    "location": 20,
    "ordered_structure": 20,
    "strat": 15,
    "trigger": 15,
    "target_space": 15,
    "gamma": 10,
    "higher_timeframe": 5,
}

LEVEL_LABELS = {
    "gamma_flip": "Main Flip",
    "local_flip": "Local Flip",
    "call_wall": "Call Wall",
    "put_wall": "Put Wall",
    "new_call_wall": "New Call Wall",
    "new_put_wall": "New Put Wall",
    "prior_day_high": "Prior-Day High",
    "prior_day_low": "Prior-Day Low",
    "current_day_high": "Current-Day High",
    "current_day_low": "Current-Day Low",
}


@dataclass(frozen=True)
class Level:
    key: str
    label: str
    value: float


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) and result > 0 else None


def normalize_levels(rows: Iterable[Mapping[str, Any]]) -> list[Level]:
    """Normalize levels and deduplicate equal prices with stable input priority."""
    result: list[Level] = []
    seen: set[float] = set()
    for row in rows:
        value = _number(row.get("value"))
        key = str(row.get("kind") or row.get("key") or "").strip().lower()
        if value is None or not key:
            continue
        price_key = round(value, 4)
        if price_key in seen:
            continue
        seen.add(price_key)
        result.append(Level(key, str(row.get("label") or LEVEL_LABELS.get(key) or key), value))
    return result


def _parse_timestamp(row: Mapping[str, Any]) -> datetime | None:
    raw = row.get("ts") or row.get("timestamp") or row.get("datetime") or row.get("time")
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw), tz=ET)
        except (OSError, ValueError):
            return None
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(ET)
    except (ValueError, TypeError):
        return None


def confluence_grade(score: Any) -> str:
    """Map one scenario score to its only letter-grade representation."""
    value = max(0.0, min(100.0, _number(score) or 0.0))
    for floor, grade in (
        (97, "A+"),
        (93, "A"),
        (85, "A-"),
        (82, "B+"),
        (76, "B"),
        (70, "B-"),
        (67, "C+"),
        (61, "C"),
        (55, "C-"),
        (52, "D+"),
        (46, "D"),
        (40, "D-"),
    ):
        if value >= floor:
            return grade
    return "F"


def _trigger_evidence(
    rows: Iterable[Mapping[str, Any]], pattern: Mapping[str, Any] | None, direction: str
) -> dict[str, Any]:
    """Require a later completed candle to break the completed pattern's trigger candle."""
    if not pattern:
        return {"armed": False, "triggered": False, "trigger_price": None}
    completed_at = str(pattern.get("completed_at") or "")
    pattern_times = {str(value) for value in pattern.get("bar_timestamps") or []}
    pattern_rows = [
        row
        for row in rows
        if str(row.get("ts") or row.get("timestamp") or "") in pattern_times
    ]
    if not pattern_rows:
        return {"armed": True, "triggered": False, "trigger_price": None}
    field = "high" if direction == "bullish" else "low"
    trigger_price = _number(pattern_rows[-1].get(field))
    if trigger_price is None:
        return {"armed": True, "triggered": False, "trigger_price": None}
    later = []
    pattern_stamp = _parse_timestamp({"ts": completed_at})
    for row in rows:
        stamp = _parse_timestamp(row)
        if pattern_stamp is not None and stamp is not None and stamp > pattern_stamp:
            later.append(row)
    triggered = any(
        (_number(row.get(field)) or trigger_price) > trigger_price
        if direction == "bullish"
        else (_number(row.get(field)) or trigger_price) < trigger_price
        for row in later
    )
    return {
        "armed": True,
        "triggered": triggered,
        "trigger_price": trigger_price,
        "triggered_at": next(
            (
                str(row.get("ts") or row.get("timestamp") or "")
                for row in later
                if ((_number(row.get(field)) or trigger_price) > trigger_price)
                if direction == "bullish"
            ),
            "",
        ) if direction == "bullish" else next(
            (
                str(row.get("ts") or row.get("timestamp") or "")
                for row in later
                if (_number(row.get(field)) or trigger_price) < trigger_price
            ),
            "",
        ),
    }


def _recent_bar_evidence(rows: Iterable[Mapping[str, Any]], level: Level) -> dict[str, bool]:
    valid: list[dict[str, float]] = []
    for row in rows:
        close = _number(row.get("close") or row.get("v") or row.get("c"))
        high = _number(row.get("high") or row.get("h"))
        low = _number(row.get("low") or row.get("l"))
        if None not in {close, high, low}:
            valid.append({"close": close, "high": high, "low": low})
    recent = valid[-3:]
    if len(recent) < 2:
        return {}
    closes = [row["close"] for row in recent]
    swept_above_index = next(
        (index for index, row in enumerate(valid) if row["high"] > level.value), None
    )
    swept_below_index = next(
        (index for index, row in enumerate(valid) if row["low"] < level.value), None
    )
    rejected_below = bool(
        swept_above_index is not None
        and any(row["close"] < level.value for row in valid[swept_above_index:])
    )
    recovered_above = bool(
        swept_below_index is not None
        and any(row["close"] > level.value for row in valid[swept_below_index:])
    )
    return {
        "close_above": closes[-1] > level.value,
        "close_below": closes[-1] < level.value,
        "held_above": all(value > level.value for value in closes[-2:]),
        "held_below": all(value < level.value for value in closes[-2:]),
        "retested_from_above": recent[-1]["low"] <= level.value and closes[-1] > level.value,
        "retested_from_below": recent[-1]["high"] >= level.value and closes[-1] < level.value,
        "swept_above": swept_above_index is not None,
        "swept_below": swept_below_index is not None,
        "rejected_below": rejected_below,
        "recovered_above": recovered_above,
    }


def _latest_completed_bar_time(rows: Iterable[Mapping[str, Any]]) -> str:
    timestamps = [stamp for row in rows if (stamp := _parse_timestamp(row)) is not None]
    latest = max(timestamps, default=None)
    return latest.isoformat() if latest is not None else ""


def _target(levels: list[Level], value: float, direction: str) -> Level | None:
    candidates = (
        [row for row in levels if row.value > value]
        if direction == "bullish"
        else [row for row in levels if row.value < value]
    )
    if not candidates:
        return None
    return min(candidates, key=lambda row: abs(row.value - value))


def _scenario_plan(
    family: ScenarioFamily, level: Level, target: Level | None, confirmed: bool
) -> dict[str, str]:
    bullish = family in {
        ScenarioFamily.FAILED_LOW,
        ScenarioFamily.BREAKOUT,
        ScenarioFamily.LOCAL_FLIP_RECLAIM,
    }
    reversal = family in {ScenarioFamily.FAILED_HIGH, ScenarioFamily.FAILED_LOW}
    direction = "bullish" if bullish else "bearish"
    level_text = f"{level.label} {level.value:,.0f}"
    if reversal:
        trigger = (
            f"Sweep below {level_text}, then complete a 5-minute close back above with bullish Strat confirmation"
            if bullish
            else f"Sweep above {level_text}, then complete a 5-minute close back below with bearish Strat confirmation"
        )
    else:
        trigger = (
            f"Complete a 5-minute close above {level_text}, then hold the retest"
            if bullish
            else f"Complete a 5-minute close below {level_text}, then fail the reclaim"
        )
    action = (
        ("Buy the confirmed reclaim" if bullish else "Sell the confirmed rejection")
        if confirmed
        else "No entry until the trigger confirms"
    )
    cancel = (
        f"Cancel on a completed 5-minute close back below {level_text}"
        if bullish
        else f"Cancel on a completed 5-minute close back above {level_text}"
    )
    return {
        "wait": f"Watch {level_text}",
        "trigger": trigger,
        "action": action,
        "target": (
            f"{target.label} {target.value:,.0f}"
            if confirmed and target
            else "Set after direction confirms"
        ),
        "cancel": cancel,
        "direction": direction,
    }


def rank_market_scenarios(
    *,
    spot: Any,
    levels: Iterable[Mapping[str, Any]],
    bars: Iterable[Mapping[str, Any]],
    strategy: Mapping[str, Any] | None = None,
    gamma_regime: str = "",
    locked: bool = False,
) -> dict[str, Any]:
    """Generate, score and rank independent structural scenarios."""
    spot_value = _number(spot)
    normalized = normalize_levels(levels)
    bars = list(bars)
    if spot_value is None or not normalized:
        return {"primary": None, "active": [], "alternatives": [], "dormant": [], "candidates": []}
    wall_span = max((row.value for row in normalized), default=spot_value) - min(
        (row.value for row in normalized), default=spot_value
    )
    near_band = min(8.0, max(2.5, wall_span * 0.025))
    candidates: list[dict[str, Any]] = []
    for level in normalized:
        families = (
            [ScenarioFamily.FAILED_HIGH, ScenarioFamily.BREAKDOWN]
            if level.value >= spot_value
            else [ScenarioFamily.FAILED_LOW, ScenarioFamily.BREAKOUT]
        )
        if level.key == "local_flip":
            families.append(
                ScenarioFamily.LOCAL_FLIP_LOSS
                if level.value >= spot_value
                else ScenarioFamily.LOCAL_FLIP_RECLAIM
            )
        evidence = _recent_bar_evidence(bars, level)
        for family in families:
            bullish = family in {
                ScenarioFamily.FAILED_LOW,
                ScenarioFamily.BREAKOUT,
                ScenarioFamily.LOCAL_FLIP_RECLAIM,
            }
            continuation = family in {
                ScenarioFamily.BREAKOUT,
                ScenarioFamily.BREAKDOWN,
                ScenarioFamily.LOCAL_FLIP_LOSS,
                ScenarioFamily.LOCAL_FLIP_RECLAIM,
            }
            direction = "bullish" if bullish else "bearish"
            pattern = detect_latest_key_level_pattern(
                bars,
                level_key=level.key,
                level_label=level.label,
                level_value=level.value,
                direction=direction,
            )
            trigger = _trigger_evidence(bars, pattern, direction)
            path_confirmed = False
            if continuation:
                path_confirmed = bool(
                    evidence.get("held_above") and evidence.get("retested_from_above")
                    if bullish
                    else evidence.get("held_below") and evidence.get("retested_from_below")
                )
            else:
                path_confirmed = bool(
                    evidence.get("swept_below") and evidence.get("recovered_above")
                    if bullish
                    else evidence.get("swept_above") and evidence.get("rejected_below")
                )
            eligible = bool(not locked and path_confirmed and pattern)
            triggered = bool(eligible and trigger["triggered"])
            distance = abs(level.value - spot_value)
            lane = (
                ScenarioLane.ACTIVE_NOW
                if triggered
                else ScenarioLane.ALTERNATIVE
                if eligible or distance <= near_band
                else ScenarioLane.DORMANT
            )
            state = (
                ScenarioState.LOCKED
                if locked
                else (
                    ScenarioState.TRIGGERED
                    if triggered
                    else (
                        ScenarioState.TRIGGER_ARMED
                        if eligible
                        else ScenarioState.ARMED
                        if lane == ScenarioLane.ALTERNATIVE
                        else ScenarioState.WATCHING
                    )
                )
            )
            target = _target(normalized, level.value, "bullish" if bullish else "bearish")
            cluster = any(
                other.key != level.key and abs(other.value - level.value) <= 3
                for other in normalized
            )
            gamma_key = gamma_regime.lower()
            gamma_aligned = "negative" in gamma_key if continuation else "positive" in gamma_key
            components = {
                "location": distance <= near_band,
                "ordered_structure": path_confirmed,
                "strat": pattern is not None,
                "trigger": triggered,
                "target_space": target is not None and abs(target.value - level.value) >= 5,
                "gamma": gamma_aligned,
                "higher_timeframe": bool((strategy or {}).get("higher_timeframe_aligned")),
            }
            breakdown = [
                {
                    "key": key,
                    "earned": weight if components[key] else 0,
                    "possible": weight,
                    "status": "confirmed" if components[key] else "missing",
                }
                for key, weight in CONFLUENCE_WEIGHTS.items()
            ]
            score = sum(row["earned"] for row in breakdown)
            plan = _scenario_plan(family, level, target, triggered)
            if pattern:
                plan["trigger"] = (
                    f"Triggered above {trigger['trigger_price']:,.2f} after {pattern['code']}"
                    if triggered and bullish
                    else f"Triggered below {trigger['trigger_price']:,.2f} after {pattern['code']}"
                    if triggered
                    else f"Break the {pattern['code']} trigger candle at {trigger['trigger_price']:,.2f}"
                    if trigger.get("trigger_price") is not None
                    else f"Complete {pattern['code']} at {level.label} {level.value:,.0f}"
                )
            candidates.append(
                {
                    "id": f"{level.key}:{family.value}",
                    "family": family.value,
                    "family_label": SCENARIO_FAMILY_LABELS[family],
                    "lane": lane.value,
                    "state": state.value,
                    "level": {"key": level.key, "label": level.label, "value": level.value},
                    "target_level": (
                        {"key": target.key, "label": target.label, "value": target.value}
                        if target
                        else None
                    ),
                    "distance": distance,
                    "direction": plan.pop("direction"),
                    "score": score,
                    "grade": confluence_grade(score) if triggered else "",
                    "quality_score": score if triggered else None,
                    "eligibility": {
                        "eligible": eligible,
                        "maturity": state.value,
                        "location": distance <= near_band,
                        "ordered_structure": path_confirmed,
                        "pattern_confirmed": pattern is not None,
                        "trigger_armed": bool(trigger["armed"] and eligible),
                        "triggered": triggered,
                    },
                    "score_max": 100,
                    "score_components": breakdown,
                    "plan": plan,
                    "evidence": evidence,
                    "strat_pattern": pattern,
                    "trigger_evidence": trigger,
                    "evidence_as_of": _latest_completed_bar_time(bars),
                }
            )
    lane_order = {
        ScenarioLane.ACTIVE_NOW.value: 0,
        ScenarioLane.ALTERNATIVE.value: 1,
        ScenarioLane.DORMANT.value: 2,
    }
    specialized_family_order = {
        ScenarioFamily.LOCAL_FLIP_LOSS.value: 0,
        ScenarioFamily.LOCAL_FLIP_RECLAIM.value: 0,
    }
    candidates.sort(
        key=lambda row: (
            lane_order[row["lane"]],
            -row["score"],
            row["distance"],
            specialized_family_order.get(row["family"], 1),
            row["id"],
        )
    )
    return {
        "primary": candidates[0] if candidates else None,
        "active": [row for row in candidates if row["lane"] == ScenarioLane.ACTIVE_NOW.value],
        "alternatives": [
            row for row in candidates if row["lane"] == ScenarioLane.ALTERNATIVE.value
        ],
        "dormant": [row for row in candidates if row["lane"] == ScenarioLane.DORMANT.value],
        "candidates": candidates,
        "score_weights": CONFLUENCE_WEIGHTS,
    }
