"""Deterministic failed-liquidity-sweep strategy evaluation for the SPX Playbook."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping, Optional


class LevelKind(StrEnum):
    CALL_WALL = "call_wall"
    PUT_WALL = "put_wall"
    GAMMA_FLIP = "gamma_flip"
    NEW_CALL_WALL = "new_call_wall"
    NEW_PUT_WALL = "new_put_wall"
    PRIOR_DAY_HIGH = "prior_day_high"
    PRIOR_DAY_LOW = "prior_day_low"
    CURRENT_DAY_HIGH = "current_day_high"
    CURRENT_DAY_LOW = "current_day_low"


class InteractionDirection(StrEnum):
    FROM_ABOVE = "from_above"
    FROM_BELOW = "from_below"


class GammaRegime(StrEnum):
    POSITIVE = "positive"
    NEGATIVE = "negative"
    TRANSITION = "transition"
    UNAVAILABLE = "unavailable"


class StrategyState(StrEnum):
    WAITING_FOR_LOCATION = "WAITING_FOR_LOCATION"
    LEVEL_BEING_TESTED = "LEVEL_BEING_TESTED"
    SWEEP_DETECTED = "SWEEP_DETECTED"
    WAITING_FOR_CLOSE_BACK_INSIDE = "WAITING_FOR_CLOSE_BACK_INSIDE"
    WAITING_FOR_5M_2_2 = "WAITING_FOR_5M_2_2"
    WAITING_FOR_TRIGGER_BREAK = "WAITING_FOR_TRIGGER_BREAK"
    REVERSAL_READY = "REVERSAL_READY"
    ACCEPTANCE_CONFIRMED = "ACCEPTANCE_CONFIRMED"
    CONTINUATION_ACTIVE = "CONTINUATION_ACTIVE"
    SETUP_INVALIDATED = "SETUP_INVALIDATED"


class EvidenceStatus(StrEnum):
    PENDING = "Pending"
    CONFIRMED = "Confirmed"
    FAILED = "Failed"
    UNAVAILABLE = "Unavailable"


class StrategyPath(StrEnum):
    PENDING = "pending"
    REVERSAL = "reversal"
    CONTINUATION = "continuation"
    INVALIDATED = "invalidated"


class TradeDirection(StrEnum):
    BULLISH = "bullish"
    BEARISH = "bearish"


LEVEL_LABELS = {
    LevelKind.CALL_WALL: "Call Wall",
    LevelKind.PUT_WALL: "Put Wall",
    LevelKind.GAMMA_FLIP: "Gamma Flip",
    LevelKind.NEW_CALL_WALL: "New Call Wall",
    LevelKind.NEW_PUT_WALL: "New Put Wall",
    LevelKind.PRIOR_DAY_HIGH: "Prior-Day High",
    LevelKind.PRIOR_DAY_LOW: "Prior-Day Low",
    LevelKind.CURRENT_DAY_HIGH: "Current-Day High",
    LevelKind.CURRENT_DAY_LOW: "Current-Day Low",
}


@dataclass(frozen=True)
class Evidence:
    confirmed: Optional[bool] = None
    timestamp: Optional[str] = None


@dataclass(frozen=True)
class StrategyLevel:
    kind: LevelKind
    value: float


@dataclass(frozen=True)
class SweepStrategyInput:
    spot: Optional[float]
    active_level: Optional[StrategyLevel]
    interaction: Optional[InteractionDirection]
    gamma_regime: GammaRegime = GammaRegime.UNAVAILABLE
    levels: tuple[StrategyLevel, ...] = ()
    location: Evidence = field(default_factory=Evidence)
    sweep: Evidence = field(default_factory=Evidence)
    close_back_inside_5m: Evidence = field(default_factory=Evidence)
    reversal_2_2_5m: Evidence = field(default_factory=Evidence)
    trigger_break: Evidence = field(default_factory=Evidence)
    acceptance: Evidence = field(default_factory=Evidence)
    continuation_retest: Evidence = field(default_factory=Evidence)
    runner_2_2_15m: Evidence = field(default_factory=Evidence)


def _finite_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) and number > 0 else None


def _enum(enum_type: type[StrEnum], value: Any, default: StrEnum | None = None) -> Any:
    try:
        return enum_type(str(value or "").strip().lower())
    except ValueError:
        return default


def _evidence(value: Any) -> Evidence:
    if isinstance(value, Evidence):
        return value
    if isinstance(value, bool):
        return Evidence(value)
    if not isinstance(value, Mapping):
        return Evidence()
    confirmed = value.get("confirmed")
    timestamp = value.get("timestamp")
    return Evidence(
        confirmed=confirmed if isinstance(confirmed, bool) else None,
        timestamp=(
            str(timestamp).strip() if isinstance(timestamp, str) and timestamp.strip() else None
        ),
    )


def normalize_strategy_input(raw: Mapping[str, Any] | None) -> SweepStrategyInput:
    """Return a safe typed input; malformed members become unavailable."""

    source = raw if isinstance(raw, Mapping) else {}
    level_rows = source.get("levels")
    levels: list[StrategyLevel] = []
    if isinstance(level_rows, (list, tuple)):
        for row in level_rows:
            if not isinstance(row, Mapping):
                continue
            kind = _enum(LevelKind, row.get("kind") or row.get("key"))
            value = _finite_number(row.get("value"))
            if kind is not None and value is not None:
                levels.append(StrategyLevel(kind, value))

    active_raw = source.get("active_level")
    active_level = None
    if isinstance(active_raw, Mapping):
        active_kind = _enum(LevelKind, active_raw.get("kind") or active_raw.get("key"))
        active_value = _finite_number(active_raw.get("value"))
        if active_kind is not None and active_value is not None:
            active_level = StrategyLevel(active_kind, active_value)

    return SweepStrategyInput(
        spot=_finite_number(source.get("spot")),
        active_level=active_level,
        interaction=_enum(InteractionDirection, source.get("interaction")),
        gamma_regime=_enum(GammaRegime, source.get("gamma_regime"), GammaRegime.UNAVAILABLE),
        levels=tuple(levels),
        location=_evidence(source.get("location")),
        sweep=_evidence(source.get("sweep")),
        close_back_inside_5m=_evidence(source.get("close_back_inside_5m")),
        reversal_2_2_5m=_evidence(source.get("reversal_2_2_5m")),
        trigger_break=_evidence(source.get("trigger_break")),
        acceptance=_evidence(source.get("acceptance")),
        continuation_retest=_evidence(source.get("continuation_retest")),
        runner_2_2_15m=_evidence(source.get("runner_2_2_15m")),
    )


def _direction(interaction: Optional[InteractionDirection]) -> Optional[TradeDirection]:
    if interaction == InteractionDirection.FROM_BELOW:
        return TradeDirection.BEARISH
    if interaction == InteractionDirection.FROM_ABOVE:
        return TradeDirection.BULLISH
    return None


def _targets(
    strategy_input: SweepStrategyInput,
    direction: Optional[TradeDirection],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    active = strategy_input.active_level
    if active is None or direction is None:
        return [], []
    unique: dict[float, StrategyLevel] = {}
    for level in strategy_input.levels:
        if level.kind == active.kind and level.value == active.value:
            continue
        unique.setdefault(level.value, level)
    candidates = [
        level
        for level in unique.values()
        if (
            level.value > active.value
            if direction == TradeDirection.BULLISH
            else level.value < active.value
        )
    ]
    candidates.sort(key=lambda level: level.value, reverse=direction == TradeDirection.BEARISH)
    rows = [
        {"key": level.kind.value, "label": LEVEL_LABELS[level.kind], "value": level.value}
        for level in candidates
    ]
    primary = rows[:1]
    expansion = rows[1:] if strategy_input.gamma_regime == GammaRegime.NEGATIVE else []
    return primary, expansion


def _check_status(evidence: Evidence, *, prerequisite_met: bool) -> EvidenceStatus:
    if not prerequisite_met:
        return EvidenceStatus.PENDING
    if evidence.confirmed is True:
        return EvidenceStatus.CONFIRMED
    if evidence.confirmed is False:
        return EvidenceStatus.FAILED
    return EvidenceStatus.UNAVAILABLE


def evaluate_failed_liquidity_sweep(
    raw_input: SweepStrategyInput | Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Evaluate one level interaction without inventing missing confirmation."""

    strategy_input = (
        raw_input
        if isinstance(raw_input, SweepStrategyInput)
        else normalize_strategy_input(raw_input)
    )
    active = strategy_input.active_level
    direction = _direction(strategy_input.interaction)
    acceptance = strategy_input.acceptance.confirmed is True
    reversal_claimed = any(
        evidence.confirmed is True
        for evidence in (
            strategy_input.close_back_inside_5m,
            strategy_input.reversal_2_2_5m,
            strategy_input.trigger_break,
        )
    )
    conflict = acceptance and reversal_claimed

    if conflict:
        state = StrategyState.SETUP_INVALIDATED
        path = StrategyPath.INVALIDATED
        missing = "Conflicting acceptance and reversal evidence must be resolved."
    elif active is None or strategy_input.interaction is None:
        state = StrategyState.WAITING_FOR_LOCATION
        path = StrategyPath.PENDING
        missing = "A valid active level and test direction are unavailable."
    elif acceptance:
        path = StrategyPath.CONTINUATION
        state = (
            StrategyState.CONTINUATION_ACTIVE
            if strategy_input.continuation_retest.confirmed is True
            else StrategyState.ACCEPTANCE_CONFIRMED
        )
        missing = (
            "Continuation is active toward the next relevant level."
            if state == StrategyState.CONTINUATION_ACTIVE
            else "Wait for a successful hold or retest beyond the level."
        )
    elif strategy_input.location.confirmed is not True:
        state = StrategyState.WAITING_FOR_LOCATION
        path = StrategyPath.PENDING
        missing = "Wait for price to reach the active level."
    elif strategy_input.sweep.confirmed is not True:
        state = StrategyState.LEVEL_BEING_TESTED
        path = StrategyPath.PENDING
        missing = "Wait for price to trade beyond the level and sweep liquidity."
    elif strategy_input.close_back_inside_5m.confirmed is None:
        state = StrategyState.SWEEP_DETECTED
        path = StrategyPath.PENDING
        missing = "Wait for a completed five-minute close back inside the swept level."
    elif strategy_input.close_back_inside_5m.confirmed is not True:
        state = StrategyState.WAITING_FOR_CLOSE_BACK_INSIDE
        path = StrategyPath.PENDING
        missing = "The sweep has not produced a completed five-minute close back inside."
    elif strategy_input.reversal_2_2_5m.confirmed is not True:
        state = StrategyState.WAITING_FOR_5M_2_2
        path = StrategyPath.PENDING
        missing = "Wait for a five-minute Strat 2-2 reversal."
    elif strategy_input.trigger_break.confirmed is not True:
        state = StrategyState.WAITING_FOR_TRIGGER_BREAK
        path = StrategyPath.PENDING
        missing = "Wait for price to break the trigger candle in the reversal direction."
    else:
        state = StrategyState.REVERSAL_READY
        path = StrategyPath.REVERSAL
        missing = "All reversal evidence is confirmed."

    if strategy_input.gamma_regime == GammaRegime.POSITIVE:
        regime_interpretation = (
            "Mean-reversion environment. Favor the nearest valid target and avoid demanding a "
            "full-range move."
        )
    elif strategy_input.gamma_regime == GammaRegime.NEGATIVE:
        regime_interpretation = (
            "Expansion environment. The direction that confirms control may travel through "
            "multiple liquidity levels."
        )
    elif strategy_input.gamma_regime == GammaRegime.TRANSITION:
        regime_interpretation = (
            "Gamma-flip transition. Confidence is reduced until rejection or acceptance is clear."
        )
    else:
        regime_interpretation = "Gamma regime unavailable. Keep the setup pending."

    target_direction = direction
    if path == StrategyPath.CONTINUATION and direction is not None:
        target_direction = (
            TradeDirection.BULLISH
            if strategy_input.interaction == InteractionDirection.FROM_BELOW
            else TradeDirection.BEARISH
        )
    primary_targets, expansion_targets = _targets(strategy_input, target_direction)
    target_is_valid = state in {
        StrategyState.REVERSAL_READY,
        StrategyState.CONTINUATION_ACTIVE,
    }
    if not target_is_valid:
        primary_targets = []
        expansion_targets = []

    location_ok = active is not None and strategy_input.location.confirmed is True
    sweep_ok = location_ok and strategy_input.sweep.confirmed is True
    close_ok = sweep_ok and strategy_input.close_back_inside_5m.confirmed is True
    reversal_ok = close_ok and strategy_input.reversal_2_2_5m.confirmed is True
    trigger_ok = reversal_ok and strategy_input.trigger_break.confirmed is True
    accepted_path = path == StrategyPath.CONTINUATION
    checklist = {
        "location": (
            _check_status(strategy_input.location, prerequisite_met=True)
            if active is not None
            else EvidenceStatus.UNAVAILABLE
        ),
        "sweep": _check_status(strategy_input.sweep, prerequisite_met=location_ok),
        "close_back_inside_5m": _check_status(
            strategy_input.close_back_inside_5m, prerequisite_met=sweep_ok
        ),
        "reversal_2_2_5m": _check_status(strategy_input.reversal_2_2_5m, prerequisite_met=close_ok),
        "trigger_break": _check_status(strategy_input.trigger_break, prerequisite_met=reversal_ok),
        "setup_ready": (
            EvidenceStatus.FAILED
            if accepted_path or path == StrategyPath.INVALIDATED
            else (
                EvidenceStatus.CONFIRMED
                if trigger_ok and state == StrategyState.REVERSAL_READY
                else EvidenceStatus.PENDING
            )
        ),
    }
    if accepted_path:
        for key in ("close_back_inside_5m", "reversal_2_2_5m", "trigger_break"):
            if checklist[key] != EvidenceStatus.CONFIRMED:
                checklist[key] = EvidenceStatus.FAILED

    timestamps = {
        key: evidence.timestamp
        for key, evidence in {
            "location": strategy_input.location,
            "sweep": strategy_input.sweep,
            "close_back_inside_5m": strategy_input.close_back_inside_5m,
            "reversal_2_2_5m": strategy_input.reversal_2_2_5m,
            "trigger_break": strategy_input.trigger_break,
            "acceptance": strategy_input.acceptance,
            "runner_2_2_15m": strategy_input.runner_2_2_15m,
        }.items()
        if evidence.timestamp
    }
    interaction_label = (
        "Testing from below"
        if strategy_input.interaction == InteractionDirection.FROM_BELOW
        else (
            "Testing from above"
            if strategy_input.interaction == InteractionDirection.FROM_ABOVE
            else "Unavailable"
        )
    )
    status_label = (
        "Acceptance confirmed"
        if path == StrategyPath.CONTINUATION
        else (
            "Rejection confirmed"
            if state == StrategyState.REVERSAL_READY
            else "Neither confirmed — wait"
        )
    )
    return {
        "state": state.value,
        "path": path.value,
        "direction": direction.value if direction and path != StrategyPath.CONTINUATION else None,
        "active_level": (
            {"key": active.kind.value, "label": LEVEL_LABELS[active.kind], "value": active.value}
            if active
            else None
        ),
        "interaction": strategy_input.interaction.value if strategy_input.interaction else None,
        "interaction_label": interaction_label,
        "rejection_acceptance_status": status_label,
        "gamma_regime": strategy_input.gamma_regime.value,
        "gamma_interpretation": regime_interpretation,
        "primary_target": primary_targets[0] if primary_targets else None,
        "expansion_targets": expansion_targets,
        "missing_evidence": missing,
        "checklist": {key: value.value for key, value in checklist.items()},
        "runner_confirmation": (
            "Confirmed"
            if strategy_input.runner_2_2_15m.confirmed is True
            else "Not required for entry"
        ),
        "evidence_timestamps": timestamps,
        "setup_ready": state == StrategyState.REVERSAL_READY,
    }
