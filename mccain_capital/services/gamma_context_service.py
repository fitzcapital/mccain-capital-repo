"""Derived SPX gamma context helpers for the Market Pulse SPX Priority card.

This module centralizes decision logic so both server-side rendering and
live client updates can share the same thresholds and narrative structure.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, TypedDict


FlipProximityState = Literal["at_flip", "very_close", "near", "far", "unavailable"]
WallProximityState = Literal["at_wall", "very_close", "near", "far", "unavailable"]
TrapZoneState = Literal["clear", "compressed_trap_zone", "knife_edge_structure", "unavailable"]


class GammaContextInput(TypedDict, total=False):
    spot: float
    gammaFlip: float
    callWall: float
    putWall: float
    nextCallWallAbove: Optional[float]
    nextPutWallBelow: Optional[float]
    netGamma: float
    regime: str
    bias: str
    expectedMove: Optional[float]
    updatedAt: Optional[str | int]


class DistanceMetrics(TypedDict):
    distance_to_flip: Optional[float]
    distance_to_call_wall: Optional[float]
    distance_to_put_wall: Optional[float]
    next_call_wall_above: Optional[float]
    next_put_wall_below: Optional[float]
    expected_move_up: Optional[float]
    expected_move_down: Optional[float]
    expected_move_range_text: str
    flip_proximity_state: FlipProximityState
    wall_proximity_state: WallProximityState
    trap_zone_state: TrapZoneState


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _extract_candidate_levels(payload: Dict[str, Any]) -> List[float]:
    out: List[float] = []
    top3 = payload.get("gamma_walls_top3") or []
    if isinstance(top3, list):
        for value in top3:
            parsed = _safe_float(value)
            if parsed is not None:
                out.append(parsed)

    # Try to recover full strike ladder from chart_json if available.
    chart = (payload.get("chart_json") or {}).get("gex")
    data = chart.get("data") if isinstance(chart, dict) else None
    if isinstance(data, list) and data:
        first_trace = data[0] if isinstance(data[0], dict) else {}
        xs = first_trace.get("x") if isinstance(first_trace, dict) else None
        if isinstance(xs, list):
            for value in xs:
                parsed = _safe_float(value)
                if parsed is not None:
                    out.append(parsed)

    deduped = sorted({round(v, 4) for v in out})
    return [float(v) for v in deduped]


def classifyProximity(
    distance: Optional[float],
    kind: Literal["flip", "wall"],
) -> FlipProximityState | WallProximityState:
    if distance is None:
        return "unavailable"
    value = abs(float(distance))
    if value <= 5:
        return "at_flip" if kind == "flip" else "at_wall"
    if value <= 10:
        return "very_close"
    if value <= 20:
        return "near"
    return "far"


def classifyTrapZone(
    spot: Optional[float],
    gamma_flip: Optional[float],
    call_wall: Optional[float],
    put_wall: Optional[float],
) -> TrapZoneState:
    if spot is None or gamma_flip is None or call_wall is None or put_wall is None:
        return "unavailable"

    cluster_span = max(call_wall, put_wall, gamma_flip) - min(call_wall, put_wall, gamma_flip)
    if cluster_span <= 15:
        return "knife_edge_structure"

    up_spread = abs(call_wall - gamma_flip)
    down_spread = abs(gamma_flip - put_wall)
    between_flip_call = min(gamma_flip, call_wall) <= spot <= max(gamma_flip, call_wall)
    between_put_flip = min(put_wall, gamma_flip) <= spot <= max(put_wall, gamma_flip)

    if (between_flip_call and up_spread <= 15) or (between_put_flip and down_spread <= 15):
        return "compressed_trap_zone"

    return "clear"


def formatLevelDistance(distance: Optional[float]) -> str:
    if distance is None:
        return "—"
    points = abs(float(distance))
    unit = "pt" if abs(points - 1.0) < 1e-9 else "pts"
    if points < 0.05:
        return f"0 {unit}"
    side = "above" if float(distance) > 0 else "below"
    return f"{points:.1f} {unit} {side}"


def _derive_next_levels(
    call_wall: Optional[float],
    put_wall: Optional[float],
    provided_call: Optional[float],
    provided_put: Optional[float],
    candidates: List[float],
) -> tuple[Optional[float], Optional[float]]:
    next_call = provided_call
    next_put = provided_put

    if next_call is None and call_wall is not None and candidates:
        higher = [value for value in candidates if value > call_wall + 0.001]
        next_call = min(higher) if higher else None
    if next_put is None and put_wall is not None and candidates:
        lower = [value for value in candidates if value < put_wall - 0.001]
        next_put = max(lower) if lower else None

    return (next_call, next_put)


def _infer_level_step(
    call_wall: Optional[float],
    put_wall: Optional[float],
    candidates: List[float],
) -> float:
    diffs: List[float] = []
    ordered = sorted({float(v) for v in candidates if _safe_float(v) is not None})
    for idx in range(1, len(ordered)):
        diff = abs(ordered[idx] - ordered[idx - 1])
        if diff > 0.01:
            diffs.append(diff)
    if diffs:
        diffs.sort()
        return max(5.0, min(25.0, float(diffs[len(diffs) // 2])))
    if call_wall is not None and put_wall is not None:
        spread = abs(float(call_wall) - float(put_wall))
        if spread > 0:
            return max(5.0, min(25.0, max(5.0, round(spread / 4.0 / 5.0) * 5.0)))
    return 10.0


def _infer_expected_move(
    expected_move: Optional[float],
    spot: Optional[float],
    gamma_flip: Optional[float],
    call_wall: Optional[float],
    put_wall: Optional[float],
    candidates: List[float],
) -> Optional[float]:
    if expected_move is not None:
        return float(expected_move)

    if spot is not None and call_wall is not None and put_wall is not None:
        derived = max(abs(float(call_wall) - float(spot)), abs(float(spot) - float(put_wall)))
        if derived > 0:
            return float(derived)
    if call_wall is not None and put_wall is not None:
        spread = abs(float(call_wall) - float(put_wall)) / 2.0
        if spread > 0:
            return float(spread)
    if spot is not None and gamma_flip is not None:
        flip_dist = abs(float(spot) - float(gamma_flip))
        if flip_dist > 0:
            return float(flip_dist)

    ordered = sorted({float(v) for v in candidates if _safe_float(v) is not None})
    if spot is not None and ordered:
        higher = [v for v in ordered if v > float(spot)]
        lower = [v for v in ordered if v < float(spot)]
        if higher and lower:
            return max(abs(min(higher) - float(spot)), abs(float(spot) - max(lower)))
        if higher:
            return abs(min(higher) - float(spot))
        if lower:
            return abs(float(spot) - max(lower))
    return None


def computeDistanceMetrics(
    data: GammaContextInput, candidate_levels: Optional[List[float]] = None
) -> DistanceMetrics:
    spot = _safe_float(data.get("spot"))
    gamma_flip = _safe_float(data.get("gammaFlip"))
    call_wall = _safe_float(data.get("callWall"))
    put_wall = _safe_float(data.get("putWall"))
    net_gamma = _safe_float(data.get("netGamma"))
    candidate_list = list(candidate_levels or [])
    expected_move = _infer_expected_move(
        expected_move=_safe_float(data.get("expectedMove")),
        spot=spot,
        gamma_flip=gamma_flip,
        call_wall=call_wall,
        put_wall=put_wall,
        candidates=candidate_list,
    )
    provided_next_call = _safe_float(data.get("nextCallWallAbove"))
    provided_next_put = _safe_float(data.get("nextPutWallBelow"))

    distance_to_flip = (
        (spot - gamma_flip) if (spot is not None and gamma_flip is not None) else None
    )
    distance_to_call_wall = (
        (spot - call_wall) if (spot is not None and call_wall is not None) else None
    )
    distance_to_put_wall = (
        (spot - put_wall) if (spot is not None and put_wall is not None) else None
    )

    next_call_wall_above, next_put_wall_below = _derive_next_levels(
        call_wall=call_wall,
        put_wall=put_wall,
        provided_call=provided_next_call,
        provided_put=provided_next_put,
        candidates=candidate_list,
    )
    if (next_call_wall_above is None and call_wall is not None) or (
        next_put_wall_below is None and put_wall is not None
    ):
        step = _infer_level_step(call_wall=call_wall, put_wall=put_wall, candidates=candidate_list)
        if next_call_wall_above is None and call_wall is not None:
            next_call_wall_above = float(call_wall) + float(step)
        if next_put_wall_below is None and put_wall is not None:
            next_put_wall_below = float(put_wall) - float(step)

    expected_move_up = (
        (spot + expected_move) if (spot is not None and expected_move is not None) else None
    )
    expected_move_down = (
        (spot - expected_move) if (spot is not None and expected_move is not None) else None
    )
    if expected_move_up is not None and expected_move_down is not None:
        expected_move_range_text = f"{expected_move_down:.1f} - {expected_move_up:.1f}"
    elif expected_move is not None:
        expected_move_range_text = f"±{expected_move:.1f}"
    else:
        expected_move_range_text = "—"

    flip_proximity = classifyProximity(distance_to_flip, "flip")
    nearest_wall_distance = None
    if distance_to_call_wall is not None and distance_to_put_wall is not None:
        nearest_wall_distance = (
            distance_to_call_wall
            if abs(distance_to_call_wall) <= abs(distance_to_put_wall)
            else distance_to_put_wall
        )
    elif distance_to_call_wall is not None:
        nearest_wall_distance = distance_to_call_wall
    elif distance_to_put_wall is not None:
        nearest_wall_distance = distance_to_put_wall
    wall_proximity = classifyProximity(nearest_wall_distance, "wall")

    trap_zone = classifyTrapZone(
        spot=spot,
        gamma_flip=gamma_flip,
        call_wall=call_wall,
        put_wall=put_wall,
    )

    # Net gamma is intentionally read for call-site parity with narrative generation.
    _ = net_gamma

    return {
        "distance_to_flip": distance_to_flip,
        "distance_to_call_wall": distance_to_call_wall,
        "distance_to_put_wall": distance_to_put_wall,
        "next_call_wall_above": next_call_wall_above,
        "next_put_wall_below": next_put_wall_below,
        "expected_move_up": expected_move_up,
        "expected_move_down": expected_move_down,
        "expected_move_range_text": expected_move_range_text,
        "flip_proximity_state": flip_proximity,
        "wall_proximity_state": wall_proximity,
        "trap_zone_state": trap_zone,
    }


def _regime_message(net_gamma: Optional[float], distance_to_flip: Optional[float]) -> Optional[str]:
    if net_gamma is None or distance_to_flip is None:
        return None

    spot_above_flip = float(distance_to_flip) > 0
    if net_gamma > 0 and spot_above_flip:
        msg = "Positive gamma, stabilizing tape. Mean reversion is favored."
    elif net_gamma > 0 and not spot_above_flip:
        msg = "Positive gamma but below flip. Contained downside unless support breaks."
    elif net_gamma < 0 and not spot_above_flip:
        msg = "Negative gamma with downside expansion risk."
    elif net_gamma < 0 and spot_above_flip:
        msg = "Negative gamma with upside squeeze risk."
    else:
        msg = "Net gamma is neutral. Price location will drive the next impulse."

    if abs(distance_to_flip) <= 10:
        msg += " Near regime transition: expect fakeouts and unstable direction."
    return msg


def buildGammaNarrative(data: GammaContextInput, metrics: DistanceMetrics) -> List[str]:
    notes: List[str] = []
    distance_to_flip = metrics.get("distance_to_flip")
    distance_to_call = metrics.get("distance_to_call_wall")
    distance_to_put = metrics.get("distance_to_put_wall")
    gamma_flip = _safe_float(data.get("gammaFlip"))
    call_wall = _safe_float(data.get("callWall"))
    put_wall = _safe_float(data.get("putWall"))
    spot = _safe_float(data.get("spot"))

    if distance_to_flip is not None:
        side = "above" if distance_to_flip > 0 else "below"
        notes.append(f"SPX is {abs(distance_to_flip):.1f} points {side} gamma flip.")

    if (
        spot is not None
        and gamma_flip is not None
        and call_wall is not None
        and min(gamma_flip, call_wall) <= spot <= max(gamma_flip, call_wall)
    ):
        notes.append("Spot is inside the neutral chop zone between gamma flip and call wall.")
    elif (
        spot is not None
        and put_wall is not None
        and gamma_flip is not None
        and min(put_wall, gamma_flip) <= spot <= max(put_wall, gamma_flip)
    ):
        notes.append("Spot is inside the lower transition pocket between put wall and gamma flip.")

    if distance_to_call is not None and abs(distance_to_call) <= 20:
        notes.append(
            "Price is approaching the call wall. Expect resistance or sweep/reject risk unless acceptance holds above it."
        )
    elif distance_to_put is not None and abs(distance_to_put) <= 20:
        notes.append(
            "Price is approaching the put wall. Expect support response or a flush-and-reclaim test."
        )

    regime_note = _regime_message(_safe_float(data.get("netGamma")), distance_to_flip)
    if regime_note:
        notes.append(regime_note)

    if metrics.get("trap_zone_state") == "knife_edge_structure":
        notes.append(
            "Flip and walls are tightly clustered. Knife-edge structure can whip both directions."
        )
    elif metrics.get("trap_zone_state") == "compressed_trap_zone":
        notes.append(
            "Local levels are compressed. Breakouts can fail quickly before directional acceptance."
        )

    if not notes:
        notes.append("Awaiting complete live gamma context for SPX.")
    if len(notes) == 1:
        notes.append("Waiting for full level alignment before issuing directional context.")

    return notes[:4]


def build_warning_badges(data: GammaContextInput, metrics: DistanceMetrics) -> List[str]:
    badges: List[str] = []
    distance_to_flip = metrics.get("distance_to_flip")
    distance_to_call = metrics.get("distance_to_call_wall")
    distance_to_put = metrics.get("distance_to_put_wall")
    net_gamma = _safe_float(data.get("netGamma"))

    if metrics.get("trap_zone_state") == "knife_edge_structure":
        badges.append("Knife Edge")
    if distance_to_flip is not None and abs(distance_to_flip) <= 10:
        badges.append("Regime Transition")
    if metrics.get("trap_zone_state") == "compressed_trap_zone":
        badges.append("Neutral Chop")
    if distance_to_call is not None and abs(distance_to_call) <= 10:
        badges.append("Call Wall Test")
    if distance_to_put is not None and abs(distance_to_put) <= 10:
        badges.append("Put Wall Test")
    if net_gamma is not None and net_gamma < 0:
        badges.append("Expansion Risk")

    if not badges:
        badges.append("Neutral Chop")

    deduped: List[str] = []
    seen = set()
    for badge in badges:
        if badge in seen:
            continue
        seen.add(badge)
        deduped.append(badge)
    return deduped


def _warning_tone(label: str) -> str:
    lowered = str(label or "").lower()
    if "knife" in lowered or "expansion" in lowered:
        return "critical"
    if "transition" in lowered or "test" in lowered:
        return "warn"
    return "neutral"


def _level_strength_label(distance: Optional[float], state: str) -> str:
    if distance is None or state == "unavailable":
        return "Unavailable"
    if state in {"at_flip", "at_wall"}:
        return "Immediate test"
    if state == "very_close":
        return "High reaction odds"
    if state == "near":
        return "In play"
    return "Background level"


def format_net_gamma_human(value: Optional[float]) -> str:
    parsed = _safe_float(value)
    if parsed is None:
        return "—"
    abs_v = abs(float(parsed))
    sign = "-" if float(parsed) < 0 else "+" if float(parsed) > 0 else ""
    if abs_v >= 1_000_000_000:
        return f"{sign}{abs_v / 1_000_000_000:.1f} billion"
    if abs_v >= 1_000_000:
        return f"{sign}{abs_v / 1_000_000:.1f} million"
    if abs_v >= 1_000:
        return f"{sign}{abs_v / 1_000:.1f} thousand"
    return f"{parsed:.0f}"


def build_spx_priority_context(
    spx_quote: Dict[str, Any], gamma_snapshot: Dict[str, Any]
) -> Dict[str, Any]:
    spot = _safe_float(spx_quote.get("price"))
    gamma_flip = _safe_float(gamma_snapshot.get("gamma_flip"))
    call_wall = _safe_float(gamma_snapshot.get("call_wall"))
    put_wall = _safe_float(gamma_snapshot.get("put_wall"))
    net_gamma = _safe_float(gamma_snapshot.get("net_gex"))
    call_wall_gamma_per_point = _safe_float(gamma_snapshot.get("call_wall_gamma_per_point"))
    put_wall_gamma_per_point = _safe_float(gamma_snapshot.get("put_wall_gamma_per_point"))
    expected_move = _safe_float(gamma_snapshot.get("expected_move"))

    # TODO(api): wire backend payload for next_call_wall_above and next_put_wall_below.
    next_call_from_backend = _safe_float(gamma_snapshot.get("next_call_wall_above"))
    next_put_from_backend = _safe_float(gamma_snapshot.get("next_put_wall_below"))

    # TODO(api): wire backend expected_move payload when provider is available.
    data: GammaContextInput = {
        "spot": spot,
        "gammaFlip": gamma_flip,
        "callWall": call_wall,
        "putWall": put_wall,
        "nextCallWallAbove": next_call_from_backend,
        "nextPutWallBelow": next_put_from_backend,
        "netGamma": net_gamma,
        "regime": str(gamma_snapshot.get("regime") or ""),
        "bias": str(gamma_snapshot.get("bias") or ""),
        "expectedMove": expected_move,
        "updatedAt": gamma_snapshot.get("asof"),
    }

    candidates = _extract_candidate_levels(gamma_snapshot)
    metrics = computeDistanceMetrics(data, candidate_levels=candidates)
    narrative = buildGammaNarrative(data, metrics)
    warning_labels = build_warning_badges(data, metrics)

    warning_badges = [{"label": label, "tone": _warning_tone(label)} for label in warning_labels]

    return {
        "input": data,
        "metrics": metrics,
        "narrative": narrative,
        "warning_badges": warning_badges,
        "net_gamma_text": format_net_gamma_human(net_gamma),
        "call_wall_gamma_per_point_text": format_net_gamma_human(call_wall_gamma_per_point),
        "put_wall_gamma_per_point_text": format_net_gamma_human(put_wall_gamma_per_point),
        "distance_to_flip_text": formatLevelDistance(metrics.get("distance_to_flip")),
        "distance_to_call_wall_text": formatLevelDistance(metrics.get("distance_to_call_wall")),
        "distance_to_put_wall_text": formatLevelDistance(metrics.get("distance_to_put_wall")),
        "flip_strength_label": _level_strength_label(
            metrics.get("distance_to_flip"),
            str(metrics.get("flip_proximity_state") or "unavailable"),
        ),
        "wall_strength_label": _level_strength_label(
            (
                metrics.get("distance_to_call_wall")
                if metrics.get("distance_to_call_wall") is not None
                else metrics.get("distance_to_put_wall")
            ),
            str(metrics.get("wall_proximity_state") or "unavailable"),
        ),
        "trap_zone_label": str(metrics.get("trap_zone_state") or "unavailable").replace("_", " "),
        "missing_expected_move": expected_move is None and metrics.get("expected_move_up") is None,
        "missing_next_levels": (
            next_call_from_backend is None
            and next_put_from_backend is None
            and metrics.get("next_call_wall_above") is None
            and metrics.get("next_put_wall_below") is None
        ),
    }


# Snake_case aliases for codebases that prefer Python naming style.
compute_distance_metrics = computeDistanceMetrics
classify_proximity = classifyProximity
classify_trap_zone = classifyTrapZone
build_gamma_narrative = buildGammaNarrative
format_level_distance = formatLevelDistance
