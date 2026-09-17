"""Point-in-time-safe intraday Market Pulse setup replay."""

from __future__ import annotations

from datetime import datetime, time, timedelta
import copy
import math
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from mccain_capital.services.market_pulse_gamma_context import signal_gamma_context
from mccain_capital.services.market_pulse_scenarios import (
    CONFLUENCE_WEIGHTS,
    confluence_grade,
    normalize_levels,
    rank_market_scenarios,
)
from mccain_capital.services.spx_strat_patterns import bar_is_at_key_level
from mccain_capital.services.market_session_calendar import session_window, market_phase


ET = ZoneInfo("America/New_York")
REPLAY_ENTRY_CUTOFF_ET = time(15, 15)
MINIMUM_TARGET_SPACE = 5.0
ESTIMATED_CONTRACT_COST = 750.0
ESTIMATED_ABSOLUTE_DELTA = 0.40
ESTIMATED_TP_RETURNS = (15, 20, 30)
DYNAMIC_LEVEL_KEYS = {
    "gamma_flip",
    "local_flip",
    "call_wall",
    "put_wall",
    "new_call_wall",
    "new_put_wall",
}


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _timestamp(row: Mapping[str, Any]) -> datetime | None:
    raw = row.get("ts") or row.get("timestamp") or row.get("time")
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ET)
    return parsed.astimezone(ET)


def _normalized_bars(rows: Iterable[Mapping[str, Any]], session_date: str) -> list[dict[str, Any]]:
    bars: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        stamp = _timestamp(row)
        values = {
            key: _number(row.get(key) if row.get(key) is not None else row.get("v"))
            for key in ("open", "high", "low", "close")
        }
        if stamp is None or any(value is None for value in values.values()):
            continue
        if session_date and stamp.date().isoformat() != session_date:
            continue
        bars.append({"ts": stamp.isoformat(), **values})
    return sorted(bars, key=lambda row: row["ts"])


def resolve_replay_session(
    rows: Iterable[Mapping[str, Any]], *, now: datetime, requested_session: str = ""
) -> dict[str, Any]:
    """Select review candles without granting live execution freshness.

    Strategy candle timestamps identify the opening of each five-minute interval.
    """

    current = now.astimezone(ET) if now.tzinfo else now.replace(tzinfo=ET)
    candidates = _normalized_bars(
        [
            row
            for row in rows
            if isinstance(row, Mapping)
            and row.get("complete") is not False
            and row.get("is_complete") is not False
        ],
        requested_session,
    )
    completed = []
    for row in candidates:
        stamp = _timestamp(row)
        window = session_window(stamp.date())
        if (
            window.is_session
            and window.opens_at <= stamp < window.closes_at
            and stamp + timedelta(minutes=5) <= min(current, window.closes_at)
        ):
            completed.append(row)
    selected = requested_session or (completed[-1]["ts"][:10] if completed else "")
    bars = [row for row in completed if row["ts"][:10] == selected]
    review_only = selected != current.date().isoformat() or market_phase(current) != "open"
    label = datetime.fromisoformat(selected).strftime("%a, %b %d") if selected else ""
    return {
        "bars": bars,
        "session_date": selected,
        "session_label": label,
        "review_only": review_only,
        "evaluated_through": bars[-1]["ts"] if bars else None,
        "available": bool(bars),
        "unavailable_reason": "" if bars else "No retained candles for this session",
    }


def _next_target(
    levels: list[Any],
    value: float,
    direction: str,
    *,
    excluded_keys: Iterable[str] = (),
) -> dict[str, Any] | None:
    excluded = {str(key) for key in excluded_keys if key}
    candidates = (
        [level for level in levels if level.key not in excluded and level.value > value]
        if direction == "bullish"
        else [level for level in levels if level.key not in excluded and level.value < value]
    )
    candidates = [level for level in candidates if abs(level.value - value) >= MINIMUM_TARGET_SPACE]
    if not candidates:
        return None
    target = min(candidates, key=lambda level: abs(level.value - value))
    return {
        "key": target.key,
        "label": target.label,
        "value": target.value,
        **({"as_of": target.as_of} if target.as_of else {}),
    }


def _estimated_scalp_targets(
    entry: Any, direction: str, option_reference: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    reference = dict(option_reference or {})
    use_tradier = reference.get("pricing_mode") == "tradier_current_quote"
    contract_cost = _number(reference.get("contract_cost")) if use_tradier else None
    absolute_delta = _number(reference.get("absolute_delta")) if use_tradier else None
    if (
        contract_cost is None
        or contract_cost <= 0
        or absolute_delta is None
        or not 0.05 <= absolute_delta <= 0.95
    ):
        use_tradier = False
        contract_cost = ESTIMATED_CONTRACT_COST
        absolute_delta = ESTIMATED_ABSOLUTE_DELTA
    entry_value = _number(entry)
    if entry_value is None or direction not in {"bullish", "bearish"}:
        return {"targets": [], "contract_cost": contract_cost, "absolute_delta": absolute_delta}
    premium = contract_cost / 100
    sign = 1 if direction == "bullish" else -1
    targets = []
    for index, return_percent in enumerate(ESTIMATED_TP_RETURNS, start=1):
        point_move = premium * (return_percent / 100) / absolute_delta
        targets.append(
            {
                "key": f"tp{index}",
                "return_percent": return_percent,
                "point_move": round(point_move, 2),
                "spx_price": round(entry_value + sign * point_move, 2),
            }
        )
    return {
        "targets": targets,
        "contract_cost": round(contract_cost, 2),
        "absolute_delta": round(absolute_delta, 4),
        "pricing_mode": "tradier_current_quote" if use_tradier else "fallback_estimate",
        "source": reference.get("source") if use_tradier else "Documented planning assumption",
        "contract_label": str(reference.get("contract_label") or "") if use_tradier else "",
        "quote_as_of": str(reference.get("as_of") or "") if use_tradier else "",
        "fallback_used": not use_tradier,
        "fallback_reason": (
            "" if use_tradier else str(reference.get("fallback_reason") or "unavailable")
        ),
        "dealer_gamma_used": False,
    }


def _gamma_at_signal(
    observations: Iterable[Mapping[str, Any]], signal_stamp: datetime | None
) -> dict[str, Any] | None:
    eligible: list[tuple[datetime, dict[str, Any]]] = []
    for source in observations:
        row = dict(source)
        observed_at = _timestamp({"ts": row.get("as_of") or row.get("timestamp")})
        if observed_at is not None and signal_stamp is not None and observed_at <= signal_stamp:
            eligible.append((observed_at, row))
    return max(eligible, key=lambda item: item[0])[1] if eligible else None


def _point_in_time_levels(
    levels: Iterable[Mapping[str, Any]], bars: list[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """Return canonical levels with session extremes known at this replay instant."""

    highs = [_number(bar.get("high")) for bar in bars]
    lows = [_number(bar.get("low")) for bar in bars]
    current_day_high = max((value for value in highs if value is not None), default=None)
    current_day_low = min((value for value in lows if value is not None), default=None)
    signal_stamp = _timestamp(bars[-1]) if bars else None
    point_in_time: list[dict[str, Any]] = []
    dynamic_levels: dict[str, tuple[datetime, dict[str, Any]]] = {}
    for source in levels:
        row = dict(source)
        key = str(row.get("key") or "").strip().lower()
        if key in DYNAMIC_LEVEL_KEYS:
            observed_at = _timestamp({"ts": row.get("as_of")})
            if observed_at is None or signal_stamp is None or observed_at > signal_stamp:
                continue
            previous = dynamic_levels.get(key)
            if previous is None or observed_at >= previous[0]:
                dynamic_levels[key] = (observed_at, row)
            continue
        if key == "current_day_high" and current_day_high is not None:
            row["value"] = current_day_high
        elif key == "current_day_low" and current_day_low is not None:
            row["value"] = current_day_low
        point_in_time.append(row)
    point_in_time.extend(row for _, row in dynamic_levels.values())
    return point_in_time


def _pattern_location_event(
    *,
    pattern: Mapping[str, Any],
    bars: list[Mapping[str, Any]],
    direction: str,
) -> str:
    """Return the qualifying location event for one completed setup pattern.

    A Strat sequence is confirmation, not location. Replay only promotes the
    sequence when its own formation either establishes the session extreme or
    performs an ordered sweep and rejection/recovery at the anchored level.
    """

    anchor = dict(pattern.get("anchor_level") or {})
    level_key = str(anchor.get("key") or "").strip().lower()
    level_value = _number(anchor.get("value"))
    pattern_times = {str(value) for value in pattern.get("bar_timestamps") or [] if value}
    if level_value is None or not pattern_times:
        return ""
    pattern_bars = [row for row in bars if str(row.get("ts") or "") in pattern_times]
    if not pattern_bars:
        return ""
    signal_close = _number(pattern_bars[-1].get("close"))
    closes_through_level = bool(
        signal_close is not None
        and (signal_close > level_value if direction == "bullish" else signal_close < level_value)
    )
    if not closes_through_level:
        return ""
    first_pattern_index = next(
        (
            index
            for index, row in enumerate(bars)
            if str(row.get("ts") or "") == str(pattern_bars[0].get("ts") or "")
        ),
        None,
    )
    prior_bars = bars[:first_pattern_index] if first_pattern_index is not None else []

    if level_key == "current_day_high" and direction == "bearish":
        prior_high = max(
            (_number(row.get("high")) for row in prior_bars),
            default=None,
        )
        pattern_high = max(
            (_number(row.get("high")) for row in pattern_bars),
            default=None,
        )
        if pattern_high == level_value and (prior_high is None or pattern_high > prior_high):
            return "cdh_made"

    if level_key == "current_day_low" and direction == "bullish":
        prior_low = min(
            (_number(row.get("low")) for row in prior_bars),
            default=None,
        )
        pattern_low = min(
            (_number(row.get("low")) for row in pattern_bars),
            default=None,
        )
        if pattern_low == level_value and (prior_low is None or pattern_low < prior_low):
            return "cdl_made"

    if direction == "bearish":
        sweep_index = next(
            (
                index
                for index, row in enumerate(pattern_bars)
                if (_number(row.get("high")) or float("-inf")) > level_value
            ),
            None,
        )
        if sweep_index is not None and any(
            (_number(row.get("close")) or float("inf")) < level_value
            for row in pattern_bars[sweep_index:]
        ):
            return "liquidity_swept"

    if direction == "bullish":
        sweep_index = next(
            (
                index
                for index, row in enumerate(pattern_bars)
                if (_number(row.get("low")) or float("inf")) < level_value
            ),
            None,
        )
        if sweep_index is not None and any(
            (_number(row.get("close")) or float("-inf")) > level_value
            for row in pattern_bars[sweep_index:]
        ):
            return "liquidity_swept"

    if str(pattern.get("family") or "") == "2-2-reversal" and any(
        bar_is_at_key_level(row, level_value) for row in pattern_bars
    ):
        return "key_level_proximity"

    return ""


def _family_matches_location_event(*, family: Any, direction: str, location_event: str) -> bool:
    """Keep sweep/failure replay separate from acceptance continuations."""

    family_key = str(family or "").strip().lower()
    if location_event in {"cdh_made", "liquidity_swept"} and direction == "bearish":
        return family_key == "failed_high"
    if location_event in {"cdl_made", "liquidity_swept"} and direction == "bullish":
        return family_key == "failed_low"
    if location_event == "key_level_proximity":
        return family_key == ("failed_high" if direction == "bearish" else "failed_low")
    return False


def evaluate_setup_outcome(
    *,
    signal: Mapping[str, Any],
    future: list[Mapping[str, Any]],
    target: Mapping[str, Any] | None,
) -> dict[str, Any]:
    entry = _number(signal.get("entry_price"))
    if entry is None:
        entry = _number(signal.get("close"))
    level = _number(signal.get("level_value"))
    if entry is None or level is None:
        return {
            "state": "unavailable",
            "setup_status": "unavailable",
            "setup_status_label": "Setup outcome unavailable",
            "target_status": "unavailable",
            "target_status_label": "Target outcome unavailable",
            "opportunity_status": "unavailable",
            "opportunity_label": "Price opportunity unavailable",
            "target_progress_percent": None,
            "mfe": None,
            "mae": None,
        }
    direction = str(signal.get("direction") or "")
    target_value = _number((target or {}).get("value"))
    favorable: list[float] = []
    adverse: list[float] = []
    terminal = "open"
    terminal_at = ""
    evaluated_through = str(signal.get("signal_time") or "")
    for bar in future:
        high = _number(bar.get("high"))
        low = _number(bar.get("low"))
        close = _number(bar.get("close"))
        if high is None or low is None or close is None:
            continue
        evaluated_through = str(bar.get("ts") or evaluated_through)
        if direction == "bullish":
            favorable.append(max(0.0, high - entry))
            adverse.append(max(0.0, entry - low))
            target_hit = target_value is not None and high >= target_value
            invalidated = close < level
        else:
            favorable.append(max(0.0, entry - low))
            adverse.append(max(0.0, high - entry))
            target_hit = target_value is not None and low <= target_value
            invalidated = close > level
        if target_hit and invalidated:
            terminal, terminal_at = "ambiguous", str(bar.get("ts") or "")
            break
        if target_hit:
            terminal, terminal_at = "target_reached", str(bar.get("ts") or "")
            break
        if invalidated:
            terminal, terminal_at = "invalidated", str(bar.get("ts") or "")
            break
    mfe = max(favorable, default=0.0)
    mae = max(adverse, default=0.0)
    target_distance = abs(target_value - entry) if target_value is not None else None
    target_progress = (
        min(100.0, max(0.0, mfe / target_distance * 100.0))
        if target_distance is not None and target_distance > 0
        else None
    )
    if terminal == "target_reached":
        setup_status, setup_label = "valid", "Setup remained valid"
        target_status, target_label = "reached", "Full target reached"
        opportunity_status, opportunity_label = "full_target", "Full target reached"
    elif terminal == "invalidated":
        setup_status, setup_label = "invalidated", "Setup invalidated"
        target_status, target_label = "not_reached", "Target not reached"
        opportunity_status = "favorable_excursion" if mfe > 0 else "limited_follow_through"
        opportunity_label = (
            "Favorable excursion before invalidation"
            if mfe > 0
            else "Limited follow-through before invalidation"
        )
    elif terminal == "ambiguous":
        setup_status, setup_label = "ambiguous", "Setup ordering ambiguous"
        target_status, target_label = "ambiguous", "Target order is ambiguous"
        opportunity_status = "ambiguous"
        opportunity_label = "Target and invalidation touched in the same candle"
    else:
        setup_status, setup_label = "open", "Setup remained open"
        target_status, target_label = "not_reached", "Target not reached"
        opportunity_status = "favorable_excursion" if mfe > 0 else "limited_follow_through"
        opportunity_label = (
            "Favorable excursion — target not reached" if mfe > 0 else "Limited follow-through"
        )
    return {
        "state": terminal,
        "at": terminal_at,
        "evaluated_through": terminal_at or evaluated_through,
        "setup_status": setup_status,
        "setup_status_label": setup_label,
        "target_status": target_status,
        "target_status_label": target_label,
        "opportunity_status": opportunity_status,
        "opportunity_label": opportunity_label,
        "target_progress_percent": target_progress,
        "mfe": mfe,
        "mae": mae,
    }


def _canonical_event_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    pattern = dict(row.get("strat_pattern") or {})
    return (
        str(pattern.get("code") or ""),
        str(pattern.get("completed_at") or row.get("signal_candle_time") or ""),
        str(row.get("direction") or ""),
    )


def _contextual_family_label(row: Mapping[str, Any]) -> str:
    level = dict(row.get("level") or {})
    level_label = str(level.get("label") or "structural level")
    return (
        f"Sweep and reclaim {level_label}"
        if str(row.get("direction") or "") == "bullish"
        else f"Sweep and reject {level_label}"
    )


def _canonicalize_setups(setups: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Collapse anchor-level representations into one completed-pattern event."""

    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for source in setups:
        row = dict(source)
        grouped.setdefault(_canonical_event_key(row), []).append(row)

    canonical: list[dict[str, Any]] = []
    for event_key, rows in grouped.items():
        rows.sort(
            key=lambda row: (
                -int(_number(row.get("score")) or 0),
                float(_number(dict(row.get("level") or {}).get("value")) or float("inf")),
                str(dict(row.get("level") or {}).get("key") or ""),
            )
        )
        primary = copy.deepcopy(rows[0])
        primary_level = dict(primary.get("level") or {})
        primary_value = _number(primary_level.get("value"))
        supporting: list[dict[str, Any]] = []
        excluded_keys = {str(primary_level.get("key") or "")}
        ignored: list[dict[str, Any]] = []
        for row in rows[1:]:
            level = dict(row.get("level") or {})
            value = _number(level.get("value"))
            if value is not None and primary_value is not None and abs(value - primary_value) <= 3:
                supporting.append({**level, "role": "supporting_confluence"})
                excluded_keys.add(str(level.get("key") or ""))
            else:
                ignored.append(level)
        score = int(_number(primary.get("score")) or 0)
        primary["score"] = score
        primary["grade"] = confluence_grade(score)
        primary["supporting_levels"] = supporting
        primary["level_cluster"] = [
            {**primary_level, "role": "primary"},
            *supporting,
        ]
        available = normalize_levels(primary.pop("_available_levels", []))
        entry_value = _number(primary.get("entry_zone"))
        target = _next_target(
            available,
            float(entry_value if entry_value is not None else primary_value or 0),
            str(primary.get("direction") or ""),
            excluded_keys=excluded_keys,
        )
        primary["target"] = target
        primary["actionable"] = target is not None
        primary["target_diagnostic"] = (
            "Nearest directional level at least 5 points from entry"
            if target
            else "No directional level beyond the anchor cluster is at least 5 points from entry"
        )
        components = list(primary.get("score_components") or [])
        for component in components:
            if component.get("key") == "target_space":
                component["earned"] = CONFLUENCE_WEIGHTS["target_space"] if target else 0
                component["status"] = "confirmed" if target else "missing"
        primary["score_components"] = components
        score = sum(int(component.get("earned") or 0) for component in components)
        primary["score"] = score
        primary["quality_score"] = score
        primary["grade"] = confluence_grade(score)
        if ignored:
            primary["ignored_anchor_levels"] = ignored
        primary["family_label"] = _contextual_family_label(primary)
        primary["setup_event_id"] = ":".join(
            (str(primary.get("ticker") or ""), str(primary.get("session_date") or ""), *event_key)
        )
        outcome_signal = {
            "entry_price": primary.get("entry_zone"),
            "level_value": primary_level.get("value"),
            "direction": primary.get("direction"),
            "signal_time": primary.get("signal_time"),
        }
        primary["outcome"] = evaluate_setup_outcome(
            signal=outcome_signal,
            future=primary.pop("_future_bars", []),
            target=target,
        )
        canonical.append(primary)
    return sorted(
        canonical,
        key=lambda row: (
            -int(row.get("score") or 0),
            str(row.get("signal_time") or ""),
            str(row.get("candidate_id") or ""),
        ),
    )


# Backward-compatible private name retained for focused callers and tests.
_outcome = evaluate_setup_outcome


def _build_intraday_setup_analysis(
    *,
    ticker: str,
    session_date: str,
    bars: Iterable[Mapping[str, Any]],
    levels: Iterable[Mapping[str, Any]],
    strategy: Mapping[str, Any] | None = None,
    gamma_regime: str = "",
    gamma_as_of: Any = None,
    gamma_observations: Iterable[Mapping[str, Any]] = (),
    option_references: Mapping[str, Mapping[str, Any]] | None = None,
    include_rejected: bool = False,
) -> dict[str, Any]:
    """Replay scenario transitions without exposing future candles to eligibility."""

    rows = _normalized_bars(bars, session_date)
    level_rows = [dict(row) for row in levels if isinstance(row, Mapping)]
    gamma_rows = [dict(row) for row in gamma_observations if isinstance(row, Mapping)]
    emitted: set[str] = set()
    observed_patterns: set[str] = set()
    setups: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    armed_candidates: dict[str, dict[str, Any]] = {}
    for index in range(1, len(rows)):
        signal_bar = rows[index]
        signal_stamp = _timestamp(signal_bar)
        if signal_stamp is not None and signal_stamp.time() > REPLAY_ENTRY_CUTOFF_ET:
            break
        bars_at_signal = rows[: index + 1]
        gamma_observation = _gamma_at_signal(gamma_rows, signal_stamp)
        observation_levels = list((gamma_observation or {}).get("levels") or [])
        levels_at_signal = _point_in_time_levels([*level_rows, *observation_levels], bars_at_signal)
        gamma_source = gamma_observation
        if not gamma_rows and gamma_as_of:
            gamma_source = {
                "as_of": gamma_as_of,
                "regime": gamma_regime,
                "source": "session_snapshot",
            }
        gamma_context = signal_gamma_context(
            gamma_source,
            signal_stamp,
            default_source="gamma_history",
        )
        point_in_time_gamma = (
            gamma_context["gamma_regime"] if gamma_context["gamma_regime"] != "unavailable" else ""
        )
        rankings = rank_market_scenarios(
            spot=signal_bar["close"],
            levels=levels_at_signal,
            bars=bars_at_signal,
            strategy=strategy,
            gamma_regime=point_in_time_gamma,
            locked=False,
        )
        candidates = list(rankings.get("candidates") or [])
        for armed_id, armed in list(armed_candidates.items()):
            direction = str(armed.get("direction") or "")
            trigger = dict(armed.get("trigger_evidence") or {})
            trigger_price = _number(trigger.get("trigger_price"))
            crossed = bool(
                trigger_price is not None
                and (
                    (_number(signal_bar.get("high")) or trigger_price) > trigger_price
                    if direction == "bullish"
                    else (_number(signal_bar.get("low")) or trigger_price) < trigger_price
                )
            )
            if crossed:
                promoted = copy.deepcopy(armed)
                promoted["state"] = "triggered"
                promoted["lane"] = "active_now"
                score = min(100, int(promoted.get("score") or 0) + CONFLUENCE_WEIGHTS["trigger"])
                promoted["score"] = score
                promoted["grade"] = confluence_grade(score)
                promoted["quality_score"] = score
                promoted["trigger_evidence"] = {
                    **trigger,
                    "triggered": True,
                    "triggered_at": signal_bar["ts"],
                }
                candidates.append(promoted)
                armed_candidates.pop(armed_id, None)
        for candidate in candidates:
            candidate_id = str(candidate.get("id") or "")
            pattern = dict(candidate.get("strat_pattern") or {})
            trigger_evidence = dict(candidate.get("trigger_evidence") or {})
            trigger_time = str(trigger_evidence.get("triggered_at") or "")
            event_id = f"{candidate_id}:{pattern.get('code', '')}:{pattern.get('completed_at', '')}"
            if not candidate_id or event_id in emitted:
                continue
            triggered = str(candidate.get("state") or "") in {"triggered", "confirmed"}
            pattern_completed_now = bool(
                pattern
                and str(pattern.get("completed_at") or "") == str(signal_bar.get("ts") or "")
            )
            anchor = dict(pattern.get("anchor_level") or {})
            observed_id = ":".join(
                (
                    str(pattern.get("code") or ""),
                    str(pattern.get("completed_at") or ""),
                    str(anchor.get("key") or ""),
                )
            )
            direction = str(candidate.get("direction") or "")
            location_event = _pattern_location_event(
                pattern=pattern,
                bars=bars_at_signal,
                direction=direction,
            )
            family_matches_location = _family_matches_location_event(
                family=candidate.get("family"),
                direction=direction,
                location_event=location_event,
            )
            pattern_observed = bool(
                not triggered
                and pattern_completed_now
                and location_event
                and family_matches_location
                and observed_id not in observed_patterns
            )
            immediate_pattern = str(pattern.get("family") or "") in {
                "2-2-reversal",
                "2-1-2",
            }
            trigger_completed_now = bool(
                triggered and trigger_time == str(signal_bar.get("ts") or "")
            )
            immediate_trigger_completed_now = bool(
                immediate_pattern
                and pattern_completed_now
                and trigger_evidence.get("triggered")
                and trigger_time == str(signal_bar.get("ts") or "")
            )
            eligible = bool(
                location_event
                and family_matches_location
                and (trigger_completed_now or immediate_trigger_completed_now)
            )
            if not eligible:
                if include_rejected and (
                    str(candidate.get("lane") or "") == "alternative" or pattern_completed_now
                ):
                    rejected.append(
                        {
                            "candidate_id": candidate_id,
                            "signal_time": signal_bar["ts"],
                            "family": candidate.get("family"),
                            "reason": " · ".join(
                                part
                                for part in (
                                    (
                                        candidate.get("plan", {}).get("trigger")
                                        if pattern
                                        else "No exact 5-minute 2-1-2 or 2-2 Reversal"
                                    ),
                                    (
                                        "No CDH/CDL creation or ordered liquidity sweep "
                                        "or key-level test inside the pattern"
                                        if pattern and not location_event
                                        else ""
                                    ),
                                    (
                                        "Location event belongs to the reversal family, not continuation"
                                        if pattern
                                        and location_event
                                        and not family_matches_location
                                        else ""
                                    ),
                                )
                                if part
                            ),
                        }
                    )
                if pattern_observed and not immediate_pattern:
                    observed_patterns.add(observed_id)
                    armed = copy.deepcopy(candidate)
                    armed["_location_event"] = location_event
                    armed_candidates[observed_id] = armed
                continue
            emitted.add(event_id)
            level = dict(candidate.get("level") or {})
            score = int(
                _number(candidate.get("quality_score")) or _number(candidate.get("score")) or 0
            )
            frozen = {
                "candidate_id": candidate_id,
                "setup_event_id": event_id,
                "ticker": str(ticker or "").upper(),
                "session_date": session_date,
                "signal_time": trigger_time or signal_bar["ts"],
                "signal_candle_time": str(pattern.get("completed_at") or signal_bar["ts"]),
                "direction": candidate.get("direction"),
                "family": candidate.get("family"),
                "family_label": candidate.get("family_label"),
                "level": level,
                "entry_zone": trigger_evidence.get("trigger_price") or signal_bar["close"],
                "entry_basis": (
                    "first_pattern_candle_boundary"
                    if pattern.get("family") == "2-2-reversal"
                    else (
                        "inside_candle_boundary"
                        if pattern.get("family") == "2-1-2"
                        else "later_confirmation_boundary"
                    )
                ),
                "estimated_tp_ladder": _estimated_scalp_targets(
                    trigger_evidence.get("trigger_price") or signal_bar["close"],
                    str(candidate.get("direction") or ""),
                    dict(
                        (option_references or {}).get(str(candidate.get("direction") or "")) or {}
                    ),
                ),
                "confirmation": candidate.get("plan", {}).get("trigger"),
                "invalidation": candidate.get("plan", {}).get("cancel"),
                "target": None,
                "score": score,
                "grade": confluence_grade(score),
                "score_components": list(candidate.get("score_components") or []),
                "strat_pattern": dict(candidate.get("strat_pattern") or {}),
                **gamma_context,
                "data_availability": {
                    "bars_as_of": signal_bar["ts"],
                    "gamma_as_of": gamma_context["gamma_as_of"],
                    "gamma_available_at_signal": gamma_context["gamma_status"] == "captured",
                    "level_as_of": anchor.get("as_of") or "",
                },
                "label": "Potential setup · not recorded execution",
                "replay_status": "triggered",
                "live_gate_complete": True,
                "location_event": location_event,
                "trigger_evidence": trigger_evidence,
                "_available_levels": levels_at_signal,
                "_future_bars": rows[index + 1 :],
            }
            setups.append(frozen)
    setups = _canonicalize_setups(setups)
    latest_signal_time = max(
        (str(row.get("signal_time") or "") for row in setups),
        default="",
    )
    return {
        "ticker": str(ticker or "").upper(),
        "session_date": session_date,
        "bar_count": len(rows),
        "setup_count": len(setups),
        "latest_signal_time": latest_signal_time,
        "setups": setups,
        "rejected": rejected if include_rejected else [],
        "entry_cutoff_label": "3:15 PM ET",
        "read_only": True,
        "disclaimer": "Potential setups only; no trades or journal entries were created.",
    }


def build_intraday_setup_events(
    *,
    ticker: str,
    session_date: str,
    bars: Iterable[Mapping[str, Any]],
    levels: Iterable[Mapping[str, Any]],
    strategy: Mapping[str, Any] | None = None,
    gamma_regime: str = "",
    gamma_as_of: Any = None,
    gamma_observations: Iterable[Mapping[str, Any]] = (),
    option_references: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Return the shared point-in-time eligible event stream for Live and Replay."""

    analysis = _build_intraday_setup_analysis(
        ticker=ticker,
        session_date=session_date,
        bars=bars,
        levels=levels,
        strategy=strategy,
        gamma_regime=gamma_regime,
        gamma_as_of=gamma_as_of,
        gamma_observations=gamma_observations,
        option_references=option_references,
        include_rejected=False,
    )
    events: list[dict[str, Any]] = []
    for setup in analysis["setups"]:
        event = copy.deepcopy(setup)
        event.pop("outcome", None)
        events.append(event)
    return events


def build_intraday_setup_replay(
    *,
    ticker: str,
    session_date: str,
    bars: Iterable[Mapping[str, Any]],
    levels: Iterable[Mapping[str, Any]],
    strategy: Mapping[str, Any] | None = None,
    gamma_regime: str = "",
    gamma_as_of: Any = None,
    gamma_observations: Iterable[Mapping[str, Any]] = (),
    option_references: Mapping[str, Mapping[str, Any]] | None = None,
    include_rejected: bool = False,
) -> dict[str, Any]:
    """Return replay outcomes built from the shared point-in-time event analysis."""

    return _build_intraday_setup_analysis(
        ticker=ticker,
        session_date=session_date,
        bars=bars,
        levels=levels,
        strategy=strategy,
        gamma_regime=gamma_regime,
        gamma_as_of=gamma_as_of,
        gamma_observations=gamma_observations,
        option_references=option_references,
        include_rejected=include_rejected,
    )
