"""Point-in-time-safe intraday Market Pulse setup replay."""

from __future__ import annotations

from datetime import datetime, time
import copy
import math
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from mccain_capital.services.market_pulse_scenarios import (
    CONFLUENCE_WEIGHTS,
    confluence_grade,
    normalize_levels,
    rank_market_scenarios,
)


ET = ZoneInfo("America/New_York")
REPLAY_ENTRY_CUTOFF_ET = time(15, 30)
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


def _next_target(levels: list[Any], value: float, direction: str) -> dict[str, Any] | None:
    candidates = (
        [level for level in levels if level.value > value]
        if direction == "bullish"
        else [level for level in levels if level.value < value]
    )
    if not candidates:
        return None
    target = min(candidates, key=lambda level: abs(level.value - value))
    return {"key": target.key, "label": target.label, "value": target.value}


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
    for source in levels:
        row = dict(source)
        key = str(row.get("key") or "").strip().lower()
        if key in DYNAMIC_LEVEL_KEYS:
            observed_at = _timestamp({"ts": row.get("as_of")})
            if observed_at is None or signal_stamp is None or observed_at > signal_stamp:
                continue
        if key == "current_day_high" and current_day_high is not None:
            row["value"] = current_day_high
        elif key == "current_day_low" and current_day_low is not None:
            row["value"] = current_day_low
        point_in_time.append(row)
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

    return ""


def _family_matches_location_event(
    *, family: Any, direction: str, location_event: str
) -> bool:
    """Keep sweep/failure replay separate from acceptance continuations."""

    family_key = str(family or "").strip().lower()
    if location_event in {"cdh_made", "liquidity_swept"} and direction == "bearish":
        return family_key == "failed_high"
    if location_event in {"cdl_made", "liquidity_swept"} and direction == "bullish":
        return family_key == "failed_low"
    return False


def _outcome(
    *,
    signal: Mapping[str, Any],
    future: list[Mapping[str, Any]],
    target: Mapping[str, Any] | None,
) -> dict[str, Any]:
    entry = _number(signal.get("close"))
    level = _number(signal.get("level_value"))
    if entry is None or level is None:
        return {"state": "unavailable", "mfe": None, "mae": None}
    direction = str(signal.get("direction") or "")
    target_value = _number((target or {}).get("value"))
    favorable: list[float] = []
    adverse: list[float] = []
    terminal = "open"
    terminal_at = ""
    for bar in future:
        high = _number(bar.get("high"))
        low = _number(bar.get("low"))
        if high is None or low is None:
            continue
        if direction == "bullish":
            favorable.append(high - entry)
            adverse.append(entry - low)
            target_hit = target_value is not None and high >= target_value
            invalidated = low <= level
        else:
            favorable.append(entry - low)
            adverse.append(high - entry)
            target_hit = target_value is not None and low <= target_value
            invalidated = high >= level
        if target_hit and invalidated:
            terminal, terminal_at = "ambiguous", str(bar.get("ts") or "")
            break
        if target_hit:
            terminal, terminal_at = "target_reached", str(bar.get("ts") or "")
            break
        if invalidated:
            terminal, terminal_at = "invalidated", str(bar.get("ts") or "")
            break
    return {
        "state": terminal,
        "at": terminal_at,
        "mfe": max(favorable, default=0.0),
        "mae": max(adverse, default=0.0),
    }


def _dedupe_and_rank_setups(setups: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return one strongest row for each pattern, completion time, and anchor."""

    strongest: dict[tuple[str, str, str], dict[str, Any]] = {}
    for source in setups:
        row = dict(source)
        pattern = dict(row.get("strat_pattern") or {})
        anchor = dict(pattern.get("anchor_level") or {})
        key = (
            str(pattern.get("code") or ""),
            str(pattern.get("completed_at") or row.get("signal_time") or ""),
            str(anchor.get("key") or dict(row.get("level") or {}).get("key") or ""),
        )
        current = strongest.get(key)
        row_rank = (bool(row.get("live_gate_complete")), int(row.get("score") or 0))
        current_rank = (
            bool(current.get("live_gate_complete")),
            int(current.get("score") or 0),
        ) if current else (False, -1)
        if current is None or row_rank > current_rank:
            strongest[key] = row
    return sorted(
        strongest.values(),
        key=lambda row: (
            -int(row.get("score") or 0),
            str(row.get("signal_time") or ""),
            str(row.get("candidate_id") or ""),
        ),
    )


def build_intraday_setup_replay(
    *,
    ticker: str,
    session_date: str,
    bars: Iterable[Mapping[str, Any]],
    levels: Iterable[Mapping[str, Any]],
    strategy: Mapping[str, Any] | None = None,
    gamma_regime: str = "",
    gamma_as_of: Any = None,
    include_rejected: bool = False,
) -> dict[str, Any]:
    """Replay scenario transitions without exposing future candles to eligibility."""

    rows = _normalized_bars(bars, session_date)
    level_rows = [dict(row) for row in levels if isinstance(row, Mapping)]
    gamma_stamp = None
    if gamma_as_of:
        gamma_stamp = _timestamp({"ts": gamma_as_of})
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
        levels_at_signal = _point_in_time_levels(level_rows, bars_at_signal)
        normalized_levels_at_signal = normalize_levels(levels_at_signal)
        point_in_time_gamma = (
            gamma_regime
            if gamma_stamp is not None and signal_stamp is not None and gamma_stamp <= signal_stamp
            else ""
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
            event_id = (
                f"{candidate_id}:{pattern.get('code', '')}:"
                f"{pattern.get('completed_at', '')}:{trigger_time}"
            )
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
            trigger_completed_now = bool(triggered and trigger_time == str(signal_bar.get("ts") or ""))
            eligible = bool(location_event and family_matches_location and trigger_completed_now)
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
                                        "inside the pattern"
                                        if pattern and not location_event
                                        else ""
                                    ),
                                    (
                                        "Location event belongs to the reversal family, not continuation"
                                        if pattern and location_event and not family_matches_location
                                        else ""
                                    ),
                                )
                                if part
                            ),
                        }
                    )
                if pattern_observed:
                    observed_patterns.add(observed_id)
                    armed = copy.deepcopy(candidate)
                    armed["_location_event"] = location_event
                    armed_candidates[observed_id] = armed
                continue
            emitted.add(event_id)
            level = dict(candidate.get("level") or {})
            target = _next_target(
                normalized_levels_at_signal,
                float(level.get("value")),
                str(candidate.get("direction") or ""),
            )
            frozen = {
                "candidate_id": candidate_id,
                "setup_event_id": event_id,
                "ticker": str(ticker or "").upper(),
                "session_date": session_date,
                "signal_time": trigger_time or signal_bar["ts"],
                "direction": candidate.get("direction"),
                "family": candidate.get("family"),
                "family_label": candidate.get("family_label"),
                "level": level,
                "entry_zone": trigger_evidence.get("trigger_price") or signal_bar["close"],
                "confirmation": candidate.get("plan", {}).get("trigger"),
                "invalidation": candidate.get("plan", {}).get("cancel"),
                "target": target,
                "score": candidate.get("quality_score"),
                "grade": candidate.get("grade"),
                "score_components": list(candidate.get("score_components") or []),
                "strat_pattern": dict(candidate.get("strat_pattern") or {}),
                "data_availability": {
                    "bars_as_of": signal_bar["ts"],
                    "gamma_as_of": gamma_stamp.isoformat() if gamma_stamp else "",
                    "gamma_available_at_signal": bool(point_in_time_gamma),
                    "level_as_of": anchor.get("as_of") or "",
                },
                "label": "Potential setup · not recorded execution",
                "replay_status": "triggered",
                "live_gate_complete": True,
                "location_event": location_event,
                "trigger_evidence": trigger_evidence,
            }
            outcome_signal = {
                "close": signal_bar["close"],
                "level_value": level.get("value"),
                "direction": candidate.get("direction"),
            }
            frozen["outcome"] = _outcome(
                signal=outcome_signal,
                future=rows[index + 1 :],
                target=target,
            )
            setups.append(frozen)
    setups = _dedupe_and_rank_setups(setups)
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
        "entry_cutoff_label": "3:30 PM ET",
        "read_only": True,
        "disclaimer": "Potential setups only; no trades or journal entries were created.",
    }
