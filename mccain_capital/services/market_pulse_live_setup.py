"""Durable, canonical live-setup monitoring for the SPX Market Pulse page."""

from __future__ import annotations

import copy
from datetime import datetime, time, timedelta
from enum import StrEnum
import fcntl
import hashlib
import json
import math
import os
import tempfile
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from mccain_capital.services.market_pulse_setup_replay import evaluate_setup_outcome


ET = ZoneInfo("America/New_York")
DEFAULT_CUTOFF = time(15, 15)
MINIMUM_ALERT_SCORE = 76
TERMINAL_STATES = {"TARGET_REACHED", "INVALIDATED", "EXPIRED"}


class LiveSetupState(StrEnum):
    WATCHING = "WATCHING"
    ARMED = "ARMED"
    CONFIRMED = "CONFIRMED"
    TARGET_REACHED = "TARGET_REACHED"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ET)
    return parsed.astimezone(ET)


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def parse_cutoff(value: Any) -> time:
    """Return a safe ET cutoff, falling back to 3:15 PM."""

    raw = str(value or "").strip()
    if not raw:
        return DEFAULT_CUTOFF
    for pattern in ("%H:%M", "%I:%M %p"):
        try:
            return datetime.strptime(raw, pattern).time()
        except ValueError:
            continue
    return DEFAULT_CUTOFF


def setup_identity(*, session_id: str, ticker: str, candidate: Mapping[str, Any]) -> str:
    pattern = dict(candidate.get("strat_pattern") or {})
    pattern_code = str(pattern.get("code") or "")
    pattern_completed_at = str(pattern.get("completed_at") or "")
    identity = {
        "session": str(session_id or ""),
        "ticker": str(ticker or "").upper(),
        "direction": str(candidate.get("direction") or ""),
        "pattern_code": pattern_code,
        "pattern_completed_at": pattern_completed_at,
    }
    if not pattern_code or not pattern_completed_at:
        level = dict(candidate.get("level") or {})
        value = _number(level.get("value"))
        identity.update(
            {
                "family": str(candidate.get("family") or ""),
                "level_key": str(level.get("key") or ""),
                "level_value": round(value, 4) if value is not None else None,
            }
        )
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:20]
    return f"setup:{identity['ticker']}:{identity['session']}:{digest}"


def alert_identity(setup_id: str, *, channel: str = "in_app") -> str:
    raw = f"{setup_id}|CONFIRMED|{channel}".encode("utf-8")
    return f"alert:{hashlib.sha256(raw).hexdigest()[:24]}"


def _latest_completed_bar(bars: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    parsed: list[tuple[datetime, dict[str, Any]]] = []
    for row in bars:
        if not isinstance(row, Mapping):
            continue
        stamp = _parse_timestamp(
            row.get("ts") or row.get("timestamp") or row.get("time") or row.get("datetime")
        )
        if stamp is not None:
            parsed.append((stamp, dict(row)))
    if not parsed:
        return {}
    stamp, row = max(parsed, key=lambda item: item[0])
    row["completed_at"] = stamp.isoformat()
    return row


def _candidate_state(candidate: Mapping[str, Any]) -> LiveSetupState:
    state = str(candidate.get("state") or "").lower()
    lane = str(candidate.get("lane") or "").lower()
    if state in {"triggered", "confirmed"} or lane == "active_now":
        return (
            LiveSetupState.CONFIRMED
            if dict(candidate.get("strat_pattern") or {}).get("code")
            else LiveSetupState.ARMED
        )
    if state in {"trigger_armed", "armed"} or lane == "alternative":
        return LiveSetupState.ARMED
    return LiveSetupState.WATCHING


def _candidate_rank(candidate: Mapping[str, Any]) -> tuple[Any, ...]:
    state_order = {
        LiveSetupState.CONFIRMED: 0,
        LiveSetupState.ARMED: 1,
        LiveSetupState.WATCHING: 2,
    }
    lane_order = {"active_now": 0, "alternative": 1, "dormant": 2}
    state = _candidate_state(candidate)
    return (
        state_order[state],
        lane_order.get(str(candidate.get("lane") or ""), 3),
        -int(_number(candidate.get("score")) or 0),
        float(_number(candidate.get("distance")) or float("inf")),
        0 if str(candidate.get("family") or "").startswith("local_flip") else 1,
        str(candidate.get("id") or ""),
    )


def _candidate_summary(
    candidate: Mapping[str, Any], *, session_id: str, ticker: str
) -> dict[str, Any]:
    return {
        "setup_id": setup_identity(session_id=session_id, ticker=ticker, candidate=candidate),
        "state": _candidate_state(candidate).value,
        "family": candidate.get("family"),
        "family_label": candidate.get("family_label"),
        "direction": candidate.get("direction"),
        "level": copy.deepcopy(candidate.get("level") or {}),
        "score": int(_number(candidate.get("score")) or 0),
        "grade": candidate.get("grade") or "—",
        "distance": _number(candidate.get("distance")),
        "strat_pattern": copy.deepcopy(candidate.get("strat_pattern") or {}),
        "next_event": (candidate.get("plan") or {}).get("trigger") or "Confirmation required",
    }


def _terminal_state(
    previous: Mapping[str, Any], candidate: Mapping[str, Any], latest_bar: Mapping[str, Any]
) -> LiveSetupState | None:
    if str(previous.get("state") or "") not in {"CONFIRMED", "TARGET_REACHED", "INVALIDATED"}:
        return None
    if str(previous.get("state") or "") in TERMINAL_STATES:
        return LiveSetupState(str(previous["state"]))
    bar_time = _parse_timestamp(latest_bar.get("completed_at"))
    confirmed_at = _parse_timestamp(previous.get("confirmed_at"))
    if bar_time is None or confirmed_at is None or bar_time <= confirmed_at:
        return None
    direction = str(candidate.get("direction") or "")
    level_value = _number((candidate.get("level") or {}).get("value"))
    target_value = _number((candidate.get("target_level") or {}).get("value"))
    close = _number(latest_bar.get("close") or latest_bar.get("c") or latest_bar.get("v"))
    high = _number(latest_bar.get("high") or latest_bar.get("h"))
    low = _number(latest_bar.get("low") or latest_bar.get("l"))
    if direction == "bullish":
        if level_value is not None and close is not None and close < level_value:
            return LiveSetupState.INVALIDATED
        if target_value is not None and high is not None and high >= target_value:
            return LiveSetupState.TARGET_REACHED
    if direction == "bearish":
        if level_value is not None and close is not None and close > level_value:
            return LiveSetupState.INVALIDATED
        if target_value is not None and low is not None and low <= target_value:
            return LiveSetupState.TARGET_REACHED
    return None


def _empty_ledger() -> dict[str, Any]:
    return {
        "version": 2,
        "setups": {},
        "deliveries": {},
        "recent": [],
        "processed_completed_candles": {},
    }


def _read_ledger(path: str) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        return _empty_ledger()
    if not isinstance(payload, dict):
        raise OSError("Live setup ledger is invalid.")
    payload.setdefault("setups", {})
    payload.setdefault("deliveries", {})
    payload.setdefault("recent", [])
    payload.setdefault("processed_completed_candles", {})
    return payload


def _event_candidate(event: Mapping[str, Any]) -> dict[str, Any]:
    """Adapt a frozen replay event to the existing durable setup identity."""

    target = dict(event.get("target") or {})
    return {
        "id": event.get("candidate_id"),
        "family": event.get("family"),
        "family_label": event.get("family_label"),
        "direction": event.get("direction"),
        "state": "confirmed",
        "lane": "active_now",
        "score": event.get("score"),
        "grade": event.get("grade"),
        "level": copy.deepcopy(event.get("level") or {}),
        "supporting_levels": copy.deepcopy(event.get("supporting_levels") or []),
        "target_level": target,
        "strat_pattern": copy.deepcopy(event.get("strat_pattern") or {}),
        "trigger_evidence": copy.deepcopy(event.get("trigger_evidence") or {}),
        "plan": {
            "trigger": event.get("confirmation"),
            "cancel": event.get("invalidation"),
            "target": target.get("label") if target else None,
        },
    }


def _freeze_event_facts(
    record: Mapping[str, Any], event: Mapping[str, Any], *, generation_id: str
) -> dict[str, Any]:
    """Apply canonical event facts without touching lifecycle or delivery history."""

    frozen = dict(record)
    same_event = bool(
        record.get("setup_event_id") and record.get("setup_event_id") == event.get("setup_event_id")
    )
    supporting_levels = copy.deepcopy(record.get("supporting_levels") or [])
    known_support = {
        str(dict(level).get("key") or "")
        for level in supporting_levels
        if isinstance(level, Mapping)
    }
    for level in event.get("supporting_levels") or []:
        key = str(dict(level).get("key") or "") if isinstance(level, Mapping) else ""
        if key and key not in known_support:
            supporting_levels.append(copy.deepcopy(level))
            known_support.add(key)
    if not same_event:
        frozen.update(
            {
                "setup_event_id": event.get("setup_event_id"),
                "candidate_id": event.get("candidate_id"),
                "family": event.get("family"),
                "family_label": event.get("family_label"),
                "direction": event.get("direction"),
                "level": copy.deepcopy(event.get("level") or {}),
                "entry_zone": event.get("entry_zone"),
                "entry_basis": event.get("entry_basis"),
                "supporting_levels": supporting_levels,
                "target_level": copy.deepcopy(event.get("target") or {}),
                "score": int(_number(event.get("score")) or 0),
                "grade": event.get("grade") or "—",
                "trigger": event.get("confirmation") or "Confirmation recorded",
                "invalidation": event.get("invalidation") or "Review invalidation unavailable",
                "target": (event.get("target") or {}).get("label") or "Review target unavailable",
                "strat_pattern": copy.deepcopy(event.get("strat_pattern") or {}),
                "trigger_evidence": copy.deepcopy(event.get("trigger_evidence") or {}),
                "evidence_at": event.get("signal_time"),
                "confirmed_at": frozen.get("confirmed_at") or event.get("signal_time"),
            }
        )
    frozen["supporting_levels"] = supporting_levels
    frozen["generation_id"] = generation_id
    return frozen


def _reconcile_event_lifecycles(
    *,
    ledger: dict[str, Any],
    events: Iterable[Mapping[str, Any]],
    bars: Iterable[Mapping[str, Any]],
    session_id: str,
    ticker: str,
    generation_id: str,
    now_et: datetime,
) -> list[dict[str, Any]]:
    """Resolve every stable event with Replay's forward completed-candle semantics."""

    ordered_bars = sorted(
        (dict(row) for row in bars if isinstance(row, Mapping)),
        key=lambda row: _parse_timestamp(row.get("ts") or row.get("timestamp"))
        or datetime.min.replace(tzinfo=ET),
    )
    reconciled: list[dict[str, Any]] = []
    setups = ledger.setdefault("setups", {})
    for event in events:
        candidate = _event_candidate(event)
        setup_id = setup_identity(session_id=session_id, ticker=ticker, candidate=candidate)
        previous = dict(setups.get(setup_id) or {})
        if not previous or previous.get("setup_event_id") != event.get("setup_event_id"):
            continue
        record = _freeze_event_facts(previous, event, generation_id=generation_id)
        signal_at = _parse_timestamp(event.get("signal_time"))
        future = [
            row
            for row in ordered_bars
            if signal_at is not None
            and (_parse_timestamp(row.get("ts") or row.get("timestamp")) or signal_at) > signal_at
        ]
        outcome = evaluate_setup_outcome(
            signal={
                "entry_price": event.get("entry_zone"),
                "level_value": (event.get("level") or {}).get("value"),
                "direction": event.get("direction"),
                "signal_time": event.get("signal_time"),
            },
            future=future,
            target=event.get("target"),
        )
        previous_state = str(previous.get("state") or "")
        outcome_state = str(outcome.get("state") or "")
        resolved_state = {
            "target_reached": LiveSetupState.TARGET_REACHED.value,
            "invalidated": LiveSetupState.INVALIDATED.value,
        }.get(outcome_state, LiveSetupState.CONFIRMED.value)
        terminal = previous_state in TERMINAL_STATES
        if terminal:
            resolved_state = previous_state
        if terminal:
            record["outcome"] = copy.deepcopy(previous.get("outcome") or {})
            record["outcome_state"] = previous.get("outcome_state")
        else:
            record["outcome"] = copy.deepcopy(outcome)
            record["outcome_state"] = outcome_state
        record["state"] = resolved_state
        if resolved_state != previous_state:
            record["revision"] = int(previous.get("revision") or 0) + 1
            record["state_changed_at"] = str(outcome.get("at") or now_et.isoformat())
        setups[setup_id] = record
        reconciled.append(record)
    return reconciled


def _persist_setup_events(
    *,
    ledger: dict[str, Any],
    events: Iterable[Mapping[str, Any]],
    session_id: str,
    ticker: str,
    generation_id: str,
    latest_completed_at: str,
    now_et: datetime,
) -> list[dict[str, Any]]:
    """Idempotently persist every unseen point-in-time event as durable history."""

    setups = ledger.setdefault("setups", {})
    recent = ledger.setdefault("recent", [])
    persisted: list[dict[str, Any]] = []
    ordered = sorted(
        events,
        key=lambda row: (
            str(row.get("signal_time") or ""),
            str(row.get("setup_event_id") or ""),
        ),
    )
    for event in ordered:
        event_id = str(event.get("setup_event_id") or "")
        if not event_id:
            continue
        candidate = _event_candidate(event)
        setup_id = setup_identity(session_id=session_id, ticker=ticker, candidate=candidate)
        previous = dict(setups.get(setup_id) or {})
        if previous.get("setup_event_id") == event_id:
            previous = _freeze_event_facts(previous, event, generation_id=generation_id)
            setups[setup_id] = previous
            persisted.append(previous)
            continue
        if previous.get("setup_event_id"):
            existing_levels = list(previous.get("supporting_levels") or [])
            alternate_level = copy.deepcopy(event.get("level") or {})
            known_keys = {
                str(dict(previous.get("level") or {}).get("key") or ""),
                *(str(dict(level).get("key") or "") for level in existing_levels),
            }
            if alternate_level and str(alternate_level.get("key") or "") not in known_keys:
                existing_levels.append({**alternate_level, "role": "supporting_confluence"})
                previous["supporting_levels"] = existing_levels
                setups[setup_id] = previous
            persisted.append(previous)
            continue
        signal_time = str(event.get("signal_time") or "")
        late_review_only = bool(signal_time and signal_time != latest_completed_at)
        state = str(previous.get("state") or "CONFIRMED")
        if state not in TERMINAL_STATES:
            state = "CONFIRMED"
        first_seen_at = str(previous.get("first_seen_at") or now_et.isoformat())
        record = _freeze_event_facts(
            {
                **previous,
                "setup_id": setup_id,
                "revision": int(previous.get("revision") or 0) or 1,
                "state": state,
                "first_seen_at": first_seen_at,
                "state_changed_at": str(previous.get("state_changed_at") or first_seen_at),
                "late_review_only": late_review_only,
                "alert_eligible": False,
                "alert_status": "late_review_only" if late_review_only else "recorded",
                "acknowledged_at": previous.get("acknowledged_at"),
            },
            event,
            generation_id=generation_id,
        )
        setups[setup_id] = record
        persisted.append(record)
        if not previous:
            recent.insert(
                0,
                {
                    "setup_id": setup_id,
                    "setup_event_id": event_id,
                    "state": state,
                    "changed_at": signal_time or now_et.isoformat(),
                    "family_label": record["family_label"],
                    "direction": record["direction"],
                    "score": record["score"],
                    "late_review_only": late_review_only,
                },
            )
    ledger["recent"] = recent[:20]
    ledger.setdefault("processed_completed_candles", {})[session_id] = latest_completed_at
    return persisted


def _write_ledger(path: str, payload: Mapping[str, Any]) -> None:
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    temp_path = ""
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=directory,
            prefix=".live-setup-ledger.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_path = handle.name
            json.dump(payload, handle, separators=(",", ":"), default=str)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)


def evaluate_live_setup_monitor(
    *,
    ticker: str,
    scenario_rankings: Mapping[str, Any],
    canonical_freshness: Mapping[str, Any],
    authoritative_action: Mapping[str, Any],
    bars: Iterable[Mapping[str, Any]],
    setup_events: Iterable[Mapping[str, Any]] | None = None,
    now: datetime,
    ledger_path: str,
    cutoff: Any = None,
    claim_delivery: bool = True,
) -> dict[str, Any]:
    """Evaluate and durably persist one canonical live setup monitor revision."""

    now_et = now.astimezone(ET) if now.tzinfo else now.replace(tzinfo=ET)
    ticker_key = str(ticker or "").upper()
    session_id = str(canonical_freshness.get("session_id") or now_et.date().isoformat())
    generation_id = str(canonical_freshness.get("generation_id") or "")
    locked = bool(canonical_freshness.get("execution_locked"))
    blockers = list(canonical_freshness.get("stale_required_components") or [])
    candidates = [
        dict(row)
        for row in list(scenario_rankings.get("candidates") or [])
        if isinstance(row, Mapping)
    ]
    candidates.sort(key=_candidate_rank)
    primary = candidates[0] if candidates else None
    next_evaluation_at = now_et + timedelta(
        seconds=max(5, int(canonical_freshness.get("sync_interval_seconds") or 15))
    )
    cutoff_time = parse_cutoff(cutoff)
    base = {
        "enabled": ticker_key == "SPX",
        "ticker": ticker_key,
        "session_id": session_id,
        "generation_id": generation_id,
        "evaluated_at": now_et.isoformat(),
        "next_evaluation_at": next_evaluation_at.isoformat(),
        "cutoff_et": cutoff_time.strftime("%I:%M %p").lstrip("0"),
        "fresh": not locked,
        "paused": locked,
        "blockers": blockers,
        "persistence": {"healthy": True, "message": "Durable setup ledger current"},
        "primary": None,
        "secondary": [],
        "recent": [],
        "alert_event": None,
    }
    if ticker_key != "SPX" or not primary:
        return base

    setup_id = setup_identity(session_id=session_id, ticker=ticker_key, candidate=primary)
    latest_bar = _latest_completed_bar(bars)
    evidence_at = str(
        latest_bar.get("completed_at") or canonical_freshness.get("generated_at") or ""
    )
    lock_path = f"{ledger_path}.lock"
    try:
        os.makedirs(os.path.dirname(ledger_path) or ".", exist_ok=True)
        with open(lock_path, "a+", encoding="utf-8") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            ledger = _read_ledger(ledger_path)
            persisted_events = _persist_setup_events(
                ledger=ledger,
                events=setup_events or [],
                session_id=session_id,
                ticker=ticker_key,
                generation_id=generation_id,
                latest_completed_at=str(latest_bar.get("completed_at") or ""),
                now_et=now_et,
            )
            persisted_events = _reconcile_event_lifecycles(
                ledger=ledger,
                events=setup_events or [],
                bars=bars,
                session_id=session_id,
                ticker=ticker_key,
                generation_id=generation_id,
                now_et=now_et,
            )
            previous = dict((ledger.get("setups") or {}).get(setup_id) or {})
            stable_event = bool(previous.get("setup_event_id"))
            known_states = {state.value for state in LiveSetupState}
            if stable_event and str(previous.get("state") or "") in known_states:
                desired = LiveSetupState(str(previous["state"]))
            else:
                desired = _candidate_state(primary)
                terminal = _terminal_state(previous, primary, latest_bar)
                if terminal is not None:
                    desired = terminal
                elif (
                    desired in {LiveSetupState.WATCHING, LiveSetupState.ARMED}
                    and now_et.time() > cutoff_time
                ):
                    desired = LiveSetupState.EXPIRED
            previous_state = str(previous.get("state") or "")
            if previous_state in TERMINAL_STATES:
                desired = LiveSetupState(previous_state)
            revision = int(previous.get("revision") or 0)
            if desired.value != previous_state or not previous:
                revision += 1
            confirmed_at = str(previous.get("confirmed_at") or "")
            if desired == LiveSetupState.CONFIRMED and not confirmed_at:
                confirmed_at = evidence_at
            first_seen_at = str(previous.get("first_seen_at") or now_et.isoformat())
            state_changed_at = (
                now_et.isoformat()
                if desired.value != previous_state
                else str(previous.get("state_changed_at") or first_seen_at)
            )
            plan = dict(primary.get("plan") or {})
            record = {
                **previous,
                "setup_id": setup_id,
                "revision": revision,
                "state": desired.value,
                "candidate_id": primary.get("id"),
                "family": primary.get("family"),
                "family_label": primary.get("family_label"),
                "direction": primary.get("direction"),
                "level": copy.deepcopy(primary.get("level") or {}),
                "supporting_levels": copy.deepcopy(
                    primary.get("supporting_levels") or previous.get("supporting_levels") or []
                ),
                "target_level": copy.deepcopy(primary.get("target_level") or {}),
                "score": int(_number(primary.get("score")) or 0),
                "grade": primary.get("grade") or "—",
                "distance": _number(primary.get("distance")),
                "location": plan.get("wait") or "Waiting for a valid location",
                "trigger": plan.get("trigger") or "Confirmation required",
                "action": plan.get("action") or "No entry until confirmation",
                "invalidation": plan.get("cancel") or "Stand down if confirmation fails",
                "target": plan.get("target") or "Set after confirmation",
                "evidence": copy.deepcopy(primary.get("evidence") or {}),
                "strat_pattern": copy.deepcopy(primary.get("strat_pattern") or {}),
                "evidence_at": evidence_at,
                "confirmed_at": confirmed_at,
                "first_seen_at": first_seen_at,
                "state_changed_at": state_changed_at,
                "generation_id": generation_id,
                "paused": locked,
                "blockers": blockers,
                "late_review_only": False,
                "alert_eligible": False,
                "alert_status": "not_eligible",
                "acknowledged_at": previous.get("acknowledged_at"),
            }
            if stable_event:
                for key in (
                    "setup_event_id",
                    "candidate_id",
                    "family",
                    "family_label",
                    "direction",
                    "level",
                    "supporting_levels",
                    "target_level",
                    "score",
                    "grade",
                    "trigger",
                    "invalidation",
                    "target",
                    "strat_pattern",
                    "trigger_evidence",
                    "evidence_at",
                    "confirmed_at",
                    "outcome",
                    "outcome_state",
                ):
                    if key in previous:
                        record[key] = copy.deepcopy(previous[key])
            confirmation_time = _parse_timestamp(confirmed_at)
            before_cutoff = bool(
                confirmation_time is not None and confirmation_time.time() <= cutoff_time
            )
            permission = str(authoritative_action.get("permission") or "").lower()
            action_state = str(authoritative_action.get("action_state") or "").upper()
            eligible = bool(
                desired == LiveSetupState.CONFIRMED
                and ticker_key == "SPX"
                and record["score"] >= MINIMUM_ALERT_SCORE
                and not locked
                and before_cutoff
                and permission in {"ready", "available", "active", "allowed"}
                and action_state == "ACTIVE"
            )
            if desired == LiveSetupState.CONFIRMED and confirmation_time and not before_cutoff:
                record["late_review_only"] = True
                record["alert_status"] = "late_review_only"
            alert_id = alert_identity(setup_id)
            deliveries = ledger.setdefault("deliveries", {})
            deliver_now = False
            if eligible:
                record["alert_eligible"] = True
                if alert_id not in deliveries and claim_delivery:
                    deliveries[alert_id] = {
                        "setup_id": setup_id,
                        "channel": "in_app",
                        "delivered_at": now_et.isoformat(),
                    }
                    deliver_now = True
                record["alert_status"] = "delivered" if alert_id in deliveries else "pending"
                record["alert_id"] = alert_id
            ledger.setdefault("setups", {})[setup_id] = record
            if desired.value != previous_state:
                recent = list(ledger.get("recent") or [])
                recent.insert(
                    0,
                    {
                        "setup_id": setup_id,
                        "state": desired.value,
                        "changed_at": state_changed_at,
                        "family_label": record["family_label"],
                        "direction": record["direction"],
                        "score": record["score"],
                    },
                )
                ledger["recent"] = recent[:20]
            _write_ledger(ledger_path, ledger)
            state_age = max(
                0,
                int((now_et - (_parse_timestamp(state_changed_at) or now_et)).total_seconds()),
            )
            record["state_age_seconds"] = state_age
            if locked:
                record["action"] = "Paused — no entry while required data is stale"
                record["alert_eligible"] = False
                record["alert_status"] = "blocked_by_freshness"
            base["primary"] = record
            base["secondary"] = [
                _candidate_summary(row, session_id=session_id, ticker=ticker_key)
                for row in candidates[1:6]
            ]
            known_secondary_ids = {str(row.get("setup_id") or "") for row in base["secondary"]}
            for event_record in reversed(persisted_events):
                event_setup_id = str(event_record.get("setup_id") or "")
                if event_setup_id == setup_id or event_setup_id in known_secondary_ids:
                    continue
                base["secondary"].append(copy.deepcopy(event_record))
                known_secondary_ids.add(event_setup_id)
                if len(base["secondary"]) >= 5:
                    break
            base["recent"] = list(ledger.get("recent") or [])[:8]
            if eligible and alert_id in deliveries:
                base["alert_event"] = {
                    "id": alert_id,
                    "setup_id": setup_id,
                    "deliver_now": deliver_now,
                    "title": (
                        f"{record['grade']} "
                        f"{record['strat_pattern'].get('code') or str(record['direction']).title()} "
                        "setup confirmed"
                    ),
                    "text": (
                        f"{record['family_label']} at {record['level'].get('label')} "
                        f"{float(record['level'].get('value') or 0):,.0f}. "
                        f"Invalidation: {record['invalidation']}. Target: {record['target']}."
                    ),
                    "created_at": deliveries[alert_id]["delivered_at"],
                    "priority": "high",
                }
            return base
    except (OSError, TypeError, ValueError) as exc:
        base["persistence"] = {
            "healthy": False,
            "message": f"Alerting paused: {exc}",
        }
        base["paused"] = True
        base["blockers"] = list(dict.fromkeys([*blockers, "setup_persistence"]))
        summary = _candidate_summary(primary, session_id=session_id, ticker=ticker_key)
        summary.update(
            {
                "paused": True,
                "alert_eligible": False,
                "alert_status": "persistence_unavailable",
                "action": "Paused — durable alert state unavailable",
                "revision": 0,
                "generation_id": generation_id,
                "state_age_seconds": 0,
            }
        )
        base["primary"] = summary
        return base


def acknowledge_setup(*, ledger_path: str, setup_id: str, now: datetime) -> bool:
    """Persist acknowledgement without changing lifecycle state."""

    lock_path = f"{ledger_path}.lock"
    os.makedirs(os.path.dirname(ledger_path) or ".", exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        ledger = _read_ledger(ledger_path)
        record = (ledger.get("setups") or {}).get(str(setup_id or ""))
        if not isinstance(record, dict):
            return False
        record["acknowledged_at"] = now.astimezone(ET).isoformat()
        record["revision"] = int(record.get("revision") or 0) + 1
        _write_ledger(ledger_path, ledger)
        return True
