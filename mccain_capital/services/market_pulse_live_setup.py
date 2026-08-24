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
    level = dict(candidate.get("level") or {})
    pattern = dict(candidate.get("strat_pattern") or {})
    value = _number(level.get("value"))
    identity = {
        "session": str(session_id or ""),
        "ticker": str(ticker or "").upper(),
        "family": str(candidate.get("family") or ""),
        "direction": str(candidate.get("direction") or ""),
        "level_key": str(level.get("key") or ""),
        "level_value": round(value, 4) if value is not None else None,
        "pattern_code": str(pattern.get("code") or ""),
        "pattern_completed_at": str(pattern.get("completed_at") or ""),
    }
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
    return {"version": 1, "setups": {}, "deliveries": {}, "recent": []}


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
    return payload


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
    now: datetime,
    ledger_path: str,
    cutoff: Any = None,
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
            previous = dict((ledger.get("setups") or {}).get(setup_id) or {})
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
                "setup_id": setup_id,
                "revision": revision,
                "state": desired.value,
                "candidate_id": primary.get("id"),
                "family": primary.get("family"),
                "family_label": primary.get("family_label"),
                "direction": primary.get("direction"),
                "level": copy.deepcopy(primary.get("level") or {}),
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
                if alert_id not in deliveries:
                    deliveries[alert_id] = {
                        "setup_id": setup_id,
                        "channel": "in_app",
                        "delivered_at": now_et.isoformat(),
                    }
                    deliver_now = True
                record["alert_status"] = "delivered"
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
            base["recent"] = list(ledger.get("recent") or [])[:8]
            if eligible:
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
