"""Sanitized Market Pulse operational health and bounded event history."""

from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta
from threading import Lock
from typing import Any


_EVENT_FIELDS = {"at", "event", "ticker", "generation_id", "component", "outcome", "reason"}
_EVENTS: deque[dict[str, str]] = deque(maxlen=200)
_LOCK = Lock()


def record_reliability_event(**values: Any) -> dict[str, str]:
    event = {
        key: str(values.get(key) or "")[:160]
        for key in _EVENT_FIELDS
        if key in values
    }
    with _LOCK:
        _EVENTS.append(event)
    return dict(event)


def recent_reliability_events(limit: int = 25) -> list[dict[str, str]]:
    with _LOCK:
        return [dict(row) for row in list(_EVENTS)[-max(0, min(limit, 200)) :]]


def clear_reliability_events() -> None:
    with _LOCK:
        _EVENTS.clear()


def build_operational_health(
    *,
    freshness: dict[str, Any] | None,
    refresh_contract: dict[str, Any] | None,
    live_setup_monitor: dict[str, Any] | None,
    now: datetime,
    last_attempt: str = "",
    consecutive_failures: int = 0,
    worker_generation_id: str = "",
    canonical_persistence_healthy: bool = True,
) -> dict[str, Any]:
    fresh = dict(freshness or {})
    contract = dict(refresh_contract or {})
    monitor = dict(live_setup_monitor or {})
    generation_id = str(fresh.get("generation_id") or "")
    blockers = list(dict.fromkeys(str(x) for x in fresh.get("stale_required_components") or []))
    locked = bool(fresh.get("execution_locked")) or bool(blockers)
    setup_persistence = bool((monitor.get("persistence") or {}).get("healthy", True))
    persistence_healthy = bool(canonical_persistence_healthy and setup_persistence)
    worker_generation = worker_generation_id or str(fresh.get("source_generation_id") or generation_id)
    adopted = not generation_id or worker_generation == generation_id
    phase = str(contract.get("market_phase") or "closed")
    failures = max(0, int(consecutive_failures or 0))

    if not persistence_healthy or (phase == "open" and locked):
        status = "critical"
    elif failures or not adopted or blockers:
        status = "warning"
    elif phase != "open":
        status = "paused"
    else:
        status = "healthy"

    retry_seconds = max(0, int(contract.get("next_check_seconds") or 0))
    retry_deadline = (now + timedelta(seconds=retry_seconds)).isoformat() if retry_seconds else ""
    return {
        "status": status,
        "generation_id": generation_id,
        "last_successful_promotion": str(fresh.get("generated_at") or ""),
        "last_attempt": str(last_attempt or now.isoformat()),
        "consecutive_failures": failures,
        "retry_deadline": retry_deadline,
        "blocking_components": blockers,
        "worker_adoption": {
            "adopted": adopted,
            "generation_id": worker_generation,
        },
        "persistence": {
            "healthy": persistence_healthy,
            "canonical": bool(canonical_persistence_healthy),
            "alert_ledger": setup_persistence,
        },
        "market_phase": phase,
        "summary": _summary(status, blockers, adopted, persistence_healthy),
    }


def _summary(status: str, blockers: list[str], adopted: bool, persistence: bool) -> str:
    if status == "healthy":
        return "Pipeline healthy"
    if status == "paused":
        return "Checks paused outside the exchange session"
    if not persistence:
        return "Persistence unavailable; execution remains locked"
    if blockers:
        return f"Waiting for current {', '.join(blockers)}"
    if not adopted:
        return "Worker is adopting the latest verified generation"
    return "Automatic recovery is in progress"
