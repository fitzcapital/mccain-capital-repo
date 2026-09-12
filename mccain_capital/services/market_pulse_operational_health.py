"""Durable, sanitized Market Pulse trust diagnostics and incident history."""

from __future__ import annotations

from collections import deque
from datetime import date, datetime, timedelta
import hashlib
import json
import os
from threading import Lock
import threading
from typing import Any

from mccain_capital import runtime as app_runtime

_EVENT_FIELDS = {
    "incident_id",
    "at",
    "event",
    "ticker",
    "generation_id",
    "component",
    "outcome",
    "status",
    "reason",
    "source",
    "started_at",
    "last_seen_at",
    "recovered_at",
    "duration_seconds",
    "failure_count",
    "age_seconds",
    "threshold_seconds",
    "latency_ms",
    "fallback_mode",
    "durable",
}
_EVENTS: deque[dict[str, Any]] = deque(maxlen=200)
_LOCK = Lock()


def _text(value: Any, limit: int = 160) -> str:
    return str(value or "").strip()[:limit]


def _integer(value: Any) -> int | None:
    try:
        return int(float(value)) if value is not None else None
    except (TypeError, ValueError):
        return None


def _read_integer_file(path: str) -> int | None:
    try:
        value = open(path, encoding="utf-8").read().strip()
    except OSError:
        return None
    return int(value) if value.isdigit() else None


def _process_thread_count() -> int:
    try:
        with open("/proc/self/status", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("Threads:"):
                    return max(1, int(line.split(":", 1)[1].strip()))
    except (OSError, ValueError):
        pass
    return max(1, threading.active_count())


def build_resource_diagnostics(
    *, gamma_runtime: dict[str, Any] | None = None, market_phase: str = "open"
) -> dict[str, Any]:
    gamma = dict(gamma_runtime or {})
    process_threads = _process_thread_count()
    container_tasks = _read_integer_file("/sys/fs/cgroup/pids.current") or process_threads
    container_limit = _read_integer_file("/sys/fs/cgroup/pids.max") or 2048
    warning_tasks = max(1, int(os.environ.get("WORKER_TASK_WARNING_THRESHOLD", "1400")))
    critical_tasks = max(
        warning_tasks + 1, int(os.environ.get("WORKER_TASK_CRITICAL_THRESHOLD", "1800"))
    )
    pressure = (
        2 if container_tasks >= critical_tasks else 1 if container_tasks >= warning_tasks else 0
    )
    gamma_age = int(gamma.get("age_seconds") if gamma.get("age_seconds") is not None else -1)
    gamma_max_age = max(60, int(os.environ.get("GAMMA_MONITOR_MAX_AGE_SECONDS", "300")))
    gamma_active = market_phase == "open"
    snapshot_status = _text(gamma.get("snapshot_status") or "invalid")
    gamma_stale = gamma_active and (
        gamma_age < 0 or gamma_age > gamma_max_age or snapshot_status in {"stale", "invalid"}
    )
    gamma_failed = gamma_active and bool(gamma.get("refresh_failed"))
    return {
        "market_open": int(gamma_active),
        "gamma_stale": int(gamma_stale),
        "gamma_refresh_failed": int(gamma_failed),
        "gamma_refresh_in_progress": int(bool(gamma.get("refresh_in_progress"))),
        "gamma_age_seconds": gamma_age,
        "gamma_max_age_seconds": gamma_max_age,
        "worker_threads": process_threads,
        "container_tasks": container_tasks,
        "container_task_limit": container_limit,
        "worker_pressure": pressure,
        "worker_warning_threshold": warning_tasks,
        "worker_critical_threshold": critical_tasks,
        "gamma_error": _text(gamma.get("last_error")),
        "gamma_last_attempt": _text(gamma.get("last_attempted_at"), 64),
    }


def _component_evidence(name: str, value: dict[str, Any] | None) -> dict[str, Any]:
    item = dict(value or {})
    status = _text(item.get("status") or "unavailable").lower()
    completeness = item.get("completeness")
    if completeness is None:
        completeness = 1.0 if status == "current" else 0.0
    return {
        "name": name,
        "required": bool(item.get("required")),
        "status": status,
        "source": _text(item.get("source") or item.get("provider") or item.get("source_label"))
        or "Not reported",
        "provider_timestamp": _text(item.get("as_of") or item.get("provider_timestamp"), 64),
        "received_at": _text(item.get("received_at"), 64),
        "age_seconds": _integer(item.get("age_seconds")),
        "threshold_seconds": _integer(item.get("threshold_seconds")),
        "latency_ms": _integer(item.get("latency_ms")),
        "completeness": max(0.0, min(1.0, float(completeness or 0.0))),
        "fallback_mode": _text(item.get("fallback_mode") or item.get("proxy_mode"), 64),
        "reason_code": _text(item.get("reason_code") or status).lower().replace(" ", "_"),
    }


def build_trust_verdict(
    *,
    freshness: dict[str, Any] | None,
    live_setup_monitor: dict[str, Any] | None = None,
    canonical_persistence_healthy: bool = True,
    worker_generation_id: str = "",
    evaluated_at: datetime | None = None,
) -> dict[str, Any]:
    """Return one fail-closed trust verdict for the canonical generation."""
    fresh = dict(freshness or {})
    monitor = dict(live_setup_monitor or {})
    components = {
        name: _component_evidence(name, value)
        for name, value in dict(fresh.get("components") or {}).items()
        if isinstance(value, dict)
    }
    blockers = list(
        dict.fromkeys(
            _text(value).lower() for value in fresh.get("stale_required_components") or []
        )
    )
    for name, component in components.items():
        if component["required"] and component["status"] != "current" and name not in blockers:
            blockers.append(name)
    setup_persistence = bool((monitor.get("persistence") or {}).get("healthy", True))
    persistence = bool(canonical_persistence_healthy and setup_persistence)
    if not persistence:
        blockers.append("persistence")
    generation_id = _text(fresh.get("generation_id"), 96)
    worker_generation = _text(
        worker_generation_id or fresh.get("source_generation_id") or generation_id, 96
    )
    adopted = not generation_id or generation_id == worker_generation
    if not persistence or bool(fresh.get("execution_locked")) or blockers:
        verdict = "Locked"
    elif not adopted or not generation_id:
        verdict = "Degraded"
    else:
        verdict = "Verified"
    reasons = list(dict.fromkeys(blockers + ([] if adopted else ["worker_adoption"])))
    if verdict == "Verified":
        summary = "All required execution inputs are current and persisted."
    elif verdict == "Degraded":
        summary = "Market context is available, but execution trust is still synchronizing."
    else:
        labels = ", ".join(reason.replace("_", " ") for reason in reasons) or "required data"
        summary = f"Execution is locked while {labels} recover."
    return {
        "verdict": verdict,
        "status": verdict.lower(),
        "summary": summary,
        "reason_codes": reasons,
        "generation_id": generation_id,
        "worker_generation_id": worker_generation,
        "worker_adopted": adopted,
        "evaluated_at": (evaluated_at or datetime.now().astimezone()).isoformat(),
        "last_verified_at": _text(fresh.get("generated_at"), 64) if verdict == "Verified" else "",
        "persistence_healthy": persistence,
        "components": components,
        "setup_evidence": {
            "detection": _text(monitor.get("status") or "no_setup"),
            "alert_ledger_persisted": setup_persistence,
            "replay_evaluated": bool(monitor.get("replay_evaluated", monitor.get("evaluated_at"))),
            "analytics_persisted": bool(monitor.get("analytics_persisted", True)),
            "no_setup_valid": not bool(monitor.get("setup") or monitor.get("trigger")),
        },
    }


def _incident_id(ticker: str, component: str, reason: str, started_at: str) -> str:
    return hashlib.sha256("|".join((ticker, component, reason, started_at)).encode()).hexdigest()[
        :24
    ]


def _daily_upsert(conn: Any, event: dict[str, Any], healthy: bool) -> None:
    session_date = event["at"][:10] if len(event["at"]) >= 10 else date.today().isoformat()
    latency = max(0, _integer(event.get("latency_ms")) or 0)
    conn.execute(
        """INSERT INTO market_pulse_reliability_daily (
        session_date,ticker,component,checks,successful_checks,fresh_checks,failure_count,
        fallback_checks,total_latency_ms,incident_seconds,updated_at)
        VALUES (?,?,?,1,?,?,?, ?,?,0,?)
        ON CONFLICT(session_date,ticker,component) DO UPDATE SET
        checks=checks+1, successful_checks=successful_checks+excluded.successful_checks,
        fresh_checks=fresh_checks+excluded.fresh_checks, failure_count=failure_count+excluded.failure_count,
        fallback_checks=fallback_checks+excluded.fallback_checks,
        total_latency_ms=total_latency_ms+excluded.total_latency_ms, updated_at=excluded.updated_at""",
        (
            session_date,
            event["ticker"],
            event["component"],
            int(healthy),
            int(healthy),
            int(not healthy),
            int(bool(event["fallback_mode"])),
            latency,
            event["at"],
        ),
    )


def _persist_event(event: dict[str, Any]) -> dict[str, Any]:
    conn = app_runtime.db()
    try:
        now = event["at"]
        healthy = event["status"] in {"verified", "healthy", "current", "recovered"}
        _daily_upsert(conn, event, healthy)
        active = conn.execute(
            "SELECT * FROM market_pulse_reliability_events WHERE ticker=? AND component=? AND recovered_at='' ORDER BY started_at DESC LIMIT 1",
            (event["ticker"], event["component"]),
        ).fetchone()
        if healthy:
            if active:
                started = datetime.fromisoformat(str(active["started_at"]))
                observed = datetime.fromisoformat(now)
                recovery_at = max(started, observed).isoformat()
                duration = max(
                    0,
                    int((max(started, observed) - started).total_seconds()),
                )
                conn.execute(
                    "UPDATE market_pulse_reliability_events SET last_seen_at=?,recovered_at=?,duration_seconds=?,alert_recovered=1,updated_at=? WHERE incident_id=?",
                    (recovery_at, recovery_at, duration, recovery_at, active["incident_id"]),
                )
                event.update(
                    {
                        "incident_id": active["incident_id"],
                        "event": "recovered",
                        "duration_seconds": duration,
                    }
                )
            else:
                event.update({"incident_id": "", "event": "verified"})
        elif active and str(active["reason"]) == event["reason"]:
            duration = max(
                0,
                int(
                    (
                        datetime.fromisoformat(now)
                        - datetime.fromisoformat(str(active["started_at"]))
                    ).total_seconds()
                ),
            )
            conn.execute(
                "UPDATE market_pulse_reliability_events SET last_seen_at=?,duration_seconds=?,failure_count=failure_count+1,generation_id=?,age_seconds=?,threshold_seconds=?,latency_ms=?,fallback_mode=?,metadata_json=?,updated_at=? WHERE incident_id=?",
                (
                    now,
                    duration,
                    event["generation_id"],
                    event.get("age_seconds"),
                    event.get("threshold_seconds"),
                    event.get("latency_ms"),
                    event["fallback_mode"],
                    json.dumps(event.get("metadata") or {}, separators=(",", ":")),
                    now,
                    active["incident_id"],
                ),
            )
            event.update(
                {
                    "incident_id": active["incident_id"],
                    "event": "updated",
                    "duration_seconds": duration,
                }
            )
        else:
            if active:
                started = datetime.fromisoformat(str(active["started_at"]))
                observed = datetime.fromisoformat(now)
                recovery_at = max(started, observed).isoformat()
                conn.execute(
                    "UPDATE market_pulse_reliability_events SET recovered_at=?,last_seen_at=?,updated_at=? WHERE incident_id=?",
                    (recovery_at, recovery_at, recovery_at, active["incident_id"]),
                )
            incident_id = _incident_id(event["ticker"], event["component"], event["reason"], now)
            conn.execute(
                """INSERT INTO market_pulse_reliability_events (
            incident_id,ticker,generation_id,component,status,reason,source,started_at,last_seen_at,
            age_seconds,threshold_seconds,latency_ms,fallback_mode,metadata_json,alert_opened,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)""",
                (
                    incident_id,
                    event["ticker"],
                    event["generation_id"],
                    event["component"],
                    event["status"],
                    event["reason"],
                    event["source"],
                    now,
                    now,
                    event.get("age_seconds"),
                    event.get("threshold_seconds"),
                    event.get("latency_ms"),
                    event["fallback_mode"],
                    json.dumps(event.get("metadata") or {}, separators=(",", ":")),
                    now,
                    now,
                ),
            )
            event.update({"incident_id": incident_id, "event": "opened"})
        conn.commit()
        event["durable"] = True
        return event
    finally:
        conn.close()


def record_reliability_event(**values: Any) -> dict[str, Any]:
    event: dict[str, Any] = {
        "at": _text(values.get("at") or datetime.now().astimezone().isoformat(), 64),
        "ticker": _text(values.get("ticker") or "SPX", 16).upper(),
        "generation_id": _text(values.get("generation_id"), 96),
        "component": _text(values.get("component") or "pipeline", 64).lower(),
        "outcome": _text(values.get("outcome"), 64).lower(),
        "status": _text(values.get("status") or values.get("outcome") or "degraded", 32).lower(),
        "reason": _text(values.get("reason") or values.get("outcome") or "unknown", 96).lower(),
        "source": _text(values.get("source"), 96),
        "age_seconds": _integer(values.get("age_seconds")),
        "threshold_seconds": _integer(values.get("threshold_seconds")),
        "latency_ms": _integer(values.get("latency_ms")),
        "fallback_mode": _text(values.get("fallback_mode"), 64),
        "metadata": dict(values.get("metadata") or {}),
        "durable": False,
    }
    try:
        event = _persist_event(event)
    except Exception:
        event["event"] = "fallback"
    sanitized = {key: event.get(key) for key in _EVENT_FIELDS if key in event}
    with _LOCK:
        _EVENTS.append(sanitized)
    # Preserve the small public return contract used by existing monitoring callers.
    return {
        key: sanitized.get(key)
        for key in {"at", "event", "ticker", "generation_id", "component", "outcome", "reason"}
        if sanitized.get(key) not in {None, ""}
    }


def _row_event(row: Any) -> dict[str, Any]:
    return {key: row[key] for key in row.keys() if key != "metadata_json"}


def recent_reliability_events(limit: int = 25) -> list[dict[str, Any]]:
    bounded = max(0, min(int(limit or 25), 200))
    try:
        conn = app_runtime.db()
        rows = conn.execute(
            "SELECT * FROM market_pulse_reliability_events ORDER BY started_at DESC LIMIT ?",
            (bounded,),
        ).fetchall()
        conn.close()
        return [_row_event(row) for row in rows]
    except Exception:
        with _LOCK:
            return [dict(row) for row in list(_EVENTS)[-bounded:]][::-1]


def clear_reliability_events() -> None:
    with _LOCK:
        _EVENTS.clear()


def prune_reliability_history(now: datetime | None = None) -> dict[str, int]:
    current = now or datetime.now().astimezone()
    conn = app_runtime.db()
    try:
        details = conn.execute(
            "DELETE FROM market_pulse_reliability_events WHERE started_at < ?",
            ((current - timedelta(days=30)).isoformat(),),
        ).rowcount
        daily = conn.execute(
            "DELETE FROM market_pulse_reliability_daily WHERE session_date < ?",
            ((current.date() - timedelta(days=180)).isoformat(),),
        ).rowcount
        conn.commit()
        return {"events": max(0, details), "daily": max(0, daily)}
    finally:
        conn.close()


def reliability_history(
    *, ticker: str = "SPX", days: int = 30, limit: int = 100, offset: int = 0
) -> dict[str, Any]:
    ticker = _text(ticker or "SPX", 16).upper()
    days, limit, offset = (
        max(1, min(int(days or 30), 180)),
        max(1, min(int(limit or 100), 200)),
        max(0, int(offset or 0)),
    )
    since = (date.today() - timedelta(days=days - 1)).isoformat()
    conn = app_runtime.db()
    try:
        daily_rows = conn.execute(
            "SELECT * FROM market_pulse_reliability_daily WHERE ticker=? AND session_date>=? ORDER BY session_date,component",
            (ticker, since),
        ).fetchall()
        event_rows = conn.execute(
            "SELECT * FROM market_pulse_reliability_events WHERE ticker=? AND started_at>=? ORDER BY started_at DESC LIMIT ? OFFSET ?",
            (ticker, since, limit, offset),
        ).fetchall()
    finally:
        conn.close()
    daily = [_row_event(row) for row in daily_rows]
    components: dict[str, dict[str, Any]] = {}
    for row in daily:
        item = components.setdefault(
            row["component"],
            {"checks": 0, "successful": 0, "latency_ms": 0, "failures": 0, "fallbacks": 0},
        )
        item["checks"] += int(row["checks"] or 0)
        item["successful"] += int(row["successful_checks"] or 0)
        item["latency_ms"] += int(row["total_latency_ms"] or 0)
        item["failures"] += int(row["failure_count"] or 0)
        item["fallbacks"] += int(row["fallback_checks"] or 0)
    for item in components.values():
        checks = max(1, item["checks"])
        item["availability_pct"] = round(item["successful"] * 100 / checks, 1)
        item["average_latency_ms"] = round(item["latency_ms"] / checks)
    incidents = [_row_event(row) for row in event_rows]
    open_incident = next((row for row in incidents if not row.get("recovered_at")), None)
    current = {
        "verdict": (
            "Locked"
            if open_incident and open_incident.get("status") == "locked"
            else "Degraded" if open_incident else "Verified"
        ),
        "reason": open_incident.get("reason") if open_incident else "no_open_incident",
        "generation_id": open_incident.get("generation_id") if open_incident else "",
        "evidence": "durable_incident_state",
    }
    return {
        "ticker": ticker,
        "days": days,
        "daily": daily,
        "components": components,
        "incidents": incidents,
        "current": current,
        "pagination": {"limit": limit, "offset": offset, "has_more": len(event_rows) == limit},
        "generated_at": datetime.now().astimezone().isoformat(),
    }


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
    from mccain_capital.services.market_pulse_setup_monitor_runtime import (
        get_server_setup_monitor_state,
    )

    fresh, contract = dict(freshness or {}), dict(refresh_contract or {})
    phase = _text(contract.get("market_phase") or "closed")
    server_monitor = get_server_setup_monitor_state(now=now)
    trust = build_trust_verdict(
        freshness=fresh,
        live_setup_monitor=live_setup_monitor,
        canonical_persistence_healthy=canonical_persistence_healthy,
        worker_generation_id=worker_generation_id,
        evaluated_at=now,
    )
    failures = max(0, int(consecutive_failures or 0))
    if phase != "open":
        status, summary = "paused", "Checks paused outside the exchange session"
    elif trust["verdict"] == "Locked":
        status, summary = "critical", trust["summary"]
    elif (
        trust["verdict"] == "Degraded"
        or failures
        or (
            server_monitor.get("enabled")
            and server_monitor.get("status") not in {"healthy", "unchanged"}
        )
    ):
        status = "warning"
        summary = (
            "Setup coverage is synchronizing."
            if server_monitor.get("enabled")
            and server_monitor.get("status") not in {"healthy", "unchanged"}
            else trust["summary"]
        )
    else:
        status, summary = "healthy", "Pipeline verified"
    retry_seconds = max(0, int(contract.get("next_check_seconds") or 0))
    retry_deadline = (now + timedelta(seconds=retry_seconds)).isoformat() if retry_seconds else ""
    return {
        "status": status,
        "generation_id": trust["generation_id"],
        "last_successful_promotion": _text(fresh.get("generated_at"), 64),
        "last_attempt": _text(last_attempt or now.isoformat(), 64),
        "consecutive_failures": failures,
        "retry_deadline": retry_deadline,
        "blocking_components": trust["reason_codes"],
        "worker_adoption": {
            "adopted": trust["worker_adopted"],
            "generation_id": trust["worker_generation_id"],
        },
        "persistence": {
            "healthy": trust["persistence_healthy"],
            "canonical": bool(canonical_persistence_healthy),
            "alert_ledger": bool(
                (live_setup_monitor or {}).get("persistence", {}).get("healthy", True)
            ),
        },
        "market_phase": phase,
        "polling_state": "active" if phase == "open" else "paused",
        "server_setup_monitor": server_monitor,
        "next_transition_at": _text(contract.get("next_transition_at"), 64),
        "next_session_open_at": _text(contract.get("next_session_open_at"), 64),
        "summary": summary,
        "trust": trust,
        "recent_incidents": recent_reliability_events(5),
    }
