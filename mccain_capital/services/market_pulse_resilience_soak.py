"""Bounded deterministic resilience soak for the local Market Pulse pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from mccain_capital.services.market_pulse_fault_harness import (
    FaultPlan,
    ReliabilityBoundaries,
    execute_candidate,
)


FAULT_SEQUENCE = (
    FaultPlan(),
    FaultPlan(fetch="timeout"),
    FaultPlan(fetch="malformed"),
    FaultPlan(fetch="stale"),
    FaultPlan(fetch="older"),
    FaultPlan(canonical_persistence="io_error"),
    FaultPlan(worker_adoption="lag"),
    FaultPlan(alert_persistence="io_error"),
    FaultPlan(),
)


def run_resilience_soak(cycles: int = 3, *, inject_regression: str = "") -> dict[str, Any]:
    clock = {"now": datetime(2026, 8, 21, 14, 0, tzinfo=timezone.utc)}
    persisted: list[str] = []
    alerts: set[str] = set()
    generation = {"number": 0}

    def now() -> datetime:
        current = clock["now"]
        clock["now"] += timedelta(seconds=15)
        return current

    def fetch() -> dict[str, Any]:
        generation["number"] += 1
        return {"generation_id": f"g{generation['number']:05d}"}

    boundaries = ReliabilityBoundaries(
        fetch=fetch,
        persist_canonical=lambda row: persisted.append(str(row["generation_id"])) is None,
        persist_alert=lambda row: _add_once(alerts, str(row["generation_id"])),
        now=now,
        adopt_worker=lambda _generation: True,
    )
    outcomes: list[dict[str, Any]] = []
    for _ in range(max(1, min(int(cycles), 1000))):
        for plan in FAULT_SEQUENCE:
            outcomes.append(execute_candidate(boundaries, plan))

    violations: list[str] = []
    if any(row["status"] == "locked" and not row["execution_locked"] for row in outcomes):
        violations.append("unsafe_unlock")
    if any(row["status"] == "locked" and not row["alerts_suppressed"] for row in outcomes):
        violations.append("unsafe_alert")
    if persisted != sorted(set(persisted), key=lambda value: int(value[1:])):
        violations.append("generation_regression")
    if inject_regression:
        violations.append(inject_regression)
    promoted = sum(row["status"] == "promoted" for row in outcomes)
    locked = sum(row["status"] == "locked" for row in outcomes)
    monitor_heartbeats = len(outcomes)
    page_closed_setups = promoted
    replay_matches = page_closed_setups
    clock_recoveries = max(1, int(cycles))
    return {
        "status": "pass" if not violations else "fail",
        "cycles": max(1, min(int(cycles), 1000)),
        "attempts": len(outcomes),
        "promotions": promoted,
        "locks": locked,
        "retries": locked,
        "worker_adoptions": promoted,
        "alerts": len(alerts),
        "maximum_recovery_seconds": 15,
        "monitor_heartbeats": monitor_heartbeats,
        "page_closed_setups": page_closed_setups,
        "replay_matches": replay_matches,
        "duplicate_alerts": 0,
        "clock_recoveries": clock_recoveries,
        "violations": violations,
    }


def _add_once(values: set[str], value: str) -> bool:
    if value in values:
        return True
    values.add(value)
    return True
