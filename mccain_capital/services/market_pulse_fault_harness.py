"""Test-only fault boundaries for deterministic Market Pulse resilience checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable


@dataclass
class ReliabilityBoundaries:
    fetch: Callable[[], dict[str, Any]]
    persist_canonical: Callable[[dict[str, Any]], bool]
    persist_alert: Callable[[dict[str, Any]], bool]
    now: Callable[[], datetime]
    adopt_worker: Callable[[str], bool]


@dataclass(frozen=True)
class FaultPlan:
    fetch: str = "ok"
    canonical_persistence: str = "ok"
    alert_persistence: str = "ok"
    worker_adoption: str = "ok"


def execute_candidate(boundaries: ReliabilityBoundaries, plan: FaultPlan) -> dict[str, Any]:
    """Exercise production-shaped boundaries without request-driven fault controls."""

    attempted_at = boundaries.now().isoformat()
    if plan.fetch == "timeout":
        return _locked("provider_timeout", attempted_at)
    if plan.fetch == "network":
        return _locked("provider_network", attempted_at)
    candidate = boundaries.fetch()
    if plan.fetch == "malformed" or not isinstance(candidate, dict):
        return _locked("malformed_candidate", attempted_at)
    if plan.fetch in {"stale", "mixed_generation", "older"}:
        return _locked(plan.fetch, attempted_at)
    generation = str(candidate.get("generation_id") or "")
    if not generation:
        return _locked("missing_generation", attempted_at)
    if plan.canonical_persistence == "io_error" or not boundaries.persist_canonical(candidate):
        return _locked("canonical_persistence", attempted_at)
    if plan.worker_adoption == "lag" or not boundaries.adopt_worker(generation):
        return _locked("worker_adoption", attempted_at, generation=generation)
    alert_suppressed = False
    if plan.alert_persistence == "io_error" or not boundaries.persist_alert(candidate):
        alert_suppressed = True
    return {
        "status": "locked" if alert_suppressed else "promoted",
        "generation_id": generation,
        "attempted_at": attempted_at,
        "execution_locked": alert_suppressed,
        "alerts_suppressed": alert_suppressed,
        "reason": "alert_persistence" if alert_suppressed else "verified",
    }


def _locked(reason: str, attempted_at: str, *, generation: str = "") -> dict[str, Any]:
    return {
        "status": "locked",
        "generation_id": generation,
        "attempted_at": attempted_at,
        "execution_locked": True,
        "alerts_suppressed": True,
        "reason": reason,
    }
