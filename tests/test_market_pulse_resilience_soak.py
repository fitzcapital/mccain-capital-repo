from datetime import datetime, timezone

from mccain_capital.services.market_pulse_fault_harness import (
    FaultPlan,
    ReliabilityBoundaries,
    execute_candidate,
)
from mccain_capital.services.market_pulse_resilience_soak import run_resilience_soak


def boundaries():
    return ReliabilityBoundaries(
        fetch=lambda: {"generation_id": "g2"},
        persist_canonical=lambda _row: True,
        persist_alert=lambda _row: True,
        now=lambda: datetime(2026, 8, 21, 14, 0, tzinfo=timezone.utc),
        adopt_worker=lambda _generation: True,
    )


def test_faults_fail_closed_and_recovery_promotes_without_reload():
    for plan in (
        FaultPlan(fetch="timeout"), FaultPlan(fetch="network"),
        FaultPlan(fetch="malformed"), FaultPlan(fetch="stale"),
        FaultPlan(fetch="mixed_generation"), FaultPlan(fetch="older"),
        FaultPlan(canonical_persistence="io_error"), FaultPlan(worker_adoption="lag"),
        FaultPlan(alert_persistence="io_error"),
    ):
        result = execute_candidate(boundaries(), plan)
        assert result["execution_locked"] and result["alerts_suppressed"]
    assert execute_candidate(boundaries(), FaultPlan())["status"] == "promoted"


def test_short_soak_passes_and_detects_a_deliberate_regression():
    report = run_resilience_soak(2)
    assert report["status"] == "pass" and report["attempts"] == 18
    assert report["maximum_recovery_seconds"] == 15
    assert run_resilience_soak(1, inject_regression="mixed_commit")["status"] == "fail"
