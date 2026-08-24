from datetime import datetime
from zoneinfo import ZoneInfo

from mccain_capital.services.market_pulse_operational_health import (
    build_operational_health,
    clear_reliability_events,
    recent_reliability_events,
    record_reliability_event,
)


NOW = datetime(2026, 8, 21, 11, 0, tzinfo=ZoneInfo("America/New_York"))


def health(**overrides):
    values = {
        "freshness": {
            "generation_id": "g2",
            "source_generation_id": "g2",
            "generated_at": NOW.isoformat(),
            "execution_locked": False,
            "stale_required_components": [],
        },
        "refresh_contract": {"market_phase": "open", "next_check_seconds": 15},
        "live_setup_monitor": {"persistence": {"healthy": True}},
        "now": NOW,
    }
    values.update(overrides)
    return build_operational_health(**values)


def test_healthy_and_paused_contracts_are_quiet_and_truthful():
    result = health()
    assert result["status"] == "healthy"
    assert result["retry_deadline"].endswith("11:00:15-04:00")
    paused = health(refresh_contract={"market_phase": "closed", "next_check_seconds": 0})
    assert paused["status"] == "paused"
    assert paused["retry_deadline"] == ""


def test_locked_persistence_failed_worker_lagged_and_recovered_states():
    locked = health(
        freshness={
            "generation_id": "g2",
            "generated_at": NOW.isoformat(),
            "execution_locked": True,
            "stale_required_components": ["gamma"],
        }
    )
    assert locked["status"] == "critical" and locked["blocking_components"] == ["gamma"]
    failed = health(canonical_persistence_healthy=False)
    assert failed["status"] == "critical" and not failed["persistence"]["healthy"]
    lagged = health(worker_generation_id="g1")
    assert lagged["status"] == "warning" and not lagged["worker_adoption"]["adopted"]
    assert health()["status"] == "healthy"


def test_event_ring_allowlists_and_bounds_sensitive_fields():
    clear_reliability_events()
    event = record_reliability_event(
        at=NOW.isoformat(), event="refresh", ticker="SPX", outcome="healthy",
        credentials="secret", raw_payload={"account": 123}, reason="ok"
    )
    assert set(event) <= {"at", "event", "ticker", "generation_id", "component", "outcome", "reason"}
    assert "secret" not in str(recent_reliability_events())
