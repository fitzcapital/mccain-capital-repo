from datetime import datetime
from zoneinfo import ZoneInfo

from mccain_capital.services.market_pulse_operational_health import (
    build_operational_health,
    build_resource_diagnostics,
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


def test_server_setup_monitor_health_is_additive_and_truthful(monkeypatch):
    monitor = {
        "enabled": True,
        "status": "degraded",
        "heartbeat_at": NOW.isoformat(),
        "heartbeat_age_seconds": 0,
        "last_evaluated_candle": "2026-08-21T10:55:00-04:00",
        "clock_status": "coherent",
        "consecutive_failures": 1,
    }
    monkeypatch.setattr(
        "mccain_capital.services.market_pulse_setup_monitor_runtime.get_server_setup_monitor_state",
        lambda **_kwargs: dict(monitor),
    )

    active = health()
    paused = health(refresh_contract={"market_phase": "closed", "next_check_seconds": 0})

    assert active["status"] == "warning"
    assert active["summary"] == "Setup coverage is synchronizing."
    assert active["server_setup_monitor"] == monitor
    assert paused["status"] == "paused"


def test_event_ring_allowlists_and_bounds_sensitive_fields():
    clear_reliability_events()
    event = record_reliability_event(
        at=NOW.isoformat(), event="refresh", ticker="SPX", outcome="healthy",
        credentials="secret", raw_payload={"account": 123}, reason="ok"
    )
    assert set(event) <= {"at", "event", "ticker", "generation_id", "component", "outcome", "reason"}
    assert "secret" not in str(recent_reliability_events())


def test_resource_diagnostics_classify_gamma_and_worker_pressure(monkeypatch):
    monkeypatch.setattr(
        "mccain_capital.services.market_pulse_operational_health._process_thread_count",
        lambda: 12,
    )
    monkeypatch.setattr(
        "mccain_capital.services.market_pulse_operational_health._read_integer_file",
        lambda path: 1800 if path.endswith("pids.current") else 2048,
    )
    result = build_resource_diagnostics(
        gamma_runtime={
            "snapshot_status": "stale",
            "age_seconds": 450,
            "refresh_failed": True,
            "refresh_in_progress": False,
        },
        market_phase="open",
    )
    assert result["gamma_stale"] == 1
    assert result["gamma_refresh_failed"] == 1
    assert result["worker_pressure"] == 2
    assert result["container_task_limit"] == 2048


def test_gamma_monitoring_is_quiet_outside_regular_session(monkeypatch):
    monkeypatch.setattr(
        "mccain_capital.services.market_pulse_operational_health._read_integer_file",
        lambda _path: None,
    )
    result = build_resource_diagnostics(
        gamma_runtime={"snapshot_status": "stale", "age_seconds": 999, "refresh_failed": True},
        market_phase="closed",
    )
    assert result["market_open"] == 0
    assert result["gamma_stale"] == 0
    assert result["gamma_refresh_failed"] == 0
