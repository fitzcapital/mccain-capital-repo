from __future__ import annotations

from datetime import datetime, timedelta
import sqlite3
from zoneinfo import ZoneInfo

import pytest

from mccain_capital import runtime
from mccain_capital.migrations import (
    _migration_0017_market_pulse_reliability_events,
    _migration_0018_market_pulse_reliability_history_index,
)
from mccain_capital.services.market_pulse_operational_health import (
    build_trust_verdict,
    prune_reliability_history,
    recent_reliability_events,
    record_reliability_event,
    reliability_history,
)


NOW = datetime(2026, 9, 2, 11, 0, tzinfo=ZoneInfo("America/New_York"))


@pytest.fixture
def reliability_db(tmp_path, monkeypatch):
    path = tmp_path / "reliability.db"
    with sqlite3.connect(path) as conn:
        _migration_0017_market_pulse_reliability_events(conn)
        _migration_0018_market_pulse_reliability_history_index(conn)
    monkeypatch.setattr(runtime, "DB_PATH", str(path))
    return path


def freshness(*, gamma="current", bars="current", locked=False):
    return {
        "generation_id": "generation-2",
        "source_generation_id": "generation-2",
        "generated_at": NOW.isoformat(),
        "execution_locked": locked,
        "stale_required_components": [
            name for name, state in (("gamma", gamma), ("bars", bars)) if state != "current"
        ],
        "components": {
            "spot": {"required": True, "status": "current", "source": "Tradier"},
            "gamma": {"required": True, "status": gamma, "source": "Gamma cache"},
            "bars": {"required": True, "status": bars, "source": "Tradier bars"},
            "volume": {
                "required": False,
                "status": "current",
                "source": "SPY volume proxy",
                "fallback_mode": "spy_timestamp_match",
                "completeness": 0.8,
            },
        },
    }


def test_trust_verdict_fails_closed_without_fresh_gamma_or_persistence():
    assert build_trust_verdict(freshness=freshness())["verdict"] == "Verified"
    stale = build_trust_verdict(freshness=freshness(gamma="stale", locked=True))
    assert stale["verdict"] == "Locked" and stale["reason_codes"] == ["gamma"]
    failed = build_trust_verdict(freshness=freshness(), canonical_persistence_healthy=False)
    assert failed["verdict"] == "Locked" and "persistence" in failed["reason_codes"]
    proxy = build_trust_verdict(freshness=freshness())["components"]["volume"]
    assert proxy["source"] == "SPY volume proxy" and proxy["completeness"] == 0.8


def test_incident_is_deduped_recovered_and_read_after_memory_boundary(reliability_db):
    opened = record_reliability_event(
        at=NOW.isoformat(),
        ticker="SPX",
        component="gamma",
        status="locked",
        reason="stale",
        generation_id="g2",
        age_seconds=400,
        threshold_seconds=300,
    )
    updated = record_reliability_event(
        at=(NOW + timedelta(seconds=15)).isoformat(),
        ticker="SPX",
        component="gamma",
        status="locked",
        reason="stale",
        generation_id="g2",
    )
    recovered = record_reliability_event(
        at=(NOW + timedelta(seconds=30)).isoformat(),
        ticker="SPX",
        component="gamma",
        status="verified",
        reason="current",
        generation_id="g3",
    )
    rows = recent_reliability_events()
    assert opened["event"] == "opened" and updated["event"] == "updated"
    assert recovered["event"] == "recovered"
    assert len(rows) == 1 and rows[0]["failure_count"] == 2
    assert rows[0]["recovered_at"] and rows[0]["duration_seconds"] == 30
    assert reliability_history(ticker="SPX", days=180)["components"]["gamma"]["checks"] == 3


def test_retention_prunes_old_detail_and_daily_rows(reliability_db):
    old = NOW - timedelta(days=200)
    record_reliability_event(
        at=old.isoformat(), ticker="SPX", component="bars", status="locked", reason="missing"
    )
    result = prune_reliability_history(NOW)
    assert result == {"events": 1, "daily": 1}


def test_reliability_page_and_bounded_api_are_available(client):
    page = client.get("/market-pulse/reliability?ticker=SPX")
    assert page.status_code == 200
    assert b"SPX Reliability" in page.data and b"Can each input be trusted?" in page.data

    response = client.get("/api/market-pulse/reliability?ticker=SPX&days=30&limit=10")
    assert response.status_code == 200
    assert response.get_json()["payload"]["pagination"]["limit"] == 10
    invalid = client.get("/api/market-pulse/reliability?days=tomorrow")
    assert invalid.status_code == 400
