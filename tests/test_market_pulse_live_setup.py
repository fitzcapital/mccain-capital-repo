from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from mccain_capital.services.market_pulse_live_setup import (
    acknowledge_setup,
    alert_identity,
    evaluate_live_setup_monitor,
    setup_identity,
)


ET = ZoneInfo("America/New_York")


def _candidate(*, state="confirmed", score=85, level=7700, target=7650):
    return {
        "id": f"failed-high-{level}",
        "family": "failed_high",
        "family_label": "Reject Current-Day High",
        "direction": "bearish",
        "state": state,
        "lane": "active_now" if state == "confirmed" else "alternative",
        "score": score,
        "grade": "A-" if score >= 85 else "B",
        "distance": 2.0,
        "level": {"key": "current_day_high", "label": "Current-Day High", "value": level},
        "target_level": {"key": "put_wall", "label": "Put Wall", "value": target},
        "plan": {
            "wait": f"Watch Current-Day High {level:,.0f}",
            "trigger": "Complete a 5-minute close below, then fail the reclaim",
            "action": "Sell the confirmed rejection",
            "cancel": f"Cancel above Current-Day High {level:,.0f}",
            "target": f"Put Wall {target:,.0f}",
        },
        "evidence": {"completed_candle": True},
        "strat_pattern": {
            "code": "2-2 REV D",
            "family": "2-2-reversal",
            "direction": "bearish",
            "completed_at": "2026-08-20T11:30:00-04:00",
            "bar_timestamps": [
                "2026-08-20T11:25:00-04:00",
                "2026-08-20T11:30:00-04:00",
            ],
            "anchor_level": {
                "key": "current_day_high",
                "label": "Current-Day High",
                "value": level,
            },
            "provenance": "5m_completed_candles",
        },
    }


def _freshness(*, locked=False):
    return {
        "session_id": "2026-08-20",
        "generation_id": "generation-1",
        "generated_at": "2026-08-20T11:30:00-04:00",
        "sync_interval_seconds": 15,
        "execution_locked": locked,
        "stale_required_components": ["gamma"] if locked else [],
    }


def _bar(clock="11:30", *, high=7701, low=7680, close=7690):
    return {
        "ts": f"2026-08-20T{clock}:00-04:00",
        "high": high,
        "low": low,
        "close": close,
    }


def _evaluate(path: Path, *, candidate=None, now=None, locked=False, bars=None):
    return evaluate_live_setup_monitor(
        ticker="SPX",
        scenario_rankings={"candidates": [candidate or _candidate()]},
        canonical_freshness=_freshness(locked=locked),
        authoritative_action={"permission": "ready", "action_state": "ACTIVE"},
        bars=bars or [_bar()],
        now=now or datetime(2026, 8, 20, 11, 31, tzinfo=ET),
        ledger_path=str(path),
        cutoff="15:15",
    )


def test_setup_and_alert_identities_are_stable_and_level_specific():
    first = setup_identity(session_id="2026-08-20", ticker="SPX", candidate=_candidate())
    repeated = setup_identity(session_id="2026-08-20", ticker="SPX", candidate=_candidate())
    other_level = setup_identity(
        session_id="2026-08-20", ticker="SPX", candidate=_candidate(level=7710)
    )

    assert first == repeated
    assert first != other_level
    assert alert_identity(first) == alert_identity(repeated)


def test_confirmed_setup_delivers_once_across_polls_and_restart(tmp_path):
    path = tmp_path / "live-setups.json"
    first = _evaluate(path)
    second = _evaluate(path, now=datetime(2026, 8, 20, 11, 32, tzinfo=ET))

    assert first["primary"]["state"] == "CONFIRMED"
    assert first["primary"]["revision"] == 1
    assert first["alert_event"]["deliver_now"] is True
    assert second["primary"]["revision"] == 1
    assert second["alert_event"]["deliver_now"] is False


def test_stale_data_pauses_alerting_and_action_language(tmp_path):
    result = _evaluate(tmp_path / "locked.json", locked=True)

    assert result["paused"] is True
    assert result["alert_event"] is None
    assert result["primary"]["alert_status"] == "blocked_by_freshness"
    assert result["primary"]["action"].startswith("Paused")


def test_cutoff_makes_confirmation_review_only(tmp_path):
    result = _evaluate(
        tmp_path / "late.json",
        now=datetime(2026, 8, 20, 15, 56, tzinfo=ET),
        bars=[_bar("15:55")],
    )

    assert result["primary"]["late_review_only"] is True
    assert result["primary"]["alert_status"] == "late_review_only"
    assert result["alert_event"] is None


def test_watching_candidate_does_not_confirm_from_intrabar_price(tmp_path):
    result = _evaluate(
        tmp_path / "watching.json",
        candidate=_candidate(state="armed"),
        bars=[_bar("11:30", high=7710, low=7640, close=7690)],
    )

    assert result["primary"]["state"] == "ARMED"
    assert result["alert_event"] is None


def test_live_monitor_refuses_confirmed_candidate_without_exact_pattern(tmp_path):
    candidate = _candidate()
    candidate.pop("strat_pattern")

    result = _evaluate(tmp_path / "missing-pattern.json", candidate=candidate)

    assert result["primary"]["state"] == "ARMED"
    assert result["alert_event"] is None


def test_unconfirmed_setup_expires_after_cutoff(tmp_path):
    result = _evaluate(
        tmp_path / "expired.json",
        candidate=_candidate(state="armed"),
        now=datetime(2026, 8, 20, 15, 16, tzinfo=ET),
        bars=[_bar("15:15")],
    )

    assert result["primary"]["state"] == "EXPIRED"
    assert result["alert_event"] is None


def test_completed_close_through_invalidation_is_terminal(tmp_path):
    path = tmp_path / "invalidated.json"
    _evaluate(path)
    invalidated = _evaluate(
        path,
        now=datetime(2026, 8, 20, 11, 37, tzinfo=ET),
        bars=[_bar("11:35", high=7710, low=7695, close=7705)],
    )

    assert invalidated["primary"]["state"] == "INVALIDATED"
    assert invalidated["alert_event"] is None


def test_terminal_state_is_monotonic_after_completed_target_bar(tmp_path):
    path = tmp_path / "terminal.json"
    _evaluate(path)
    reached = _evaluate(
        path,
        now=datetime(2026, 8, 20, 11, 37, tzinfo=ET),
        bars=[_bar("11:35", high=7690, low=7648, close=7660)],
    )
    repeated = _evaluate(
        path,
        now=datetime(2026, 8, 20, 11, 42, tzinfo=ET),
        bars=[_bar("11:40", high=7710, low=7690, close=7705)],
    )

    assert reached["primary"]["state"] == "TARGET_REACHED"
    assert repeated["primary"]["state"] == "TARGET_REACHED"
    assert repeated["primary"]["revision"] == reached["primary"]["revision"]


def test_concurrent_workers_create_one_delivery(tmp_path):
    path = tmp_path / "concurrent.json"
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: _evaluate(path), range(8)))

    assert sum(bool(row.get("alert_event", {}).get("deliver_now")) for row in results) == 1
    ledger = json.loads(path.read_text(encoding="utf-8"))
    assert len(ledger["deliveries"]) == 1
    assert len(ledger["setups"]) == 1


def test_acknowledgement_is_durable_without_state_change(tmp_path):
    path = tmp_path / "ack.json"
    result = _evaluate(path)
    setup_id = result["primary"]["setup_id"]

    assert acknowledge_setup(
        ledger_path=str(path), setup_id=setup_id, now=datetime(2026, 8, 20, 11, 33, tzinfo=ET)
    )
    restored = _evaluate(path, now=datetime(2026, 8, 20, 11, 34, tzinfo=ET))
    assert restored["primary"]["acknowledged_at"]
    assert restored["primary"]["state"] == "CONFIRMED"


def test_persistence_failure_fails_alerting_closed(tmp_path):
    directory = tmp_path / "not-a-file"
    directory.mkdir()
    result = _evaluate(directory)

    assert result["paused"] is True
    assert result["alert_event"] is None
    assert result["persistence"]["healthy"] is False
    assert "setup_persistence" in result["blockers"]


def test_market_pulse_page_contains_live_monitor_contract(client):
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)

    assert 'id="marketPulseLiveSetupMonitor"' in body
    assert 'id="marketPulseLiveSetupCountdown"' in body
    assert "market-pulse-app-notification" in body
    assert "/api/market-pulse/live-setup/ack" in body
    assert "LIVE_SETUP_DELIVERIES_KEY" in body


def test_canonical_api_includes_generation_bound_live_monitor(client):
    response = client.get("/api/market-pulse/context?ticker=SPX")
    assert response.status_code == 200
    payload = response.get_json()["payload"]
    monitor = payload["live_setup_monitor"]
    assert monitor["ticker"] == "SPX"
    assert monitor["generation_id"] == payload["canonical_freshness"]["generation_id"]
    assert "next_evaluation_at" in monitor
    assert "persistence" in monitor


def test_client_contract_rejects_mixed_and_out_of_order_monitor_revisions():
    template = (
        Path(__file__).resolve().parents[1] / "mccain_capital/templates/core/market_pulse.html"
    ).read_text(encoding="utf-8")

    assert 'throw new Error("Live setup monitor belongs to a different generation.")' in template
    assert "shouldAcceptRevision(currentLiveSetupId" in template
    assert 'actionCode !== "ACTIVE" || freshness.execution_locked' in template
    assert "shouldDeliver(event, delivered)" in template
