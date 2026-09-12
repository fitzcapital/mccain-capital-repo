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


def _evaluate(
    path: Path,
    *,
    candidate=None,
    now=None,
    locked=False,
    bars=None,
    setup_events=None,
    claim_delivery=True,
):
    return evaluate_live_setup_monitor(
        ticker="SPX",
        scenario_rankings={"candidates": [candidate or _candidate()]},
        canonical_freshness=_freshness(locked=locked),
        authoritative_action={"permission": "ready", "action_state": "ACTIVE"},
        bars=bars or [_bar()],
        setup_events=setup_events,
        now=now or datetime(2026, 8, 20, 11, 31, tzinfo=ET),
        ledger_path=str(path),
        cutoff="15:15",
        claim_delivery=claim_delivery,
    )


def _setup_event(*, clock="11:25", level=7710, event_id="event-secondary"):
    return {
        "setup_event_id": event_id,
        "candidate_id": f"failed-high-{level}",
        "signal_time": f"2026-08-20T{clock}:00-04:00",
        "family": "failed_high",
        "family_label": "Reject Prior-Day High",
        "direction": "bearish",
        "entry_zone": level - 1,
        "level": {"key": "prior_day_high", "label": "Prior-Day High", "value": level},
        "target": {"key": "put_wall", "label": "Put Wall", "value": 7650},
        "score": 82,
        "grade": "B+",
        "confirmation": "2-2 reversal trigger broken",
        "invalidation": "Cancel above Prior-Day High",
        "strat_pattern": {
            "code": "2-2 REV D",
            "family": "2-2-reversal",
            "completed_at": f"2026-08-20T{clock}:00-04:00",
        },
        "trigger_evidence": {"triggered_at": f"2026-08-20T{clock}:00-04:00"},
    }


def test_stable_event_family_is_not_overwritten_by_current_primary(tmp_path):
    path = tmp_path / "frozen-family.json"
    event = _setup_event(clock="11:30", level=7710, event_id="event-failed-high")
    primary = _candidate(level=7710)
    primary["family"] = "breakdown"
    primary["family_label"] = "Acceptance Below"

    result = _evaluate(
        path,
        candidate=primary,
        setup_events=[event],
        bars=[_bar("11:30", high=7710, low=7690, close=7700)],
    )
    record = next(row for row in json.loads(path.read_text())["setups"].values())

    assert result["primary"]["family"] == "failed_high"
    assert record["family"] == "failed_high"
    assert record["strat_pattern"] == event["strat_pattern"]
    assert record["outcome_state"] == "open"
    assert record["state"] == "CONFIRMED"


def test_live_event_outcome_matches_replay_and_terminal_state_is_monotonic(tmp_path):
    path = tmp_path / "event-outcome.json"
    event = _setup_event(clock="11:30", level=7710, event_id="event-invalidated")
    primary = _candidate(level=7710)
    primary["family"] = "breakdown"

    first = _evaluate(
        path,
        candidate=primary,
        setup_events=[event],
        bars=[
            _bar("11:30", high=7710, low=7690, close=7700),
            _bar("11:35", high=7715, low=7700, close=7712),
        ],
        now=datetime(2026, 8, 20, 11, 36, tzinfo=ET),
    )
    setup_id = first["primary"]["setup_id"]
    assert acknowledge_setup(
        ledger_path=str(path), setup_id=setup_id, now=datetime(2026, 8, 20, 11, 37, tzinfo=ET)
    )
    changed_event = json.loads(json.dumps(event))
    changed_event["target"] = {
        "key": "current_day_high",
        "label": "Current-Day High",
        "value": 7720,
    }
    repeated = _evaluate(
        path,
        candidate=primary,
        setup_events=[changed_event],
        bars=[_bar("11:30", high=7710, low=7690, close=7700)],
        now=datetime(2026, 8, 20, 11, 38, tzinfo=ET),
    )

    assert first["primary"]["state"] == "INVALIDATED"
    assert first["primary"]["outcome"]["at"].endswith("11:35:00-04:00")
    assert repeated["primary"]["state"] == "INVALIDATED"
    assert repeated["primary"]["family"] == "failed_high"
    assert repeated["primary"]["target_level"] == event["target"]
    assert repeated["primary"]["outcome_state"] == "invalidated"
    assert repeated["primary"]["outcome"] == first["primary"]["outcome"]
    assert repeated["primary"]["acknowledged_at"]


def test_setup_and_alert_identities_are_stable_across_anchor_levels():
    first = setup_identity(session_id="2026-08-20", ticker="SPX", candidate=_candidate())
    repeated = setup_identity(session_id="2026-08-20", ticker="SPX", candidate=_candidate())
    other_level = setup_identity(
        session_id="2026-08-20", ticker="SPX", candidate=_candidate(level=7710)
    )

    assert first == repeated
    assert first == other_level
    assert alert_identity(first) == alert_identity(repeated)

    later = _candidate(level=7710)
    later["strat_pattern"]["completed_at"] = "2026-08-20T11:35:00-04:00"
    assert setup_identity(session_id="2026-08-20", ticker="SPX", candidate=later) != first


def test_confirmed_setup_delivers_once_across_polls_and_restart(tmp_path):
    path = tmp_path / "live-setups.json"
    first = _evaluate(path)
    second = _evaluate(path, now=datetime(2026, 8, 20, 11, 32, tzinfo=ET))

    assert first["primary"]["state"] == "CONFIRMED"
    assert first["primary"]["revision"] == 1
    assert first["alert_event"]["deliver_now"] is True
    assert second["primary"]["revision"] == 1
    assert second["alert_event"]["deliver_now"] is False


def test_server_evaluation_persists_setup_without_claiming_browser_delivery(tmp_path):
    path = tmp_path / "server-owned.json"
    server = _evaluate(path, claim_delivery=False)
    browser = _evaluate(path, now=datetime(2026, 8, 20, 11, 32, tzinfo=ET))

    ledger = json.loads(path.read_text(encoding="utf-8"))
    assert server["primary"]["alert_status"] == "pending"
    assert server["alert_event"] is None
    assert browser["alert_event"]["deliver_now"] is True
    assert len(ledger["setups"]) == 1
    assert len(ledger["deliveries"]) == 1


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


def test_replay_eligible_secondary_event_is_durable_review_history(tmp_path):
    path = tmp_path / "all-events.json"
    event = _setup_event()

    first = _evaluate(path, setup_events=[event])
    repeated = _evaluate(
        path,
        now=datetime(2026, 8, 20, 11, 32, tzinfo=ET),
        setup_events=[event],
    )
    ledger = json.loads(path.read_text(encoding="utf-8"))
    secondary = next(row for row in first["secondary"] if row.get("setup_event_id"))

    assert len(ledger["setups"]) == 2
    assert secondary["setup_event_id"] == "event-secondary"
    assert secondary["late_review_only"] is True
    assert secondary["alert_status"] == "late_review_only"
    assert ledger["processed_completed_candles"]["2026-08-20"].endswith("11:30:00-04:00")
    assert len(repeated["recent"]) == len(first["recent"])
    assert len(json.loads(path.read_text(encoding="utf-8"))["setups"]) == 2


def test_same_pattern_anchor_variants_are_persisted_as_one_event(tmp_path):
    path = tmp_path / "same-candle.json"
    events = [
        _setup_event(clock="11:30", level=7710, event_id="event-a"),
        _setup_event(clock="11:30", level=7720, event_id="event-b"),
    ]
    events[1]["level"] = {
        "key": "current_day_high",
        "label": "Current-Day High",
        "value": 7720,
    }

    _evaluate(path, setup_events=events)
    _evaluate(path, setup_events=list(reversed(events)))
    ledger = json.loads(path.read_text(encoding="utf-8"))

    event_records = [row for row in ledger["setups"].values() if row.get("setup_event_id")]
    assert {row["setup_event_id"] for row in event_records} == {"event-a"}
    assert len(event_records) == 1
    assert all(row["late_review_only"] is False for row in event_records)
    assert event_records[0]["supporting_levels"] == [
        {
            "key": "current_day_high",
            "label": "Current-Day High",
            "value": 7720,
            "role": "supporting_confluence",
        }
    ]


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


def test_live_recovery_uses_durable_point_in_time_level_observations(client, monkeypatch):
    from mccain_capital.services import core

    captured = {}
    monkeypatch.setattr(
        core,
        "_market_pulse_durable_level_observations",
        lambda ticker, session_date: [
            {
                "key": "new_put_wall",
                "value": 7645,
                "as_of": f"{session_date}T09:30:00-04:00",
            }
        ],
    )

    def events(**kwargs):
        captured["levels"] = kwargs["levels"]
        return []

    monkeypatch.setattr(core, "build_intraday_setup_events", events)
    monkeypatch.setattr(
        core,
        "evaluate_live_setup_monitor",
        lambda **kwargs: {"ticker": kwargs["ticker"], "primary": None},
    )
    with client.application.app_context():
        core._market_pulse_live_setup_monitor(
            ticker="SPX",
            playbook_view={"scenario_rankings": {"candidates": [{}]}},
            canonical_freshness={"session_id": "2026-09-01"},
            execution_chart={"strategy_bars_5m": [_bar()]},
            market_structure_snapshot={},
            playbook_quote={},
            gamma_snapshot={},
            now_et=datetime(2026, 9, 1, 11, 40, tzinfo=ET),
        )

    assert any(
        row.get("key") == "new_put_wall"
        and row.get("value") == 7645
        and row.get("as_of") == "2026-09-01T09:30:00-04:00"
        for row in captured["levels"]
    )


def test_durable_level_recovery_includes_frozen_event_target(tmp_path, monkeypatch):
    from mccain_capital.services import core

    ledger_path = tmp_path / "live-setups.json"
    ledger_path.write_text(
        json.dumps(
            {
                "setups": {
                    "event-1": {
                        "setup_event_id": "event-1",
                        "evidence_at": "2026-09-01T11:15:00-04:00",
                        "level": {
                            "key": "new_put_wall",
                            "label": "New Put Wall",
                            "value": 7645,
                        },
                        "target_level": {
                            "key": "put_wall",
                            "label": "Put Wall",
                            "value": 7650,
                        },
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(core, "_market_pulse_live_setup_ledger_file", lambda ticker: ledger_path)

    observations = core._market_pulse_durable_level_observations(
        "SPX", session_date="2026-09-01"
    )

    assert {(row["key"], row["value"], row["as_of"]) for row in observations} == {
        ("new_put_wall", 7645.0, "2026-09-01T11:15:00-04:00"),
        ("put_wall", 7650.0, "2026-09-01T11:15:00-04:00"),
    }


def test_canonical_gamma_history_is_durable_bounded_and_deduplicated(tmp_path, monkeypatch):
    from mccain_capital.services import core

    history_path = tmp_path / "gamma-history.json"
    monkeypatch.setattr(core, "_market_pulse_gamma_history_file", lambda ticker: history_path)
    payload = {
        "canonical_freshness": {
            "session_id": "2026-09-01",
            "generation_id": "generation-1",
            "gamma_generation_id": "gamma-1",
            "execution_locked": False,
        },
        "gamma_snapshot": {"computed_at": "2026-09-01T11:15:00-04:00"},
        "market_structure_snapshot": {
            "gamma_regime": "negative_gamma",
            "main_flip": 7680,
            "call_wall": 7700,
        },
    }

    assert core._market_pulse_record_gamma_observation("SPX", payload) is True
    assert core._market_pulse_record_gamma_observation("SPX", payload) is False
    observations = core._market_pulse_gamma_observations(
        "SPX", session_date="2026-09-01"
    )

    assert len(observations) == 1
    assert observations[0]["regime"] == "negative_gamma"
    assert observations[0]["generation_id"] == "gamma-1"
    assert observations[0]["levels"][0]["as_of"] == "2026-09-01T11:15:00-04:00"


def test_client_contract_rejects_mixed_and_out_of_order_monitor_revisions():
    template = (
        Path(__file__).resolve().parents[1] / "mccain_capital/templates/core/market_pulse.html"
    ).read_text(encoding="utf-8")

    assert 'throw new Error("Live setup monitor belongs to a different generation.")' in template
    assert "shouldAcceptRevision(currentLiveSetupId" in template
    assert 'actionCode !== "ACTIVE" || freshness.execution_locked' in template
    assert "shouldDeliver(event, delivered)" in template
