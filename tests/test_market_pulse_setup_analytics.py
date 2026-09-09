from __future__ import annotations

from datetime import date, datetime
import json
import sqlite3

import pytest

from mccain_capital.migrations import _migration_0016_market_pulse_setup_events
from mccain_capital.services import market_pulse_setup_analytics as analytics


@pytest.fixture
def analytics_db(tmp_path, monkeypatch):
    path = tmp_path / "analytics.db"

    def connect():
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        _migration_0016_market_pulse_setup_events(conn)
        return conn

    monkeypatch.setattr(analytics, "db", connect)
    analytics._CACHE.clear()
    return connect


def setup(
    event_id: str,
    signal_time: str,
    *,
    family: str = "failed_high",
    direction: str = "bearish",
    outcome: str = "open",
    target: float = 7650,
    mfe: float | None = 4,
    mae: float | None = 2,
) -> dict:
    terminal_at = signal_time.replace("10:44", "10:55") if outcome != "open" else ""
    return {
        "setup_event_id": event_id,
        "ticker": "SPX",
        "session_date": signal_time[:10],
        "signal_time": signal_time,
        "signal_candle_time": signal_time,
        "family": family,
        "family_label": "Sweep and reject high"
        if direction == "bearish"
        else "Sweep and recover low",
        "direction": direction,
        "level": {"key": "prior_day_high", "label": "Prior-Day High", "value": 7670},
        "entry_zone": 7668,
        "target": {"key": "put_wall", "label": "Put Wall", "value": target},
        "score": 85,
        "grade": "A-",
        "strat_pattern": {"code": "2-2 REV D", "family": "2-2-reversal"},
        "outcome": {
            "state": outcome,
            "at": terminal_at,
            "evaluated_through": terminal_at or signal_time,
            "mfe": mfe,
            "mae": mae,
            "target_progress_percent": 50 if mfe is not None else None,
        },
    }


def test_upsert_freezes_signal_facts_and_advances_terminal_monotonically(analytics_db):
    first = setup("event-1", "2026-09-01T10:44:00-04:00")
    assert analytics.upsert_records([first]) == 1

    terminal = setup(
        "event-1",
        "2026-09-01T10:44:00-04:00",
        family="failed_low",
        direction="bullish",
        outcome="target_reached",
        target=7800,
        mfe=10,
    )
    analytics.upsert_records([terminal])
    analytics.upsert_records([first])

    with analytics_db() as conn:
        row = conn.execute(
            "SELECT * FROM market_pulse_setup_events WHERE setup_event_id = 'event-1'"
        ).fetchone()
    assert row["family"] == "failed_high"
    assert row["direction"] == "bearish"
    assert row["target_value"] == 7650
    assert row["outcome_state"] == "target_reached"
    assert row["mfe"] == 10


def test_backfill_is_idempotent_and_preserves_unavailable_fields(analytics_db, tmp_path):
    source = setup("event-1", "2026-09-01T10:44:00-04:00", mfe=None, mae=None)
    path = tmp_path / "ledger.json"
    path.write_text(json.dumps({"setups": {"one": source}}), encoding="utf-8")

    assert analytics.backfill_ledger(str(path)) == 1
    analytics.backfill_ledger(str(path))
    with analytics_db() as conn:
        count = conn.execute("SELECT COUNT(*) FROM market_pulse_setup_events").fetchone()[0]
        row = conn.execute("SELECT mfe, mae FROM market_pulse_setup_events").fetchone()
    assert count == 1
    assert row["mfe"] is None
    assert row["mae"] is None


def test_legacy_display_target_uses_structured_target_level(analytics_db):
    source = setup("event-legacy", "2026-09-01T10:44:00-04:00")
    source["target_level"] = source.pop("target")
    source["target"] = "Put Wall"

    analytics.upsert_records([source])

    with analytics_db() as conn:
        row = conn.execute(
            "SELECT target_key, target_value FROM market_pulse_setup_events"
        ).fetchone()
    assert row["target_key"] == "put_wall"
    assert row["target_value"] == 7650


def test_filtered_metrics_time_bucket_and_pagination_are_consistent(analytics_db):
    analytics.upsert_records(
        [
            setup(
                "event-1",
                "2026-09-01T10:44:00-04:00",
                outcome="target_reached",
                mfe=8,
                mae=1,
            ),
            setup(
                "event-2",
                "2026-09-01T11:10:00-04:00",
                outcome="invalidated",
                mfe=2,
                mae=7,
            ),
            setup("event-3", "2026-09-02T10:35:00-04:00", outcome="open", mfe=None),
        ]
    )

    payload = analytics.analytics_payload(
        {
            "start_date": "2026-09-01",
            "end_date": "2026-09-02",
            "start_time": "10:30",
            "end_time": "10:59",
            "page_size": "1",
        }
    )

    assert payload["metrics"]["total_setups"] == 2
    assert payload["metrics"]["terminal_sample_size"] == 1
    assert payload["metrics"]["target_reached_rate"] == 100.0
    assert payload["metrics"]["open_count"] == 1
    assert payload["metrics"]["median_mfe"] == {"value": 8.0, "sample_size": 1}
    assert payload["charts"]["outcomes_by_time_bucket"] == [
        {
            "bucket": "10:30–10:59 AM",
            "target_reached": 1,
            "invalidated": 0,
            "open": 1,
            "unavailable": 0,
        }
    ]
    assert payload["time_heatmap"][0]["total_occurrences"] == 2
    assert payload["time_heatmap"][0]["evaluated_count"] == 1
    assert payload["insights"]["best_time_bucket"]["early_evidence"] is True
    assert payload["ledger"]["total"] == 2
    assert payload["ledger"]["pages"] == 2
    assert len(payload["ledger"]["rows"]) == 1


@pytest.mark.parametrize(
    "values",
    [
        {"start_date": "bad"},
        {"start_date": "2026-01-01", "end_date": "2026-09-01"},
        {"start_time": "15:00", "end_time": "10:00"},
        {"outcome": "winner"},
        {"page_size": "201"},
    ],
)
def test_invalid_filters_fail_closed(analytics_db, values):
    with pytest.raises(analytics.AnalyticsFilterError):
        analytics.analytics_payload(values)


def test_family_occurrences_keep_frozen_prices_and_latest_five(analytics_db):
    events = [setup(f"event-{i}", f"2026-09-01T10:{i:02d}:00-04:00") for i in range(7)]
    events[-1]["entry_zone"] = None
    analytics.upsert_records(events)
    payload = analytics.analytics_payload(
        {"start_date": "2026-09-01", "end_date": "2026-09-01", "sort": "signal_asc"}
    )
    family = payload["family_comparison"][0]
    assert family["total_occurrences"] == 7
    assert [event["setup_event_id"] for event in family["occurrences"]] == [
        "event-6", "event-5", "event-4", "event-3", "event-2"
    ]
    latest = family["occurrences"][0]
    assert latest["level_value"] == 7670
    assert latest["target_value"] == 7650
    assert latest["entry_value"] is None
    assert latest["pattern_code"] == "2-2 REV D"


def test_default_filter_scope_is_today_and_history_remains_explicit(analytics_db):
    filters = analytics.normalize_filters({"ticker": "SPX"})
    assert (
        filters["start_date"]
        == filters["end_date"]
        == datetime.now(analytics.ET).date().isoformat()
    )

    historical = analytics.normalize_filters(
        {"ticker": "SPX", "start_date": "2026-08-01", "end_date": "2026-09-01"}
    )
    assert historical["start_date"] == "2026-08-01"
    assert historical["end_date"] == "2026-09-01"


def test_session_presets_skip_weekends_and_exchange_holidays():
    start, end = analytics._preset_dates("last_3_sessions", date(2026, 9, 7))
    assert end == date(2026, 9, 7)
    assert start == date(2026, 9, 2)
    week_start, week_end = analytics._preset_dates("this_week", date(2026, 9, 9))
    assert (week_start, week_end) == (date(2026, 9, 7), date(2026, 9, 9))
    all_start, _ = analytics._preset_dates("all_history", date(2026, 9, 9))
    assert all_start == date(2000, 1, 1)


def test_explicit_dates_use_custom_while_named_preset_is_authoritative():
    custom = analytics.normalize_filters({"start_date": "2026-08-01", "end_date": "2026-08-15"})
    assert custom["preset"] == "custom"
    named = analytics.normalize_filters(
        {"preset": "today", "start_date": "2026-08-01", "end_date": "2026-08-15"}
    )
    assert named["preset"] == "today"
    assert named["start_date"] == named["end_date"]


def test_dst_timestamp_is_bucketed_in_new_york_time(analytics_db):
    analytics.upsert_records(
        [setup("event-dst", "2026-03-09T14:44:00+00:00", outcome="target_reached")]
    )
    payload = analytics.analytics_payload({"start_date": "2026-03-09", "end_date": "2026-03-09"})
    assert payload["charts"]["outcomes_by_time_bucket"][0]["bucket"] == "10:30–10:59 AM"


def test_query_plan_uses_ticker_session_index(analytics_db):
    with analytics_db() as conn:
        plan = " ".join(
            str(row["detail"])
            for row in conn.execute(
                "EXPLAIN QUERY PLAN SELECT * FROM market_pulse_setup_events "
                "WHERE ticker = ? AND session_date BETWEEN ? AND ?",
                ("SPX", date(2026, 9, 1).isoformat(), date(2026, 9, 2).isoformat()),
            ).fetchall()
        )
    assert "idx_mp_setup_events_ticker_session" in plan


def test_setup_analytics_page_contract(client):
    body = client.get("/market-pulse/setup-analytics?ticker=SPX").get_data(as_text=True)
    assert "SPX Setup Analytics" in body
    assert 'data-chart="sessions"' in body
    assert 'data-chart="heatmap-overview"' in body
    assert 'data-chart="heatmap-timing"' in body
    assert 'data-family-cards="overview"' in body
    assert 'data-family-cards="all"' in body
    assert 'data-chart="excursion"' in body
    assert "data-analytics-insight" in body
    assert "data-filter-drawer" in body
    assert 'data-analytics-contracts type="number" min="1" max="1000" step="1" value="3"' in body
    assert "Today’s Live Replay evidence first" in body
    assert 'data-today="' in body
    assert 'data-analytics-tab="overview"' in body
    assert 'data-analytics-tab="ledger"' in body
    assert "setupAnalyticsLedgerRows" in body
    assert "<table" not in body
    assert "not option fills, returns, or realized profit" in body
    assert "Setup Analytics</span>" in body


def test_legacy_history_counts_frequency_but_not_performance(analytics_db):
    analytics.upsert_records(
        [
            setup("evaluated", "2026-09-01T10:44:00-04:00", outcome="target_reached"),
            setup("legacy", "2026-09-01T10:50:00-04:00", outcome="unavailable", mfe=None, mae=None),
        ]
    )
    payload = analytics.analytics_payload({"start_date": "2026-09-01", "end_date": "2026-09-01"})

    assert payload["metrics"]["total_setups"] == 2
    assert payload["metrics"]["target_reached_rate"] == 100.0
    assert payload["coverage"]["complete_outcome_count"] == 1
    assert payload["coverage"]["outcome_coverage_percent"] == 50.0
    assert payload["time_heatmap"][0]["total_occurrences"] == 2
    assert payload["time_heatmap"][0]["evaluated_count"] == 1


def test_market_pulse_replay_links_to_analytics(client):
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)
    assert "Analyze setups" in body
    assert "/market-pulse/setup-analytics?ticker=SPX" in body
    assert 'url.searchParams.set("session_date", requestedReplaySession)' in body


def test_setup_analytics_api_is_bounded_and_read_only(client):
    valid = client.get(
        "/api/market-pulse/setup-analytics?ticker=SPX&start_date=2026-09-01&end_date=2026-09-01"
    )
    assert valid.status_code == 200
    payload = valid.get_json()["payload"]
    assert {"metrics", "charts", "ledger", "coverage"}.issubset(payload)
    assert payload["interpretation"].endswith("realized profit.")
    assumptions = payload["profit_estimate_assumptions"]
    assert {
        key: assumptions[key]
        for key in (
            "contract_cost",
            "absolute_delta",
            "multiplier",
            "planned_return_range",
            "planned_profit_range",
        )
    } == {
        "contract_cost": 750,
        "absolute_delta": 0.4,
        "multiplier": 100,
        "planned_return_range": [15, 20],
        "planned_profit_range": [112.5, 150.0],
    }
    assert set(assumptions["anchors"]) == {"bullish", "bearish"}

    invalid = client.get(
        "/api/market-pulse/setup-analytics?ticker=SPX&start_date=2026-01-01&end_date=2026-09-01"
    )
    assert invalid.status_code == 400
    assert invalid.get_json()["error"] == "invalid_filters"
