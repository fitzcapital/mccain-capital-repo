from __future__ import annotations

from datetime import date, datetime
import json
import sqlite3

import pytest

from mccain_capital.migrations import (
    _migration_0016_market_pulse_setup_events,
    _migration_0019_market_pulse_setup_gamma_context,
)
from mccain_capital.services import market_pulse_setup_analytics as analytics


@pytest.fixture
def analytics_db(tmp_path, monkeypatch):
    path = tmp_path / "analytics.db"

    def connect():
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        _migration_0016_market_pulse_setup_events(conn)
        _migration_0019_market_pulse_setup_gamma_context(conn)
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
    gamma_regime: str = "",
    gamma_as_of: str = "",
) -> dict:
    terminal_at = signal_time.replace("10:44", "10:55") if outcome != "open" else ""
    record = {
        "setup_event_id": event_id,
        "ticker": "SPX",
        "session_date": signal_time[:10],
        "signal_time": signal_time,
        "signal_candle_time": signal_time,
        "family": family,
        "family_label": (
            "Sweep and reject high" if direction == "bearish" else "Sweep and recover low"
        ),
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
    if gamma_regime:
        record.update(
            {
                "gamma_regime": gamma_regime,
                "gamma_as_of": gamma_as_of or signal_time,
                "gamma_source": "gamma-history",
                "gamma_status": "captured",
            }
        )
    return record


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


def test_gamma_context_is_validated_persisted_and_immutable(analytics_db):
    first = setup(
        "gamma-event",
        "2026-09-01T10:44:00-04:00",
        gamma_regime="negative_gamma",
        gamma_as_of="2026-09-01T10:40:00-04:00",
    )
    analytics.upsert_records([first])
    update = setup(
        "gamma-event",
        "2026-09-01T10:44:00-04:00",
        outcome="target_reached",
        gamma_regime="positive_gamma",
        gamma_as_of="2026-09-01T10:43:00-04:00",
    )
    analytics.upsert_records([update])

    with analytics_db() as conn:
        row = conn.execute(
            "SELECT gamma_regime, gamma_as_of, gamma_source, gamma_status "
            "FROM market_pulse_setup_events WHERE setup_event_id = 'gamma-event'"
        ).fetchone()
    assert tuple(row) == (
        "negative",
        "2026-09-01T10:40:00-04:00",
        "gamma-history",
        "captured",
    )

    future = setup(
        "future-gamma",
        "2026-09-01T10:44:00-04:00",
        gamma_regime="positive_gamma",
        gamma_as_of="2026-09-01T10:45:00-04:00",
    )
    assert analytics.canonical_record(future)["gamma_regime"] == "unavailable"


def test_same_event_can_enrich_only_previously_unavailable_gamma(analytics_db):
    source = setup("enriched-gamma", "2026-09-01T10:44:00-04:00")
    analytics.upsert_records([source])
    source.update(
        {
            "gamma_regime": "positive_gamma",
            "gamma_as_of": "2026-09-01T10:40:00-04:00",
            "gamma_source": "gamma-history",
            "gamma_status": "captured",
        }
    )
    analytics.upsert_records([source])

    with analytics_db() as conn:
        row = conn.execute(
            "SELECT gamma_regime, gamma_as_of, gamma_source, gamma_status "
            "FROM market_pulse_setup_events WHERE setup_event_id = 'enriched-gamma'"
        ).fetchone()
    assert tuple(row) == (
        "positive",
        "2026-09-01T10:40:00-04:00",
        "gamma-history",
        "captured",
    )


def test_gamma_filter_coverage_and_named_comparison_exclude_unavailable(analytics_db):
    analytics.upsert_records(
        [
            setup(
                "negative-gamma",
                "2026-09-01T10:44:00-04:00",
                outcome="target_reached",
                gamma_regime="negative_gamma",
                gamma_as_of="2026-09-01T10:40:00-04:00",
            ),
            setup("legacy-gamma", "2026-09-01T11:10:00-04:00", outcome="invalidated"),
        ]
    )

    all_rows = analytics.analytics_payload({"start_date": "2026-09-01", "end_date": "2026-09-01"})
    assert all_rows["metrics"]["total_setups"] == 2
    assert all_rows["coverage"]["gamma_captured_count"] == 1
    assert all_rows["coverage"]["gamma_unavailable_count"] == 1
    assert all_rows["coverage"]["gamma_coverage_percent"] == 50.0
    assert [row["filter_gamma"] for row in all_rows["gamma_comparison"]] == ["negative"]

    filtered = analytics.analytics_payload(
        {
            "start_date": "2026-09-01",
            "end_date": "2026-09-01",
            "gamma": "negative",
        }
    )
    assert filtered["metrics"]["total_setups"] == 1
    assert filtered["ledger"]["rows"][0]["gamma_regime"] == "negative"


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
        {"gamma": "today_guess"},
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
        "event-6",
        "event-5",
        "event-4",
        "event-3",
        "event-2",
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
    assert "data-analytics-leaders" in body
    assert "What worked best" in body
    assert "data-filter-drawer" in body
    assert 'data-analytics-contracts type="number" min="1" max="1000" step="1" value="3"' in body
    assert "Today’s Live Replay evidence first" in body
    assert 'data-today="' in body
    assert 'data-analytics-tab="overview"' in body
    assert 'data-analytics-tab="ledger"' in body
    assert '<select name="gamma">' in body
    assert "Gamma at signal" in body
    assert "setupAnalyticsLedgerRows" in body
    assert "<table" not in body
    assert "not option fills, returns, or realized profit" in body
    assert "Setup Analytics</span>" in body


def test_tools_menu_has_prominent_setup_analytics_quick_link(client):
    body = client.get("/market-pulse/setup-analytics?ticker=SPX").get_data(as_text=True)

    assert '<div class="menuTitle">Quick Links</div>' in body
    assert 'class="btn menuQuickLink active"' in body
    assert 'href="/market-pulse/setup-analytics?ticker=SPX"' in body
    assert "SPX Setup Analytics</span>" in body


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


def test_analytics_counts_pattern_aliases_once_and_preserves_best_evidence(analytics_db):
    weaker = setup(
        "SPX:2026-09-09:2-1-2D:2026-09-09T11:20:00-04:00:bearish",
        "2026-09-09T11:20:00-04:00",
        outcome="invalidated",
        mfe=None,
        mae=None,
    )
    weaker["strat_pattern"] = {"code": "2-1-2D", "family": "2-1-2-reversal"}
    weaker["score"] = 65
    stronger = setup(
        "SPX:2026-09-09:2-2 REV D:2026-09-09T11:20:00-04:00:bearish",
        "2026-09-09T11:20:00-04:00",
        outcome="invalidated",
        mfe=5.4,
        mae=5.5,
    )
    stronger["score"] = 85
    analytics.upsert_records([weaker, stronger])

    payload = analytics.analytics_payload({"start_date": "2026-09-09", "end_date": "2026-09-09"})

    assert payload["metrics"]["total_setups"] == 1
    assert payload["ledger"]["total"] == 1
    assert payload["ledger"]["rows"][0]["pattern_code"] == "2-2 REV D"
    assert payload["metrics"]["median_mfe"] == {"value": 5.4, "sample_size": 1}
    assert payload["coverage"]["duplicate_rows_excluded"] == 1
    assert payload["charts"]["occurrences_by_session"] == [
        {"session_date": "2026-09-09", "count": 1}
    ]


def test_analytics_keeps_distinct_candles_levels_and_directions(analytics_db):
    rows = [
        setup("one", "2026-09-10T10:00:00-04:00", outcome="target_reached"),
        setup("two", "2026-09-10T10:05:00-04:00", outcome="target_reached"),
        setup(
            "three",
            "2026-09-10T10:00:00-04:00",
            direction="bullish",
            family="failed_low",
            outcome="target_reached",
        ),
    ]
    rows.append(setup("four", "2026-09-10T10:00:00-04:00", outcome="target_reached"))
    rows[-1]["level"] = {"key": "put_wall", "label": "Put Wall", "value": 7600}
    analytics.upsert_records(rows)

    payload = analytics.analytics_payload({"start_date": "2026-09-10", "end_date": "2026-09-10"})

    assert payload["metrics"]["total_setups"] == 4
    assert payload["coverage"]["duplicate_rows_excluded"] == 0


def test_analytics_request_does_not_generate_image_files(analytics_db, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    analytics.upsert_records(
        [setup("image-free", "2026-09-10T10:00:00-04:00", outcome="target_reached")]
    )

    analytics.analytics_payload({"start_date": "2026-09-10", "end_date": "2026-09-10"})

    assert not [
        path
        for path in tmp_path.rglob("*")
        if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"}
    ]


def test_history_leaders_prefer_established_evidence_over_tiny_perfect_sample(analytics_db):
    events = []
    for index in range(2):
        event = setup(
            f"tiny-{index}",
            f"2026-09-01T12:{30 + index:02d}:00-04:00",
            family="tiny_family",
            outcome="target_reached",
            mfe=4,
            mae=1,
        )
        event["family_label"] = "Tiny perfect setup"
        events.append(event)
    for index, outcome in enumerate(
        ["target_reached", "target_reached", "target_reached", "target_reached", "invalidated"]
    ):
        event = setup(
            f"established-{index}",
            f"2026-09-02T10:{30 + index:02d}:00-04:00",
            family="established_family",
            outcome=outcome,
            mfe=8,
            mae=2,
        )
        event["family_label"] = "Repeatable setup"
        events.append(event)
    analytics.upsert_records(events)

    payload = analytics.analytics_payload({"preset": "all_history"})
    leader = payload["leaders"]["setup"]

    assert leader["label"] == "Repeatable setup"
    assert leader["target_reached_count"] == 4
    assert leader["evaluated_count"] == 5
    assert leader["evidence"]["label"] == "Established for this sample"
    assert leader["filter_family"] == "established_family"
    assert payload["leaders"]["time"]["filter_start_time"] == "10:30"
    assert payload["leaders"]["combination"]["filter_end_time"] == "10:59"
    assert payload["leaders"]["horizon_label"] == "All-history leaders"


def test_history_leaders_label_early_evidence_and_withhold_empty_winner(analytics_db):
    analytics.upsert_records(
        [setup("early", "2026-09-01T11:10:00-04:00", outcome="target_reached")]
    )
    early = analytics.analytics_payload({"start_date": "2026-09-01", "end_date": "2026-09-01"})
    assert early["leaders"]["setup"]["evidence"]["label"] == "Single example"

    empty = analytics.analytics_payload({"start_date": "2026-09-02", "end_date": "2026-09-02"})
    assert empty["leaders"]["setup"] is None
    assert "No best-performing setup can be measured yet" in empty["leaders"]["unavailable_reason"]


def test_history_leader_ties_are_stable_and_missing_excursions_are_safe():
    base = {
        "total_occurrences": 5,
        "evaluated_count": 5,
        "target_reached_count": 3,
        "target_reached_rate": 60.0,
        "adjusted_target_rate": analytics._wilson_lower_bound(3, 5),
        "median_mfe": {"value": None, "sample_size": 0},
        "median_mae": {"value": None, "sample_size": 0},
        "evidence": analytics._evidence_maturity(5),
    }
    winner = analytics._rank_leader(
        [{**base, "label": "Zulu setup"}, {**base, "label": "Alpha setup"}]
    )

    assert winner["label"] == "Alpha setup"
    assert winner["median_mfe"]["value"] is None
    assert winner["median_mae"]["value"] is None


def test_history_leader_does_not_call_zero_percent_established_cohort_best():
    early = {
        "label": "Early performer",
        "total_occurrences": 2,
        "evaluated_count": 2,
        "target_reached_count": 2,
        "adjusted_target_rate": analytics._wilson_lower_bound(2, 2),
        "median_mfe": {"value": 4.0, "sample_size": 2},
        "median_mae": {"value": 1.0, "sample_size": 2},
    }
    established = {
        "label": "Established non-performer",
        "total_occurrences": 7,
        "evaluated_count": 7,
        "target_reached_count": 0,
        "adjusted_target_rate": analytics._wilson_lower_bound(0, 7),
        "median_mfe": {"value": 1.0, "sample_size": 7},
        "median_mae": {"value": 5.0, "sample_size": 7},
    }

    assert analytics._rank_leader([established, early])["label"] == "Early performer"


def test_history_leaders_follow_the_same_family_and_time_filters(analytics_db):
    bearish = setup(
        "bearish-filtered",
        "2026-09-01T10:35:00-04:00",
        family="failed_high",
        outcome="target_reached",
    )
    bullish = setup(
        "bullish-filtered",
        "2026-09-01T13:35:00-04:00",
        family="failed_low",
        direction="bullish",
        outcome="target_reached",
    )
    analytics.upsert_records([bearish, bullish])

    payload = analytics.analytics_payload(
        {
            "start_date": "2026-09-01",
            "end_date": "2026-09-01",
            "family": "failed_low",
            "start_time": "13:30",
            "end_time": "13:59",
        }
    )

    assert payload["metrics"]["total_setups"] == 1
    assert payload["leaders"]["setup"]["filter_family"] == "failed_low"
    assert payload["leaders"]["time"]["label"] == "1:30–1:59 PM"
    assert payload["leaders"]["combination"]["direction"] == "bullish"


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
    assert (
        "not option fills, actual contract returns, or realized profit" in payload["interpretation"]
    )
    assert "Gamma is frozen signal-time context" in payload["interpretation"]
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
