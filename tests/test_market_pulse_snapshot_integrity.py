from datetime import datetime

from mccain_capital.services import core


def _cached_snapshot(
    *,
    spot=6612.25,
    main_flip=6785.0,
    local_flip=6593.0,
    call_wall=6600.0,
    put_wall=6575.0,
    last_valid_time="2026-04-07T15:59:00-04:00",
):
    return {
        "market_structure_snapshot": {
            "spot_meta": {"value": spot, "as_of": last_valid_time},
            "spot": spot,
            "snapshot_timestamp": last_valid_time,
            "last_valid_snapshot_time": last_valid_time,
            "level_meta": {
                "main_flip": {"value": main_flip},
                "local_flip": {"value": local_flip},
                "call_wall": {"value": call_wall},
                "put_wall": {"value": put_wall},
                "next_call_wall": {"value": 6620.0},
                "next_put_wall": {"value": 6590.0},
            },
        }
    }


def test_after_hours_spot_prefers_official_close_over_zero():
    spot = core._market_pulse_resolve_spot_snapshot(
        session_mode="after_hours",
        spx_quote={"price": None, "change_pct": None, "prior_session_series": [{"close": 6588.75}], "asof": "2026-04-07T16:01:00-04:00"},
        gamma_snapshot={},
        now_et=datetime.fromisoformat("2026-04-07T18:15:00-04:00"),
        last_good_snapshot=None,
    )
    assert spot["value"] == 6588.75
    assert spot["source"] == "official_close"
    assert spot["state"] == "AFTER_HOURS_VALID"


def test_after_hours_spot_uses_last_good_snapshot_when_needed():
    spot = core._market_pulse_resolve_spot_snapshot(
        session_mode="closed",
        spx_quote={"price": None, "asof": ""},
        gamma_snapshot={"spot": None},
        now_et=datetime.fromisoformat("2026-04-07T21:00:00-04:00"),
        last_good_snapshot=_cached_snapshot(spot=6601.5),
    )
    assert spot["value"] == 6601.5
    assert spot["source"] == "last_good_snapshot"
    assert spot["state"] == "AFTER_HOURS_VALID"


def test_invalid_spot_never_coerces_zero():
    spot = core._market_pulse_resolve_spot_snapshot(
        session_mode="closed",
        spx_quote={"price": 0, "asof": "2026-04-07T20:00:00-04:00"},
        gamma_snapshot={"spot": 0},
        now_et=datetime.fromisoformat("2026-04-07T21:00:00-04:00"),
        last_good_snapshot=None,
    )
    assert spot["value"] is None
    assert spot["source"] == "unavailable"


def test_invalid_after_hours_gamma_uses_last_valid_snapshot_for_planning():
    payload = core._market_pulse_resolve_gamma_payload(
        gamma_snapshot={"snapshot_status": "invalid"},
        last_good_snapshot=_cached_snapshot(),
        session_mode="after_hours",
        now_et=datetime.fromisoformat("2026-04-07T18:30:00-04:00"),
    )
    assert payload["levels_source"] == "last_valid_snapshot"
    assert payload["gamma_data_status"] == "stale_but_usable"
    assert payload["gamma_regime_meta"]["value"] == "unconfirmed"
    assert payload["local_flip_meta"]["value"] == 6593.0
    assert payload["local_flip_meta"]["source"] == "prior_valid_session"


def test_invalid_after_hours_gamma_without_usable_snapshot_is_unavailable():
    payload = core._market_pulse_resolve_gamma_payload(
        gamma_snapshot={"snapshot_status": "invalid"},
        last_good_snapshot=_cached_snapshot(local_flip=None),
        session_mode="closed",
        now_et=datetime.fromisoformat("2026-04-08T18:30:00-04:00"),
    )
    assert payload["levels_source"] == "unavailable"
    assert payload["gamma_data_status"] == "invalid"
    assert payload["gamma_regime_meta"]["value"] == "unavailable"


def test_execution_chart_reuses_last_valid_session_bars_after_close():
    chart = core._market_pulse_execution_chart_viewmodel(
        spx_quote={
            "symbol": "SPX",
            "price": 6605.0,
            "series": [
                {"ts": "2026-04-07T15:40:00-04:00", "close": 6598.0, "volume": 10},
                {"ts": "2026-04-07T15:45:00-04:00", "close": 6601.0, "volume": 12},
                {"ts": "2026-04-07T15:50:00-04:00", "close": 6603.0, "volume": 11},
                {"ts": "2026-04-07T15:55:00-04:00", "close": 6604.0, "volume": 14},
                {"ts": "2026-04-07T16:00:00-04:00", "close": 6605.0, "volume": 15},
                {"ts": "2026-04-07T16:05:00-04:00", "close": 6606.0, "volume": 9},
                {"ts": "2026-04-07T16:10:00-04:00", "close": 6607.0, "volume": 7},
                {"ts": "2026-04-07T16:15:00-04:00", "close": 6608.0, "volume": 8},
            ],
            "prior_session_series": [],
        },
        gamma_snapshot={
            "gamma_flip_combined_basket": 6785.0,
            "local_flip_aggregated_gamma": 6593.0,
            "call_wall_aggregated_gamma": 6600.0,
            "put_wall_aggregated_gamma": 6575.0,
            "regime": "Negative Gamma",
        },
        macro_events=[],
        now_et=datetime.fromisoformat("2026-04-07T21:10:00-04:00"),
        resolved_spot=6605.0,
        resolved_levels={
            "main_flip": 6785.0,
            "local_flip": 6593.0,
            "call_wall": 6600.0,
            "put_wall": 6575.0,
        },
    )
    assert chart["mode"] == "last_session_replay"
    assert chart["latest_price"] == 6608.0


def test_playbook_snapshot_valid_rejects_zero_and_unavailable():
    assert core._market_pulse_playbook_snapshot_valid(
        {
            "market_structure_snapshot": {
                "app_state": "UNAVAILABLE",
                "spot_meta": {"value": None},
                "levels_source": "unavailable",
            }
        }
    ) is False


def test_structure_snapshot_after_hours_uses_planning_labels_not_unknown():
    snapshot = core._market_pulse_structure_snapshot(
        spx_quote={"price": 6608.0, "asof": "2026-04-07T16:00:00-04:00"},
        gamma_snapshot={
            "snapshot_status": "invalid",
            "last_successful_compute": "2026-04-07T15:59:00-04:00",
            "next_call_wall_above": 6620.0,
            "next_put_wall_below": 6590.0,
        },
        execution_chart={
            "mode": "last_session_replay",
            "last_valid_session_label": "Tue Apr 7",
            "summary": "Replay available",
        },
        execution_model={
            "levels": {
                "spot": 6608.0,
                "main_flip": 6785.0,
                "local_flip": 6593.0,
                "call_wall": 6600.0,
                "put_wall": 6575.0,
            },
            "local_bias": {
                "state": "unknown",
                "title": "LOCAL FLIP UNKNOWN",
                "label": "WAIT",
                "context": "LOCAL FLIP UNKNOWN",
            },
            "location": {
                "zone": "Unknown",
                "nearest_level_name": "Call Wall",
            },
            "playbook": {
                "status": "NO_TRADE",
                "tone": "warn",
                "best_look": "Wait for local flip to print",
                "need": "Live local flip or usable intraday anchor",
                "why": "Macro context exists, but the intraday local flip is unavailable.",
            },
            "posture_summary": "Macro context exists, but the intraday local flip is unavailable.",
        },
        now_et=datetime.fromisoformat("2026-04-07T20:30:00-04:00"),
    )
    assert snapshot["levels_source"] == "last_valid_snapshot"
    assert snapshot["trade_state"] == "PLANNING_ONLY"
    assert snapshot["bias_context"] == "ABOVE CALL WALL"
    assert snapshot["bias_label"] == "EXTENSION RISK"
    assert snapshot["current_read"] == "Above Call Wall"
    assert snapshot["plan_note"].startswith("Closed-session planning:")
