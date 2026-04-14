from datetime import datetime

from mccain_capital.services import core


def _execution_model(
    *,
    spot,
    main_flip,
    local_flip,
    call_wall,
    put_wall,
    raw_net_gamma=None,
    trap_zone_state="clear",
):
    return core._market_pulse_execution_model(
        spx_quote={"price": spot},
        gamma_snapshot={
            "gamma_flip_combined_basket": main_flip,
            "call_wall_aggregated_gamma": call_wall,
            "put_wall_aggregated_gamma": put_wall,
            "net_gex": raw_net_gamma,
        },
        execution_chart={
            "mode": "live_session",
            "levels": [
                {"key": "gamma_flip", "value": main_flip},
                {"key": "local_flip", "value": local_flip},
                {"key": "call_wall", "value": call_wall},
                {"key": "put_wall", "value": put_wall},
            ],
        },
        spx_priority_context={"metrics": {"trap_zone_state": trap_zone_state}},
    )


def test_dashboard_decision_maps_negative_inside_range_to_two_way_and_reduced():
    model = _execution_model(
        spot=6582,
        main_flip=6940,
        local_flip=6575,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=-10,
    )
    panel = core._dashboard_decision_viewmodel(
        daily_brief={"plan_a": "ignored", "no_trade": "ignored"},
        risk_posture_title="Reduced size",
        risk_posture_detail="Use less size",
        data_trust={"tone": "positive"},
        readiness={"pct": 100.0},
        dashboard_vix={"price": 22.0},
        gamma_strip={"entries": [], "state": "live", "status_text": "ok"},
        execution_model=model,
    )
    assert panel["bias"] == "Two-way / responsive"
    assert panel["risk_size"] == "Reduced size"
    assert panel["plan"] == model["playbook"]["best_look"]


def test_dashboard_decision_maps_positive_above_local_to_buy_dips_and_normal():
    model = _execution_model(
        spot=6610,
        main_flip=6500,
        local_flip=6590,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=10,
    )
    panel = core._dashboard_decision_viewmodel(
        daily_brief={"plan_a": "ignored", "no_trade": "ignored"},
        risk_posture_title="Reduced size",
        risk_posture_detail="Use less size",
        data_trust={"tone": "positive"},
        readiness={"pct": 100.0},
        dashboard_vix={"price": 14.0},
        gamma_strip={"entries": [], "state": "live", "status_text": "ok"},
        execution_model=model,
    )
    assert panel["bias"] == "Buy dips bias"
    assert panel["risk_size"] == "Normal size"


def test_dashboard_decision_missing_snapshot_degrades_to_unavailable():
    panel = core._dashboard_decision_viewmodel(
        daily_brief={"plan_a": "fallback plan", "no_trade": "fallback gate"},
        risk_posture_title="Reduced size",
        risk_posture_detail="Use less size",
        data_trust={"tone": "positive"},
        readiness={"pct": 100.0},
        dashboard_vix={"price": 16.0},
        gamma_strip={
            "entries": [],
            "state": "unavailable",
            "status_text": "Invalid snapshot: gamma levels unavailable",
        },
        execution_model={
            "macro_regime": {"state": "unknown", "title": "REGIME UNKNOWN"},
            "local_bias": {"state": "unknown"},
            "levels": {
                "spot": 6582.0,
                "main_flip": None,
                "local_flip": None,
                "call_wall": None,
                "put_wall": None,
            },
            "playbook": {
                "status": "NO TRADE",
                "tone": "negative",
                "best_look": "Wait",
                "avoid": "Avoid force",
                "need": "Need valid data",
            },
            "posture_summary": "Invalid snapshot: gamma levels unavailable",
        },
    )
    assert panel["bias"] == "Unavailable"
    assert panel["risk_size"] == "No trade / stand down"
    assert panel["plan"] == "Wait"


def test_dashboard_decision_local_flip_missing_stays_aligned_but_degraded():
    model = _execution_model(
        spot=6582,
        main_flip=6500,
        local_flip=None,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=10,
    )
    panel = core._dashboard_decision_viewmodel(
        daily_brief={"plan_a": "ignored", "no_trade": "ignored"},
        risk_posture_title="Reduced size",
        risk_posture_detail="Use less size",
        data_trust={"tone": "positive"},
        readiness={"pct": 100.0},
        dashboard_vix={"price": 16.0},
        gamma_strip={"entries": [], "state": "live", "status_text": "ok"},
        execution_model=model,
    )
    assert panel["bias"] == "Positive"
    assert panel["playbook_status"] in {"NO TRADE", "CAUTION"}


def test_dashboard_gamma_strip_reads_levels_from_execution_model():
    model = _execution_model(
        spot=6610,
        main_flip=6500,
        local_flip=6590,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=10,
    )
    strip = core._dashboard_gamma_strip_viewmodel(
        execution_model=model,
        gamma_snapshot={"snapshot_status": "healthy"},
    )
    entry_map = {entry["key"]: entry["value"] for entry in strip["entries"]}
    assert entry_map["regime"] == "POSITIVE"
    assert entry_map["main_flip"] == "6500"
    assert entry_map["local_flip"] == "6590"
    assert entry_map["call_wall"] == "6690"
    assert entry_map["put_wall"] == "6450"


def test_dashboard_gamma_strip_shows_no_local_band_label_for_null_local_flip():
    model = _execution_model(
        spot=6582,
        main_flip=6500,
        local_flip=None,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=10,
    )
    strip = core._dashboard_gamma_strip_viewmodel(
        execution_model=model,
        gamma_snapshot={"snapshot_status": "healthy", "local_flip_found": False},
    )
    entry_map = {entry["key"]: entry["value"] for entry in strip["entries"]}
    assert entry_map["local_flip"] == "None in local band"


def test_dashboard_decision_prefers_market_structure_snapshot_over_execution_model():
    model = _execution_model(
        spot=6610,
        main_flip=6500,
        local_flip=6590,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=10,
    )
    panel = core._dashboard_decision_viewmodel(
        daily_brief={"plan_a": "ignored", "no_trade": "ignored", "headline": "brief"},
        risk_posture_title="Reduced size",
        risk_posture_detail="Use less size",
        data_trust={"tone": "positive"},
        readiness={"pct": 100.0},
        dashboard_vix={"price": 14.0},
        gamma_strip={"entries": [], "state": "stale", "status_text": "ok"},
        execution_model=model,
        market_structure_snapshot={
            "trade_state": "PLANNING_ONLY",
            "trade_state_label": "PLANNING ONLY",
            "bias": "Bullish above Local Flip 6593",
            "best_look": "Buy dip above Local Flip",
            "required_trigger": "Next live session dip-hold above Local Flip",
            "plan_note": "After-hours planning from the last valid session snapshot.",
            "regime_confidence_label": "Low confidence",
            "bias_state": "bullish_above_local_flip",
        },
    )
    assert panel["bias"] == "Bullish above Local Flip 6593"
    assert panel["risk_size"] == "Planning only"
    assert panel["status"] == "PLANNING ONLY"
    assert panel["plan"] == "Buy dip above Local Flip"


def test_dashboard_gamma_strip_prefers_market_structure_snapshot():
    strip = core._dashboard_gamma_strip_viewmodel(
        market_structure_snapshot={
            "gamma_data_status": "partial",
            "levels_source": "last_valid_snapshot",
            "gamma_regime": "unconfirmed",
            "gamma_regime_label": "Unconfirmed",
            "gamma_regime_reason_label": "Last valid structure",
            "session_mode_label": "After Hours",
            "levels_source_label": "Last valid snapshot",
            "last_valid_snapshot_time_label": "Tue Apr 07 4:00 PM ET",
            "main_flip": 6785.0,
            "local_flip": 6593.0,
            "call_wall": 6600.0,
            "put_wall": 6575.0,
        }
    )
    entry_map = {entry["key"]: entry["value"] for entry in strip["entries"]}
    assert strip["state"] == "stale"
    assert "After Hours" in strip["status_text"]
    regime_entry = next(entry for entry in strip["entries"] if entry["key"] == "regime")
    assert entry_map["regime"] == "UNCONFIRMED"
    assert regime_entry["detail"] == "Last valid structure"
    assert entry_map["main_flip"] == "6785"
    assert entry_map["local_flip"] == "6593"


def test_dashboard_decision_prefers_canonical_playbook_view():
    panel = core._dashboard_decision_viewmodel(
        daily_brief={"plan_a": "fallback plan", "no_trade": "fallback gate", "headline": "brief"},
        risk_posture_title="Reduced size",
        risk_posture_detail="Use less size",
        data_trust={"tone": "positive"},
        readiness={"pct": 100.0},
        dashboard_vix={"price": 15.0},
        playbook_view={
            "trade_state": "planning_only",
            "trade_state_label": "PLANNING ONLY",
            "bias_state": "conditional_bullish",
            "bias_label": "Conditional bullish above Local Flip 6815",
            "plan_label": "Buy dips above Local Flip only if next session confirms",
            "trade_gate_label": "Next live session dip-hold above Local Flip",
            "risk_label": "Planning only",
            "context_lead_label": "Planning posture is valid, but live confirmation is still required.",
            "context_score": 52,
            "context_grade": "D",
        },
        gamma_strip={"entries": [], "state": "stale", "status_text": "ok"},
    )
    assert panel["bias"] == "Conditional bullish above Local Flip 6815"
    assert panel["risk_size"] == "Planning only"
    assert panel["status"] == "PLANNING ONLY"
    assert panel["plan"] == "Buy dips above Local Flip only if next session confirms"
    assert panel["trade_gate"] == "Next live session dip-hold above Local Flip"


def test_dashboard_gamma_strip_never_renders_one_sided_secondary_levels():
    strip = core._dashboard_gamma_strip_viewmodel(
        market_structure_snapshot={
            "gamma_data_status": "fresh_valid",
            "levels_source": "live_session_snapshot",
            "gamma_regime": "positive",
            "gamma_regime_label": "Positive Gamma",
            "gamma_regime_reason_label": "Provider-backed",
            "session_mode_label": "Regular",
            "snapshot_timestamp_label": "Fri Apr 10 9:37 AM ET",
            "spot": 6832.5,
            "main_flip": 6815.0,
            "local_flip": 6815.0,
            "call_wall": 6850.0,
            "put_wall": 6755.0,
            "next_call_wall": 6880.0,
            "next_put_wall": None,
        }
    )
    entry_map = {entry["key"]: entry["value"] for entry in strip["entries"]}
    assert entry_map["next_call_wall"] == "--"
    assert entry_map["next_put_wall"] == "--"


def test_dashboard_daily_brief_uses_canonical_secondary_structure_only():
    brief = core._dashboard_daily_brief_viewmodel(
        now_et=datetime.fromisoformat("2026-04-10T09:37:54-04:00"),
        dashboard_spx={"price": 6748.0, "pct_change": -0.4, "day_open": 6760.0},
        dashboard_vix={"price": 18.2},
        gamma_snapshot={
            "gamma_flip": 6785.0,
            "local_flip": 6593.0,
            "call_wall": 6600.0,
            "put_wall": 6755.0,
            "next_call_wall_above": 6880.0,
            "next_put_wall_below": 6725.0,
        },
        market_structure_snapshot={
            "spot": 6748.0,
            "main_flip": 6785.0,
            "local_flip": 6593.0,
            "call_wall": 6850.0,
            "put_wall": 6755.0,
            "next_call_wall": None,
            "next_put_wall": None,
            "secondary_structure_displayable": False,
        },
        news_snapshot={"macro_events": []},
        today_count=0,
        today_net=0.0,
    )
    assert "6725" not in brief["execution_triggers"]
