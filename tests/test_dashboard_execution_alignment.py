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
        gamma_strip={"entries": [], "state": "unavailable", "status_text": "Invalid snapshot: gamma levels unavailable"},
        execution_model={
            "macro_regime": {"state": "unknown", "title": "REGIME UNKNOWN"},
            "local_bias": {"state": "unknown"},
            "levels": {"spot": 6582.0, "main_flip": None, "local_flip": None, "call_wall": None, "put_wall": None},
            "playbook": {"status": "NO TRADE", "tone": "negative", "best_look": "Wait", "avoid": "Avoid force", "need": "Need valid data"},
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
