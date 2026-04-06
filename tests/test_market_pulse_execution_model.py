from datetime import datetime

from mccain_capital.services import core


def _build_model(
    *,
    spot,
    main_flip,
    local_flip,
    call_wall,
    put_wall,
    raw_net_gamma=None,
    mode="live_session",
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
            "mode": mode,
            "levels": [
                {"key": "gamma_flip", "value": main_flip},
                {"key": "local_flip", "value": local_flip},
                {"key": "call_wall", "value": call_wall},
                {"key": "put_wall", "value": put_wall},
            ],
        },
        spx_priority_context={"metrics": {"trap_zone_state": trap_zone_state}},
    )


def test_execution_model_positive_macro_bearish_local_is_not_go():
    model = _build_model(
        spot=6582,
        main_flip=6500,
        local_flip=6590,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=10,
    )
    assert model["macro_regime"]["state"] == "positive"
    assert model["local_bias"]["state"] == "below_local"
    assert model["local_bias"]["label"] == "SELL RIPS"
    assert model["playbook"]["status"] in {"WATCH", "NO TRADE", "CAUTION"}
    assert model["playbook"]["status"] != "GO"


def test_execution_model_positive_macro_bullish_local_is_watch_or_go():
    model = _build_model(
        spot=6610,
        main_flip=6500,
        local_flip=6590,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=10,
    )
    assert model["macro_regime"]["state"] == "positive"
    assert model["local_bias"]["state"] == "above_local"
    assert model["local_bias"]["label"] == "BUY DIPS"
    assert model["playbook"]["status"] in {"WATCH", "GO"}


def test_execution_model_negative_macro_bearish_local_respects_put_wall_proximity():
    model = _build_model(
        spot=6410,
        main_flip=6500,
        local_flip=6440,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=-10,
    )
    assert model["macro_regime"]["state"] == "negative"
    assert model["local_bias"]["state"] == "below_local"
    assert model["local_bias"]["label"] == "SELL RIPS"
    assert model["playbook"]["status"] in {"WATCH", "GO", "CAUTION"}
    assert "put wall" in model["playbook"]["avoid"].lower()


def test_execution_model_neutral_macro_uses_flip_zone():
    model = _build_model(
        spot=6499,
        main_flip=6500,
        local_flip=6498,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=10,
    )
    assert model["macro_regime"]["state"] == "neutral"
    assert model["macro_regime"]["title"] == "FLIP ZONE"
    assert model["playbook"]["status"] in {"NO TRADE", "CAUTION"}


def test_execution_model_flags_raw_net_gamma_sign_conflict():
    model = _build_model(
        spot=6410,
        main_flip=6500,
        local_flip=6440,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=10,
    )
    assert model["macro_regime"]["state"] == "negative"
    assert model["conflicts"]["net_gamma_sign_conflict"] is True


def test_execution_model_missing_local_flip_degrades_cleanly():
    model = _build_model(
        spot=6582,
        main_flip=6500,
        local_flip=None,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=10,
    )
    assert model["local_bias"]["state"] == "unknown"
    assert model["playbook"]["status"] in {"NO TRADE", "CAUTION"}
    assert "local flip" in model["playbook"]["why"].lower()


def test_execution_model_missing_local_flip_in_valid_band_surfaces_explicit_no_band():
    model = core._market_pulse_execution_model(
        spx_quote={"price": 6582},
        gamma_snapshot={
            "snapshot_status": "healthy",
            "local_flip_found": False,
            "gamma_flip_combined_basket": 6500,
            "call_wall_aggregated_gamma": 6690,
            "put_wall_aggregated_gamma": 6450,
            "net_gex": 10,
        },
        execution_chart={
            "mode": "live_session",
            "levels": [
                {"key": "gamma_flip", "value": 6500},
                {"key": "local_flip", "value": None},
                {"key": "call_wall", "value": 6690},
                {"key": "put_wall", "value": 6450},
            ],
        },
        spx_priority_context={"metrics": {"trap_zone_state": "clear"}},
    )
    assert model["local_bias"]["title"] == "NONE IN LOCAL BAND"
    assert model["local_bias"]["state"] == "unknown"
    assert model["local_bias"]["label"] == "NO BAND"


def test_execution_model_negative_macro_with_unknown_local_degrades_readiness():
    model = _build_model(
        spot=6410,
        main_flip=6500,
        local_flip=None,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=-10,
    )
    assert model["macro_regime"]["state"] == "negative"
    assert model["local_bias"]["state"] == "unknown"
    assert model["playbook"]["status"] in {"NO TRADE", "CAUTION"}


def test_execution_model_inside_range_far_from_trigger_is_no_trade():
    model = _build_model(
        spot=6580,
        main_flip=6940,
        local_flip=6490,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=10,
        trap_zone_state="compressed_trap_zone",
    )
    assert model["location"]["inside_range"] is True
    assert model["location"]["midrange"] is True
    assert model["playbook"]["status"] == "NO TRADE"


def test_execution_model_macro_and_local_can_disagree_without_contradicting_verdict():
    model = _build_model(
        spot=6582,
        main_flip=6500,
        local_flip=6590,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=10,
    )
    assert model["macro_regime"]["state"] == "positive"
    assert model["local_bias"]["state"] == "below_local"
    assert model["playbook"]["status"] != "GO"


def test_execution_model_ladder_rows_are_sorted_by_value():
    model = _build_model(
        spot=6582,
        main_flip=6940,
        local_flip=6575,
        call_wall=6690,
        put_wall=6450,
        raw_net_gamma=10,
    )
    values = [row["value"] for row in model["ladder_rows"]]
    assert values == sorted(values, reverse=True)
    labels = [row["label"] for row in model["ladder_rows"]]
    assert "Main Flip" in labels
    assert "Local Flip" in labels


def test_market_pulse_series_vwap_uses_available_rows_without_chart_threshold():
    vwap = core._market_pulse_series_vwap(
        [
            {"close": 100.0, "volume": 10},
            {"close": 103.0, "volume": 20},
            {"close": 101.0, "volume": 30},
        ]
    )
    assert vwap == 101.5


def test_execution_chart_uses_gamma_snapshot_local_flip():
    chart = core._market_pulse_execution_chart_viewmodel(
        spx_quote={
            "symbol": "SPX",
            "price": 6582.0,
            "vwap": None,
            "prior_session_day": "2026-04-02",
            "prior_session_series": [
                {"ts": "2026-04-02T14:30:00-04:00", "close": 6570.0, "volume": 100},
                {"ts": "2026-04-02T14:35:00-04:00", "close": 6580.0, "volume": 200},
                {"ts": "2026-04-02T14:40:00-04:00", "close": 6590.0, "volume": 300},
                {"ts": "2026-04-02T14:45:00-04:00", "close": 6585.0, "volume": 200},
                {"ts": "2026-04-02T14:50:00-04:00", "close": 6578.0, "volume": 150},
                {"ts": "2026-04-02T14:55:00-04:00", "close": 6582.0, "volume": 180},
                {"ts": "2026-04-02T15:00:00-04:00", "close": 6588.0, "volume": 170},
                {"ts": "2026-04-02T15:05:00-04:00", "close": 6584.0, "volume": 120},
            ],
        },
        gamma_snapshot={
            "gamma_flip_combined_basket": 6940,
            "local_flip_aggregated_gamma": 6578.5,
            "call_wall_aggregated_gamma": 6690,
            "put_wall_aggregated_gamma": 6450,
            "regime": "Positive Gamma",
        },
        macro_events=[],
        now_et=datetime.fromisoformat("2026-04-05T10:00:00-04:00"),
    )
    local_flip = next(row["value"] for row in chart["levels"] if row["key"] == "local_flip")
    assert round(local_flip, 2) == 6578.5
