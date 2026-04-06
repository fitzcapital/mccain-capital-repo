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
