from mccain_capital.services import gamma_context_service as svc


def test_classify_proximity_flip_thresholds():
    assert svc.classifyProximity(4.9, "flip") == "at_flip"
    assert svc.classifyProximity(7, "flip") == "very_close"
    assert svc.classifyProximity(20, "flip") == "near"
    assert svc.classifyProximity(21, "flip") == "far"


def test_classify_proximity_wall_thresholds():
    assert svc.classifyProximity(-5, "wall") == "at_wall"
    assert svc.classifyProximity(10, "wall") == "very_close"
    assert svc.classifyProximity(-14, "wall") == "near"
    assert svc.classifyProximity(35, "wall") == "far"


def test_classify_trap_zone_detects_knife_edge_and_compressed():
    assert svc.classifyTrapZone(5005, 5000, 5007, 4993) == "knife_edge_structure"
    assert svc.classifyTrapZone(5010, 5000, 5014, 4970) == "compressed_trap_zone"
    assert svc.classifyTrapZone(4950, 5000, 5035, 4965) == "clear"


def test_compute_distance_metrics_expected_move_and_states():
    metrics = svc.computeDistanceMetrics(
        {
            "spot": 5008,
            "gammaFlip": 5000,
            "callWall": 5016,
            "putWall": 4985,
            "expectedMove": 12,
            "netGamma": 1.0,
            "nextCallWallAbove": 5030,
            "nextPutWallBelow": 4970,
        }
    )
    assert metrics["distance_to_flip"] == 8
    assert metrics["distance_to_call_wall"] == -8
    assert metrics["distance_to_put_wall"] == 23
    assert metrics["expected_move_up"] == 5020
    assert metrics["expected_move_down"] == 4996
    assert metrics["flip_proximity_state"] == "very_close"
    assert metrics["wall_proximity_state"] == "very_close"
    assert metrics["next_call_wall_above"] == 5030
    assert metrics["next_put_wall_below"] == 4970


def test_build_gamma_narrative_includes_regime_and_transition_note():
    data = {
        "spot": 5004,
        "gammaFlip": 5000,
        "callWall": 5010,
        "putWall": 4985,
        "netGamma": -20,
    }
    metrics = svc.computeDistanceMetrics(data)
    lines = svc.buildGammaNarrative(data, metrics)
    joined = " ".join(lines).lower()
    assert "negative gamma" in joined
    assert "regime transition" in joined


def test_build_spx_priority_context_uses_chart_levels_for_next_walls():
    context = svc.build_spx_priority_context(
        spx_quote={"price": 5008},
        gamma_snapshot={
            "gamma_flip": 5000,
            "call_wall": 5010,
            "put_wall": 4990,
            "net_gex": 1234,
            "chart_json": {
                "gex": {
                    "data": [
                        {
                            "x": [4970, 4980, 4990, 5000, 5010, 5020, 5030],
                            "y": [1, 1, 1, 1, 1, 1, 1],
                        }
                    ]
                }
            },
        },
    )
    assert context["metrics"]["next_call_wall_above"] == 5020
    assert context["metrics"]["next_put_wall_below"] == 4980


def test_format_level_distance():
    assert svc.formatLevelDistance(None) == "—"
    assert svc.formatLevelDistance(0) == "0 pts"
    assert svc.formatLevelDistance(-4.5) == "4.5 pts below"
