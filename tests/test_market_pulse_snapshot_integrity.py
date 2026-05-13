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
                "next_put_wall": {"value": 6550.0},
            },
        }
    }


def _execution_model(
    *,
    spot=6612.25,
    main_flip=6785.0,
    local_flip=6593.0,
    call_wall=6600.0,
    put_wall=6575.0,
    best_look="Buy dips above Local Flip",
):
    return {
        "levels": {
            "spot": spot,
            "main_flip": main_flip,
            "local_flip": local_flip,
            "call_wall": call_wall,
            "put_wall": put_wall,
        },
        "local_bias": {
            "state": "unknown",
            "title": "LOCAL FLIP UNKNOWN",
            "label": "WAIT",
            "context": "LOCAL FLIP UNKNOWN",
        },
        "location": {
            "zone": "Inside Range",
            "nearest_level_name": "Call Wall",
            "status": "Inside Range",
        },
        "playbook": {
            "status": "WAIT",
            "tone": "warn",
            "best_look": best_look,
            "need": "Confirmation at the nearest level",
            "why": "Core levels are usable for intraday planning.",
        },
        "posture_summary": "Core levels are usable for intraday planning.",
    }


def test_after_hours_spot_prefers_official_close_over_zero():
    spot = core._market_pulse_resolve_spot_snapshot(
        session_mode="after_hours",
        spx_quote={
            "price": None,
            "change_pct": None,
            "prior_session_series": [{"close": 6588.75}],
            "asof": "2026-04-07T16:01:00-04:00",
        },
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


def test_invalid_after_hours_gamma_rejects_incoherent_last_valid_snapshot():
    payload = core._market_pulse_resolve_gamma_payload(
        gamma_snapshot={"snapshot_status": "invalid"},
        last_good_snapshot=_cached_snapshot(call_wall=6575.0, put_wall=6600.0),
        session_mode="closed",
        now_et=datetime.fromisoformat("2026-04-07T18:30:00-04:00"),
    )
    assert payload["levels_source"] == "unavailable"
    assert payload["gamma_data_status"] == "invalid"
    assert payload["structure_invariant_status"] == "hard_invalid"
    assert "wall_order_invalid" in payload["structure_invariant_issues"]


def test_malformed_wall_order_downgrades_live_gamma_payload_before_render():
    payload = core._market_pulse_resolve_gamma_payload(
        gamma_snapshot={
            "snapshot_status": "healthy",
            "last_successful_compute": "2026-04-07T15:59:00-04:00",
            "regime": "Positive Gamma",
            "gamma_flip_combined_basket": 6785.0,
            "local_flip_aggregated_gamma": 6593.0,
            "call_wall_aggregated_gamma": 6575.0,
            "put_wall_aggregated_gamma": 6600.0,
            "next_call_wall_above": 6620.0,
            "next_put_wall_below": 6590.0,
        },
        last_good_snapshot=None,
        session_mode="regular",
        now_et=datetime.fromisoformat("2026-04-07T15:59:30-04:00"),
    )
    assert payload["levels_source"] == "unavailable"
    assert payload["gamma_data_status"] == "invalid"
    assert payload["gamma_regime_meta"]["value"] == "unavailable"
    assert payload["structure_invariant_status"] == "hard_invalid"
    assert "wall_order_invalid" in payload["structure_invariant_issues"]
    assert payload["level_meta"]["call_wall"]["value"] is None
    assert payload["level_meta"]["put_wall"]["value"] is None


def test_soft_invariant_failure_sanitizes_bad_next_wall_and_downgrades_board():
    payload = core._market_pulse_resolve_gamma_payload(
        gamma_snapshot={
            "snapshot_status": "healthy",
            "last_successful_compute": "2026-04-07T15:59:00-04:00",
            "regime": "Positive Gamma",
            "gamma_flip_combined_basket": 6785.0,
            "local_flip_aggregated_gamma": 6593.0,
            "call_wall_aggregated_gamma": 6600.0,
            "put_wall_aggregated_gamma": 6575.0,
            "next_call_wall_above": 6590.0,
            "next_put_wall_below": 6590.0,
        },
        last_good_snapshot=None,
        session_mode="regular",
        now_et=datetime.fromisoformat("2026-04-07T15:59:30-04:00"),
    )
    assert payload["levels_source"] == "live_session_snapshot"
    assert payload["gamma_data_status"] == "partial"
    assert payload["gamma_regime_meta"]["value"] == "unconfirmed"
    assert payload["structure_invariant_status"] == "soft_invalid"
    assert "next_call_wall_not_above_call_wall" in payload["structure_invariant_issues"]
    assert "next_put_wall_not_below_put_wall" in payload["structure_invariant_issues"]
    assert payload["level_meta"]["next_call_wall"]["value"] is None
    assert payload["level_meta"]["next_put_wall"]["value"] is None


def test_gamma_reason_label_explains_partial_and_fallback_states():
    assert (
        core._market_pulse_gamma_reason_label(
            gamma_regime="unconfirmed",
            gamma_data_status="partial",
            levels_source="live_session_snapshot",
            structure_invariant_status="ok",
            structure_invariant_issues=[],
        )
        == "Partial board"
    )
    assert (
        core._market_pulse_gamma_reason_label(
            gamma_regime="unconfirmed",
            gamma_data_status="partial",
            levels_source="last_valid_snapshot",
            structure_invariant_status="ok",
            structure_invariant_issues=[],
        )
        == "Last valid structure"
    )
    assert (
        core._market_pulse_gamma_reason_label(
            gamma_regime="unconfirmed",
            gamma_data_status="partial",
            levels_source="live_session_snapshot",
            structure_invariant_status="soft_invalid",
            structure_invariant_issues=[
                "next_call_wall_not_above_call_wall",
                "next_put_wall_not_below_put_wall",
            ],
        )
        == "Next walls sanitized"
    )


def test_gamma_failure_label_ignores_successful_tradier_fetch_text():
    assert (
        core._market_pulse_gamma_failure_label(
            "Tradier live options chain fetched successfully."
        )
        == ""
    )


def test_canonical_playbook_view_softens_copy_for_unconfirmed_planning_bias():
    playbook = core._build_playbook_view_model(
        market_structure_snapshot={
            "spot": 6824.66,
            "session_mode": "after_hours",
            "session_mode_label": "After Hours",
            "trade_state": "PLANNING_ONLY",
            "trade_state_label": "PLANNING ONLY",
            "levels_source": "last_valid_snapshot",
            "gamma_data_status": "partial",
            "gamma_regime": "unconfirmed",
            "gamma_regime_label": "Unconfirmed",
            "gamma_regime_reason_label": "Last valid structure",
            "regime_confidence": "low",
            "regime_confidence_label": "Low confidence",
            "planning_bias": "bullish_above_local_flip",
            "planning_bias_label": "Bullish above Local Flip 6815",
            "main_flip": 6815.0,
            "local_flip": 6815.0,
            "call_wall": 6830.0,
            "put_wall": 6600.0,
            "current_read": "Above Local Flip",
            "pullback_level": "LF 6815",
            "next_destination": "CW 6830",
            "best_look": "Buy dips above Local Flip",
            "required_trigger": "Next live session dip-hold above Local Flip",
            "invalidation": "Lose Local Flip 6815",
            "plan_note": "After-hours planning from the last valid session snapshot.",
            "context_grade": "D",
            "context_score": 52,
            "context_tone": "warn",
            "tradeability": "PLANNING_ONLY",
        },
        execution_model={
            "neutral_band_local": 2.5,
            "location": {
                "zone": "Near Call Wall",
                "status": "Resistance nearby",
                "nearest_level_name": "Call Wall",
                "distance_points": 5.3,
            },
            "distances": {
                "to_main_flip": 9.66,
                "to_local_flip": 9.66,
                "to_call_wall": -5.34,
                "to_put_wall": 224.66,
            },
            "distance_rows": [],
        },
    )
    assert playbook["bias_label"] == "Conditional bullish above Local Flip 6815"
    assert playbook["plan_label"] == "Buy dips above Local Flip only if next session confirms"
    assert playbook["trade_gate_label"] == "Next live session dip-hold above Local Flip"
    assert playbook["decision_label"] == "Planning only"
    assert playbook["bias_summary_label"] == "Buy dips only on confirmation"
    assert "planning only until live confirmation" in playbook["hero_summary"].lower()
    assert playbook["ui_flags"]["is_planning_only"] is True
    assert playbook["ui_flags"]["is_unconfirmed_gamma"] is True


def test_canonical_playbook_view_uses_explicit_unavailable_copy():
    playbook = core._build_playbook_view_model(
        market_structure_snapshot={
            "spot": 694.77,
            "session_mode": "regular",
            "session_mode_label": "Regular",
            "trade_state": "UNAVAILABLE",
            "trade_state_label": "UNAVAILABLE",
            "levels_source": "unavailable",
            "gamma_data_status": "invalid",
            "gamma_regime": "unavailable",
            "gamma_regime_label": "Regime Unavailable",
            "gamma_regime_reason_label": "Gamma snapshot unavailable",
            "gamma_regime_subtitle": "Gamma snapshot unavailable",
            "regime_confidence": "none",
            "regime_confidence_label": "No confidence",
            "planning_bias": "unavailable",
            "planning_bias_label": "Unavailable",
            "current_read": "Setup Pending",
            "plan_note": "",
            "tradeability": "NO_TRADE",
        },
        execution_model={
            "location": {"zone": "Unknown", "status": "Awaiting structure"},
            "playbook": {"why": "Gamma snapshot unavailable."},
        },
    )
    assert playbook["decision_label"] == "No trade"
    assert playbook["bias_summary_label"] == "Wait for cleaner structure"
    assert playbook["hero_reason_label"] == "Gamma snapshot unavailable. Wait for validated levels."
    assert playbook["hero_summary"] == "Gamma snapshot unavailable, wait for validated levels before acting."


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
    assert (
        core._market_pulse_playbook_snapshot_valid(
            {
                "market_structure_snapshot": {
                    "app_state": "UNAVAILABLE",
                    "spot_meta": {"value": None},
                    "levels_source": "unavailable",
                }
            }
        )
        is False
    )


def test_playbook_snapshot_valid_rejects_invariant_violations():
    assert (
        core._market_pulse_playbook_snapshot_valid(
            {
                "market_structure_snapshot": {
                    "app_state": "AFTER_HOURS_VALID",
                    "spot_meta": {"value": 6612.25},
                    "levels_source": "last_valid_snapshot",
                    "level_meta": {
                        "call_wall": {"value": 6575.0},
                        "put_wall": {"value": 6600.0},
                        "local_flip": {"value": 6593.0},
                    },
                }
            }
        )
        is False
    )


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
    assert snapshot["trigger_validation"]["manual_label"] == "Manual confirmation required"
    assert snapshot["trigger_validation"]["status_badge"] == "PLANNING"
    assert (
        snapshot["trigger_validation"]["status_line"]
        == "PLANNING ONLY — NEXT LIVE TRIGGER REQUIRED"
    )
    assert "Call Wall" in snapshot["trigger_validation"]["items"]["sweep"]["line"]
    assert snapshot["trigger_validation"]["items"]["volume"]["active"] is False


def test_structure_snapshot_promotes_partial_regular_board_to_provisional_regime():
    snapshot = core._market_pulse_structure_snapshot(
        spx_quote={"price": 6832.5, "asof": "2026-04-10T09:37:54-04:00"},
        gamma_snapshot={
            "snapshot_status": "healthy",
            "regime": "Negative Gamma",
            "spot_price_used": 6832.5,
            "gamma_flip_combined_basket": 6985.0,
            "local_flip_aggregated_gamma": 6770.0,
            "call_wall_aggregated_gamma": 6850.0,
            "put_wall_aggregated_gamma": None,
            "next_call_wall_above": None,
            "next_put_wall_below": None,
        },
        execution_chart={"mode": "live_session", "summary": "Live session"},
        execution_model=_execution_model(
            spot=6832.5,
            main_flip=6985.0,
            local_flip=6770.0,
            call_wall=6850.0,
            put_wall=None,
            best_look="Sell rips near resistance",
        ),
        now_et=datetime.fromisoformat("2026-04-10T09:37:54-04:00"),
    )
    assert snapshot["gamma_data_status"] == "partial"
    assert snapshot["gamma_regime_state"] == "provisional"
    assert snapshot["gamma_board_status"] == "Partial"
    assert snapshot["gamma_regime"] == "negative"
    assert snapshot["gamma_regime_label"] == "Provisional Negative Gamma"
    assert snapshot["gamma_regime_subtitle"] == "Core levels present · medium confidence"
    assert snapshot["gamma_regime_confidence"] == "Medium"
    assert snapshot["regime_confidence"] == "medium"
    assert snapshot["gamma_regime_reason_label"] == "Intraday usable"


def test_structure_snapshot_partial_board_missing_main_flip_stays_unconfirmed():
    snapshot = core._market_pulse_structure_snapshot(
        spx_quote={"price": 6832.5, "asof": "2026-04-10T09:37:54-04:00"},
        gamma_snapshot={
            "snapshot_status": "healthy",
            "regime": "Positive Gamma",
            "spot_price_used": 6832.5,
            "gamma_flip_combined_basket": None,
            "local_flip_aggregated_gamma": 6770.0,
            "call_wall_aggregated_gamma": 6850.0,
            "put_wall_aggregated_gamma": 6755.0,
            "next_call_wall_above": None,
            "next_put_wall_below": None,
        },
        execution_chart={"mode": "live_session", "summary": "Live session"},
        execution_model=_execution_model(
            spot=6832.5,
            main_flip=None,
            local_flip=6770.0,
            call_wall=6850.0,
            put_wall=6755.0,
        ),
        now_et=datetime.fromisoformat("2026-04-10T09:37:54-04:00"),
    )
    assert snapshot["gamma_data_status"] == "partial"
    assert snapshot["gamma_regime_state"] == "unconfirmed"
    assert snapshot["gamma_regime"] == "unconfirmed"
    assert snapshot["gamma_regime_label"] == "Unconfirmed"
    assert snapshot["gamma_regime_subtitle"] == "Waiting on main flip"
    assert snapshot["gamma_regime_reason_label"] == "Waiting on main flip"
    assert snapshot["gamma_regime_missing_reason"] == "waiting_on_main_flip"
    assert snapshot["gamma_regime_confidence"] == "Low"


def test_gamma_regime_viewmodel_preserves_signal_when_execution_levels_unavailable():
    regime = core._market_pulse_gamma_regime_viewmodel(
        {"regime": "strong_positive"},
        gamma_data_status="invalid",
        regime_status="unavailable",
        missing_reason="no_usable_snapshot",
    )
    assert regime["gamma_regime"] == "positive"
    assert regime["gamma_regime_label"] == "Positive Gamma"
    assert regime["gamma_regime_subtitle"] == "Execution levels unavailable"


def test_structure_snapshot_fresh_valid_board_remains_confirmed():
    snapshot = core._market_pulse_structure_snapshot(
        spx_quote={"price": 6832.5, "asof": "2026-04-10T09:37:54-04:00"},
        gamma_snapshot={
            "snapshot_status": "healthy",
            "regime": "Strong Positive Gamma",
            "spot_price_used": 6832.5,
            "gamma_flip_combined_basket": 6815.0,
            "local_flip_aggregated_gamma": 6815.0,
            "call_wall_aggregated_gamma": 6850.0,
            "put_wall_aggregated_gamma": 6755.0,
            "next_call_wall_above": 6880.0,
            "next_put_wall_below": 6725.0,
        },
        execution_chart={"mode": "live_session", "summary": "Live session"},
        execution_model=_execution_model(
            spot=6832.5,
            main_flip=6815.0,
            local_flip=6815.0,
            call_wall=6850.0,
            put_wall=6755.0,
        ),
        now_et=datetime.fromisoformat("2026-04-10T09:37:54-04:00"),
    )
    assert snapshot["gamma_data_status"] == "fresh_valid"
    assert snapshot["gamma_regime_state"] == "confirmed"
    assert snapshot["gamma_board_status"] == "Complete"
    assert snapshot["gamma_regime"] == "positive"
    assert snapshot["gamma_regime_label"] == "Positive Gamma"
    assert snapshot["gamma_regime_subtitle"] == "Strong mean reversion / pinning active"
    assert snapshot["gamma_regime_confidence"] == "High"


def test_apply_resolved_levels_preserves_raw_regime_direction():
    out = core._market_pulse_apply_resolved_levels(
        {
            "regime": "Positive Gamma",
            "gamma_flip_combined_basket": 6815.0,
        },
        {
            "gamma_data_status": "partial",
            "gamma_regime_meta": {
                "value": "unconfirmed",
                "label": "Unconfirmed",
                "subtitle": "Gamma board incomplete",
            },
            "level_meta": {
                "main_flip": {"value": 6815.0},
                "local_flip": {"value": 6815.0},
                "call_wall": {"value": 6850.0},
                "put_wall": {"value": 6755.0},
                "next_call_wall": {"value": None},
                "next_put_wall": {"value": None},
            },
        },
    )
    assert out["regime"] == "Positive Gamma"
    assert out["resolved_gamma_data_status"] == "partial"


def test_gamma_resolution_infers_next_put_wall_and_keeps_secondary_pair_displayable():
    payload = core._market_pulse_resolve_gamma_payload(
        gamma_snapshot={
            "snapshot_status": "healthy",
            "last_successful_compute": "2026-04-10T09:37:54-04:00",
            "regime": "Positive Gamma",
            "spot_price_used": 6832.5,
            "gamma_flip_combined_basket": 6815.0,
            "local_flip_aggregated_gamma": 6815.0,
            "call_wall_aggregated_gamma": 6850.0,
            "put_wall_aggregated_gamma": 6755.0,
            "next_call_wall_above": 6880.0,
            "next_put_wall_below": None,
            "gamma_walls_top3": [6880.0, 6725.0, 6700.0],
            "chart_json": {
                "gex": {"data": [{"x": [6700.0, 6725.0, 6755.0, 6815.0, 6850.0, 6880.0]}]}
            },
        },
        last_good_snapshot=None,
        session_mode="regular",
        now_et=datetime.fromisoformat("2026-04-10T09:37:54-04:00"),
    )
    canonical = payload["canonical_structure"]
    assert canonical["next_call_wall"] == 6880.0
    assert canonical["next_put_wall"] == 6725.0
    assert canonical["next_call_wall_source"] == "provider"
    assert canonical["next_put_wall_source"] == "inferred"
    assert canonical["next_put_wall_confidence"] in {"high", "medium"}
    assert canonical["secondary_structure_complete"] is True
    assert canonical["secondary_structure_displayable"] is True
    assert canonical["resolution_source"] == "Mixed provider + inferred"
    assert payload["level_meta"]["next_put_wall"]["value"] == 6725.0


def test_gamma_resolution_suppresses_one_sided_secondary_band_when_next_put_is_weak():
    payload = core._market_pulse_resolve_gamma_payload(
        gamma_snapshot={
            "snapshot_status": "healthy",
            "last_successful_compute": "2026-04-10T09:37:54-04:00",
            "regime": "Positive Gamma",
            "spot_price_used": 6832.5,
            "gamma_flip_combined_basket": 6815.0,
            "local_flip_aggregated_gamma": 6815.0,
            "call_wall_aggregated_gamma": 6850.0,
            "put_wall_aggregated_gamma": 6755.0,
            "next_call_wall_above": 6880.0,
            "next_put_wall_below": None,
            "gamma_walls_top3": [6880.0],
            "chart_json": {"gex": {"data": [{"x": [6500.0, 6755.0, 6815.0, 6850.0, 6880.0]}]}},
        },
        last_good_snapshot=None,
        session_mode="regular",
        now_et=datetime.fromisoformat("2026-04-10T09:37:54-04:00"),
    )
    canonical = payload["canonical_structure"]
    assert canonical["next_call_wall"] is None
    assert canonical["next_put_wall"] is None
    assert canonical["secondary_structure_complete"] is False
    assert canonical["secondary_structure_displayable"] is False
    assert canonical["degraded_reason"] in {
        "no_trustworthy_next_put_wall_candidate",
        "heuristic_next_put_wall_not_supported",
    }
    assert payload["level_meta"]["next_call_wall"]["value"] is None
    assert payload["level_meta"]["next_put_wall"]["value"] is None


def test_structure_snapshot_exposes_canonical_secondary_metadata():
    snapshot = core._market_pulse_structure_snapshot(
        spx_quote={"price": 6832.5, "asof": "2026-04-10T09:37:54-04:00"},
        gamma_snapshot={
            "snapshot_status": "healthy",
            "regime": "Positive Gamma",
            "spot_price_used": 6832.5,
            "gamma_flip_combined_basket": 6815.0,
            "local_flip_aggregated_gamma": 6815.0,
            "call_wall_aggregated_gamma": 6850.0,
            "put_wall_aggregated_gamma": 6755.0,
            "next_call_wall_above": 6880.0,
            "next_put_wall_below": None,
            "gamma_walls_top3": [6880.0, 6725.0, 6700.0],
            "chart_json": {
                "gex": {"data": [{"x": [6700.0, 6725.0, 6755.0, 6815.0, 6850.0, 6880.0]}]}
            },
        },
        execution_chart={"mode": "live_session", "summary": "Live session"},
        execution_model=_execution_model(
            spot=6832.5,
            main_flip=6815.0,
            local_flip=6815.0,
            call_wall=6850.0,
            put_wall=6755.0,
        ),
        now_et=datetime.fromisoformat("2026-04-10T09:37:54-04:00"),
    )
    assert snapshot["next_call_wall"] == 6880.0
    assert snapshot["next_put_wall"] == 6725.0
    assert snapshot["secondary_structure_displayable"] is True
    assert snapshot["resolution_source"] == "Mixed provider + inferred"


def test_gamma_resolution_uses_grouped_strike_rows_for_secondary_candidate_universe():
    payload = core._market_pulse_resolve_gamma_payload(
        gamma_snapshot={
            "snapshot_status": "healthy",
            "last_successful_compute": "2026-04-10T09:37:54-04:00",
            "regime": "Positive Gamma",
            "spot_price_used": 6832.5,
            "gamma_flip_combined_basket": 6815.0,
            "local_flip_aggregated_gamma": 6815.0,
            "call_wall_aggregated_gamma": 6850.0,
            "put_wall_aggregated_gamma": 6755.0,
            "next_call_wall_above": None,
            "next_put_wall_below": None,
            "gamma_walls_top3": [6850.0],
            "chart_json": {"gex": None},
            "grouped_strike_rows": [
                {"strike": 6700.0, "put_oi": 400.0, "put_side_gex": 12.0, "net_gex": -12.0},
                {"strike": 6725.0, "put_oi": 1250.0, "put_side_gex": 28.0, "net_gex": -28.0},
                {"strike": 6755.0, "put_oi": 1700.0, "put_side_gex": 30.0, "net_gex": -30.0},
                {"strike": 6815.0, "call_oi": 1400.0, "call_side_gex": 16.0, "net_gex": 16.0},
                {"strike": 6850.0, "call_oi": 1600.0, "call_side_gex": 24.0, "net_gex": 24.0},
                {"strike": 6875.0, "call_oi": 900.0, "call_side_gex": 14.0, "net_gex": 14.0},
            ],
        },
        last_good_snapshot=None,
        session_mode="regular",
        now_et=datetime.fromisoformat("2026-04-10T09:37:54-04:00"),
    )
    canonical = payload["canonical_structure"]
    ladder = canonical["secondary_candidate_ladder"]
    assert [row["strike"] for row in ladder] == [6700.0, 6725.0, 6755.0, 6815.0, 6850.0, 6875.0]
    assert canonical["next_put_wall"] in {6725.0, 6730.0}
    assert canonical["next_put_wall_source"] == "inferred"
    assert canonical["secondary_structure_displayable"] is True
