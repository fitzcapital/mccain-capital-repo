import json

import pytest

from mccain_capital.services.spx_liquidity_sweep import evaluate_failed_liquidity_sweep


LEVELS = [
    {"kind": "new_call_wall", "value": 6100},
    {"kind": "call_wall", "value": 6075},
    {"kind": "prior_day_high", "value": 6065},
    {"kind": "gamma_flip", "value": 6050},
    {"kind": "put_wall", "value": 6025},
    {"kind": "prior_day_low", "value": 6010},
    {"kind": "new_put_wall", "value": 6000},
]


def strategy_input(**overrides):
    payload = {
        "spot": 6075,
        "active_level": {"kind": "call_wall", "value": 6075},
        "interaction": "from_below",
        "gamma_regime": "positive",
        "levels": LEVELS,
        "location": True,
        "sweep": True,
        "close_back_inside_5m": True,
        "reversal_2_2_5m": True,
        "trigger_break": True,
    }
    payload.update(overrides)
    return payload


@pytest.mark.parametrize(
    ("overrides", "state"),
    [
        ({"location": None}, "WAITING_FOR_LOCATION"),
        ({"sweep": None}, "LEVEL_BEING_TESTED"),
        ({"close_back_inside_5m": None}, "SWEEP_DETECTED"),
        ({"close_back_inside_5m": False}, "WAITING_FOR_CLOSE_BACK_INSIDE"),
        ({"reversal_2_2_5m": None}, "WAITING_FOR_5M_2_2"),
        ({"trigger_break": None}, "WAITING_FOR_TRIGGER_BREAK"),
        ({}, "REVERSAL_READY"),
        (
            {
                "acceptance": True,
                "close_back_inside_5m": None,
                "reversal_2_2_5m": None,
                "trigger_break": None,
            },
            "ACCEPTANCE_CONFIRMED",
        ),
        (
            {
                "acceptance": True,
                "continuation_retest": True,
                "close_back_inside_5m": None,
                "reversal_2_2_5m": None,
                "trigger_break": None,
            },
            "CONTINUATION_ACTIVE",
        ),
        ({"acceptance": True}, "SETUP_INVALIDATED"),
    ],
)
def test_every_strategy_state(overrides, state):
    result = evaluate_failed_liquidity_sweep(strategy_input(**overrides))
    assert result["state"] == state


def test_trigger_cannot_skip_required_sequence():
    result = evaluate_failed_liquidity_sweep(
        strategy_input(sweep=None, close_back_inside_5m=True, reversal_2_2_5m=True)
    )
    assert result["state"] == "LEVEL_BEING_TESTED"
    assert result["setup_ready"] is False
    assert result["checklist"]["trigger_break"] == "Pending"


def test_positive_gamma_failed_high_sweep_is_bearish_scalp():
    result = evaluate_failed_liquidity_sweep(strategy_input())
    assert result["state"] == "REVERSAL_READY"
    assert result["direction"] == "bearish"
    assert result["primary_target"]["label"] == "Prior-Day High"
    assert result["expansion_targets"] == []
    assert "Mean-reversion environment" in result["gamma_interpretation"]


def test_negative_gamma_failed_high_sweep_has_expansion_and_runner_context():
    result = evaluate_failed_liquidity_sweep(
        strategy_input(
            gamma_regime="negative",
            active_level={"kind": "prior_day_high", "value": 6065},
            runner_2_2_15m=True,
        )
    )
    assert result["state"] == "REVERSAL_READY"
    assert result["primary_target"]["label"] == "Gamma Flip"
    assert [row["label"] for row in result["expansion_targets"]] == [
        "Put Wall",
        "Prior-Day Low",
        "New Put Wall",
    ]
    assert result["runner_confirmation"] == "Confirmed"


def test_break_and_hold_above_call_wall_is_bullish_continuation_not_fade():
    result = evaluate_failed_liquidity_sweep(
        strategy_input(
            acceptance=True,
            continuation_retest=True,
            close_back_inside_5m=None,
            reversal_2_2_5m=None,
            trigger_break=None,
        )
    )
    assert result["state"] == "CONTINUATION_ACTIVE"
    assert result["path"] == "continuation"
    assert result["direction"] is None
    assert result["primary_target"]["label"] == "New Call Wall"
    assert result["checklist"]["setup_ready"] == "Failed"


def test_incomplete_sweep_stays_pending():
    result = evaluate_failed_liquidity_sweep(
        strategy_input(
            sweep=None, close_back_inside_5m=None, reversal_2_2_5m=None, trigger_break=None
        )
    )
    assert result["state"] == "LEVEL_BEING_TESTED"
    assert result["setup_ready"] is False
    assert "sweep liquidity" in result["missing_evidence"]
    assert result["primary_target"] is None
    assert result["expansion_targets"] == []


def test_acceptance_without_retest_withholds_target():
    result = evaluate_failed_liquidity_sweep(
        strategy_input(
            acceptance=True,
            close_back_inside_5m=None,
            reversal_2_2_5m=None,
            trigger_break=None,
        )
    )

    assert result["state"] == "ACCEPTANCE_CONFIRMED"
    assert result["primary_target"] is None
    assert result["expansion_targets"] == []


def test_bullish_failed_low_sweep():
    result = evaluate_failed_liquidity_sweep(
        strategy_input(
            active_level={"kind": "put_wall", "value": 6025},
            spot=6025,
            interaction="from_above",
        )
    )
    assert result["state"] == "REVERSAL_READY"
    assert result["direction"] == "bullish"
    assert result["primary_target"]["label"] == "Gamma Flip"


def test_active_level_and_regime_recompute_targets_without_carrying_state():
    positive = evaluate_failed_liquidity_sweep(strategy_input())
    changed = evaluate_failed_liquidity_sweep(
        strategy_input(
            active_level={"kind": "put_wall", "value": 6025},
            interaction="from_above",
            gamma_regime="negative",
            sweep=None,
            close_back_inside_5m=None,
            reversal_2_2_5m=None,
            trigger_break=None,
        )
    )
    assert positive["state"] == "REVERSAL_READY"
    assert changed["state"] == "LEVEL_BEING_TESTED"
    assert changed["direction"] == "bullish"
    assert changed["primary_target"] is None
    assert changed["expansion_targets"] == []


def test_missing_and_malformed_data_are_safe_and_json_serializable():
    result = evaluate_failed_liquidity_sweep(
        {
            "spot": float("nan"),
            "active_level": {"kind": "call_wall", "value": "bad"},
            "interaction": "sideways",
            "gamma_regime": "mystery",
            "levels": [{"kind": "call_wall", "value": object()}],
            "trigger_break": True,
        }
    )
    assert result["state"] == "WAITING_FOR_LOCATION"
    assert result["checklist"]["location"] == "Unavailable"
    assert json.loads(json.dumps(result))["setup_ready"] is False


def test_timestamps_are_preserved_when_supplied():
    result = evaluate_failed_liquidity_sweep(
        strategy_input(sweep={"confirmed": True, "timestamp": "2026-08-07T10:00:00-04:00"})
    )
    assert result["evidence_timestamps"]["sweep"] == "2026-08-07T10:00:00-04:00"
