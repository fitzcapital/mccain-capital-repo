from datetime import datetime
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from mccain_capital.services.market_pulse_setup_replay import _outcome
from mccain_capital.services.market_pulse_setup_replay import _estimated_scalp_targets
from mccain_capital.services.market_pulse_setup_replay import _gamma_at_signal
from mccain_capital.services.market_pulse_setup_replay import _next_target
from mccain_capital.services.market_pulse_setup_replay import _point_in_time_levels
from mccain_capital.services.market_pulse_setup_replay import build_intraday_setup_events
from mccain_capital.services.market_pulse_setup_replay import build_intraday_setup_replay


ET = ZoneInfo("America/New_York")


def test_estimated_scalp_targets_convert_contract_returns_to_spx_prices():
    bearish = _estimated_scalp_targets(7685.13, "bearish")
    bullish = _estimated_scalp_targets(7685.13, "bullish")

    assert bearish["targets"] == [
            {"key": "tp1", "return_percent": 15, "point_move": 2.81, "spx_price": 7682.32},
            {"key": "tp2", "return_percent": 20, "point_move": 3.75, "spx_price": 7681.38},
            {"key": "tp3", "return_percent": 30, "point_move": 5.62, "spx_price": 7679.51},
        ]
    assert bearish["contract_cost"] == 750
    assert bearish["absolute_delta"] == 0.4
    assert bearish["fallback_used"] is True
    assert bearish["dealer_gamma_used"] is False
    assert [row["spx_price"] for row in bullish["targets"]] == [7687.94, 7688.88, 7690.76]


def test_estimated_scalp_targets_use_tradier_ntm_reference_without_gamma_math():
    result = _estimated_scalp_targets(
        5000,
        "bullish",
        {
            "pricing_mode": "tradier_current_quote",
            "source": "Tradier current options snapshot",
            "contract_label": "SPXW 2026-08-19 5000C",
            "contract_cost": 1000,
            "absolute_delta": 0.50,
            "as_of": "2026-08-19T14:00:00+00:00",
        },
    )

    assert result["targets"][1]["spx_price"] == 5004
    assert result["pricing_mode"] == "tradier_current_quote"
    assert result["fallback_used"] is False
    assert result["contract_label"].endswith("5000C")
    assert result["dealer_gamma_used"] is False


def test_gamma_selector_never_projects_a_future_observation_backward():
    signal = datetime.fromisoformat("2026-08-19T10:00:00-04:00")
    selected = _gamma_at_signal(
        [
            {"as_of": "2026-08-19T09:45:00-04:00", "regime": "negative_gamma"},
            {"as_of": "2026-08-19T10:15:00-04:00", "regime": "positive_gamma"},
        ],
        signal,
    )

    assert selected["regime"] == "negative_gamma"


def test_runner_is_directional_and_five_points_from_actual_entry():
    from mccain_capital.services.market_pulse_scenarios import normalize_levels

    levels = normalize_levels(
        [
            {"key": "local_flip", "value": 101},
            {"key": "put_wall", "value": 95},
            {"key": "prior_day_low", "value": 90},
        ]
    )

    target = _next_target(levels, 99.6, "bearish", excluded_keys={"local_flip"})

    assert target["value"] == 90


def test_normalized_dynamic_level_preserves_observation_timestamp():
    from mccain_capital.services.market_pulse_scenarios import normalize_levels

    level = normalize_levels(
        [{"key": "call_wall", "value": 5010, "as_of": "2026-08-19T09:45:00-04:00"}]
    )[0]

    assert level.as_of == "2026-08-19T09:45:00-04:00"


def _bar(clock: str, *, open_: float, high: float, low: float, close: float):
    stamp = datetime.fromisoformat(f"2026-08-19T{clock}:00").replace(tzinfo=ET)
    return {
        "ts": stamp.isoformat(),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
    }


def _replay(
    bars,
    *,
    gamma_as_of="2026-08-19T09:30:00-04:00",
    include_rejected=False,
    levels=None,
):
    replay_levels = levels or [
        {"key": "local_flip", "label": "Local Flip", "value": 100},
        {"key": "put_wall", "label": "Put Wall", "value": 95},
        {"key": "call_wall", "label": "Call Wall", "value": 105},
    ]
    replay_levels = [
        {
            **row,
            **(
                {"as_of": gamma_as_of}
                if row.get("key")
                in {
                    "gamma_flip",
                    "local_flip",
                    "call_wall",
                    "put_wall",
                    "new_call_wall",
                    "new_put_wall",
                }
                else {}
            ),
        }
        for row in replay_levels
    ]
    return build_intraday_setup_replay(
        ticker="SPX",
        session_date="2026-08-19",
        bars=bars,
        levels=replay_levels,
        gamma_regime="negative_gamma",
        gamma_as_of=gamma_as_of,
        include_rejected=include_rejected,
    )


def test_outcome_separates_full_target_from_setup_and_excursion_status():
    outcome = _outcome(
        signal={"close": 100, "level_value": 101, "direction": "bearish"},
        future=[_bar("10:00", open_=100, high=100.2, low=95, close=96)],
        target={"value": 95},
    )

    assert outcome["state"] == "target_reached"
    assert outcome["setup_status"] == "valid"
    assert outcome["target_status"] == "reached"
    assert outcome["opportunity_label"] == "Full target reached"
    assert outcome["target_progress_percent"] == 100.0


def test_outcome_preserves_favorable_excursion_before_invalidation():
    outcome = _outcome(
        signal={"close": 100, "level_value": 101, "direction": "bearish"},
        future=[
            _bar("10:00", open_=100, high=100.2, low=97.5, close=98),
            _bar("10:05", open_=98, high=101.2, low=98, close=101.1),
        ],
        target={"value": 95},
    )

    assert outcome["state"] == "invalidated"
    assert outcome["setup_status_label"] == "Setup invalidated"
    assert outcome["target_status_label"] == "Target not reached"
    assert outcome["opportunity_label"] == "Favorable excursion before invalidation"
    assert outcome["target_progress_percent"] == 50.0


def test_outcome_marks_same_candle_target_and_invalidation_as_ambiguous():
    outcome = _outcome(
        signal={"close": 100, "level_value": 101, "direction": "bearish"},
        future=[_bar("10:00", open_=100, high=101.2, low=94.8, close=101.1)],
        target={"value": 95},
    )

    assert outcome["state"] == "ambiguous"
    assert outcome["setup_status"] == "ambiguous"
    assert outcome["target_status"] == "ambiguous"
    assert outcome["opportunity_label"] == ("Target and invalidation touched in the same candle")
    assert outcome["at"].endswith("10:00:00-04:00")
    assert outcome["evaluated_through"] == outcome["at"]


def test_outcome_does_not_invalidate_on_wick_when_close_holds_anchor():
    outcome = _outcome(
        signal={
            "close": 100,
            "level_value": 101,
            "direction": "bearish",
            "signal_time": "2026-08-19T09:55:00-04:00",
        },
        future=[_bar("10:00", open_=100, high=102, low=98, close=100.5)],
        target={"value": 95},
    )

    assert outcome["state"] == "open"
    assert outcome["evaluated_through"].endswith("10:00:00-04:00")


def test_outcome_invalidates_on_completed_close_and_records_time():
    outcome = _outcome(
        signal={"close": 100, "level_value": 101, "direction": "bearish"},
        future=[_bar("10:00", open_=100, high=102, low=98, close=101.25)],
        target={"value": 95},
    )

    assert outcome["state"] == "invalidated"
    assert outcome["at"].endswith("10:00:00-04:00")


def test_outcome_target_touch_records_later_candle_time():
    outcome = _outcome(
        signal={"close": 100, "level_value": 101, "direction": "bearish"},
        future=[_bar("10:05", open_=100, high=100.5, low=94.5, close=96)],
        target={"value": 95},
    )

    assert outcome["state"] == "target_reached"
    assert outcome["at"].endswith("10:05:00-04:00")


def test_outcome_labels_no_favorable_move_as_limited_follow_through():
    outcome = _outcome(
        signal={"close": 100, "level_value": 101, "direction": "bearish"},
        future=[_bar("10:00", open_=100, high=100.5, low=100, close=100.2)],
        target={"value": 95},
    )

    assert outcome["state"] == "open"
    assert outcome["opportunity_label"] == "Limited follow-through"
    assert outcome["target_progress_percent"] == 0.0


def test_outcome_uses_frozen_trigger_boundary_instead_of_signal_candle_close():
    outcome = _outcome(
        signal={
            "entry_price": 100,
            "close": 98,
            "level_value": 102,
            "direction": "bearish",
        },
        future=[_bar("10:00", open_=98, high=99, low=95, close=96)],
        target={"value": 90},
    )

    assert outcome["mfe"] == 5
    assert outcome["mae"] == 0
    assert outcome["target_progress_percent"] == 50


def test_bearish_22_reversal_uses_second_candle_as_signal_without_third_candle():
    result = _replay(
        [
            _bar("09:30", open_=99, high=100, low=98, close=99),
            _bar("09:35", open_=99, high=102, low=98.5, close=101.5),
            _bar("09:40", open_=101.5, high=101.8, low=97.5, close=99),
        ],
        levels=[
            {"key": "prior_day_high", "label": "Prior-Day High", "value": 101},
            {"key": "prior_day_low", "label": "Prior-Day Low", "value": 95},
        ],
    )

    setup = next(row for row in result["setups"] if row["strat_pattern"]["code"] == "2-2 REV D")
    assert setup["signal_time"].endswith("09:40:00-04:00")
    assert setup["signal_candle_time"] == setup["signal_time"]
    assert setup["entry_zone"] == 98.5
    assert setup["entry_basis"] == "first_pattern_candle_boundary"
    assert setup["trigger_evidence"]["trigger_source"] == "pattern_signal_candle"


def test_bearish_22_reversal_is_not_reissued_on_later_2d_continuations():
    result = _replay(
        [
            _bar("09:50", open_=7678.05, high=7684.65, low=7674.89, close=7684.65),
            _bar("09:55", open_=7685.01, high=7686.98, low=7683.38, close=7686.94),
            _bar("10:00", open_=7688.36, high=7688.36, low=7685.13, close=7687.56),
            _bar("10:05", open_=7687.50, high=7687.71, low=7683.45, close=7684.41),
            _bar("10:10", open_=7684.48, high=7684.72, low=7679.63, close=7679.82),
            _bar("10:15", open_=7679.85, high=7681.56, low=7677.39, close=7677.97),
            _bar("10:20", open_=7678.09, high=7678.92, low=7672.53, close=7672.61),
        ],
        levels=[
            {"key": "current_day_high", "label": "Current-Day High", "value": 7688.36},
            {"key": "current_day_low", "label": "Current-Day Low", "value": 7672.53},
        ],
    )

    bearish_reversals = [
        row for row in result["setups"] if row["strat_pattern"]["code"] == "2-2 REV D"
    ]
    assert len(bearish_reversals) == 1
    assert bearish_reversals[0]["signal_time"].endswith("10:05:00-04:00")
    assert (
        bearish_reversals[0]["strat_pattern"]["completed_at"] == bearish_reversals[0]["signal_time"]
    )


def test_audited_1130_reversal_qualifies_at_call_wall_with_narrow_proximity():
    result = _replay(
        [
            _bar("11:20", open_=7760.82, high=7767.08, low=7760.82, close=7766.22),
            _bar("11:25", open_=7766.17, high=7769.81, low=7765.97, close=7767.92),
            _bar("11:30", open_=7767.99, high=7768.18, low=7762.96, close=7762.99),
            _bar("11:35", open_=7762.89, high=7763.59, low=7754.14, close=7754.56),
        ],
        levels=[
            {"key": "call_wall", "label": "Call Wall", "value": 7770.00},
            {"key": "prior_day_high", "label": "Prior-Day High", "value": 7741.27},
        ],
    )

    setups = [row for row in result["setups"] if row["strat_pattern"]["code"] == "2-2 REV D"]
    assert len(setups) == 1
    assert setups[0]["signal_time"].endswith("11:30:00-04:00")
    assert setups[0]["level"]["key"] == "call_wall"


def test_replay_uses_latest_durable_dynamic_level_known_at_signal():
    bars = [
        _bar("11:20", open_=7760.82, high=7767.08, low=7760.82, close=7766.22),
        _bar("11:25", open_=7766.17, high=7769.81, low=7765.97, close=7767.92),
        _bar("11:30", open_=7767.99, high=7768.18, low=7762.96, close=7762.99),
    ]
    levels = _point_in_time_levels(
        [
            {
                "key": "call_wall",
                "value": 7770,
                "as_of": "2026-08-19T11:17:00-04:00",
            },
            {
                "key": "call_wall",
                "value": 7780,
                "as_of": "2026-08-19T12:00:00-04:00",
            },
        ],
        bars,
    )

    assert levels == [
        {
            "key": "call_wall",
            "value": 7770,
            "as_of": "2026-08-19T11:17:00-04:00",
        }
    ]


def test_same_pattern_at_nearby_highs_is_one_setup_with_level_confluence():
    result = _replay(
        [
            _bar("09:30", open_=99, high=100, low=98, close=99),
            _bar("09:35", open_=99, high=102, low=98.5, close=101.5),
            _bar("09:40", open_=101.5, high=101.8, low=97.5, close=99),
        ],
        levels=[
            {"key": "prior_day_high", "label": "Prior-Day High", "value": 101},
            {"key": "current_day_high", "label": "Current-Day High", "value": 102},
            {"key": "prior_day_low", "label": "Prior-Day Low", "value": 90},
        ],
    )

    setups = [row for row in result["setups"] if row["strat_pattern"]["code"] == "2-2 REV D"]
    assert len(setups) == 1
    setup = setups[0]
    assert setup["level"]["key"] == "prior_day_high"
    assert setup["supporting_levels"] == [
        {
            "key": "current_day_high",
            "label": "Current-Day High",
            "value": 102.0,
            "role": "supporting_confluence",
        }
    ]
    assert setup["family_label"] == "Sweep and reject Prior-Day High"
    assert setup["score"] == 85
    assert setup["grade"] == "A-"
    assert setup["target"]["key"] == "prior_day_low"


def test_replay_marks_setup_non_actionable_without_meaningful_target():
    result = _replay(
        [
            _bar("09:30", open_=99, high=100, low=98, close=99),
            _bar("09:35", open_=99, high=102, low=98.5, close=101.5),
            _bar("09:40", open_=101.5, high=101.8, low=97.5, close=99),
        ],
        levels=[{"key": "prior_day_high", "label": "Prior-Day High", "value": 101}],
    )

    setup = result["setups"][0]
    assert setup["target"] is None
    assert setup["actionable"] is False
    assert "at least 5 points" in setup["target_diagnostic"]


def test_bullish_22_reversal_keeps_third_candle_as_follow_through_only():
    pattern_bars = [
        _bar("09:30", open_=101, high=102, low=100, close=101),
        _bar("09:35", open_=101, high=101.5, low=98, close=98.5),
        _bar("09:40", open_=98.5, high=102.5, low=98.2, close=100),
    ]
    signal_only = _replay(
        pattern_bars,
        levels=[
            {"key": "prior_day_low", "label": "Prior-Day Low", "value": 99},
            {"key": "prior_day_high", "label": "Prior-Day High", "value": 105},
        ],
    )
    with_follow_through = _replay(
        [*pattern_bars, _bar("09:45", open_=100, high=104, low=99.5, close=103)],
        levels=[
            {"key": "prior_day_low", "label": "Prior-Day Low", "value": 99},
            {"key": "prior_day_high", "label": "Prior-Day High", "value": 105},
        ],
    )

    before = next(
        row for row in signal_only["setups"] if row["strat_pattern"]["code"] == "2-2 REV U"
    )
    after = next(
        row for row in with_follow_through["setups"] if row["strat_pattern"]["code"] == "2-2 REV U"
    )
    assert before["signal_time"].endswith("09:40:00-04:00")
    assert after["signal_time"] == before["signal_time"]
    assert after["entry_zone"] == before["entry_zone"] == 101.5
    assert after["outcome"]["mfe"] == 2.5


def test_replay_qualifies_212_on_third_candle_and_measures_later_outcome():
    result = _replay(
        [
            _bar("09:30", open_=100, high=101, low=99, close=100),
            _bar("09:35", open_=100, high=102, low=99.5, close=99.8),
            _bar("09:40", open_=99.8, high=101.5, low=99.6, close=99.7),
            _bar("09:45", open_=99.7, high=100.5, low=98, close=99),
            _bar("09:50", open_=99, high=99, low=94, close=95),
        ]
    )

    setup = next(row for row in result["setups"] if row["strat_pattern"]["code"] == "2-1-2D")
    assert setup["signal_time"].endswith("09:45:00-04:00")
    assert setup["entry_zone"] == 99.6
    assert setup["entry_basis"] == "inside_candle_boundary"
    assert setup["trigger_evidence"]["trigger_boundary_source"] == "inside_candle"
    assert setup["target"] is None
    assert setup["outcome"]["state"] == "open"
    assert setup["strat_pattern"]["code"] == "2-1-2D"
    assert setup["data_availability"]["gamma_available_at_signal"] is True
    assert result["read_only"] is True


def test_future_bars_change_outcome_without_changing_signal_eligibility():
    signal_bars = [
        _bar("09:30", open_=100, high=101, low=99, close=100),
        _bar("09:35", open_=100, high=102, low=99.5, close=99.8),
        _bar("09:40", open_=99.8, high=101.5, low=99.6, close=99.7),
        _bar("09:45", open_=99.7, high=100.5, low=98, close=99),
    ]
    before = _replay(signal_bars)
    after = _replay(signal_bars + [_bar("09:50", open_=98.5, high=99, low=94, close=95)])
    before_setup = next(row for row in before["setups"] if row["strat_pattern"]["code"] == "2-1-2D")
    after_setup = next(row for row in after["setups"] if row["strat_pattern"]["code"] == "2-1-2D")

    assert before_setup["signal_time"].endswith("09:45:00-04:00")
    assert after_setup["signal_time"] == before_setup["signal_time"]
    assert after_setup["entry_zone"] == before_setup["entry_zone"] == 99.6
    assert before_setup["outcome"]["state"] == "open"
    assert after_setup["outcome"]["state"] == "open"


def test_shared_live_events_match_replay_signal_identity():
    bars = [
        _bar("09:30", open_=100, high=101, low=99, close=100),
        _bar("09:35", open_=100, high=102, low=99.5, close=99.8),
        _bar("09:40", open_=99.8, high=101.5, low=99.6, close=99.7),
        _bar("09:45", open_=99.7, high=100.5, low=98, close=99),
    ]
    levels = [
        {
            "key": "local_flip",
            "label": "Local Flip",
            "value": 100,
            "as_of": "2026-08-19T09:30:00-04:00",
        },
        {"key": "put_wall", "label": "Put Wall", "value": 95, "as_of": "2026-08-19T09:30:00-04:00"},
        {
            "key": "call_wall",
            "label": "Call Wall",
            "value": 105,
            "as_of": "2026-08-19T09:30:00-04:00",
        },
    ]
    replay = _replay(bars)
    events = build_intraday_setup_events(
        ticker="SPX",
        session_date="2026-08-19",
        bars=bars,
        levels=levels,
        gamma_regime="negative_gamma",
        gamma_as_of="2026-08-19T09:30:00-04:00",
    )

    assert [row["setup_event_id"] for row in events] == [
        row["setup_event_id"] for row in replay["setups"]
    ]
    assert all("outcome" not in row for row in events)


def test_bullish_212_uses_inside_high_and_later_candle_only_as_follow_through():
    pattern_bars = [
        _bar("09:30", open_=101, high=102, low=100, close=101),
        _bar("09:35", open_=101, high=101.5, low=98, close=98.5),
        _bar("09:40", open_=98.5, high=101, low=98.5, close=99),
        _bar("09:45", open_=99, high=102, low=98.8, close=100),
    ]
    levels = [
        {"key": "prior_day_low", "label": "Prior-Day Low", "value": 99},
        {"key": "prior_day_high", "label": "Prior-Day High", "value": 105},
    ]
    signal_only = _replay(pattern_bars, levels=levels)
    with_follow_through = _replay(
        [*pattern_bars, _bar("09:50", open_=100, high=104, low=99.5, close=103)],
        levels=levels,
    )

    before = next(row for row in signal_only["setups"] if row["strat_pattern"]["code"] == "2-1-2U")
    after = next(
        row for row in with_follow_through["setups"] if row["strat_pattern"]["code"] == "2-1-2U"
    )
    assert before["signal_time"].endswith("09:45:00-04:00")
    assert after["signal_time"] == before["signal_time"]
    assert after["entry_zone"] == before["entry_zone"] == 101
    assert after["entry_basis"] == "inside_candle_boundary"
    assert after["outcome"]["mfe"] == 3


def test_replay_accepts_cdh_creation_when_signal_closes_back_below():
    result = _replay(
        [
            _bar("11:45", open_=99, high=100, low=96, close=98),
            _bar("11:50", open_=98, high=105, low=97, close=104),
            _bar("11:55", open_=104, high=104.5, low=98, close=100),
            _bar("12:00", open_=100, high=101, low=95, close=96),
        ],
        levels=[
            {"key": "current_day_high", "label": "Current-Day High", "value": 105},
            {"key": "put_wall", "label": "Put Wall", "value": 95},
        ],
    )

    setup = next(row for row in result["setups"] if row["strat_pattern"]["code"] == "2-1-2D")
    assert setup["signal_time"].endswith("12:00:00-04:00")
    assert setup["strat_pattern"]["completed_at"] == setup["signal_time"]


def test_replay_rejects_random_212_that_only_intersects_a_wall():
    result = _replay(
        [
            _bar("11:45", open_=100, high=101, low=99, close=100),
            _bar("11:50", open_=100, high=106, low=99, close=105),
            _bar("11:55", open_=105, high=105.5, low=101, close=103),
            _bar("12:00", open_=103, high=104, low=98, close=99),
            _bar("12:05", open_=99, high=100, low=96, close=97),
        ],
        levels=[
            {"key": "call_wall", "label": "Call Wall", "value": 106},
            {"key": "put_wall", "label": "Put Wall", "value": 95},
        ],
        include_rejected=True,
    )

    assert not any(
        row["strat_pattern"]["code"] == "2-1-2D" and row["level"]["key"] == "call_wall"
        for row in result["setups"]
    )
    assert any(
        "No CDH/CDL creation or ordered liquidity sweep" in row["reason"]
        for row in result["rejected"]
    )


def test_replay_accepts_212_after_ordered_liquidity_sweep_inside_pattern():
    result = _replay(
        [
            _bar("11:45", open_=99, high=100, low=96, close=98),
            _bar("11:50", open_=98, high=106, low=97, close=104),
            _bar("11:55", open_=104, high=104.5, low=101, close=103),
            _bar("12:00", open_=103, high=104, low=98, close=99),
            _bar("12:05", open_=99, high=100, low=96, close=97),
        ],
        levels=[
            {"key": "call_wall", "label": "Call Wall", "value": 105},
            {"key": "put_wall", "label": "Put Wall", "value": 95},
        ],
    )

    setup = next(
        row
        for row in result["setups"]
        if row["strat_pattern"]["code"] == "2-1-2D" and row["level"]["key"] == "call_wall"
    )
    assert setup["location_event"] == "liquidity_swept"
    assert setup["family"] == "failed_high"
    assert (
        len(
            [
                row
                for row in result["setups"]
                if row["strat_pattern"]["code"] == "2-1-2D" and row["level"]["key"] == "call_wall"
            ]
        )
        == 1
    )


def test_replay_does_not_project_dynamic_level_before_observation_time():
    result = _replay(
        [
            _bar("11:45", open_=99, high=100, low=96, close=98),
            _bar("11:50", open_=98, high=106, low=97, close=104),
            _bar("11:55", open_=104, high=104.5, low=101, close=103),
            _bar("12:00", open_=103, high=104, low=98, close=99),
            _bar("12:05", open_=99, high=100, low=96, close=97),
        ],
        gamma_as_of="2026-08-19T13:00:00-04:00",
        levels=[{"key": "call_wall", "label": "Call Wall", "value": 105}],
    )

    assert result["setups"] == []


def test_later_high_does_not_erase_earlier_current_day_high_212_down():
    result = _replay(
        [
            _bar("09:30", open_=100, high=101, low=99, close=100),
            _bar("09:35", open_=100, high=103, low=99.5, close=102),
            _bar("09:40", open_=102, high=103, low=100, close=102),
            _bar("09:45", open_=102, high=103, low=98, close=99),
            _bar("09:50", open_=99, high=110, low=98.5, close=109),
            _bar("09:55", open_=109, high=109.5, low=97, close=98),
        ],
        levels=[
            {"key": "current_day_high", "label": "Current-Day High", "value": 110},
            {"key": "put_wall", "label": "Put Wall", "value": 95},
        ],
    )

    setup = next(
        row
        for row in result["setups"]
        if row["strat_pattern"]["code"] == "2-1-2D" and row["level"]["key"] == "current_day_high"
    )
    assert setup["signal_time"].endswith("09:45:00-04:00")
    assert setup["strat_pattern"]["completed_at"] == setup["signal_time"]
    assert setup["level"]["value"] == 103
    assert setup["target"]["value"] == 95


def test_212_below_call_wall_is_not_retroactively_confirmed_by_later_candles():
    result = _replay(
        [
            _bar("10:45", open_=7672.15, high=7673.32, low=7669.00, close=7673.30),
            _bar("10:50", open_=7673.37, high=7677.47, low=7673.37, close=7674.59),
            _bar("10:55", open_=7674.52, high=7677.00, low=7673.91, close=7676.74),
            _bar("11:00", open_=7676.64, high=7682.21, low=7676.42, close=7679.61),
            _bar("11:05", open_=7679.74, high=7682.55, low=7679.74, close=7681.73),
            _bar("11:10", open_=7681.82, high=7682.33, low=7679.17, close=7680.55),
            _bar("11:15", open_=7680.53, high=7680.62, low=7673.40, close=7675.13),
            _bar("11:20", open_=7675.13, high=7677.75, low=7675.06, close=7675.69),
            _bar("11:25", open_=7675.55, high=7678.30, low=7675.00, close=7676.61),
            _bar("11:30", open_=7676.52, high=7676.76, low=7670.06, close=7670.37),
            _bar("11:35", open_=7670.39, high=7673.09, low=7670.39, close=7672.63),
            _bar("11:40", open_=7672.62, high=7679.02, low=7672.47, close=7678.46),
        ],
        levels=[
            {"key": "call_wall", "label": "Call Wall", "value": 7680},
            {"key": "new_call_wall", "label": "New Call Wall", "value": 7685},
        ],
    )

    assert not [
        row
        for row in result["setups"]
        if row["strat_pattern"]["code"] == "2-1-2U" and row["level"]["key"] == "call_wall"
    ]


def test_gamma_observation_after_signal_is_not_used_retroactively():
    result = _replay(
        [
            _bar("09:30", open_=100, high=101, low=99, close=100),
            _bar("09:35", open_=100, high=102, low=99.5, close=99.8),
            _bar("09:40", open_=99.8, high=101.5, low=99.6, close=99.7),
            _bar("09:45", open_=99.7, high=100.5, low=98, close=99),
            _bar("09:50", open_=99, high=99.5, low=96, close=97),
        ],
        gamma_as_of="2026-08-19T10:00:00-04:00",
        levels=[
            {"key": "prior_day_high", "label": "Prior-Day High", "value": 100},
            {"key": "prior_day_low", "label": "Prior-Day Low", "value": 95},
        ],
    )

    setup = next(row for row in result["setups"] if row["strat_pattern"]["code"] == "2-1-2D")
    assert setup["data_availability"]["gamma_available_at_signal"] is False


def test_replay_endpoint_and_template_are_read_only_and_discoverable(client):
    response = client.get("/api/market-pulse/setup-replay?ticker=SPX")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ok"] is True
    assert payload["payload"]["read_only"] is True

    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)
    assert 'id="marketPulseSetupReplay"' in body
    assert 'id="marketPulseReplayList"' in body
    assert '"setup_replay_api_url"' in body
    assert "Pattern trigger" in body
    assert "Follow-through after signal" in body
    assert "Target touched at ${replayTime(row.outcome?.at)}" in body
    assert "Invalidated on the ${replayTime(row.outcome?.at)} close" in body
    assert "Still open as of the ${replayTime(row.outcome?.evaluated_through)} candle" in body
    assert "Target and invalidation in the ${replayTime(row.outcome?.at)} candle" in body
    assert "targetDistance.toFixed(1)} pts from entry" in body
    assert "row.outcome?.target_progress_percent != null" in body
    assert "+${target.return_percent}% est. → SPX" in body
    assert "Dealer Gamma unavailable at signal" in body
    assert "Gamma is confluence only" in body
    assert "Tradier NTM" in body
    assert 'kind: "pattern_trigger"' in body

    chart = (Path(__file__).resolve().parents[1] / "static/js/spx_hero_chart.js").read_text(
        encoding="utf-8"
    )
    assert "market-pulse-setup-replay-updated" in chart
    assert "setupReplayMarkers" in chart


def test_replay_is_full_width_collapsible_and_ranks_signal_quality(client):
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)
    replay_start = body.index('id="marketPulseSetupReplay"')
    scenario_end = body.index("</section>", replay_start)

    assert replay_start < scenario_end
    assert 'class="marketPulseSetupReplayBody"' in body
    assert "Highest signal quality first" in body
    assert "Number(right.score || 0) - Number(left.score || 0)" in body
    assert "REPLAY_REQUEST_TIMEOUT_MS = 10000" in body
    assert "window.setTimeout(loadSetupReplay, 250)" in body
    assert "Retry replay" in body
    assert "signal: controller.signal" in body

    styles = (Path(__file__).resolve().parents[1] / "static/css/market_pulse.css").read_text(
        encoding="utf-8"
    )
    assert "grid-column:1 / -1" in styles
    assert "max-height:min(52vh,560px)" in styles
    assert "overflow:auto" in styles
    assert ".marketPulseReplayUnavailable" in styles


def test_replay_results_are_sorted_best_to_least_then_by_signal_time():
    result = _replay(
        [
            _bar("09:35", open_=101, high=103, low=99, close=102),
            _bar("09:40", open_=102, high=104, low=98, close=99),
            _bar("09:45", open_=99, high=101, low=96, close=97),
            _bar("09:50", open_=97, high=100, low=94, close=95),
            _bar("09:55", open_=95, high=98, low=93, close=96),
        ]
    )

    scores = [int(row["score"]) for row in result["setups"]]
    assert scores == sorted(scores, reverse=True)


def test_replay_excludes_new_setup_completions_after_315_pm():
    qualifying = [
        _bar("15:15", open_=100, high=101, low=99, close=100),
        _bar("15:20", open_=100, high=103, low=99.5, close=102),
        _bar("15:25", open_=102, high=102.5, low=100, close=101),
        _bar("15:30", open_=101, high=101.5, low=98, close=99),
    ]
    after_cutoff = [
        *qualifying[:-1],
        _bar("15:35", open_=101, high=101.5, low=98, close=99),
    ]

    allowed = _replay(qualifying)
    excluded = _replay(after_cutoff)

    assert allowed["entry_cutoff_label"] == "3:15 PM ET"
    assert all(
        datetime.fromisoformat(row["signal_time"]).astimezone(ET).time()
        <= datetime.strptime("15:15", "%H:%M").time()
        for row in allowed["setups"]
    )
    assert allowed["setup_count"] == 0
    assert excluded["setup_count"] == 0


def test_replay_includes_pattern_completed_exactly_at_315_pm():
    result = _replay(
        [
            _bar("15:05", open_=99, high=100, low=98, close=99),
            _bar("15:10", open_=99, high=102, low=98.5, close=101.5),
            _bar("15:15", open_=101.5, high=101.8, low=97.5, close=99),
        ],
        levels=[
            {"key": "prior_day_high", "label": "Prior-Day High", "value": 101},
            {"key": "prior_day_low", "label": "Prior-Day Low", "value": 90},
        ],
    )

    assert result["setup_count"] == 1
    assert result["setups"][0]["signal_time"].endswith("15:15:00-04:00")


def test_market_pulse_header_uses_completed_candle_and_compact_controls(client):
    root = Path(__file__).resolve().parents[1]
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)
    styles = (root / "static/css/market_pulse.css").read_text(encoding="utf-8")
    gamma_context = (root / "static/js/market_pulse_gamma_context.js").read_text(encoding="utf-8")
    chart = (root / "static/js/spx_hero_chart.js").read_text(encoding="utf-8")

    assert "Last candle ·" in body
    assert "last_completed_candle_time_label" in gamma_context
    assert "Last candle · ${snapshotLabel}" in chart
    assert body.count('id="marketPulseRefreshCountdown"') == 1
    assert body.count("marketPulseUtilityIconButton") == 3
    actions_start = body.index("marketPulsePlaybookActions marketPulseCockpitUtility")
    countdown_start = body.index("marketPulsePollCountdown", actions_start)
    sticky_start = body.index("marketPulsePlaybookPinToggle", actions_start)
    assert actions_start < countdown_start < sticky_start
    assert "marketPulseRefreshSpin" in styles
    assert "New setups after 3:15 PM ET excluded" in body


def test_context_refresh_uses_single_flight_and_returns_cached_generation(client):
    from mccain_capital.services import core

    core._market_pulse_context_response_cache.clear()
    first = client.get("/api/market-pulse/context?ticker=SPX")
    assert first.status_code == 200
    assert first.get_json()["refresh_outcome"]["status"] in {"promoted", "partial"}

    assert core._market_pulse_context_refresh_lock.acquire(blocking=False)
    try:
        second = client.get("/api/market-pulse/context?ticker=SPX")
    finally:
        core._market_pulse_context_refresh_lock.release()

    assert second.status_code == 200
    payload = second.get_json()
    assert payload["refresh_outcome"]["status"] == "unchanged"
    assert payload["payload"]["ticker"] == "SPX"


def test_context_refresh_returns_not_modified_without_provider_work(client, monkeypatch):
    from mccain_capital.services import core

    core._market_pulse_context_response_cache.clear()
    first = client.get("/api/market-pulse/context?ticker=SPX")
    assert first.status_code == 200
    generation = first.get_json()["payload"]["canonical_freshness"]["generation_id"]

    monkeypatch.setattr(
        core,
        "_market_pulse_context_api_impl",
        lambda: (_ for _ in ()).throw(AssertionError("automatic check forced provider work")),
    )
    current = client.get(
        "/api/market-pulse/context?ticker=SPX",
        headers={"If-None-Match": f'"{generation}"'},
    )

    assert current.status_code == 304
    assert current.headers["X-Market-Pulse-Outcome"] == "unchanged"
    assert int(current.headers["X-Market-Pulse-Next-Check"]) > 0


def test_after_hours_automatic_refresh_returns_retained_generation_without_provider_work(
    client, monkeypatch
):
    from mccain_capital import runtime as app_runtime
    from mccain_capital.services import core

    closed_at = datetime(2026, 8, 20, 20, 0, tzinfo=ET)
    monkeypatch.setattr(app_runtime, "now_et", lambda: closed_at)
    core._market_pulse_context_response_cache.clear()
    first = client.get("/api/market-pulse/context?ticker=SPX")
    assert first.status_code == 200
    generation = first.get_json()["payload"]["canonical_freshness"]["generation_id"]

    monkeypatch.setattr(
        core,
        "_market_pulse_context_api_impl",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("after-hours automatic check reached provider refresh")
        ),
    )
    retained = client.get(
        f"/api/market-pulse/context?ticker=SPX&automatic=1&generation={generation}"
    )

    assert retained.status_code == 200
    body = retained.get_json()
    assert retained.headers["X-Market-Pulse-Outcome"] == "session_paused"
    assert retained.headers["X-Market-Pulse-Next-Check"] == "0"
    contract = body["payload"]["refresh_contract"]
    assert {
        key: contract[key]
        for key in (
            "automatic_refresh_enabled",
            "canonical_interval_seconds",
            "market_phase",
            "mode",
            "next_check_seconds",
            "next_session_open_at",
        )
    } == {
        "automatic_refresh_enabled": False,
        "canonical_interval_seconds": 15,
        "market_phase": "closed",
        "mode": "session_paused",
        "next_check_seconds": 0,
        "next_session_open_at": "2026-08-21T09:30:00-04:00",
    }
    assert contract["next_transition_at"] == "2026-08-21T09:30:00-04:00"
    assert contract["server_time"] == "2026-08-20T20:00:00-04:00"


def test_market_pulse_refresh_contract_enables_only_regular_session():
    from mccain_capital.services import core

    live = core._market_pulse_refresh_contract(
        datetime(2026, 8, 20, 11, 0, tzinfo=ET), {"sync_interval_seconds": 15}
    )
    closed = core._market_pulse_refresh_contract(
        datetime(2026, 8, 21, 20, 0, tzinfo=ET), {"sync_interval_seconds": 15}
    )

    assert live["automatic_refresh_enabled"] is True
    assert live["next_check_seconds"] == 15
    assert closed["automatic_refresh_enabled"] is False
    assert closed["next_check_seconds"] == 0
    assert closed["next_session_open_at"] == "2026-08-24T09:30:00-04:00"


def test_after_hours_automatic_refresh_without_cache_still_avoids_provider_work(
    client, monkeypatch
):
    from mccain_capital import runtime as app_runtime
    from mccain_capital.services import core

    monkeypatch.setattr(
        app_runtime,
        "now_et",
        lambda: datetime(2026, 8, 20, 20, 0, tzinfo=ET),
    )
    core._market_pulse_context_response_cache.clear()
    monkeypatch.setattr(
        core,
        "_market_pulse_context_api_impl",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("closed automatic request reached provider refresh")
        ),
    )

    response = client.get("/api/market-pulse/context?ticker=SPX&automatic=1")

    assert response.status_code == 200
    assert response.headers["X-Market-Pulse-Outcome"] == "session_paused"
    assert response.get_json()["payload"]["refresh_contract"]["automatic_refresh_enabled"] is False


def test_after_hours_manual_refresh_remains_one_shot(client, monkeypatch):
    from mccain_capital import runtime as app_runtime
    from mccain_capital.services import core

    monkeypatch.setattr(
        app_runtime,
        "now_et",
        lambda: datetime(2026, 8, 20, 20, 0, tzinfo=ET),
    )
    core._market_pulse_context_response_cache.clear()
    calls = []

    def fake_impl(**_kwargs):
        calls.append(True)
        return core.jsonify(
            {
                "ok": True,
                "refresh_outcome": {"status": "unchanged"},
                "payload": {
                    "ticker": "SPX",
                    "refresh_contract": core._market_pulse_refresh_contract(app_runtime.now_et()),
                },
            }
        )

    monkeypatch.setattr(core, "_market_pulse_context_api_impl", fake_impl)
    response = client.get("/api/market-pulse/context?ticker=SPX&refresh=1")

    assert response.status_code == 200
    assert calls == [True]
    assert response.get_json()["payload"]["refresh_contract"]["automatic_refresh_enabled"] is False


def test_market_pulse_stream_client_respects_refresh_contract():
    script = (
        Path(__file__).resolve().parents[1] / "static/js/market_pulse_gamma_context.js"
    ).read_text(encoding="utf-8")

    assert "automaticRefreshAllowed" in script
    assert "if (!automaticRefreshAllowed())" in script
    assert "Live stream paused · last valid session retained" in script


def test_context_refresh_starts_bounded_recovery_when_locked_generation_is_overdue(
    client, monkeypatch
):
    from mccain_capital import runtime as app_runtime
    from mccain_capital.services import core

    live_at = datetime(2026, 8, 20, 11, 29, tzinfo=ET)
    monkeypatch.setattr(app_runtime, "now_et", lambda: live_at)
    core._market_pulse_context_response_cache.clear()
    core._market_pulse_context_recovery_attempts.clear()
    first = client.get("/api/market-pulse/context?ticker=SPX")
    assert first.status_code == 200
    cached = core._market_pulse_context_response_cache["SPX"]
    freshness = cached["canonical_freshness"]
    generation = freshness["generation_id"]
    freshness["generated_at"] = (app_runtime.now_et() - timedelta(minutes=20)).isoformat()
    freshness["execution_locked"] = True
    freshness["stale_required_components"] = ["gamma"]
    calls = []

    def recover(**kwargs):
        calls.append(kwargs)
        return core.jsonify(
            {
                "ok": True,
                "refresh_outcome": {"status": "partial"},
                "payload": cached,
            }
        )

    monkeypatch.setattr(core, "_market_pulse_context_api_impl", recover)
    response = client.get(
        f"/api/market-pulse/context?ticker=SPX&automatic=1&generation={generation}",
        headers={"If-None-Match": f'"{generation}"'},
    )

    assert response.status_code == 200
    assert calls == [{"recover_stale": True}]


def test_context_refresh_uses_short_retry_during_automatic_recovery_cooldown(client, monkeypatch):
    from mccain_capital import runtime as app_runtime
    from mccain_capital.services import core

    live_at = datetime(2026, 8, 20, 11, 29, tzinfo=ET)
    monkeypatch.setattr(app_runtime, "now_et", lambda: live_at)
    core._market_pulse_context_response_cache.clear()
    core._market_pulse_context_recovery_attempts.clear()
    first = client.get("/api/market-pulse/context?ticker=SPX")
    cached = core._market_pulse_context_response_cache["SPX"]
    freshness = cached["canonical_freshness"]
    generation = freshness["generation_id"]
    freshness["generated_at"] = (live_at - timedelta(minutes=20)).isoformat()
    freshness["execution_locked"] = True
    freshness["stale_required_components"] = ["gamma"]
    core._market_pulse_context_recovery_attempts["SPX"] = live_at
    monkeypatch.setattr(
        core,
        "_market_pulse_context_api_impl",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("cooldown poll repeated provider recovery")
        ),
    )

    response = client.get(
        f"/api/market-pulse/context?ticker=SPX&automatic=1&generation={generation}"
    )

    assert first.status_code == 200
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["refresh_outcome"]["retry_classification"] == ("automatic_recovery_cooldown")
    assert payload["refresh_outcome"]["next_retry_seconds"] == 15


def test_setup_replay_prefers_canonical_context_when_playbook_snapshot_has_no_bars(
    client, monkeypatch
):
    from mccain_capital.services import core

    bars = [
        _bar("09:30", open_=100, high=101, low=99, close=100),
        _bar("09:35", open_=100, high=102, low=99.5, close=99.8),
        _bar("09:40", open_=99.8, high=101.5, low=99.6, close=99.7),
        _bar("09:45", open_=99.7, high=100.5, low=98, close=99),
        _bar("09:50", open_=99, high=99, low=94, close=95),
    ]
    canonical = {
        "ticker": "SPX",
        "canonical_freshness": {
            "generation_id": "context-with-bars",
            "symbol": "SPX",
            "session_id": "2026-08-19",
            "generated_at": "2026-08-19T10:00:00-04:00",
            "components": {
                name: {"as_of": "2026-08-19T10:00:00-04:00"} for name in ("spot", "bars", "gamma")
            },
        },
        "market_structure_snapshot": {
            "local_flip": 100,
            "put_wall": 95,
            "call_wall": 105,
            "gamma_regime": "negative_gamma",
        },
        "playbook_quote": {"day_high": 102, "day_low": 94},
        "gamma_snapshot": {"computed_at": "2026-08-19T09:30:00-04:00"},
        "execution_chart": {"strategy_bars_5m": bars},
    }
    empty_snapshot = dict(canonical)
    empty_snapshot["execution_chart"] = {"strategy_bars_5m": []}
    core._market_pulse_context_response_cache.clear()
    core._market_pulse_context_response_cache["SPX"] = canonical
    monkeypatch.setattr(
        core,
        "_market_pulse_cached_playbook_snapshot",
        lambda *_args, **_kwargs: empty_snapshot,
    )

    response = client.get("/api/market-pulse/setup-replay?ticker=SPX&session_date=2026-08-19")

    assert response.status_code == 200
    payload = response.get_json()["payload"]
    assert payload["bar_count"] == len(bars)
    assert any(row["strat_pattern"]["code"] == "2-1-2D" for row in payload["setups"])
    core._market_pulse_context_response_cache.clear()


def test_setup_replay_overlays_latest_completed_hero_bars(client, monkeypatch):
    from mccain_capital.services import core
    from mccain_capital.services import tradier_hero_chart_service

    now = datetime(2026, 8, 19, 15, 21, tzinfo=ET)
    cached_bars = [
        _bar("09:30", open_=100, high=101, low=99, close=100),
        _bar("12:05", open_=100, high=101, low=99, close=100),
    ]
    snapshot = {
        "ticker": "SPX",
        "canonical_freshness": {
            "generation_id": "cached-fallback",
            "symbol": "SPX",
            "session_id": "2026-08-19",
            "generated_at": "2026-08-19T15:15:00-04:00",
            "components": {
                name: {"as_of": "2026-08-19T15:15:00-04:00"}
                for name in ("spot", "bars", "gamma")
            },
        },
        "market_structure_snapshot": {"local_flip": 100, "put_wall": 95, "call_wall": 105},
        "playbook_quote": {"day_high": 102, "day_low": 94},
        "gamma_snapshot": {"computed_at": "2026-08-19T15:15:00-04:00"},
        "execution_chart": {"strategy_bars_5m": cached_bars},
    }
    live_rows = []
    for clock in ("09:30", "12:05", "15:15"):
        stamp = datetime.fromisoformat(f"2026-08-19T{clock}:00").replace(tzinfo=ET)
        live_rows.append(
            {
                "time": int(stamp.timestamp()),
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100,
                "volume": 10,
            }
        )

    core._market_pulse_context_response_cache.clear()
    core._market_pulse_context_response_cache["SPX"] = snapshot
    monkeypatch.setattr(core, "_market_pulse_cached_playbook_snapshot", lambda *_a, **_k: {})
    monkeypatch.setattr(core, "_load_market_pulse_playbook_disk_cache", lambda: {})
    monkeypatch.setattr(core, "_market_pulse_market_hours", lambda _now: True)
    monkeypatch.setattr(
        tradier_hero_chart_service,
        "get_intraday_bars",
        lambda **_kwargs: {
            "bars": live_rows,
            "previous_session_bar_count": 0,
            "current_session_bar_count": len(live_rows),
        },
    )

    with client.application.app_context():
        result = core._market_pulse_setup_replay_source_snapshot(now, ticker="SPX")

    bars = result["execution_chart"]["strategy_bars_5m"]
    assert len(bars) == 3
    assert bars[-1]["ts"].startswith("2026-08-19T15:15:00")
    core._market_pulse_context_response_cache.clear()


def test_setup_replay_keeps_cached_bars_when_live_overlay_fails(client, monkeypatch):
    from mccain_capital.services import core
    from mccain_capital.services import tradier_hero_chart_service

    now = datetime(2026, 8, 19, 15, 21, tzinfo=ET)
    snapshot = {
        "ticker": "SPX",
        "canonical_freshness": {
            "generation_id": "historical-fallback",
            "symbol": "SPX",
            "session_id": "2026-08-19",
            "generated_at": "2026-08-19T15:15:00-04:00",
            "components": {
                name: {"as_of": "2026-08-19T15:15:00-04:00"}
                for name in ("spot", "bars", "gamma")
            },
        },
        "market_structure_snapshot": {"local_flip": 100, "put_wall": 95, "call_wall": 105},
        "playbook_quote": {"day_high": 102, "day_low": 94},
        "gamma_snapshot": {"computed_at": "2026-08-19T15:15:00-04:00"},
        "execution_chart": {"strategy_bars_5m": [
            _bar("09:30", open_=100, high=101, low=99, close=100),
            _bar("09:35", open_=100, high=102, low=99.5, close=99.8),
            _bar("09:40", open_=99.8, high=101.5, low=99.6, close=99.7),
            _bar("09:45", open_=99.7, high=100.5, low=98, close=99),
            _bar("09:50", open_=99, high=99, low=94, close=95),
        ]},
    }
    core._market_pulse_context_response_cache.clear()
    core._market_pulse_context_response_cache["SPX"] = snapshot
    monkeypatch.setattr(core, "_market_pulse_cached_playbook_snapshot", lambda *_a, **_k: {})
    monkeypatch.setattr(core, "_load_market_pulse_playbook_disk_cache", lambda: {})
    monkeypatch.setattr(core, "_market_pulse_market_hours", lambda _now: True)
    monkeypatch.setattr(
        tradier_hero_chart_service,
        "get_intraday_bars",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("provider unavailable")),
    )

    with client.application.app_context():
        result = core._market_pulse_setup_replay_source_snapshot(now, ticker="SPX")

    assert result["execution_chart"]["strategy_bars_5m"][-1]["ts"].startswith(
        "2026-08-19T09:50:00"
    )
    core._market_pulse_context_response_cache.clear()


def test_setup_replay_historical_request_never_uses_current_hero_bars(client, monkeypatch):
    from mccain_capital.services import core
    from mccain_capital.services import tradier_hero_chart_service

    now = datetime(2026, 8, 20, 15, 21, tzinfo=ET)
    snapshot = {
        "ticker": "SPX",
        "canonical_freshness": {
            "generation_id": "historical-fallback",
            "symbol": "SPX",
            "session_id": "2026-08-19",
            "generated_at": "2026-08-19T15:15:00-04:00",
            "components": {
                name: {"as_of": "2026-08-19T15:15:00-04:00"}
                for name in ("spot", "bars", "gamma")
            },
        },
        "market_structure_snapshot": {"local_flip": 100, "put_wall": 95, "call_wall": 105},
        "playbook_quote": {"day_high": 102, "day_low": 94},
        "gamma_snapshot": {"computed_at": "2026-08-19T15:15:00-04:00"},
        "execution_chart": {"strategy_bars_5m": [
            _bar("09:30", open_=100, high=101, low=99, close=100),
            _bar("09:35", open_=100, high=102, low=99.5, close=99.8),
            _bar("09:40", open_=99.8, high=101.5, low=99.6, close=99.7),
            _bar("09:45", open_=99.7, high=100.5, low=98, close=99),
            _bar("09:50", open_=99, high=99, low=94, close=95),
        ]},
    }
    core._market_pulse_context_response_cache.clear()
    core._market_pulse_context_response_cache["SPX"] = snapshot
    monkeypatch.setattr(core, "_market_pulse_cached_playbook_snapshot", lambda *_a, **_k: {})
    monkeypatch.setattr(core, "_load_market_pulse_playbook_disk_cache", lambda: {})
    monkeypatch.setattr(
        tradier_hero_chart_service,
        "get_intraday_bars",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("historical replay fetched live bars")),
    )

    with client.application.app_context():
        result = core._market_pulse_setup_replay_source_snapshot(
            now,
            ticker="SPX",
            session_date="2026-08-19",
        )

    assert result["execution_chart"]["strategy_bars_5m"][-1]["ts"].startswith(
        "2026-08-19T09:50:00"
    )
    core._market_pulse_context_response_cache.clear()


def test_cached_context_current_rejects_live_generation_past_refresh_window():
    from mccain_capital.services import core

    now = datetime(2026, 8, 20, 11, 29, tzinfo=ET)
    freshness = {
        "generated_at": (now - timedelta(minutes=20)).isoformat(),
        "sync_interval_seconds": 15,
        "execution_locked": False,
        "stale_required_components": [],
    }

    assert core._market_pulse_cached_context_is_current(freshness, now) is False


def test_context_refresh_promotes_newer_cached_generation_without_provider_work(
    client, monkeypatch
):
    from mccain_capital.services import core

    core._market_pulse_context_response_cache.clear()
    first = client.get("/api/market-pulse/context?ticker=SPX")
    assert first.status_code == 200
    current_generation = first.get_json()["payload"]["canonical_freshness"]["generation_id"]

    monkeypatch.setattr(
        core,
        "_market_pulse_context_api_impl",
        lambda: (_ for _ in ()).throw(AssertionError("cached promotion forced provider work")),
    )
    stale_tab = client.get(
        "/api/market-pulse/context?ticker=SPX&generation=older-generation",
        headers={"If-None-Match": '"older-generation"'},
    )

    assert stale_tab.status_code == 200
    payload = stale_tab.get_json()
    assert payload["refresh_outcome"]["status"] == "promoted"
    assert payload["payload"]["canonical_freshness"]["generation_id"] == current_generation


def test_requested_context_refresh_rebuilds_instead_of_reusing_cached_playbook(client, monkeypatch):
    from mccain_capital.services import core

    core._market_pulse_context_response_cache.clear()
    cached = {
        "ticker": "SPX",
        "market_structure_snapshot": {
            "spot": 100.0,
            "main_flip": 110.0,
            "local_flip": 100.0,
            "call_wall": 105.0,
            "put_wall": 95.0,
        },
        "playbook_quote": {"price": 100.0},
        "canonical_freshness": {"generation_id": "cached"},
    }
    rebuilt = dict(cached)
    rebuilt["server_ts"] = "rebuilt-on-request"
    calls = []

    monkeypatch.setattr(
        core,
        "_market_pulse_cached_playbook_snapshot",
        lambda *_args, **_kwargs: cached,
    )

    def rebuild(**kwargs):
        calls.append(kwargs)
        return rebuilt

    monkeypatch.setattr(core, "get_or_build_market_pulse_snapshot", rebuild)
    response = client.get("/api/market-pulse/context?ticker=SPX&refresh=1")

    assert response.status_code == 200
    assert len(calls) == 1
    assert calls[0]["force_refresh"] is True


def test_market_pulse_refresh_coordinator_and_countdown_contract(client):
    root = Path(__file__).resolve().parents[1]
    coordinator = (root / "static/js/market_pulse_refresh_coordinator.js").read_text(
        encoding="utf-8"
    )
    chart = (root / "static/js/spx_hero_chart.js").read_text(encoding="utf-8")
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)

    assert "marketPulseRefreshCoordinator" in coordinator
    assert "const runDue" in coordinator
    assert "lane.inFlight" in coordinator
    assert "market_pulse_refresh_coordinator.js" in body
    assert 'id="marketPulseRefreshCountdown"' in body
    assert "`Retry ${seconds}s`" in body
    assert "`${seconds}s`" in body
    assert 'nextCanonicalCheckMode === "retry" ? "Retry" : "Refresh"' in body
    assert "Math.min(30000, Number(delaySeconds || 15) * 1000)" in body
    assert "armCanonicalRefreshWatchdog" in body
    assert "deadline < now - 2000" in body
    assert "deadline > now + 30000" in body
    assert "market-pulse-component-updated" in chart
    assert "refreshCoordinator.register(refreshLaneNames.bars" in chart
    assert "refreshCoordinator.register(refreshLaneNames.levels" in chart
    assert "refreshCoordinator.register(refreshLaneNames.quote" in chart


def test_setup_replay_marker_and_legend_are_historical_not_live(client):
    root = Path(__file__).resolve().parents[1]
    chart = (root / "static/js/spx_hero_chart.js").read_text(encoding="utf-8")
    body = client.get("/market-pulse?ticker=SPX").get_data(as_text=True)

    assert 'text: "◇"' in chart
    assert 'text: "S"' not in chart
    assert "Replay setup · historical, not a live entry" in body


def test_tape_endpoint_does_not_create_per_request_provider_pool():
    source = (Path(__file__).resolve().parents[1] / "mccain_capital/services/core.py").read_text(
        encoding="utf-8"
    )
    assert "ThreadPoolExecutor" not in source
    assert "_market_pulse_tape_refresh_lock" in source
