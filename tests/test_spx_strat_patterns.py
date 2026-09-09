from mccain_capital.services.spx_strat_patterns import classify_strat_bar
from mccain_capital.services.spx_strat_patterns import detect_latest_key_level_pattern


def _bar(ts, high, low):
    return {"ts": ts, "open": low + 0.5, "high": high, "low": low, "close": high - 0.5}


def test_classifies_inside_directional_and_outside_bars():
    prior = _bar("09:30", 100, 90)
    assert classify_strat_bar(prior, _bar("09:35", 99, 91)) == "1"
    assert classify_strat_bar(prior, _bar("09:35", 101, 90)) == "2U"
    assert classify_strat_bar(prior, _bar("09:35", 100, 89)) == "2D"
    assert classify_strat_bar(prior, _bar("09:35", 101, 89)) == "3"


def test_detects_bullish_and_bearish_212_at_level():
    bullish = [
        _bar("09:30", 101, 99),
        _bar("09:35", 101, 98),
        _bar("09:40", 100.5, 98.5),
        _bar("09:45", 102, 98.5),
    ]
    bearish = [
        _bar("10:00", 101, 99),
        _bar("10:05", 102, 99),
        _bar("10:10", 101.5, 99.5),
        _bar("10:15", 101, 98),
    ]
    up = detect_latest_key_level_pattern(
        bullish, level_key="current_day_low", level_label="Current-Day Low", level_value=99
    )
    down = detect_latest_key_level_pattern(
        bearish, level_key="current_day_high", level_label="Current-Day High", level_value=100
    )
    assert up["code"] == "2-1-2U"
    assert up["direction"] == "bullish"
    assert down["code"] == "2-1-2D"
    assert down["direction"] == "bearish"


def test_detects_opposing_22_reversals_at_level():
    bearish = [
        _bar("09:30", 100, 98),
        _bar("09:35", 102, 98.5),
        _bar("09:40", 101.5, 97.5),
    ]
    bullish = [
        _bar("10:00", 102, 100),
        _bar("10:05", 101.5, 98),
        _bar("10:10", 102.5, 98.5),
    ]
    down = detect_latest_key_level_pattern(
        bearish, level_key="current_day_high", level_label="Current-Day High", level_value=101
    )
    up = detect_latest_key_level_pattern(
        bullish, level_key="current_day_low", level_label="Current-Day Low", level_value=99
    )
    assert down["code"] == "2-2 REV D"
    assert up["code"] == "2-2 REV U"


def test_key_level_location_accepts_quarter_point_boundary_but_not_more():
    bars = [
        _bar("11:20", 7767.08, 7760.82),
        _bar("11:25", 7769.75, 7765.97),
        _bar("11:30", 7768.18, 7762.96),
    ]
    boundary = detect_latest_key_level_pattern(
        bars,
        level_key="call_wall",
        level_label="Call Wall",
        level_value=7770.00,
    )
    outside = detect_latest_key_level_pattern(
        bars,
        level_key="call_wall",
        level_label="Call Wall",
        level_value=7770.01,
    )
    assert boundary["code"] == "2-2 REV D"
    assert outside is None


def test_key_level_proximity_does_not_promote_continuation():
    continuation = [
        _bar("11:25", 7770.00, 7765.97),
        _bar("11:30", 7768.18, 7762.96),
        _bar("11:35", 7763.59, 7754.14),
    ]
    assert (
        detect_latest_key_level_pattern(
            continuation,
            level_key="call_wall",
            level_label="Call Wall",
            level_value=7770.00,
        )
        is None
    )


def test_rejects_outside_bar_generic_break_and_midrange_pattern():
    outside = [
        _bar("09:30", 100, 90),
        _bar("09:35", 101, 89),
        _bar("09:40", 102, 88),
    ]
    generic = [
        _bar("10:00", 100, 98),
        _bar("10:05", 101, 98),
        _bar("10:10", 102, 98),
    ]
    valid_midrange = [
        _bar("11:00", 101, 99),
        _bar("11:05", 102, 99),
        _bar("11:10", 101.5, 99.5),
        _bar("11:15", 101, 98),
    ]
    for rows, level in ((outside, 100), (generic, 100), (valid_midrange, 110)):
        assert (
            detect_latest_key_level_pattern(
                rows, level_key="call_wall", level_label="Call Wall", level_value=level
            )
            is None
        )
