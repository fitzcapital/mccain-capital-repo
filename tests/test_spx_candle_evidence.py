from datetime import datetime
from zoneinfo import ZoneInfo

from mccain_capital.services.spx_candle_evidence import derive_completed_candle_evidence
from mccain_capital.services.spx_candle_evidence import normalize_completed_bars


ET = ZoneInfo("America/New_York")


def _bar(minute, open_, high, low, close):
    return {
        "ts": datetime(2026, 8, 10, 9, minute, tzinfo=ET).isoformat(),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
    }


def test_bearish_failed_sweep_is_derived_in_order():
    rows = [
        _bar(30, 99.5, 101.0, 99.0, 100.5),
        _bar(35, 100.5, 101.0, 99.2, 99.5),
        _bar(40, 99.5, 101.5, 99.4, 101.0),
        _bar(45, 101.0, 101.2, 98.8, 99.0),
        _bar(50, 99.0, 99.2, 98.0, 98.2),
    ]
    payload = derive_completed_candle_evidence(
        rows_5m=rows,
        rows_15m=[],
        now=datetime(2026, 8, 10, 10, 0, tzinfo=ET),
        ticker="SPX",
        session_date="2026-08-10",
        level_key="call_wall",
        level_value=100.0,
        interaction="from_below",
    )
    evidence = payload["evidence"]
    assert evidence["sweep"]["confirmed"] is True
    assert evidence["close_back_inside_5m"]["timestamp"].endswith("09:35:00-04:00")
    assert evidence["reversal_2_2_5m"]["timestamp"].endswith("09:45:00-04:00")
    assert evidence["trigger_break"]["timestamp"].endswith("09:45:00-04:00")
    assert evidence["strat_pattern_5m"]["code"] == "2-2 REV D"
    assert "acceptance" not in evidence


def test_bullish_failed_sweep_is_symmetric():
    rows = [
        _bar(30, 100.5, 101.0, 99.0, 99.5),
        _bar(35, 99.5, 100.8, 99.0, 100.5),
        _bar(40, 100.5, 100.6, 98.5, 99.0),
        _bar(45, 99.0, 101.2, 98.8, 101.0),
        _bar(50, 101.0, 102.0, 100.8, 101.8),
    ]
    payload = derive_completed_candle_evidence(
        rows_5m=rows,
        rows_15m=[],
        now=datetime(2026, 8, 10, 10, 0, tzinfo=ET),
        ticker="SPX",
        session_date="2026-08-10",
        level_key="put_wall",
        level_value=100.0,
        interaction="from_above",
    )
    assert payload["evidence"]["trigger_break"]["confirmed"] is True


def test_unfinished_candle_is_not_normalized_or_confirmed():
    now = datetime(2026, 8, 10, 9, 33, tzinfo=ET)
    rows = [_bar(30, 99.5, 101.0, 99.0, 100.5)]
    assert (
        normalize_completed_bars(rows, now=now, timeframe_minutes=5, session_date="2026-08-10")
        == []
    )


def test_cross_session_and_malformed_bars_are_rejected():
    now = datetime(2026, 8, 10, 10, 0, tzinfo=ET)
    rows = [_bar(30, 99.5, 101.0, 99.0, 100.5), {"ts": "bad", "close": 100}]
    assert (
        normalize_completed_bars(rows, now=now, timeframe_minutes=5, session_date="2026-08-11")
        == []
    )
