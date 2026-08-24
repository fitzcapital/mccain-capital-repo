from datetime import date, datetime
from zoneinfo import ZoneInfo

from mccain_capital.services.market_session_calendar import (
    is_session_day,
    market_phase,
    next_session_open,
    session_window,
)


ET = ZoneInfo("America/New_York")


def test_regular_session_and_weekend():
    regular = session_window(date(2026, 8, 21))
    assert regular.status == "regular"
    assert regular.opens_at.hour == 9 and regular.closes_at.hour == 16
    assert not is_session_day(date(2026, 8, 22))


def test_observed_holiday_and_year_transition():
    assert session_window(date(2026, 7, 3)).status == "holiday"
    now = datetime(2026, 12, 31, 18, 0, tzinfo=ET)
    assert next_session_open(now) == datetime(2027, 1, 4, 9, 30, tzinfo=ET)


def test_scheduled_early_close_stops_open_phase_at_one():
    day = date(2026, 11, 27)
    window = session_window(day)
    assert window.status == "early_close"
    assert window.closes_at == datetime(2026, 11, 27, 13, 0, tzinfo=ET)
    assert market_phase(datetime(2026, 11, 27, 12, 59, tzinfo=ET)) == "open"
    assert market_phase(datetime(2026, 11, 27, 13, 0, tzinfo=ET)) == "afterhours"


def test_exceptional_closure_override_fails_closed():
    day = date(2026, 8, 21)
    overrides = {day: "Exchange emergency closure"}
    assert session_window(day, exceptional_closures=overrides).status == "exceptional_closure"
    assert market_phase(datetime(2026, 8, 21, 11, 0, tzinfo=ET), exceptional_closures=overrides) == "closed"


def test_dst_boundaries_keep_eastern_open_and_correct_utc_offsets():
    winter = session_window(date(2026, 3, 6)).opens_at
    summer = session_window(date(2026, 3, 9)).opens_at
    assert winter is not None and winter.utcoffset().total_seconds() == -5 * 3600
    assert summer is not None and summer.utcoffset().total_seconds() == -4 * 3600
    assert market_phase(datetime(2026, 3, 9, 9, 30, tzinfo=ET)) == "open"
