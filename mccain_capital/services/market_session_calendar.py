"""Deterministic US equity session calendar used by Market Pulse.

The calendar is intentionally small and local.  Exceptional closures can be
supplied by callers/tests without exposing a request-controlled fault switch.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from typing import Any, Mapping
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")
REGULAR_OPEN = time(9, 30)
REGULAR_CLOSE = time(16, 0)
EARLY_CLOSE = time(13, 0)


@dataclass(frozen=True)
class SessionWindow:
    session_date: date
    opens_at: datetime | None
    closes_at: datetime | None
    status: str
    reason: str = ""

    @property
    def is_session(self) -> bool:
        return self.opens_at is not None and self.closes_at is not None


def _observed_fixed_holiday(year: int, month: int, day_num: int) -> date:
    holiday = date(year, month, day_num)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(days=(weekday - first.weekday()) % 7 + (n - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    cursor = date(year + (month == 12), 1 if month == 12 else month + 1, 1) - timedelta(days=1)
    return cursor - timedelta(days=(cursor.weekday() - weekday) % 7)


def _easter_sunday(year: int) -> date:
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    offset = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * offset) // 451
    month = (h + offset - 7 * m + 114) // 31
    day_num = (h + offset - 7 * m + 114) % 31 + 1
    return date(year, month, day_num)


@lru_cache(maxsize=16)
def market_holidays(year: int) -> dict[date, str]:
    holidays = {
        _observed_fixed_holiday(year, 1, 1): "New Year's Day",
        _nth_weekday(year, 1, 0, 3): "Martin Luther King Jr. Day",
        _nth_weekday(year, 2, 0, 3): "Presidents Day",
        _easter_sunday(year) - timedelta(days=2): "Good Friday",
        _last_weekday(year, 5, 0): "Memorial Day",
        _observed_fixed_holiday(year, 6, 19): "Juneteenth",
        _observed_fixed_holiday(year, 7, 4): "Independence Day",
        _nth_weekday(year, 9, 0, 1): "Labor Day",
        _nth_weekday(year, 11, 3, 4): "Thanksgiving",
        _observed_fixed_holiday(year, 12, 25): "Christmas Day",
    }
    # An observed New Year's Day may fall in the previous calendar year.
    next_new_year = _observed_fixed_holiday(year + 1, 1, 1)
    if next_new_year.year == year:
        holidays[next_new_year] = "New Year's Day"
    return holidays


@lru_cache(maxsize=16)
def scheduled_early_closes(year: int) -> dict[date, str]:
    thanksgiving = _nth_weekday(year, 11, 3, 4)
    candidates = {thanksgiving + timedelta(days=1): "Day after Thanksgiving"}
    july_fourth = date(year, 7, 4)
    july_early = date(year, 7, 3)
    if july_fourth.weekday() in (1, 2, 3, 4):
        candidates[july_early] = "Independence Day early close"
    christmas_eve = date(year, 12, 24)
    if christmas_eve.weekday() < 5:
        candidates[christmas_eve] = "Christmas Eve early close"
    holidays = market_holidays(year)
    return {
        day: reason
        for day, reason in candidates.items()
        if day.weekday() < 5 and day not in holidays
    }


def session_window(
    day: date,
    *,
    exceptional_closures: Mapping[date, str] | None = None,
) -> SessionWindow:
    overrides = exceptional_closures or {}
    if day in overrides:
        return SessionWindow(day, None, None, "exceptional_closure", str(overrides[day]))
    if day.weekday() >= 5:
        return SessionWindow(day, None, None, "weekend", "Weekend")
    holiday = market_holidays(day.year).get(day, "")
    if holiday:
        return SessionWindow(day, None, None, "holiday", holiday)
    close_time = EARLY_CLOSE if day in scheduled_early_closes(day.year) else REGULAR_CLOSE
    reason = scheduled_early_closes(day.year).get(day, "")
    return SessionWindow(
        day,
        datetime.combine(day, REGULAR_OPEN, EASTERN),
        datetime.combine(day, close_time, EASTERN),
        "early_close" if reason else "regular",
        reason,
    )


def is_session_day(day: date, *, exceptional_closures: Mapping[date, str] | None = None) -> bool:
    return session_window(day, exceptional_closures=exceptional_closures).is_session


def holiday_name(day: date) -> str:
    return market_holidays(day.year).get(day, "")


def market_phase(
    now: datetime,
    *,
    exceptional_closures: Mapping[date, str] | None = None,
) -> str:
    current = now.astimezone(EASTERN) if now.tzinfo else now.replace(tzinfo=EASTERN)
    window = session_window(current.date(), exceptional_closures=exceptional_closures)
    if not window.is_session:
        return "closed"
    assert window.opens_at is not None and window.closes_at is not None
    if window.opens_at <= current < window.closes_at:
        return "open"
    minute = current.hour * 60 + current.minute
    if 4 * 60 <= minute < 9 * 60 + 30:
        return "premarket"
    if window.closes_at <= current < current.replace(hour=20, minute=0, second=0, microsecond=0):
        return "afterhours"
    return "closed"


def next_session_open(
    now: datetime,
    *,
    exceptional_closures: Mapping[date, str] | None = None,
) -> datetime:
    current = now.astimezone(EASTERN) if now.tzinfo else now.replace(tzinfo=EASTERN)
    today = session_window(current.date(), exceptional_closures=exceptional_closures)
    if today.opens_at is not None and current < today.opens_at:
        return today.opens_at
    candidate = current.date() + timedelta(days=1)
    for _ in range(370):
        window = session_window(candidate, exceptional_closures=exceptional_closures)
        if window.opens_at is not None:
            return window.opens_at
        candidate += timedelta(days=1)
    raise RuntimeError("Unable to resolve the next exchange session")


def session_contract(
    now: datetime,
    *,
    exceptional_closures: Mapping[date, str] | None = None,
    canonical_interval_seconds: int = 15,
) -> dict[str, Any]:
    """Return the server-authoritative lifecycle contract for a Market Pulse client."""

    current = now.astimezone(EASTERN) if now.tzinfo else now.replace(tzinfo=EASTERN)
    window = session_window(current.date(), exceptional_closures=exceptional_closures)
    phase = market_phase(current, exceptional_closures=exceptional_closures)
    next_open = next_session_open(current, exceptional_closures=exceptional_closures)
    next_transition = next_open
    if window.is_session:
        assert window.opens_at is not None and window.closes_at is not None
        if current < window.opens_at:
            next_transition = window.opens_at
        elif current < window.closes_at:
            next_transition = window.closes_at

    interval_seconds = max(5, int(canonical_interval_seconds or 15))
    polling_allowed = phase == "open"
    return {
        "phase": phase,
        "market_phase": phase,
        "session_date": current.date().isoformat(),
        "server_time": current.isoformat(),
        "regular_session_open_at": window.opens_at.isoformat() if window.opens_at else "",
        "regular_session_close_at": window.closes_at.isoformat() if window.closes_at else "",
        "next_transition_at": next_transition.isoformat(),
        "next_session_open_at": next_open.isoformat(),
        "automatic_refresh_enabled": polling_allowed,
        "automatic_polling_allowed": polling_allowed,
        "canonical_interval_seconds": interval_seconds,
        "recommended_cadence_seconds": interval_seconds if polling_allowed else 0,
        "next_check_seconds": interval_seconds if polling_allowed else 0,
        "session_status": window.status,
        "session_reason": window.reason,
    }
