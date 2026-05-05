"""UI rendering adapters for service modules without direct app_core coupling."""

from __future__ import annotations

import json
import os
import re
import secrets
from datetime import date, datetime, timedelta
import urllib.error
import urllib.request
from zoneinfo import ZoneInfo
from typing import Any, Mapping

from flask import current_app, render_template, render_template_string, session

from mccain_capital.auth import auth_enabled, effective_username, is_authenticated
from mccain_capital import runtime as app_runtime
from mccain_capital.runtime import get_setting_value, now_iso, set_setting_value

APP_TITLE = "McCain Capital"
TZ = ZoneInfo("America/New_York")
FOREX_FACTORY_WEEKLY_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
FOREX_FACTORY_NEXT_WEEKLY_URL = "https://nfs.faireconomy.media/ff_calendar_nextweek.json"
FOREX_FACTORY_MONTHLY_URL = "https://nfs.faireconomy.media/ff_calendar_thismonth.json"
FOREX_FACTORY_CACHE_TTL_SECONDS = 900
_forex_factory_cache: dict[str, object] = {"fetched_at": None, "payload": None}
_forex_factory_next_week_cache: dict[str, object] = {"fetched_at": None, "payload": None}
_forex_factory_month_cache: dict[str, object] = {"fetched_at": None, "payload": None}
_POST_FORM_BLOCK_RE = re.compile(
    r"(<form\b[^>]*\bmethod\s*=\s*([\"']?)post\2[^>]*>)(.*?)(</form>)",
    flags=re.IGNORECASE | re.DOTALL,
)
TRADING_WINDOW_DEFAULTS = {
    "enabled": "1",
    "start_et": "09:30",
    "done_by_et": "11:30",
    "test_mode": "0",
    "test_date": "",
    "test_time_et": "",
}
TRADING_WINDOW_UPCOMING_NOTICE_MINUTES = 60


def _is_market_session(day: date) -> bool:
    return day.weekday() < 5 and not _market_holiday_name(day)


def _market_holiday_name(day: date) -> str:
    return _market_holidays(day.year).get(day, "")


def _market_holidays(year: int) -> dict[date, str]:
    easter = _easter_sunday(year)
    return {
        _observed_fixed_holiday(year, 1, 1): "New Years Day",
        _nth_weekday_of_month(year, 1, 0, 3): "Martin Luther King Jr. Day",
        _nth_weekday_of_month(year, 2, 0, 3): "Presidents Day",
        easter - timedelta(days=2): "Good Friday",
        _last_weekday_of_month(year, 5, 0): "Memorial Day",
        _observed_fixed_holiday(year, 6, 19): "Juneteenth",
        _observed_fixed_holiday(year, 7, 4): "Independence Day",
        _nth_weekday_of_month(year, 9, 0, 1): "Labor Day",
        _nth_weekday_of_month(year, 11, 3, 4): "Thanksgiving",
        _observed_fixed_holiday(year, 12, 25): "Christmas Day",
    }


def _observed_fixed_holiday(year: int, month: int, day_num: int) -> date:
    holiday = date(year, month, day_num)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    return first + timedelta(days=delta + ((n - 1) * 7))


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    if month == 12:
        cursor = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        cursor = date(year, month + 1, 1) - timedelta(days=1)
    while cursor.weekday() != weekday:
        cursor -= timedelta(days=1)
    return cursor


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    weekday_offset = (32 + (2 * e) + (2 * i) - h - k) % 7
    m = (a + (11 * h) + (22 * weekday_offset)) // 451
    month = (h + weekday_offset - (7 * m) + 114) // 31
    day_num = ((h + weekday_offset - (7 * m) + 114) % 31) + 1
    return date(year, month, day_num)


def _upload_file(name: str) -> str:
    return app_runtime.upload_path(name)


def _forex_factory_cache_file() -> str:
    return _upload_file(".forex_factory_weekly_cache.json")


def _forex_factory_next_week_cache_file() -> str:
    return _upload_file(".forex_factory_next_weekly_cache.json")


def _forex_factory_month_cache_file() -> str:
    return _upload_file(".forex_factory_monthly_cache.json")


def _vanquish_manual_lock_path() -> str:
    return _upload_file(".vanquish_manual_lock.json")


def csrf_token_value() -> str:
    token = str(session.get("_csrf_token") or "").strip()
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token


def csrf_input_html() -> str:
    return f'<input type="hidden" name="csrf_token" value="{csrf_token_value()}">'


def _inject_csrf_inputs(content_html: str, csrf_input: str) -> str:
    html = str(content_html or "")
    if not html:
        return html

    def _inject(match: re.Match[str]) -> str:
        open_tag, _, body, close_tag = match.groups()
        if 'name="csrf_token"' in body or "name='csrf_token'" in body:
            return match.group(0)
        return f"{open_tag}{csrf_input}{body}{close_tag}"

    return _POST_FORM_BLOCK_RE.sub(_inject, html)


def _static_version(static_root: str) -> str:
    branding_paths = [
        os.path.join(static_root, "logo-primary.png"),
        os.path.join(static_root, "logo-flat.png"),
        os.path.join(static_root, "logo.png"),
        os.path.join(static_root, "favicon.ico"),
        os.path.join(static_root, "icon-192.png"),
        os.path.join(static_root, "icon-180.png"),
    ]
    app_css_path = os.path.join(static_root, "css", "app.css")
    market_pulse_css_path = os.path.join(static_root, "css", "market_pulse.css")
    dashboard_command_js_path = os.path.join(static_root, "js", "dashboard_command_center.js")
    hero_chart_js_path = os.path.join(static_root, "js", "spx_hero_chart.js")
    market_pulse_gamma_js_path = os.path.join(static_root, "js", "market_pulse_gamma_context.js")
    try:
        mtimes = [
            os.path.getmtime(path)
            for path in [
                *branding_paths,
                app_css_path,
                market_pulse_css_path,
                dashboard_command_js_path,
                hero_chart_js_path,
                market_pulse_gamma_js_path,
            ]
            if os.path.exists(path)
        ]
        return str(int(max(mtimes))) if mtimes else now_iso().replace(":", "").replace("-", "")
    except Exception:
        return now_iso().replace(":", "").replace("-", "")


def _load_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            parsed = json.load(f)
            return parsed if isinstance(parsed, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _save_json(path: str, payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except OSError:
        return


def _parse_iso_dt(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.astimezone(TZ)
    except ValueError:
        return None


def _load_manual_vanquish_lock(now_et: datetime) -> dict:
    payload = _load_json(_vanquish_manual_lock_path())
    until_dt = _parse_iso_dt(str(payload.get("until_at") or ""))
    active = bool(until_dt and until_dt > now_et)
    remaining_seconds = int((until_dt - now_et).total_seconds()) if active and until_dt else 0
    return {
        "active": active,
        "until_at": until_dt.isoformat() if until_dt else "",
        "until_epoch": int(until_dt.timestamp()) if until_dt else 0,
        "unlock_label": until_dt.strftime("%b %d, %Y %I:%M %p ET") if until_dt else "",
        "started_at": str(payload.get("started_at") or ""),
        "duration_minutes": int(payload.get("duration_minutes") or 0),
        "remaining_seconds": max(0, remaining_seconds),
        "source": str(payload.get("source") or "manual"),
        "reason": "Manual lock timer is active." if active else "",
    }


def set_manual_vanquish_lock(minutes: int, *, source: str = "manual") -> dict:
    now_et = datetime.now(TZ)
    safe_minutes = max(1, min(480, int(minutes or 1)))
    until_dt = now_et + timedelta(minutes=safe_minutes)
    payload = {
        "started_at": now_et.isoformat(),
        "until_at": until_dt.isoformat(),
        "duration_minutes": safe_minutes,
        "source": str(source or "manual"),
        "updated_at": now_iso(),
    }
    _save_json(_vanquish_manual_lock_path(), payload)
    return _load_manual_vanquish_lock(now_et)


def clear_manual_vanquish_lock() -> None:
    try:
        os.remove(_vanquish_manual_lock_path())
    except OSError:
        return


def _parse_hhmm_minutes(value: str) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.hour * 60 + parsed.minute
        except ValueError:
            continue
    return None


def _minutes_to_hhmm(minutes: int) -> str:
    clamped = max(0, min(23 * 60 + 59, int(minutes)))
    hour = clamped // 60
    minute = clamped % 60
    return f"{hour:02d}:{minute:02d}"


def _setting_bool(key: str, default: bool) -> bool:
    raw = str(get_setting_value(key, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _setting_text(key: str, default: str = "") -> str:
    return str(get_setting_value(key, default) or "").strip()


def _resolve_window_times(
    start_text: str, done_text: str, stop_text: str = ""
) -> tuple[str, str, str]:
    start_min = _parse_hhmm_minutes(start_text)
    done_min = _parse_hhmm_minutes(done_text)
    stop_min = _parse_hhmm_minutes(stop_text)
    if start_min is None:
        start_min = 9 * 60 + 30
    if done_min is None:
        done_min = start_min + 120
    if stop_min is None:
        stop_min = done_min + 30
    if done_min <= start_min:
        done_min = min(23 * 60 + 58, start_min + 30)
    if stop_min <= done_min:
        stop_min = min(23 * 60 + 59, done_min + 15)
    if stop_min <= done_min:
        stop_min = min(23 * 60 + 59, done_min + 1)
    return (
        _minutes_to_hhmm(start_min),
        _minutes_to_hhmm(done_min),
        _minutes_to_hhmm(stop_min),
    )


def _form_bool(form: Mapping[str, Any], key: str, default: bool = False) -> bool:
    getlist = getattr(form, "getlist", None)
    if callable(getlist):
        values = getlist(key)
    elif key in form:
        values = [form.get(key)]
    else:
        values = []
    if not values:
        return default
    return any(
        str(value or "").strip().lower() in {"1", "true", "on", "yes"} for value in values
    )


def save_trading_window_settings(form: Mapping[str, Any]) -> dict[str, Any]:
    enabled = _form_bool(form, "tw_enabled")
    test_mode = _form_bool(form, "tw_test_mode")
    try:
        upcoming_notice_minutes = int(
            str(form.get("tw_upcoming_notice_minutes") or "").strip()
            or TRADING_WINDOW_UPCOMING_NOTICE_MINUTES
        )
    except ValueError:
        upcoming_notice_minutes = TRADING_WINDOW_UPCOMING_NOTICE_MINUTES
    upcoming_notice_minutes = max(0, min(240, upcoming_notice_minutes))

    start_et, done_by_et, hard_stop_et = _resolve_window_times(
        str(form.get("tw_start_et") or ""),
        str(form.get("tw_done_by_et") or ""),
        str(form.get("tw_hard_stop_et") or form.get("tw_done_by_et") or ""),
    )
    hard_stop_et = done_by_et

    raw_test_date = str(form.get("tw_test_date") or "").strip()
    try:
        normalized_test_date = (
            date.fromisoformat(raw_test_date).isoformat() if raw_test_date else ""
        )
    except ValueError:
        normalized_test_date = ""
    normalized_test_time = _minutes_to_hhmm(
        _parse_hhmm_minutes(str(form.get("tw_test_time_et") or "")) or 0
    )

    set_setting_value("trading_window_enabled", "1" if enabled else "0")
    set_setting_value("trading_window_start_et", start_et)
    set_setting_value("trading_window_done_by_et", done_by_et)
    set_setting_value("trading_window_hard_stop_et", hard_stop_et)
    set_setting_value("trading_window_test_mode", "1" if test_mode else "0")
    set_setting_value("trading_window_test_date", normalized_test_date)
    set_setting_value("trading_window_test_time_et", normalized_test_time)
    set_setting_value("trading_window_upcoming_notice_minutes", str(upcoming_notice_minutes))
    return get_trading_window_state()


def get_trading_window_state() -> dict[str, Any]:
    enabled = _setting_bool("trading_window_enabled", True)
    start_et, done_by_et, hard_stop_et = _resolve_window_times(
        _setting_text("trading_window_start_et", TRADING_WINDOW_DEFAULTS["start_et"]),
        _setting_text("trading_window_done_by_et", TRADING_WINDOW_DEFAULTS["done_by_et"]),
        _setting_text(
            "trading_window_hard_stop_et",
            _setting_text("trading_window_done_by_et", TRADING_WINDOW_DEFAULTS["done_by_et"]),
        ),
    )

    test_mode = _setting_bool("trading_window_test_mode", False)
    test_date_text = _setting_text("trading_window_test_date", TRADING_WINDOW_DEFAULTS["test_date"])
    test_time_et = _minutes_to_hhmm(
        _parse_hhmm_minutes(
            _setting_text("trading_window_test_time_et", TRADING_WINDOW_DEFAULTS["test_time_et"])
        )
        or 0
    )

    now_et = datetime.now(TZ)
    using_test_clock = False
    if test_mode:
        try:
            test_day = date.fromisoformat(test_date_text) if test_date_text else now_et.date()
            test_minutes = _parse_hhmm_minutes(test_time_et)
            if test_minutes is not None:
                now_et = datetime(
                    test_day.year,
                    test_day.month,
                    test_day.day,
                    test_minutes // 60,
                    test_minutes % 60,
                    tzinfo=TZ,
                )
                using_test_clock = True
        except ValueError:
            using_test_clock = False

    session_day = now_et.date()
    holiday_name = _market_holiday_name(session_day)
    is_trading_day = _is_market_session(session_day)
    now_min = now_et.hour * 60 + now_et.minute
    start_min = _parse_hhmm_minutes(start_et) or 0
    done_min = _parse_hhmm_minutes(done_by_et) or (start_min + 120)
    upcoming_notice_minutes = app_runtime.parse_int(
        _setting_text(
            "trading_window_upcoming_notice_minutes", str(TRADING_WINDOW_UPCOMING_NOTICE_MINUTES)
        )
    )
    if upcoming_notice_minutes is None:
        upcoming_notice_minutes = TRADING_WINDOW_UPCOMING_NOTICE_MINUTES
    upcoming_notice_minutes = max(0, min(240, int(upcoming_notice_minutes)))
    minutes_until_start = start_min - now_min
    next_change_minutes: int | None = None
    primary_reason = "Opening volatility protection"
    if not enabled:
        state = "off"
        state_label = "Trading Window Off"
        message = "Trading window controls are disabled."
        rail_label = "Window Off"
        rail_detail = "Window disabled"
        pill_tone = "neutral"
        show_banner = False
        next_change_minutes = None
        primary_reason = "Window guidance disabled"
    elif not is_trading_day and not using_test_clock:
        state = "closed"
        state_label = "Do Not Trade"
        if session_day.weekday() >= 5:
            message = "Trading window is hidden because today is not a trading day."
            primary_reason = "Weekend protection"
        else:
            message = f"Trading window is hidden for {holiday_name}."
            primary_reason = holiday_name or "Market closed"
        rail_label = "Do Not Trade"
        rail_detail = "Closed Today"
        pill_tone = "negative"
        show_banner = False
        next_change_minutes = None
    elif now_min < start_min:
        state = "upcoming" if minutes_until_start <= upcoming_notice_minutes else "pending"
        state_label = "Do Not Trade"
        message = f"Wait for the window to open at {start_et} ET."
        rail_label = "Do Not Trade"
        rail_detail = f"Starts {start_et} ET"
        pill_tone = "negative"
        show_banner = state == "upcoming"
        next_change_minutes = max(0, minutes_until_start)
        primary_reason = "Opening volatility protection"
    elif now_min < done_min:
        state = "active"
        state_label = "Trading Allowed"
        message = f"Execution window is open. Be done by {done_by_et} ET."
        rail_label = "Trading Allowed"
        rail_detail = f"Done By {done_by_et} ET"
        pill_tone = "positive"
        show_banner = True
        next_change_minutes = max(0, done_min - now_min)
        primary_reason = "Inside execution window"
    else:
        state = "stop"
        state_label = "Done Trading"
        message = f"You are done for the session. Window closed at {done_by_et} ET."
        rail_label = "Do Not Trade"
        rail_detail = f"Closed {done_by_et} ET"
        pill_tone = "negative"
        show_banner = False
        next_change_minutes = None
        primary_reason = "Session cutoff reached"

    progress_pct = 0.0
    if done_min > start_min:
        progress_pct = max(
            0.0, min(100.0, ((now_min - start_min) / (done_min - start_min)) * 100.0)
        )

    return {
        "enabled": enabled,
        "start_et": start_et,
        "done_by_et": done_by_et,
        "hard_stop_et": hard_stop_et,
        "upcoming_notice_minutes": upcoming_notice_minutes,
        "test_mode": test_mode,
        "using_test_clock": using_test_clock,
        "is_trading_day": is_trading_day,
        "show_banner": bool(show_banner),
        "holiday_name": holiday_name,
        "test_date": (test_date_text or now_et.date().isoformat()),
        "test_time_et": test_time_et,
        "state": state,
        "pill_tone": pill_tone,
        "state_label": state_label,
        "rail_label": rail_label,
        "rail_detail": rail_detail,
        "message": message,
        "now_label": now_et.strftime("%a %b %-d · %I:%M %p ET"),
        "now_time_et": now_et.strftime("%H:%M"),
        "now_min": now_min,
        "start_min": start_min,
        "done_min": done_min,
        "progress_pct": progress_pct,
        "next_change_minutes": next_change_minutes,
        "primary_reason": primary_reason,
    }


def _load_forex_factory_disk_cache() -> list[dict] | None:
    cached = _load_json(_forex_factory_cache_file())
    payload = cached.get("payload")
    return payload if isinstance(payload, list) else None


def _save_forex_factory_disk_cache(payload: list[dict]) -> None:
    try:
        with open(_forex_factory_cache_file(), "w", encoding="utf-8") as f:
            json.dump({"saved_at": now_iso(), "payload": payload}, f)
    except OSError:
        return


def _load_forex_factory_month_disk_cache() -> list[dict] | None:
    cached = _load_json(_forex_factory_month_cache_file())
    payload = cached.get("payload")
    return payload if isinstance(payload, list) else None


def _save_forex_factory_month_disk_cache(payload: list[dict]) -> None:
    try:
        with open(_forex_factory_month_cache_file(), "w", encoding="utf-8") as f:
            json.dump({"saved_at": now_iso(), "payload": payload}, f)
    except OSError:
        return


def _load_forex_factory_next_week_disk_cache() -> list[dict] | None:
    cached = _load_json(_forex_factory_next_week_cache_file())
    payload = cached.get("payload")
    return payload if isinstance(payload, list) else None


def _save_forex_factory_next_week_disk_cache(payload: list[dict]) -> None:
    try:
        with open(_forex_factory_next_week_cache_file(), "w", encoding="utf-8") as f:
            json.dump({"saved_at": now_iso(), "payload": payload}, f)
    except OSError:
        return


def get_system_status() -> dict:
    sync_path = _upload_file(".vanquish_sync_last_run.json")
    auto_path = _upload_file(".vanquish_auto_sync.json")
    last_sync = _load_json(sync_path)
    auto_sync = _load_json(auto_path)
    updated_raw = str(last_sync.get("updated_at") or "")
    updated_human = ""
    if updated_raw:
        try:
            dt = datetime.fromisoformat(updated_raw.replace("Z", "+00:00"))
            updated_human = dt.astimezone(TZ).strftime("%b %d, %Y %I:%M %p ET")
        except Exception:
            updated_human = updated_raw
    auto_sync_time_raw = str(auto_sync.get("run_time_et") or "").strip()
    auto_sync_time_display = auto_sync_time_raw
    if auto_sync_time_raw:
        try:
            auto_sync_time_display = datetime.strptime(auto_sync_time_raw, "%H:%M").strftime(
                "%I:%M %p"
            )
        except ValueError:
            try:
                auto_sync_time_display = datetime.strptime(auto_sync_time_raw, "%H:%M:%S").strftime(
                    "%I:%M %p"
                )
            except ValueError:
                auto_sync_time_display = auto_sync_time_raw
    return {
        "last_sync_status": str(last_sync.get("status") or "unknown"),
        "last_sync_stage": str(last_sync.get("stage") or ""),
        "last_sync_updated_at": updated_raw,
        "last_sync_updated_human": updated_human,
        "auto_sync_enabled": bool(auto_sync.get("enabled")),
        "auto_sync_time": auto_sync_time_display,
        "auto_sync_last_run_date": str(auto_sync.get("last_run_date") or ""),
    }


def get_vanquish_profit_lock_state() -> dict:
    """Return dashboard/web-link lock state once daily goal is hit."""
    goal = float((current_app and current_app.config.get("VANQUISH_DAILY_LOCK_GOAL")) or 500.0)
    try:
        # Optional runtime override; keeps default at 500.
        from mccain_capital import runtime as app_runtime

        goal = float(app_runtime.get_setting_float("vanquish_daily_lock_goal", goal) or goal)
    except Exception:
        pass
    if goal < 0:
        goal = 0.0

    now_et = datetime.now(TZ)
    minutes = now_et.hour * 60 + now_et.minute
    is_market_day = now_et.weekday() < 5
    in_market_hours = is_market_day and (570 <= minutes < 960)  # 9:30a-4:00p ET

    day_net = 0.0
    try:
        from mccain_capital.repositories import trades as trades_repo

        day_net = float(trades_repo.day_net(now_et.date().isoformat()) or 0.0)
    except Exception:
        day_net = 0.0

    active_goal = bool(goal > 0 and day_net >= goal and in_market_hours)
    unlock_dt = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    manual = _load_manual_vanquish_lock(now_et)
    goal_unlock_dt = unlock_dt if active_goal else None
    manual_unlock_dt = _parse_iso_dt(manual.get("until_at", ""))
    unlock_candidates = [x for x in [goal_unlock_dt, manual_unlock_dt] if isinstance(x, datetime)]
    effective_unlock = max(unlock_candidates) if unlock_candidates else unlock_dt
    active = bool(active_goal or manual.get("active"))
    reason_parts = []
    if active_goal:
        reason_parts.append(f"Daily goal hit ({day_net:,.2f} / {goal:,.2f}).")
    if manual.get("active"):
        reason_parts.append(
            f"Manual lock timer active ({int(manual.get('duration_minutes') or 0)}m)."
        )
    return {
        "active": active,
        "active_goal": active_goal,
        "active_manual": bool(manual.get("active")),
        "goal": goal,
        "day_net": day_net,
        "remaining": max(0.0, goal - day_net),
        "manual_until_label": str(manual.get("unlock_label") or ""),
        "manual_until_epoch": int(manual.get("until_epoch") or 0),
        "manual_remaining_seconds": int(manual.get("remaining_seconds") or 0),
        "unlock_epoch": int(effective_unlock.timestamp()),
        "unlock_label": effective_unlock.strftime("%b %d, %Y %I:%M %p ET"),
        "reason": (
            " ".join(reason_parts).strip()
            + f" Vanquish access locked until {effective_unlock.strftime('%I:%M %p ET')}."
            if active
            else ""
        ),
    }


def macro_event_short_label(title: str, impact: str = "High") -> str:
    normalized = re.sub(r"\s+", " ", str(title or "").strip()).lower()
    patterns = (
        ("fomc", "FOMC"),
        ("fed chair", "Fed"),
        ("rate decision", "FOMC"),
        ("non-farm", "NFP"),
        ("nonfarm", "NFP"),
        ("payroll", "NFP"),
        ("unemployment", "Jobs"),
        ("job openings", "JOLTS"),
        ("cpi", "CPI"),
        ("ppi", "PPI"),
        ("pce", "PCE"),
        ("inflation", "Inflation"),
        ("gdp", "GDP"),
        ("retail sales", "Retail"),
        ("ism", "ISM"),
        ("pmi", "PMI"),
    )
    for needle, label in patterns:
        if needle in normalized:
            return label
    impact_label = str(impact or "High").strip().title() or "High"
    return f"{impact_label} Impact"


def _macro_notice_detail(
    *,
    title: str,
    impact: str,
    starts_at: datetime,
    forecast: str = "",
    previous: str = "",
) -> str:
    parts = [
        title,
        f"{impact} impact",
        starts_at.strftime("%b %-d %I:%M %p ET"),
    ]
    if forecast:
        parts.append(f"Forecast {forecast}")
    if previous:
        parts.append(f"Previous {previous}")
    return " · ".join(parts)


def _global_top_notice() -> dict | None:
    now_et = datetime.now(TZ)
    payload: list[dict] = []
    weekly_payload = get_forex_factory_feed()
    next_week_payload = get_forex_factory_next_week_feed()
    if isinstance(weekly_payload, list):
        payload.extend(weekly_payload)
    if isinstance(next_week_payload, list):
        payload.extend(next_week_payload)

    if not payload:
        return None

    cutoff = now_et - timedelta(minutes=1)
    events: list[dict] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        if str(row.get("country") or "").upper() != "USD":
            continue
        impact = str(row.get("impact") or "").title()
        if impact not in {"High", "Medium", "Low"}:
            continue
        raw_date = str(row.get("date") or "").strip()
        if not raw_date:
            continue
        try:
            starts_at = datetime.fromisoformat(raw_date)
        except ValueError:
            continue
        if starts_at < cutoff:
            continue
        day_prefix = "" if starts_at.date() == now_et.date() else f"{starts_at.strftime('%a')} "
        title = str(row.get("title") or "USD high impact").strip() or "USD high impact"
        forecast = str(row.get("forecast") or "").strip()
        previous = str(row.get("previous") or "").strip()
        event_detail = _macro_notice_detail(
            title=title,
            impact=impact,
            starts_at=starts_at,
            forecast=forecast,
            previous=previous,
        )
        events.append(
            {
                "label": macro_event_short_label(title, impact),
                "time_short": starts_at.strftime("%-I:%M"),
                "time_label": f"{day_prefix}{starts_at.strftime('%-I:%M %p ET')}",
                "detail": event_detail,
                "level": impact.lower(),
                "starts_at": starts_at.isoformat(),
            }
        )
    if not events:
        return None
    events.sort(key=lambda item: str(item.get("starts_at") or ""))
    first = events[0]
    first_starts_at = datetime.fromisoformat(str(first["starts_at"]))
    detail_href = (
        f"/candle-opens?y={first_starts_at.year}&m={first_starts_at.month}"
        f"#news-day-{first_starts_at.date().isoformat()}"
    )
    return {
        "text": f"{first['label']} {first['time_short']}",
        "detail": str(first.get("detail") or ""),
        "href": detail_href,
        "level": str(first.get("level") or "high"),
        "count": 1,
        "events": [first],
    }


def get_forex_factory_feed() -> list[dict] | None:
    now_et = datetime.now(TZ)
    fetched_at = _forex_factory_cache.get("fetched_at")
    cached_payload = _forex_factory_cache.get("payload")
    if (
        isinstance(fetched_at, datetime)
        and (now_et - fetched_at).total_seconds() < FOREX_FACTORY_CACHE_TTL_SECONDS
    ):
        return cached_payload if isinstance(cached_payload, list) else None

    try:
        req = urllib.request.Request(
            FOREX_FACTORY_WEEKLY_URL,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json,*/*",
            },
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            payload = json.load(resp)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError):
        if isinstance(cached_payload, list):
            return cached_payload
        disk_payload = _load_forex_factory_disk_cache()
        if isinstance(disk_payload, list):
            _forex_factory_cache["payload"] = disk_payload
            return disk_payload
        return None

    if isinstance(payload, list):
        _forex_factory_cache["fetched_at"] = now_et
        _forex_factory_cache["payload"] = payload
        _save_forex_factory_disk_cache(payload)
        return payload

    return cached_payload if isinstance(cached_payload, list) else _load_forex_factory_disk_cache()


def get_forex_factory_next_week_feed() -> list[dict] | None:
    now_et = datetime.now(TZ)
    fetched_at = _forex_factory_next_week_cache.get("fetched_at")
    cached_payload = _forex_factory_next_week_cache.get("payload")
    if (
        isinstance(fetched_at, datetime)
        and (now_et - fetched_at).total_seconds() < FOREX_FACTORY_CACHE_TTL_SECONDS
    ):
        return cached_payload if isinstance(cached_payload, list) else None

    try:
        req = urllib.request.Request(
            FOREX_FACTORY_NEXT_WEEKLY_URL,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json,*/*",
            },
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            payload = json.load(resp)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError):
        if isinstance(cached_payload, list):
            return cached_payload
        disk_payload = _load_forex_factory_next_week_disk_cache()
        if isinstance(disk_payload, list):
            _forex_factory_next_week_cache["payload"] = disk_payload
            return disk_payload
        return None

    if isinstance(payload, list):
        _forex_factory_next_week_cache["fetched_at"] = now_et
        _forex_factory_next_week_cache["payload"] = payload
        _save_forex_factory_next_week_disk_cache(payload)
        return payload

    return (
        cached_payload
        if isinstance(cached_payload, list)
        else _load_forex_factory_next_week_disk_cache()
    )


def get_forex_factory_month_feed() -> list[dict] | None:
    now_et = datetime.now(TZ)
    fetched_at = _forex_factory_month_cache.get("fetched_at")
    cached_payload = _forex_factory_month_cache.get("payload")
    if (
        isinstance(fetched_at, datetime)
        and (now_et - fetched_at).total_seconds() < FOREX_FACTORY_CACHE_TTL_SECONDS
    ):
        return cached_payload if isinstance(cached_payload, list) else None

    try:
        req = urllib.request.Request(
            FOREX_FACTORY_MONTHLY_URL,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json,*/*",
            },
        )
        with urllib.request.urlopen(req, timeout=3) as resp:
            payload = json.load(resp)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError):
        if isinstance(cached_payload, list):
            return cached_payload
        disk_payload = _load_forex_factory_month_disk_cache()
        if isinstance(disk_payload, list):
            _forex_factory_month_cache["payload"] = disk_payload
            return disk_payload
        # Fall back to weekly feed if monthly endpoint is unavailable.
        return get_forex_factory_feed()

    if isinstance(payload, list):
        _forex_factory_month_cache["fetched_at"] = now_et
        _forex_factory_month_cache["payload"] = payload
        _save_forex_factory_month_disk_cache(payload)
        return payload

    return (
        cached_payload
        if isinstance(cached_payload, list)
        else _load_forex_factory_month_disk_cache()
    )


def _profile_context() -> dict:
    try:
        from mccain_capital.services.profile import profile_template_context

        return profile_template_context()
    except Exception:
        return {}


def render_page(content_html: str, *, active: str, title: str = APP_TITLE, **page_ctx):
    static_root = current_app.static_folder or "static"
    logo_primary = "logo-primary.png"
    logo_flat = "logo-flat.png"
    logo_default = "logo.png"
    logo_filename = (
        logo_primary if os.path.exists(os.path.join(static_root, logo_primary)) else logo_default
    )
    logo_flat_filename = (
        logo_flat if os.path.exists(os.path.join(static_root, logo_flat)) else logo_filename
    )
    top_notice = page_ctx.pop("top_notice", None) or _global_top_notice()
    if isinstance(top_notice, dict):
        text = str(top_notice.get("text") or "")
        text = re.sub(r"^\s*[🔴🟠🟡🟢🔵]\s*", "", text)
        top_notice = {**top_notice, "text": text}
    vanquish_lock = page_ctx.pop("vanquish_lock", None)
    trading_window = page_ctx.pop("trading_window", None)
    csrf_token = csrf_token_value()
    csrf_input = csrf_input_html()
    if not isinstance(vanquish_lock, dict):
        vanquish_lock = get_vanquish_profit_lock_state()
    if not isinstance(trading_window, dict):
        trading_window = get_trading_window_state()
    now_et = app_runtime.now_et()
    header_date_label = f"{now_et.strftime('%a %b')} {now_et.day}, {now_et.year}"
    return render_template(
        "base.html",
        title=title,
        brand_title=APP_TITLE,
        logo_filename=logo_filename,
        logo_flat_filename=logo_flat_filename,
        static_v=_static_version(static_root),
        auth_enabled=auth_enabled(),
        authenticated=is_authenticated(),
        auth_username=effective_username(),
        **_profile_context(),
        current_date=app_runtime.today_iso(),
        header_date_label=header_date_label,
        system_status=get_system_status(),
        top_notice=top_notice,
        vanquish_lock=vanquish_lock,
        trading_window=trading_window,
        csrf_token=csrf_token,
        csrf_input=csrf_input,
        content=_inject_csrf_inputs(content_html, csrf_input),
        active=active,
        **page_ctx,
    )


def simple_msg(msg: str) -> str:
    return render_template_string(
        """
        <div class=\"card\"><div class=\"toolbar\">
          <div class=\"pill\">⚠️</div>
          <div style=\"margin-top:10px\">{{ msg }}</div>
          <div class=\"hr\"></div>
          <div class=\"rightActions\">
            <a class=\"btn primary\" href=\"/trades\">Back</a>
          </div>
        </div></div>
        """,
        msg=msg,
    )
