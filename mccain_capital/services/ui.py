"""UI rendering adapters for service modules without direct app_core coupling."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
import urllib.error
import urllib.request
from zoneinfo import ZoneInfo

from flask import current_app, render_template, render_template_string

from mccain_capital.auth import auth_enabled, effective_username, is_authenticated
from mccain_capital.runtime import UPLOAD_DIR, now_iso

APP_TITLE = "McCain Capital"
TZ = ZoneInfo("America/New_York")
FOREX_FACTORY_WEEKLY_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
FOREX_FACTORY_NEXT_WEEKLY_URL = "https://nfs.faireconomy.media/ff_calendar_nextweek.json"
FOREX_FACTORY_MONTHLY_URL = "https://nfs.faireconomy.media/ff_calendar_thismonth.json"
FOREX_FACTORY_CACHE_TTL_SECONDS = 900
_forex_factory_cache: dict[str, object] = {"fetched_at": None, "payload": None}
_forex_factory_next_week_cache: dict[str, object] = {"fetched_at": None, "payload": None}
_forex_factory_month_cache: dict[str, object] = {"fetched_at": None, "payload": None}
FOREX_FACTORY_CACHE_FILE = os.path.join(UPLOAD_DIR, ".forex_factory_weekly_cache.json")
FOREX_FACTORY_NEXT_WEEK_CACHE_FILE = os.path.join(UPLOAD_DIR, ".forex_factory_next_weekly_cache.json")
FOREX_FACTORY_MONTH_CACHE_FILE = os.path.join(UPLOAD_DIR, ".forex_factory_monthly_cache.json")
VANQUISH_MANUAL_LOCK_PATH = os.path.join(UPLOAD_DIR, ".vanquish_manual_lock.json")


def _static_version(static_root: str) -> str:
    logo_path = os.path.join(static_root, "logo.png")
    favicon_path = os.path.join(static_root, "favicon.ico")
    try:
        return str(int(max(os.path.getmtime(logo_path), os.path.getmtime(favicon_path))))
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
    payload = _load_json(VANQUISH_MANUAL_LOCK_PATH)
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
    _save_json(VANQUISH_MANUAL_LOCK_PATH, payload)
    return _load_manual_vanquish_lock(now_et)


def clear_manual_vanquish_lock() -> None:
    try:
        os.remove(VANQUISH_MANUAL_LOCK_PATH)
    except OSError:
        return


def _load_forex_factory_disk_cache() -> list[dict] | None:
    cached = _load_json(FOREX_FACTORY_CACHE_FILE)
    payload = cached.get("payload")
    return payload if isinstance(payload, list) else None


def _save_forex_factory_disk_cache(payload: list[dict]) -> None:
    try:
        with open(FOREX_FACTORY_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"saved_at": now_iso(), "payload": payload}, f)
    except OSError:
        return


def _load_forex_factory_month_disk_cache() -> list[dict] | None:
    cached = _load_json(FOREX_FACTORY_MONTH_CACHE_FILE)
    payload = cached.get("payload")
    return payload if isinstance(payload, list) else None


def _save_forex_factory_month_disk_cache(payload: list[dict]) -> None:
    try:
        with open(FOREX_FACTORY_MONTH_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"saved_at": now_iso(), "payload": payload}, f)
    except OSError:
        return


def _load_forex_factory_next_week_disk_cache() -> list[dict] | None:
    cached = _load_json(FOREX_FACTORY_NEXT_WEEK_CACHE_FILE)
    payload = cached.get("payload")
    return payload if isinstance(payload, list) else None


def _save_forex_factory_next_week_disk_cache(payload: list[dict]) -> None:
    try:
        with open(FOREX_FACTORY_NEXT_WEEK_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"saved_at": now_iso(), "payload": payload}, f)
    except OSError:
        return


def get_system_status() -> dict:
    sync_path = os.path.join(UPLOAD_DIR, ".vanquish_sync_last_run.json")
    auto_path = os.path.join(UPLOAD_DIR, ".vanquish_auto_sync.json")
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
    goal = float(
        (current_app and current_app.config.get("VANQUISH_DAILY_LOCK_GOAL"))
        or 500.0
    )
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
        reason_parts.append(
            f"Daily goal hit ({day_net:,.2f} / {goal:,.2f})."
        )
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
            " ".join(reason_parts).strip() + f" Vanquish access locked until {effective_unlock.strftime('%I:%M %p ET')}."
            if active
            else ""
        ),
    }


def _global_top_notice() -> dict | None:
    now_et = datetime.now(TZ)
    payload = get_forex_factory_feed()

    if not isinstance(payload, list):
        return None

    cutoff = now_et - timedelta(minutes=1)
    for row in payload:
        if not isinstance(row, dict):
            continue
        if str(row.get("country") or "").upper() != "USD":
            continue
        if str(row.get("impact") or "").title() != "High":
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
        detail_href = f"/candle-opens?y={starts_at.year}&m={starts_at.month}#news-day-{starts_at.date().isoformat()}"
        return {
            "label": "Red Folder",
            "text": f"🔴 {day_prefix}{starts_at.strftime('%-I:%M %p ET')}",
            "detail": f"High impact · {starts_at.strftime('%b %-d %I:%M %p ET')} · {title}",
            "href": detail_href,
            "level": "high",
        }
    return None


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
        cached_payload if isinstance(cached_payload, list) else _load_forex_factory_month_disk_cache()
    )


def render_page(content_html: str, *, active: str, title: str = APP_TITLE, **page_ctx):
    static_root = current_app.static_folder or "static"
    top_notice = page_ctx.pop("top_notice", None) or _global_top_notice()
    vanquish_lock = page_ctx.pop("vanquish_lock", None)
    if not isinstance(vanquish_lock, dict):
        vanquish_lock = get_vanquish_profit_lock_state()
    return render_template(
        "base.html",
        title=title,
        brand_title=APP_TITLE,
        static_v=_static_version(static_root),
        auth_enabled=auth_enabled(),
        authenticated=is_authenticated(),
        auth_username=effective_username(),
        system_status=get_system_status(),
        top_notice=top_notice,
        vanquish_lock=vanquish_lock,
        content=content_html,
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
