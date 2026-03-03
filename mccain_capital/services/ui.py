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
FOREX_FACTORY_CACHE_TTL_SECONDS = 900
_forex_factory_cache: dict[str, object] = {"fetched_at": None, "payload": None}
FOREX_FACTORY_CACHE_FILE = os.path.join(UPLOAD_DIR, ".forex_factory_weekly_cache.json")


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


def render_page(content_html: str, *, active: str, title: str = APP_TITLE, **page_ctx):
    static_root = current_app.static_folder or "static"
    top_notice = page_ctx.pop("top_notice", None) or _global_top_notice()
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
