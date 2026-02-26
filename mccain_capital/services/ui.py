"""UI rendering adapters for service modules without direct app_core coupling."""

from __future__ import annotations

import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import current_app, render_template, render_template_string

from mccain_capital.auth import auth_enabled, effective_username, is_authenticated
from mccain_capital.repositories import trades as trades_repo
from mccain_capital.runtime import UPLOAD_DIR, money, now_iso, today_iso

APP_TITLE = "McCain Capital 🏛️"
TZ = ZoneInfo("America/New_York")


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


def _parse_run_time(raw: str) -> tuple[int, int] | None:
    txt = (raw or "").strip()
    if not txt:
        return None
    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            dt = datetime.strptime(txt, fmt)
            return dt.hour, dt.minute
        except ValueError:
            continue
    return None


def _build_notifications(last_sync: dict, auto_sync: dict) -> list[dict]:
    notes: list[dict] = []
    status = str(last_sync.get("status") or "").lower()
    stage = str(last_sync.get("stage") or "").lower()
    updated_raw = str(last_sync.get("updated_at") or "")
    updated_human = ""
    if updated_raw:
        try:
            updated_human = (
                datetime.fromisoformat(updated_raw.replace("Z", "+00:00"))
                .astimezone(TZ)
                .strftime("%b %d, %Y %I:%M %p ET")
            )
        except Exception:
            updated_human = updated_raw
    if status == "success":
        notes.append(
            {
                "level": "success",
                "title": "Sync Success",
                "body": (
                    f"Latest broker sync completed at {updated_human}."
                    if updated_human
                    else "Latest broker sync completed."
                ),
            }
        )
    elif status == "failed":
        msg = str(last_sync.get("message") or "Latest broker sync failed.")
        notes.append({"level": "error", "title": "Sync Failed", "body": msg})

    run_time_raw = str(auto_sync.get("run_time_et") or "")
    run_time_hm = _parse_run_time(run_time_raw)
    auto_enabled = bool(auto_sync.get("enabled"))
    run_weekends = bool(auto_sync.get("run_weekends"))
    last_run_date = str(auto_sync.get("last_run_date") or "")
    now_local = datetime.now(TZ)
    today = now_local.date().isoformat()
    auto_missed = False
    if auto_enabled and run_time_hm and (run_weekends or now_local.weekday() < 5):
        hh, mm = run_time_hm
        if (now_local.hour, now_local.minute) >= (hh, mm) and last_run_date != today:
            auto_missed = True
    if auto_enabled and auto_missed:
        run_time_display = run_time_raw
        try:
            run_time_display = datetime.strptime(run_time_raw, "%H:%M").strftime("%I:%M %p")
        except ValueError:
            try:
                run_time_display = datetime.strptime(run_time_raw, "%H:%M:%S").strftime("%I:%M %p")
            except ValueError:
                pass
        notes.append(
            {
                "level": "warning",
                "title": "Auto-Sync Missed",
                "body": f"Scheduled run for {run_time_display} ET has not completed today.",
            }
        )
    elif auto_enabled and status == "failed" and stage == "auto_config":
        notes.append(
            {
                "level": "warning",
                "title": "Auto-Sync Skipped",
                "body": "Auto-sync is enabled but credentials/account config is incomplete.",
            }
        )

    try:
        rc = trades_repo.get_risk_controls()
        max_loss = float(rc.get("daily_max_loss", 0.0) or 0.0)
        enforce = int(rc.get("enforce_lockout", 0) or 0)
        guardrail = trades_repo.trade_lockout_state(
            today_iso(), daily_max_loss=max_loss, enforce_lockout=enforce
        )
        if enforce and max_loss > 0:
            if guardrail.get("locked"):
                notes.append(
                    {
                        "level": "error",
                        "title": "Guardrail Locked",
                        "body": (
                            f"Day net {money(guardrail.get('day_net') or 0)} reached max loss "
                            f"{money(guardrail.get('daily_max_loss') or 0)}."
                        ),
                    }
                )
            else:
                notes.append(
                    {
                        "level": "success",
                        "title": "Guardrail Active",
                        "body": (
                            f"Risk lockout armed at max loss {money(guardrail.get('daily_max_loss') or 0)}."
                        ),
                    }
                )
    except Exception:
        pass
    return notes


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
    notifications = _build_notifications(last_sync=last_sync, auto_sync=auto_sync)
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
        "notifications": notifications,
    }


def render_page(content_html: str, *, active: str, title: str = APP_TITLE):
    static_root = current_app.static_folder or "static"
    return render_template(
        "base.html",
        title=title,
        static_v=_static_version(static_root),
        auth_enabled=auth_enabled(),
        authenticated=is_authenticated(),
        auth_username=effective_username(),
        system_status=get_system_status(),
        content=content_html,
        active=active,
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
