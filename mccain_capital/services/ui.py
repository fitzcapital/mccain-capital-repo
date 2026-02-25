"""UI rendering adapters for service modules without direct app_core coupling."""

from __future__ import annotations

import json
import os

from flask import current_app, render_template, render_template_string

from mccain_capital.auth import auth_enabled, effective_username, is_authenticated
from mccain_capital.runtime import UPLOAD_DIR, now_iso

APP_TITLE = "McCain Capital 🏛️"


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


def _system_status() -> dict:
    sync_path = os.path.join(UPLOAD_DIR, ".vanquish_sync_last_run.json")
    auto_path = os.path.join(UPLOAD_DIR, ".vanquish_auto_sync.json")
    last_sync = _load_json(sync_path)
    auto_sync = _load_json(auto_path)
    return {
        "last_sync_status": str(last_sync.get("status") or "unknown"),
        "last_sync_stage": str(last_sync.get("stage") or ""),
        "last_sync_updated_at": str(last_sync.get("updated_at") or ""),
        "auto_sync_enabled": bool(auto_sync.get("enabled")),
        "auto_sync_time": str(auto_sync.get("run_time_et") or ""),
        "auto_sync_last_run_date": str(auto_sync.get("last_run_date") or ""),
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
        system_status=_system_status(),
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
