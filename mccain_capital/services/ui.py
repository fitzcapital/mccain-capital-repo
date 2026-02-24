"""UI rendering adapters for service modules without direct app_core coupling."""

from __future__ import annotations

import os

from flask import current_app, render_template, render_template_string

from mccain_capital.auth import auth_enabled, effective_username, is_authenticated
from mccain_capital.runtime import now_iso

APP_TITLE = "McCain Capital 🏛️"


def _static_version(static_root: str) -> str:
    logo_path = os.path.join(static_root, "logo.png")
    favicon_path = os.path.join(static_root, "favicon.ico")
    try:
        return str(int(max(os.path.getmtime(logo_path), os.path.getmtime(favicon_path))))
    except Exception:
        return now_iso().replace(":", "").replace("-", "")


def render_page(content_html: str, *, active: str, title: str = APP_TITLE):
    static_root = current_app.static_folder or "static"
    return render_template(
        "base.html",
        title=title,
        static_v=_static_version(static_root),
        auth_enabled=auth_enabled(),
        authenticated=is_authenticated(),
        auth_username=effective_username(),
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
