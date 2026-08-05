"""Profile and local account metadata service."""

from __future__ import annotations

import base64
import json
import re
from typing import Any

from flask import flash, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from mccain_capital import auth
from mccain_capital.runtime import get_setting_value, now_iso, set_setting_value

PROFILE_SETTING_KEY = "auth_user_profiles"
DEFAULT_ADMIN_USERS = {"admin", "fitz"}
AVATAR_COLORS = {
    "blue": "#58a6ff",
    "cyan": "#5de4ff",
    "green": "#48f0a0",
    "gold": "#ffd66b",
    "violet": "#a78bfa",
}
MAX_PHOTO_BYTES = 750_000
PROFILE_TICKERS = ("SPX", "SPY", "QQQ")


def _normalize_username(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_.-]+", "", str(value or "").strip())
    return cleaned[:40]


def _display_name(username: str) -> str:
    return str(username or "User").replace("_", " ").replace("-", " ").title()


def _initials(name: str, username: str) -> str:
    source = str(name or username or "U").strip()
    parts = [p for p in re.split(r"\s+", source) if p]
    if len(parts) >= 2:
        return (parts[0][:1] + parts[1][:1]).upper()
    return source[:2].upper()


def _default_profile(username: str) -> dict[str, Any]:
    username = _normalize_username(username) or auth.effective_username()
    display_name = _display_name(username)
    return {
        "username": username,
        "display_name": display_name,
        "title": "Owner",
        "email": "",
        "avatar_initials": _initials(display_name, username),
        "avatar_color": "blue",
        "photo_data_url": "",
        "market_pulse_default_ticker": "SPY",
        "dashboard_default_ticker": "SPY",
        "is_admin": username.lower() in DEFAULT_ADMIN_USERS,
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }


def _clean_ticker(value: Any, default: str = "SPY") -> str:
    ticker = str(value or "").strip().upper()
    return ticker if ticker in PROFILE_TICKERS else default


def _load_store() -> dict[str, Any]:
    raw = str(get_setting_value(PROFILE_SETTING_KEY, "") or "").strip()
    if not raw:
        return {"users": {}}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {"users": {}}
    users = parsed.get("users") if isinstance(parsed, dict) else {}
    if not isinstance(users, dict):
        users = {}
    return {"users": users}


def _save_store(store: dict[str, Any]) -> None:
    set_setting_value(PROFILE_SETTING_KEY, json.dumps(store, separators=(",", ":")))


def _clean_profile(row: dict[str, Any], username: str) -> dict[str, Any]:
    base = _default_profile(username)
    if isinstance(row, dict):
        base.update(
            {
                "display_name": str(row.get("display_name") or base["display_name"]).strip()[:80],
                "title": str(row.get("title") or base["title"]).strip()[:80],
                "email": str(row.get("email") or "").strip()[:120],
                "avatar_initials": str(row.get("avatar_initials") or base["avatar_initials"])
                .strip()
                .upper()[:3],
                "avatar_color": str(row.get("avatar_color") or base["avatar_color"]).strip(),
                "photo_data_url": str(row.get("photo_data_url") or "").strip(),
                "market_pulse_default_ticker": _clean_ticker(
                    row.get("market_pulse_default_ticker"), base["market_pulse_default_ticker"]
                ),
                "dashboard_default_ticker": _clean_ticker(
                    row.get("dashboard_default_ticker"), base["dashboard_default_ticker"]
                ),
                "is_admin": bool(row.get("is_admin")),
                "created_at": str(row.get("created_at") or base["created_at"]),
                "updated_at": str(row.get("updated_at") or base["updated_at"]),
            }
        )
    base["username"] = username
    if username.lower() in DEFAULT_ADMIN_USERS:
        base["is_admin"] = True
    if base["avatar_color"] not in AVATAR_COLORS:
        base["avatar_color"] = "blue"
    if not base["avatar_initials"]:
        base["avatar_initials"] = _initials(base["display_name"], username)
    return base


def get_profile(username: str | None = None) -> dict[str, Any]:
    username = (
        _normalize_username(username or auth.effective_username()) or auth.effective_username()
    )
    store = _load_store()
    profile = _clean_profile(store["users"].get(username) or {}, username)
    if username not in store["users"]:
        store["users"][username] = profile
        _save_store(store)
    return profile


def is_admin(username: str | None = None) -> bool:
    profile = get_profile(username or auth.effective_username())
    return bool(profile.get("is_admin"))


def profile_template_context() -> dict[str, Any]:
    if not auth.auth_enabled() or not auth.is_authenticated():
        return {}
    profile = get_profile(auth.effective_username())
    return {
        "auth_profile": profile,
        "auth_display_name": profile.get("display_name") or auth.effective_username(),
        "auth_avatar_color": AVATAR_COLORS.get(str(profile.get("avatar_color") or ""), "#58a6ff"),
        "auth_is_admin": bool(profile.get("is_admin")),
    }


def _all_profiles() -> list[dict[str, Any]]:
    store = _load_store()
    current_username = auth.effective_username()
    if current_username not in store["users"]:
        store["users"][current_username] = _default_profile(current_username)
        _save_store(store)
    profiles = [
        _clean_profile(row, username)
        for username, row in sorted(store["users"].items(), key=lambda item: item[0].lower())
    ]
    if not any(row["username"] == current_username for row in profiles):
        profiles.insert(0, get_profile(current_username))
    return profiles


def _photo_data_url() -> str | None:
    compressed = str(request.form.get("photo_data_url") or "").strip()
    if compressed:
        match = re.match(
            r"^data:(image/(?:png|jpeg|webp|gif));base64,([A-Za-z0-9+/=]+)$", compressed
        )
        if not match:
            raise ValueError("Profile photo could not be processed. Try another image.")
        try:
            raw = base64.b64decode(match.group(2), validate=True)
        except Exception as exc:
            raise ValueError("Profile photo could not be processed. Try another image.") from exc
        if len(raw) > MAX_PHOTO_BYTES:
            raise ValueError(
                "Profile photo must be under 750 KB. Try compressing or choosing a smaller image."
            )
        return compressed

    uploaded = request.files.get("photo")
    if not uploaded or not uploaded.filename:
        return None
    content_type = str(uploaded.content_type or "").lower()
    if content_type not in {"image/png", "image/jpeg", "image/webp", "image/gif"}:
        raise ValueError("Profile photo must be PNG, JPG, WEBP, or GIF.")
    data = uploaded.read(MAX_PHOTO_BYTES + 1)
    if len(data) > MAX_PHOTO_BYTES:
        raise ValueError(
            "Profile photo must be under 750 KB. Try compressing or choosing a smaller image."
        )
    encoded = base64.b64encode(data).decode("ascii")
    return f"data:{content_type};base64,{encoded}"


def profile_page():
    from mccain_capital.services.ui import render_page

    profile = get_profile(auth.effective_username())
    try:
        from mccain_capital.services.auth_passkeys import _load_passkeys

        passkey_count = len(_load_passkeys())
    except Exception:
        passkey_count = 0
    return render_page(
        render_template(
            "profile.html",
            profile=profile,
            profiles=_all_profiles(),
            avatar_colors=AVATAR_COLORS,
            is_admin=is_admin(),
            passkey_count=passkey_count,
        ),
        active="profile",
        title="McCain Capital · Profile",
    )


def update_profile_details():
    username = auth.effective_username()
    store = _load_store()
    profile = _clean_profile(store["users"].get(username) or {}, username)
    try:
        photo_data_url = _photo_data_url()
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("profile_page"))

    display_name = str(request.form.get("display_name") or "").strip()[:80]
    title = str(request.form.get("title") or "").strip()[:80]
    email = str(request.form.get("email") or "").strip()[:120]
    initials = str(request.form.get("avatar_initials") or "").strip().upper()[:3]
    avatar_color = str(request.form.get("avatar_color") or "").strip()
    market_pulse_default_ticker = _clean_ticker(
        request.form.get("market_pulse_default_ticker"),
        str(profile.get("market_pulse_default_ticker") or "SPY"),
    )
    dashboard_default_ticker = _clean_ticker(
        request.form.get("dashboard_default_ticker"),
        str(profile.get("dashboard_default_ticker") or "SPY"),
    )

    profile.update(
        {
            "display_name": display_name or profile["display_name"],
            "title": title or profile["title"],
            "email": email,
            "avatar_initials": initials or _initials(display_name, username),
            "avatar_color": (
                avatar_color if avatar_color in AVATAR_COLORS else profile["avatar_color"]
            ),
            "market_pulse_default_ticker": market_pulse_default_ticker,
            "dashboard_default_ticker": dashboard_default_ticker,
            "updated_at": now_iso(),
        }
    )
    if photo_data_url is not None:
        profile["photo_data_url"] = photo_data_url
    if request.form.get("remove_photo") == "1":
        profile["photo_data_url"] = ""
    store["users"][username] = profile
    _save_store(store)
    flash("Profile updated.", "success")
    return redirect(url_for("profile_page"))


def update_password():
    if not auth.auth_enabled():
        flash("Create login credentials before changing a password.", "warning")
        return redirect(url_for("setup_page"))
    current_password = request.form.get("current_password") or ""
    new_password = request.form.get("new_password") or ""
    confirm_password = request.form.get("confirm_password") or ""
    if not check_password_hash(auth.effective_password_hash(), current_password):
        flash("Current password is not correct.", "danger")
    elif len(new_password) < 8:
        flash("New password must be at least 8 characters.", "danger")
    elif new_password != confirm_password:
        flash("New passwords do not match.", "danger")
    else:
        set_setting_value("auth_password_hash", generate_password_hash(new_password))
        flash("Password updated.", "success")
    return redirect(url_for("profile_page"))


def admin_update_user():
    if not is_admin():
        flash("Admin rights are required to update users.", "danger")
        return redirect(url_for("profile_page"))

    original_username = _normalize_username(request.form.get("original_username") or "")
    username = _normalize_username(request.form.get("username") or "")
    if not original_username or not username:
        flash("Username is required.", "danger")
        return redirect(url_for("profile_page"))

    store = _load_store()
    existing = _clean_profile(store["users"].pop(original_username, {}) or {}, username)
    display_name = str(request.form.get("display_name") or "").strip()[:80]
    title = str(request.form.get("title") or "").strip()[:80]
    initials = str(request.form.get("avatar_initials") or "").strip().upper()[:3]
    avatar_color = str(request.form.get("avatar_color") or "").strip()
    requested_admin = request.form.get("is_admin") == "1"
    existing.update(
        {
            "username": username,
            "display_name": display_name or existing["display_name"],
            "title": title or existing["title"],
            "avatar_initials": initials or _initials(display_name, username),
            "avatar_color": (
                avatar_color if avatar_color in AVATAR_COLORS else existing["avatar_color"]
            ),
            "is_admin": requested_admin or username.lower() in DEFAULT_ADMIN_USERS,
            "updated_at": now_iso(),
        }
    )
    store["users"][username] = existing
    _save_store(store)

    if original_username == auth.effective_username():
        set_setting_value("auth_username", username)
        session["auth_user"] = username
    flash("User details updated.", "success")
    return redirect(url_for("profile_page"))
