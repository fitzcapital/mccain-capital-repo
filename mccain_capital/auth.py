"""Authentication helpers shared across app startup and UI rendering."""

from __future__ import annotations

import os

from flask import session
from werkzeug.security import generate_password_hash

from mccain_capital.runtime import get_setting_value

APP_USERNAME = os.environ.get("APP_USERNAME", "owner")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")
APP_PASSWORD_HASH = os.environ.get("APP_PASSWORD_HASH", "")


def _legacy_auth_values() -> tuple[str, str, str]:
    """Read mutable auth values from app_core when available (test compatibility)."""
    try:
        from mccain_capital import app_core

        return (
            getattr(app_core, "APP_USERNAME", APP_USERNAME),
            getattr(app_core, "APP_PASSWORD", APP_PASSWORD),
            getattr(app_core, "APP_PASSWORD_HASH", APP_PASSWORD_HASH),
        )
    except Exception:
        return APP_USERNAME, APP_PASSWORD, APP_PASSWORD_HASH


def effective_password_hash() -> str:
    db_hash = (get_setting_value("auth_password_hash", "") or "").strip()
    if db_hash:
        return db_hash
    _, legacy_password, legacy_password_hash = _legacy_auth_values()
    if legacy_password_hash:
        return legacy_password_hash
    if legacy_password:
        return generate_password_hash(legacy_password)
    return ""


def effective_username() -> str:
    db_user = (get_setting_value("auth_username", "") or "").strip()
    if db_user:
        return db_user
    legacy_user, _, _ = _legacy_auth_values()
    return legacy_user


def auth_enabled() -> bool:
    return bool(effective_password_hash())


def is_authenticated() -> bool:
    return bool(session.get("auth_ok")) and session.get("auth_user") == effective_username()
