"""Core domain service gateway.

Core routes still rely on legacy implementations in ``app_core``. This module
keeps that dependency localized behind explicit delegator functions.
"""

from __future__ import annotations


def _legacy():
    from mccain_capital import app_core

    return app_core


def home():
    return _legacy().home()


def setup_page():
    return _legacy().setup_page()


def login_page():
    return _legacy().login_page()


def logout_page():
    return _legacy().logout_page()


def healthz():
    return _legacy().healthz()


def favicon():
    return _legacy().favicon()


def dashboard():
    return _legacy().dashboard()


def analytics_page():
    return _legacy().analytics_page()


def calculator():
    return _legacy().calculator()


def links_page():
    return _legacy().links_page()


def export_json():
    return _legacy().export_json()


def backup_data():
    return _legacy().backup_data()


def restore_data():
    return _legacy().restore_data()


def chart():
    return _legacy().chart()


def strat_page():
    return _legacy().strat_page()
