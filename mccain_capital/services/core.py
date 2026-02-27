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


def dashboard_recompute_balances():
    return _legacy().dashboard_recompute_balances()


def analytics_page():
    from mccain_capital.services import analytics as analytics_svc

    return analytics_svc.analytics_page()


def session_replay_page():
    from mccain_capital.services import analytics as analytics_svc

    return analytics_svc.session_replay_page()


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


def strat_page():
    from mccain_capital.services import strat as strat_svc

    return strat_svc.strat_page()
