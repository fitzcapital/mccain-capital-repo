"""Core endpoint handlers (delegating to service layer)."""

from mccain_capital.services import core as svc
from mccain_capital.services import goals as goals_svc


def home():
    return svc.home()


def setup_page():
    return svc.setup_page()


def login_page():
    return svc.login_page()


def logout_page():
    return svc.logout_page()


def healthz():
    return svc.healthz()


def favicon():
    return svc.favicon()


def dashboard():
    return svc.dashboard()


def command_calendar_page():
    return svc.command_calendar_page()


def dashboard_recompute_balances():
    return svc.dashboard_recompute_balances()


def candle_opens_page():
    return svc.candle_opens_page()


def analytics_page():
    return svc.analytics_page()


def session_replay_page():
    return svc.session_replay_page()


def calculator():
    return svc.calculator()


def goals_tracker():
    return goals_svc.goals_tracker()


def links_page():
    return svc.links_page()


def export_json():
    return svc.export_json()


def backup_data():
    return svc.backup_data()


def restore_data():
    return svc.restore_data()


def payouts_page():
    return goals_svc.payouts_page()
