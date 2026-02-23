"""Core endpoint handlers (delegating to legacy implementation)."""

from mccain_capital import legacy_app as legacy


def home():
    return legacy.home()


def setup_page():
    return legacy.setup_page()


def login_page():
    return legacy.login_page()


def logout_page():
    return legacy.logout_page()


def healthz():
    return legacy.healthz()


def favicon():
    return legacy.favicon()


def dashboard():
    return legacy.dashboard()


def analytics_page():
    return legacy.analytics_page()


def calculator():
    return legacy.calculator()


def goals_tracker():
    return legacy.goals_tracker()


def links_page():
    return legacy.links_page()


def export_json():
    return legacy.export_json()


def backup_data():
    return legacy.backup_data()


def restore_data():
    return legacy.restore_data()


def payouts_page():
    return legacy.payouts_page()


def chart():
    return legacy.chart()
