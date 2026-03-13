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


def market_pulse_page():
    return svc.market_pulse_page()


def stream_market():
    return svc.stream_market()


def stream_market_ws():
    return svc.stream_market_ws()


def stream_options_panel():
    return svc.stream_options_panel()


def command_calendar_page():
    return svc.command_calendar_page()


def dashboard_recompute_balances():
    return svc.dashboard_recompute_balances()


def dashboard_milestone_update():
    return svc.dashboard_milestone_update()


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


def system_check_page():
    return svc.system_check_page()


def vanquish_blocklist_download():
    return svc.vanquish_blocklist_download()


def vanquish_lock_control():
    return svc.vanquish_lock_control()


def vanquish_lock_state():
    return svc.vanquish_lock_state()


def trading_window_config():
    return svc.trading_window_config()


def export_json():
    return svc.export_json()


def backup_data():
    return svc.backup_data()


def restore_data():
    return svc.restore_data()


def payouts_page():
    return goals_svc.payouts_page()


def market_pulse_gamma_artifact(name: str):
    return svc.market_pulse_gamma_artifact(name)
