"""Core endpoint handlers (delegating to service layer)."""

from mccain_capital.services import auth_passkeys as passkeys_svc
from mccain_capital.services import core as svc
from mccain_capital.services import goals as goals_svc


def home():
    return svc.home()


def setup_page():
    return svc.setup_page()


def login_page():
    return svc.login_page()


def passkeys_page():
    return passkeys_svc.passkeys_page()


def passkeys_register_options():
    return passkeys_svc.passkeys_register_options()


def passkeys_register_verify():
    return passkeys_svc.passkeys_register_verify()


def passkeys_auth_options():
    return passkeys_svc.passkeys_auth_options()


def passkeys_auth_verify():
    return passkeys_svc.passkeys_auth_verify()


def passkeys_delete():
    return passkeys_svc.passkeys_delete()


def logout_page():
    return svc.logout_page()


def healthz():
    return svc.healthz()


def favicon():
    return svc.favicon()


def dashboard():
    return svc.dashboard()


def dashboard_calendar_fragment():
    return svc.dashboard_calendar_fragment()


def dashboard_planning_refresh_api():
    return svc.dashboard_planning_refresh_api()


def dashboard_tape_refresh_api():
    return svc.dashboard_tape_refresh_api()


def market_pulse_page():
    return svc.market_pulse_page()


def market_pulse_feed_page():
    return svc.market_pulse_feed_page()


def market_pulse_news_feed_api():
    return svc.market_pulse_news_feed_api()


def hero_bars_api():
    return svc.hero_bars_api()


def hero_levels_api():
    return svc.hero_levels_api()


def hero_stream_session_api():
    return svc.hero_stream_session_api()


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


def dashboard_brief_update():
    return svc.dashboard_brief_update()


def dashboard_pace_update():
    return svc.dashboard_pace_update()


def candle_opens_page():
    return svc.candle_opens_page()


def analytics_page():
    return svc.analytics_page()


def analytics_dashboard_api():
    return svc.analytics_dashboard_api()


def session_replay_page():
    return svc.session_replay_page()


def calculator():
    return svc.calculator()


def goals_tracker():
    return goals_svc.goals_tracker()


def links_page():
    return svc.links_page()


def notifications_test_page():
    return svc.notifications_test_page()


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
