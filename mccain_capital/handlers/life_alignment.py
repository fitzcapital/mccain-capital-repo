"""Life Alignment endpoint handlers."""

from mccain_capital.services import life_alignment as svc


def life_alignment_page():
    return svc.life_alignment_page()


def api_today():
    return svc.api_today()


def api_save_today():
    return svc.api_save_today()


def api_history():
    return svc.api_history()


def api_analytics():
    return svc.api_analytics()
