"""Self-control endpoint handlers."""

from mccain_capital.services import self_control as svc


def self_control_page():
    return svc.self_control_page()


def self_control_state_api():
    return svc.self_control_state_api()


def self_control_session_start():
    return svc.self_control_session_start()


def self_control_session_cancel():
    return svc.self_control_session_cancel()


def self_control_session_acknowledge_unlock():
    return svc.self_control_session_acknowledge_unlock()


def self_control_site_add():
    return svc.self_control_site_add()


def self_control_site_toggle(site_id: int):
    return svc.self_control_site_toggle(site_id)


def self_control_site_delete(site_id: int):
    return svc.self_control_site_delete(site_id)


def self_control_preset_start(slug: str):
    return svc.self_control_preset_start(slug)


def self_control_rule_toggle(slug: str):
    return svc.self_control_rule_toggle(slug)


def self_control_rule_trigger(slug: str):
    return svc.self_control_rule_trigger(slug)
