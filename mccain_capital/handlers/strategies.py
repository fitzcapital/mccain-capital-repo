"""Strategy endpoint handlers (delegating to service layer)."""

from mccain_capital.services import strategies as svc


def strategies_page():
    return svc.strategies_page()


def strategies_new():
    return svc.strategies_new()


def strategies_edit(sid: int):
    return svc.strategies_edit(sid)


def strategies_delete(sid: int):
    return svc.strategies_delete(sid)


def strat_page():
    return svc.strat_page()
