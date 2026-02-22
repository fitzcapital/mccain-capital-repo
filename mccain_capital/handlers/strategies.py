"""Strategy endpoint handlers (delegating to legacy implementation)."""

from mccain_capital import legacy_app as legacy


def strategies_page():
    return legacy.strategies_page()


def strategies_new():
    return legacy.strategies_new()


def strategies_edit(sid: int):
    return legacy.strategies_edit(sid)


def strategies_delete(sid: int):
    return legacy.strategies_delete(sid)


def strat_page():
    return legacy.strat_page()
