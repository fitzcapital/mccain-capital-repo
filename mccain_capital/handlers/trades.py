"""Trades endpoint handlers (delegating to legacy implementation)."""

from mccain_capital import legacy_app as legacy


def trades_page():
    return legacy.trades_page()


def trades_duplicate(trade_id: int):
    return legacy.trades_duplicate(trade_id)


def trades_delete(trade_id: int):
    return legacy.trades_delete(trade_id)


def trades_delete_many():
    return legacy.trades_delete_many()


def trades_copy_many():
    return legacy.trades_copy_many()


def trades_edit(trade_id: int):
    return legacy.trades_edit(trade_id)


def trades_review(trade_id: int):
    return legacy.trades_review(trade_id)


def trades_clear():
    return legacy.trades_clear()


def trades_paste():
    return legacy.trades_paste()


def trades_new_manual():
    return legacy.trades_new_manual()


def trades_paste_broker():
    return legacy.trades_paste_broker()


def trades_upload_pdf():
    return legacy.trades_upload_pdf()


def trades_risk_controls():
    return legacy.trades_risk_controls()
