"""Trades endpoint handlers (delegating to service layer)."""

from mccain_capital.services import trades as svc


def trades_page():
    return svc.trades_page()


def trades_duplicate(trade_id: int):
    return svc.trades_duplicate(trade_id)


def trades_delete(trade_id: int):
    return svc.trades_delete(trade_id)


def trades_delete_many():
    return svc.trades_delete_many()


def trades_copy_many():
    return svc.trades_copy_many()


def trades_edit(trade_id: int):
    return svc.trades_edit(trade_id)


def trades_review(trade_id: int):
    return svc.trades_review(trade_id)


def trades_clear():
    return svc.trades_clear()


def trades_paste():
    return svc.trades_paste()


def trades_new_manual():
    return svc.trades_new_manual()


def trades_paste_broker():
    return svc.trades_paste_broker()


def trades_upload_pdf():
    return svc.trades_upload_pdf()


def trades_risk_controls():
    return svc.trades_risk_controls()


def trades_open_positions():
    return svc.trades_open_positions()


def trades_rebuild_reviews():
    return svc.trades_rebuild_reviews()
