"""Journal endpoint handlers (delegating to service layer)."""

from mccain_capital.services import journal as svc


def journal_home():
    return svc.journal_home()


def life_journal_home():
    return svc.life_journal_home()


def edit_life_entry(entry_id: int):
    return svc.edit_life_entry(entry_id)


def journal_weekly_review():
    return svc.journal_weekly_review()


def journal_trades_for_date():
    return svc.journal_trades_for_date()


def journal_capture_asset(name: str):
    return svc.journal_capture_asset(name)


def new_entry():
    return svc.new_entry()


def edit_entry(entry_id: int):
    return svc.edit_entry(entry_id)


def delete_entry_route(entry_id: int):
    return svc.delete_entry_route(entry_id)


def delete_life_entry_route(entry_id: int):
    return svc.delete_life_entry_route(entry_id)
