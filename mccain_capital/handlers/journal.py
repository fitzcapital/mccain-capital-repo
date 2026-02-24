"""Journal endpoint handlers (delegating to service layer)."""

from mccain_capital.services import journal as svc


def journal_home():
    return svc.journal_home()


def new_entry():
    return svc.new_entry()


def edit_entry(entry_id: int):
    return svc.edit_entry(entry_id)


def delete_entry_route(entry_id: int):
    return svc.delete_entry_route(entry_id)
