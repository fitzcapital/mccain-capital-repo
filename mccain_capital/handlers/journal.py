"""Journal endpoint handlers (delegating to legacy implementation)."""

from mccain_capital import legacy_app as legacy


def journal_home():
    return legacy.journal_home()


def new_entry():
    return legacy.new_entry()


def edit_entry(entry_id: int):
    return legacy.edit_entry(entry_id)


def delete_entry_route(entry_id: int):
    return legacy.delete_entry_route(entry_id)
