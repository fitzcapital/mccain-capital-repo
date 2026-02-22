"""Books endpoint handlers (delegating to legacy implementation)."""

from mccain_capital import legacy_app as legacy


def books_page():
    return legacy.books_page()


def books_open(name: str):
    return legacy.books_open(name)
