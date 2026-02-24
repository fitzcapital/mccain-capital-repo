"""Books endpoint handlers (delegating to service layer)."""

from mccain_capital.services import books as svc


def books_page():
    return svc.books_page()


def books_open(name: str):
    return svc.books_open(name)
