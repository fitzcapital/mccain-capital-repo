"""Books route registrations."""

from mccain_capital.handlers import books as h


def register(app):
    app.add_url_rule("/books", endpoint="books_page", view_func=h.books_page)
    app.add_url_rule("/books/open/<path:name>", endpoint="books_open", view_func=h.books_open)
