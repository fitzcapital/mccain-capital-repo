"""Journal route registrations."""

from mccain_capital.handlers import journal as h


def register(app):
    app.add_url_rule("/journal", endpoint="journal_home", view_func=h.journal_home)
    app.add_url_rule("/new", endpoint="new_entry", view_func=h.new_entry, methods=["GET", "POST"])
    app.add_url_rule("/edit/<int:entry_id>", endpoint="edit_entry", view_func=h.edit_entry, methods=["GET", "POST"])
    app.add_url_rule("/delete/<int:entry_id>", endpoint="delete_entry_route", view_func=h.delete_entry_route, methods=["POST"])
