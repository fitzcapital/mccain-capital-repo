"""Strategies route registrations."""

from mccain_capital.handlers import strategies as h


def register(app):
    app.add_url_rule("/strategies", endpoint="strategies_page", view_func=h.strategies_page)
    app.add_url_rule("/strategies/new", endpoint="strategies_new", view_func=h.strategies_new, methods=["GET", "POST"])
    app.add_url_rule("/strategies/edit/<int:sid>", endpoint="strategies_edit", view_func=h.strategies_edit, methods=["GET", "POST"])
    app.add_url_rule("/strategies/delete/<int:sid>", endpoint="strategies_delete", view_func=h.strategies_delete, methods=["POST"])
    app.add_url_rule("/strat", endpoint="strat_page", view_func=h.strat_page, methods=["GET"])
