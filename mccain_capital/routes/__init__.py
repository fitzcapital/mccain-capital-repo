"""Route registration hub."""

from mccain_capital.routes import books, core, journal, strategies, trades


def register_all_routes(app):
    """Register every endpoint from grouped route modules."""
    core.register(app)
    journal.register(app)
    trades.register(app)
    strategies.register(app)
    books.register(app)
