"""Route registration hub."""

from mccain_capital.routes import (
    books,
    core,
    journal,
    life_alignment,
    self_control,
    strategies,
    trades,
)


def register_all_routes(app):
    """Register every endpoint from grouped route modules."""
    core.register(app)
    journal.register(app)
    life_alignment.register(app)
    self_control.register(app)
    trades.register(app)
    strategies.register(app)
    books.register(app)
