"""Life Alignment route registrations."""

from mccain_capital.handlers import life_alignment as h


def register(app):
    app.add_url_rule(
        "/life-alignment", endpoint="life_alignment_page", view_func=h.life_alignment_page
    )
    app.add_url_rule(
        "/api/life-alignment/today",
        endpoint="life_alignment_today",
        view_func=h.api_today,
        methods=["GET"],
    )
    app.add_url_rule(
        "/api/life-alignment/today",
        endpoint="life_alignment_save_today",
        view_func=h.api_save_today,
        methods=["POST"],
    )
    app.add_url_rule(
        "/api/life-alignment/history",
        endpoint="life_alignment_history",
        view_func=h.api_history,
    )
    app.add_url_rule(
        "/api/life-alignment/analytics",
        endpoint="life_alignment_analytics",
        view_func=h.api_analytics,
    )
