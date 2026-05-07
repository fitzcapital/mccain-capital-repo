"""Forward Pace route registrations."""

from mccain_capital.handlers import forward_pace as h


def register(app):
    app.add_url_rule("/forward-pace", endpoint="forward_pace_page", view_func=h.forward_pace_page)
    app.add_url_rule(
        "/api/forward-pace/projection",
        endpoint="forward_pace_projection",
        view_func=h.api_projection,
        methods=["POST"],
    )
    app.add_url_rule(
        "/forward-pace/pdf",
        endpoint="forward_pace_pdf",
        view_func=h.download_pdf,
        methods=["POST"],
    )
