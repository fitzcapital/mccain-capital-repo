"""Trades route registrations."""

from mccain_capital.handlers import trades as h


def register(app):
    app.add_url_rule("/trades", endpoint="trades_page", view_func=h.trades_page)
    app.add_url_rule(
        "/trades/risk-controls",
        endpoint="trades_risk_controls",
        view_func=h.trades_risk_controls,
        methods=["GET", "POST"],
    )
    app.add_url_rule(
        "/trades/duplicate/<int:trade_id>",
        endpoint="trades_duplicate",
        view_func=h.trades_duplicate,
        methods=["POST"],
    )
    app.add_url_rule(
        "/trades/delete/<int:trade_id>",
        endpoint="trades_delete",
        view_func=h.trades_delete,
        methods=["POST"],
    )
    app.add_url_rule(
        "/trades/delete_many",
        endpoint="trades_delete_many",
        view_func=h.trades_delete_many,
        methods=["POST"],
    )
    app.add_url_rule(
        "/trades/copy_many",
        endpoint="trades_copy_many",
        view_func=h.trades_copy_many,
        methods=["POST"],
    )
    app.add_url_rule(
        "/trades/edit/<int:trade_id>",
        endpoint="trades_edit",
        view_func=h.trades_edit,
        methods=["GET", "POST"],
    )
    app.add_url_rule(
        "/trades/review/<int:trade_id>",
        endpoint="trades_review",
        view_func=h.trades_review,
        methods=["GET", "POST"],
    )
    app.add_url_rule(
        "/trades/clear", endpoint="trades_clear", view_func=h.trades_clear, methods=["POST"]
    )
    app.add_url_rule(
        "/trades/paste", endpoint="trades_paste", view_func=h.trades_paste, methods=["GET", "POST"]
    )
    app.add_url_rule(
        "/trades/new",
        endpoint="trades_new_manual",
        view_func=h.trades_new_manual,
        methods=["GET", "POST"],
    )
    app.add_url_rule(
        "/trades/paste/broker",
        endpoint="trades_paste_broker",
        view_func=h.trades_paste_broker,
        methods=["GET", "POST"],
    )
    app.add_url_rule(
        "/trades/upload/statement",
        endpoint="trades_upload_pdf",
        view_func=h.trades_upload_pdf,
        methods=["GET", "POST"],
    )
