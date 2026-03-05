"""Core app route registrations."""

from mccain_capital.handlers import core as h


def register(app):
    app.add_url_rule("/", endpoint="home", view_func=h.home)
    app.add_url_rule(
        "/setup", endpoint="setup_page", view_func=h.setup_page, methods=["GET", "POST"]
    )
    app.add_url_rule(
        "/login", endpoint="login_page", view_func=h.login_page, methods=["GET", "POST"]
    )
    app.add_url_rule("/logout", endpoint="logout_page", view_func=h.logout_page)
    app.add_url_rule("/healthz", endpoint="healthz", view_func=h.healthz)
    app.add_url_rule("/favicon.ico", endpoint="favicon", view_func=h.favicon)
    app.add_url_rule("/dashboard", endpoint="dashboard", view_func=h.dashboard)
    app.add_url_rule("/market-pulse", endpoint="market_pulse_page", view_func=h.market_pulse_page)
    app.add_url_rule("/stream/market", endpoint="stream_market", view_func=h.stream_market)
    app.add_url_rule(
        "/calendar", endpoint="command_calendar_page", view_func=h.command_calendar_page
    )
    app.add_url_rule(
        "/dashboard/recompute-balances",
        endpoint="dashboard_recompute_balances",
        view_func=h.dashboard_recompute_balances,
        methods=["POST"],
    )
    app.add_url_rule(
        "/dashboard/milestone",
        endpoint="dashboard_milestone_update",
        view_func=h.dashboard_milestone_update,
        methods=["POST"],
    )
    app.add_url_rule("/candle-opens", endpoint="candle_opens_page", view_func=h.candle_opens_page)
    app.add_url_rule("/analytics", endpoint="analytics_page", view_func=h.analytics_page)
    app.add_url_rule(
        "/analytics/replay", endpoint="session_replay_page", view_func=h.session_replay_page
    )
    app.add_url_rule(
        "/calculator", endpoint="calculator", view_func=h.calculator, methods=["GET", "POST"]
    )
    app.add_url_rule(
        "/goals", endpoint="goals_tracker", view_func=h.goals_tracker, methods=["GET", "POST"]
    )
    app.add_url_rule("/links", endpoint="links_page", view_func=h.links_page)
    app.add_url_rule("/export.json", endpoint="export_json", view_func=h.export_json)
    app.add_url_rule("/admin/backup", endpoint="backup_data", view_func=h.backup_data)
    app.add_url_rule(
        "/admin/restore", endpoint="restore_data", view_func=h.restore_data, methods=["GET", "POST"]
    )
    app.add_url_rule(
        "/payouts", endpoint="payouts_page", view_func=h.payouts_page, methods=["GET", "POST"]
    )
