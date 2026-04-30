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
    app.add_url_rule("/auth/passkeys", endpoint="passkeys_page", view_func=h.passkeys_page)
    app.add_url_rule(
        "/auth/passkeys/register/options",
        endpoint="passkeys_register_options",
        view_func=h.passkeys_register_options,
        methods=["POST"],
    )
    app.add_url_rule(
        "/auth/passkeys/register/verify",
        endpoint="passkeys_register_verify",
        view_func=h.passkeys_register_verify,
        methods=["POST"],
    )
    app.add_url_rule(
        "/auth/passkeys/auth/options",
        endpoint="passkeys_auth_options",
        view_func=h.passkeys_auth_options,
        methods=["POST"],
    )
    app.add_url_rule(
        "/auth/passkeys/auth/verify",
        endpoint="passkeys_auth_verify",
        view_func=h.passkeys_auth_verify,
        methods=["POST"],
    )
    app.add_url_rule(
        "/auth/passkeys/delete",
        endpoint="passkeys_delete",
        view_func=h.passkeys_delete,
        methods=["POST"],
    )
    app.add_url_rule("/logout", endpoint="logout_page", view_func=h.logout_page)
    app.add_url_rule("/profile", endpoint="profile_page", view_func=h.profile_page)
    app.add_url_rule(
        "/profile/details",
        endpoint="profile_update_details",
        view_func=h.profile_update_details,
        methods=["POST"],
    )
    app.add_url_rule(
        "/profile/password",
        endpoint="profile_update_password",
        view_func=h.profile_update_password,
        methods=["POST"],
    )
    app.add_url_rule(
        "/profile/admin/user",
        endpoint="profile_admin_update_user",
        view_func=h.profile_admin_update_user,
        methods=["POST"],
    )
    app.add_url_rule("/healthz", endpoint="healthz", view_func=h.healthz)
    app.add_url_rule("/favicon.ico", endpoint="favicon", view_func=h.favicon)
    app.add_url_rule("/dashboard", endpoint="dashboard", view_func=h.dashboard)
    app.add_url_rule("/market-pulse", endpoint="market_pulse_page", view_func=h.market_pulse_page)
    app.add_url_rule(
        "/market-pulse/feed", endpoint="market_pulse_feed_page", view_func=h.market_pulse_feed_page
    )
    app.add_url_rule(
        "/api/market-pulse/news",
        endpoint="market_pulse_news_feed_api",
        view_func=h.market_pulse_news_feed_api,
        methods=["GET"],
    )
    app.add_url_rule(
        "/api/market-pulse/context",
        endpoint="market_pulse_context_api",
        view_func=h.market_pulse_context_api,
        methods=["GET"],
    )
    app.add_url_rule(
        "/api/market-pulse/tape",
        endpoint="market_pulse_tape_api",
        view_func=h.market_pulse_tape_api,
        methods=["GET"],
    )
    app.add_url_rule("/api/hero/bars", endpoint="hero_bars_api", view_func=h.hero_bars_api)
    app.add_url_rule("/api/hero/levels", endpoint="hero_levels_api", view_func=h.hero_levels_api)
    app.add_url_rule("/api/hero/quote", endpoint="hero_quote_api", view_func=h.hero_quote_api)
    app.add_url_rule(
        "/api/hero/stream-session",
        endpoint="hero_stream_session_api",
        view_func=h.hero_stream_session_api,
        methods=["GET"],
    )
    app.add_url_rule(
        "/market-pulse/gamma-artifact/<path:name>",
        endpoint="market_pulse_gamma_artifact",
        view_func=h.market_pulse_gamma_artifact,
    )
    app.add_url_rule("/stream/market", endpoint="stream_market", view_func=h.stream_market)
    app.add_url_rule("/ws/market", endpoint="stream_market_ws", view_func=h.stream_market_ws)
    app.add_url_rule(
        "/stream/options_panel",
        endpoint="stream_options_panel",
        view_func=h.stream_options_panel,
    )
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
    app.add_url_rule(
        "/dashboard/brief",
        endpoint="dashboard_brief_update",
        view_func=h.dashboard_brief_update,
        methods=["POST"],
    )
    app.add_url_rule(
        "/dashboard/pace",
        endpoint="dashboard_pace_update",
        view_func=h.dashboard_pace_update,
        methods=["POST"],
    )
    app.add_url_rule(
        "/api/dashboard/reflection",
        endpoint="dashboard_reflection_update",
        view_func=h.dashboard_reflection_update,
        methods=["POST"],
    )
    app.add_url_rule(
        "/api/dashboard/behavior",
        endpoint="dashboard_behavior_update",
        view_func=h.dashboard_behavior_update,
        methods=["POST"],
    )
    app.add_url_rule(
        "/dashboard/calendar-fragment",
        endpoint="dashboard_calendar_fragment",
        view_func=h.dashboard_calendar_fragment,
        methods=["GET"],
    )
    app.add_url_rule(
        "/api/dashboard/planning",
        endpoint="dashboard_planning_refresh_api",
        view_func=h.dashboard_planning_refresh_api,
        methods=["GET"],
    )
    app.add_url_rule(
        "/api/dashboard/tape",
        endpoint="dashboard_tape_refresh_api",
        view_func=h.dashboard_tape_refresh_api,
        methods=["GET"],
    )
    app.add_url_rule("/candle-opens", endpoint="candle_opens_page", view_func=h.candle_opens_page)
    app.add_url_rule("/analytics", endpoint="analytics_page", view_func=h.analytics_page)
    app.add_url_rule(
        "/api/analytics/dashboard",
        endpoint="analytics_dashboard_api",
        view_func=h.analytics_dashboard_api,
        methods=["GET"],
    )
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
    app.add_url_rule(
        "/ops/notifications-test",
        endpoint="notifications_test_page",
        view_func=h.notifications_test_page,
    )
    app.add_url_rule(
        "/ops/system-check", endpoint="system_check_page", view_func=h.system_check_page
    )
    app.add_url_rule(
        "/ops/vanquish-blocklist",
        endpoint="vanquish_blocklist_download",
        view_func=h.vanquish_blocklist_download,
    )
    app.add_url_rule(
        "/ops/vanquish-lock",
        endpoint="vanquish_lock_control",
        view_func=h.vanquish_lock_control,
        methods=["POST"],
    )
    app.add_url_rule(
        "/ops/vanquish-lock-state",
        endpoint="vanquish_lock_state",
        view_func=h.vanquish_lock_state,
        methods=["GET"],
    )
    app.add_url_rule(
        "/ops/trading-window",
        endpoint="trading_window_config",
        view_func=h.trading_window_config,
        methods=["GET", "POST"],
    )
    app.add_url_rule("/export.json", endpoint="export_json", view_func=h.export_json)
    app.add_url_rule(
        "/admin/backup", endpoint="backup_data", view_func=h.backup_data, methods=["POST"]
    )
    app.add_url_rule(
        "/admin/restore", endpoint="restore_data", view_func=h.restore_data, methods=["GET", "POST"]
    )
    app.add_url_rule(
        "/payouts", endpoint="payouts_page", view_func=h.payouts_page, methods=["GET", "POST"]
    )
