"""Self-control route registrations."""

from mccain_capital.handlers import self_control as h


def register(app):
    app.add_url_rule("/self-control", endpoint="self_control_page", view_func=h.self_control_page)
    app.add_url_rule(
        "/api/self-control/state",
        endpoint="self_control_state_api",
        view_func=h.self_control_state_api,
    )
    app.add_url_rule(
        "/self-control/session/start",
        endpoint="self_control_session_start",
        view_func=h.self_control_session_start,
        methods=["POST"],
    )
    app.add_url_rule(
        "/self-control/session/cancel",
        endpoint="self_control_session_cancel",
        view_func=h.self_control_session_cancel,
        methods=["POST"],
    )
    app.add_url_rule(
        "/self-control/session/acknowledge-unlock",
        endpoint="self_control_session_acknowledge_unlock",
        view_func=h.self_control_session_acknowledge_unlock,
        methods=["POST"],
    )
    app.add_url_rule(
        "/self-control/sites/add",
        endpoint="self_control_site_add",
        view_func=h.self_control_site_add,
        methods=["POST"],
    )
    app.add_url_rule(
        "/self-control/sites/<int:site_id>/toggle",
        endpoint="self_control_site_toggle",
        view_func=h.self_control_site_toggle,
        methods=["POST"],
    )
    app.add_url_rule(
        "/self-control/sites/<int:site_id>/delete",
        endpoint="self_control_site_delete",
        view_func=h.self_control_site_delete,
        methods=["POST"],
    )
    app.add_url_rule(
        "/self-control/presets/<slug>/start",
        endpoint="self_control_preset_start",
        view_func=h.self_control_preset_start,
        methods=["POST"],
    )
    app.add_url_rule(
        "/self-control/rules/<slug>/toggle",
        endpoint="self_control_rule_toggle",
        view_func=h.self_control_rule_toggle,
        methods=["POST"],
    )
    app.add_url_rule(
        "/self-control/rules/<slug>/trigger",
        endpoint="self_control_rule_trigger",
        view_func=h.self_control_rule_trigger,
        methods=["POST"],
    )
