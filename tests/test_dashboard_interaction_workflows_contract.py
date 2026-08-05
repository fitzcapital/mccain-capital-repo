"""Regression contracts for the Dashboard interaction workflow upgrade."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _dashboard(client) -> str:
    response = client.get("/dashboard", follow_redirects=True)
    assert response.status_code == 200
    return response.get_data(as_text=True)


def test_dashboard_loads_shared_interaction_assets(client):
    body = _dashboard(client)

    assert "dashboard_interactions.css" in body
    assert "dashboard_interactions.js" in body
    assert 'id="dashboardOperationStatus"' in body
    assert 'class="dashboardWorkflowNav"' in body


def test_dashboard_surfaces_use_accessible_shared_contract(client):
    body = _dashboard(client)

    for surface_id in ("dashboardResetModal", "dashboardCommandPalette"):
        assert body.count(f'id="{surface_id}"') == 1
    assert body.count('data-dashboard-surface') >= 2
    assert 'data-dashboard-surface-open="dashboardResetModal"' in body
    assert 'data-dashboard-surface-open="dashboardCommandPalette"' in body
    assert 'aria-labelledby="dashboardResetModalTitle"' in body
    assert 'aria-labelledby="dashboardCommandPaletteTitle"' in body


def test_pressure_check_is_a_guided_guarded_workflow(client):
    body = _dashboard(client)
    script = (ROOT / "static/js/dashboard_interactions.js").read_text(encoding="utf-8")
    command_script = (ROOT / "static/js/dashboard_command_center.js").read_text(encoding="utf-8")

    for anchor in (
        'id="dashboardResetTriggerCategory"',
        'id="dashboardResetNote"',
        'id="dashboardResetDuration"',
        'id="dashboardResetTimerStart"',
        'data-reset-check="setup"',
        'data-reset-check="confirmation"',
        'data-reset-check="rules"',
        'data-reset-check="stop"',
        'data-reset-check="risk"',
    ):
        assert anchor in body
    assert "validateProceed" in script
    assert "syncReflection" in script
    assert "window.dashboardPressureCheck.validateProceed()" in command_script
    assert 'data-reset-action="stand-down">Done for the Day' in body


def test_calendar_preview_is_a_date_scoped_session_inspector(client):
    body = _dashboard(client)
    script = (ROOT / "static/js/dashboard_command_center.js").read_text(encoding="utf-8")

    for anchor in (
        'id="calendarPreview"',
        "Session inspector",
        'id="calendarPreviewJournal"',
        'id="calendarPreviewDebrief"',
        'id="calendarPreviewReconcile"',
        'id="calendarPreviewOpen"',
    ):
        assert anchor in body
    assert "`/trades?d=${encodeURIComponent(d.iso)}`" in script
    assert "`/journal?d=${encodedDate}`" in script
    assert "entry_type=trade_debrief" in script
    assert "ws=reconcile&d=${encodedDate}" in script


def test_broker_metrics_drawer_keeps_manual_and_diagnostic_actions(client):
    body = _dashboard(client)

    # The drawer is account-specific and is rendered whenever the test fixture has an account.
    if 'id="dashboardBrokerDrawer"' not in body:
        assert "Select an account" in body or "Add Account" in body
        return
    assert 'data-dashboard-surface-open="dashboardBrokerDrawer"' in body
    assert 'data-dashboard-broker-form="manual"' in body
    assert 'data-dashboard-broker-form="refresh"' in body
    assert 'data-dashboard-broker-form="seed"' in body
    assert "Manual values remain primary until a refresh succeeds" in body
    assert "Failed refreshes preserve the manual values" in body


def test_command_palette_reuses_dashboard_actions_and_ignores_editable_shortcuts():
    script = (ROOT / "static/js/dashboard_interactions.js").read_text(encoding="utf-8")

    assert 'event.key.toLowerCase() !== "k"' in script
    assert "isEditable(event.target)" in script
    assert 'document.getElementById("dashboardResetTrigger")?.click()' in script
    assert 'document.getElementById("dashboardPlanningRefreshBtn")?.click()' in script
    assert 'document.getElementById("dashboardTapeRefreshBtn")?.click()' in script
    assert 'role", "option"' in script
    assert 'event.key === "ArrowDown"' in script
    assert 'event.key === "Enter"' in script


def test_surface_controller_covers_focus_lifecycle_and_lazy_rebinding():
    script = (ROOT / "static/js/dashboard_interactions.js").read_text(encoding="utf-8")

    assert "focusableWithin" in script
    assert "activeInvoker" in script
    assert "invoker.focus()" in script
    assert 'event.key !== "Tab"' in script
    assert "MutationObserver" in script
    assert 'dataset.surfaceTriggerBound === "1"' in script
    assert 'classList.add("dashboardSurfaceOpen", "modalOpen")' in script


def test_operation_feedback_preserves_confirmed_content_on_refresh_failure():
    interactions = (ROOT / "static/js/dashboard_interactions.js").read_text(encoding="utf-8")
    command_center = (ROOT / "static/js/dashboard_command_center.js").read_text(
        encoding="utf-8"
    )

    assert 'const state = ["success", "stale", "error"]' in interactions
    assert "Manual values were preserved" in interactions
    assert "Confirmed values remain visible" in command_center
    assert "Confirmed context remains visible" in command_center
    assert 'setAttribute("aria-busy", "true")' in interactions


def test_dashboard_interactions_are_responsive_and_reduced_motion_aware():
    styles = (ROOT / "static/css/dashboard_interactions.css").read_text(encoding="utf-8")

    assert "@media (max-width: 720px)" in styles
    assert "height: min(92dvh, 900px)" in styles
    assert "env(safe-area-inset-bottom)" in styles
    assert "@media (prefers-reduced-motion: reduce)" in styles
    assert ".dashboardSurface--drawer .dashboardSurfaceFrame" in styles
