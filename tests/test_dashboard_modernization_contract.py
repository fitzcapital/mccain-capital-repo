"""Functional-parity anchors for the Dashboard modernization."""


def test_dashboard_modernization_preserves_operating_sections(client):
    response = client.get("/dashboard", follow_redirects=True)

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    for selector_anchor in (
        'id="dashboardModeShell"',
        'aria-label="Discipline rail"',
        'id="dashboardCommandDeck"',
        'class="dashboardStageSection dashboardStageCommand',
        'id="dashboardPlanningSection"',
        'class="dashboardStageSection dashboardStageMonitor"',
        'id="dashboardHealthSurface"',
        'id="advancedDashboardWidgets"',
    ):
        assert selector_anchor in body


def test_dashboard_modernization_preserves_interaction_hooks(client):
    response = client.get("/dashboard", follow_redirects=True)

    body = response.get_data(as_text=True)
    for hook in (
        "data-discipline-state",
        "data-discipline-mode",
        "data-dashboard-readiness",
        "data-dashboard-live-sync",
        "data-trade-gate-toggle",
        "data-gamma-key",
        "data-watch-symbol",
        "data-intention-preset",
        "data-routine-check",
        "data-alignment-check",
        "data-reflection-answer",
        "data-calendar-endpoint",
    ):
        assert hook in body

    for unique_id in (
        "dashboardResetModal",
        "dashboardPlanningRefreshBtn",
        "dashboardTapeStreamStatus",
        "dashboardTapeRefreshBtn",
        "dashboardWakeLockBtn",
        "dashboardCalendarLazy",
    ):
        assert body.count(f'id="{unique_id}"') == 1


def test_dashboard_modernization_preserves_primary_destinations(client):
    response = client.get("/dashboard", follow_redirects=True)

    body = response.get_data(as_text=True)
    for destination in (
        "/market-pulse?ticker=",
        "/ops/trading-window",
        "/calendar",
        "/trades/upload/statement",
        "/ops/backups",
        "/analytics?tab=diagnostics",
        "/analytics?tab=behavior",
        "/ops/alerts",
    ):
        assert destination in body

    assert "dashboard_command_center.js" in body


def test_dashboard_exposes_decision_context_and_disclosed_reference_deck(client):
    body = client.get("/dashboard", follow_redirects=True).get_data(as_text=True)

    assert 'id="dashboardDecisionContext"' in body
    assert 'aria-label="Immediate decision context"' in body
    assert body.index('id="dashboardCommandDeck"') < body.index(
        'id="dashboardDecisionContext"'
    )
    assert body.index('id="dashboardDecisionContext"') < body.index(
        'id="dashboardTodayLayer"'
    )
    assert '<details class="dashboardReferenceFold" id="dashboardReviewLayer">' in body
    assert "Performance, broker context, consistency, and forward pace" in body
