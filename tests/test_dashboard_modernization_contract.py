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


def test_dashboard_exposes_performance_hub_and_disclosed_reference_deck(client):
    body = client.get("/dashboard", follow_redirects=True).get_data(as_text=True)

    command = body[body.index('id="dashboardCommandDeck"') : body.index('id="dashboardTodayLayer"')]
    assert "Trading Business Dashboard" in command
    assert "Net earned" in command
    assert "Drawdown cushion" in command
    assert "Open Market Pulse" in command
    assert 'id="dashboardDecisionContext"' not in body
    assert body.index('id="dashboardCommandDeck"') < body.index('id="dashboardTodayLayer"')
    assert body.count("Execution Plan") == 1
    assert '<details id="daily-brief-card"' in body
    assert "Plan context" in body
    planning = body[
        body.index('id="dashboardPlanningSection"') : body.index(
            'class="dashboardStageColumn dashboardStageSide"'
        )
    ]
    assert planning.count("Open Market Pulse") == 1
    assert planning.count('href="/ops/trading-window"') == 1
    assert '<details class="card dashboardFoundationCard dashboardSupportingDisclosure"' in body
    assert '<details class="dashboardReferenceFold" id="dashboardReviewLayer">' in body
    assert "Performance, broker context, consistency, and forward pace" in body


def test_dashboard_market_tape_is_an_spx_session_snapshot(client):
    body = client.get("/dashboard", follow_redirects=True).get_data(as_text=True)

    assert 'id="dashboardSpxSnapshot"' in body
    assert "SPX Session Snapshot" in body
    assert 'id="dashboardSpxSpot"' in body
    assert 'id="dashboardSpxRangePosition"' in body
    assert 'id="dashboardSpxLow"' in body
    assert 'id="dashboardSpxHigh"' in body
    assert 'id="dashboardSpxMiniChart"' in body
    assert 'id="dashboardSpxMiniChartCanvas"' in body
    assert "Last hour · 5-minute candles" in body
    assert 'id="dashboardSpxFromOpen"' in body
    assert 'id="dashboardSpxRangeValue"' in body
    assert 'id="dashboardSpxRangeStat"' in body
    assert 'id="dashboardSpxCharacter"' in body
    assert 'id="dashboardMarketCanvasRead"' in body
    assert "The Dashboard describes location only" in body
    assert "Context only" in body
    assert 'id="dashboardTapeMatrix"' not in body
    assert "dashboardTapeChartLane--primary" not in body
    assert "dashboardTapeChartLane--confirm" not in body
    assert "setup marker" not in body.lower()
    for symbol in ("SPY", "QQQ", "IWM", "VIX"):
        assert f'data-comparison-symbol="{symbol}"' in body


def test_dashboard_wide_desktop_uses_a_bounded_reading_canvas():
    css = open("static/css/command_surfaces.css", encoding="utf-8").read()

    assert "Wide-desktop Dashboard density" in css
    assert "--primary-reading-canvas:1180px" in css
    assert "width:min(calc(100% - 40px),var(--primary-reading-canvas)) !important" in css
    assert "max-width:var(--primary-reading-canvas) !important" in css
    assert "body.page-dashboard .dashboardModeShell" in css

    base = open("mccain_capital/templates/base.html", encoding="utf-8").read()
    assert "rev=market-pulse-canvas-20260823d" in base
    assert "body.page-dashboard .dashboardStageSide" in css
    assert "grid-column:1 / -1 !important" in css


def test_dashboard_workflow_navigation_can_be_pinned_and_remembers_the_choice():
    template = open("mccain_capital/templates/dashboard.html", encoding="utf-8").read()
    script = open("static/js/dashboard_interactions.js", encoding="utf-8").read()
    css = open("static/css/dashboard_interactions.css", encoding="utf-8").read()
    base = open("mccain_capital/templates/base.html", encoding="utf-8").read()

    assert "data-dashboard-workflow-pin" in template
    assert 'aria-pressed="true"' in template
    assert "mc_dashboard_workflow_pinned" in script
    assert "bindWorkflowPin" in script
    assert ".dashboardWorkflowNav.is-pinned" in css
    assert "rev=dashboard-stage-composition-20260823b" in base
    assert "dashboardStage dashboardStage--singleColumn" in template
    assert 'class="dashboardStageColumn dashboardStageSide"' in template
    assert "data-dashboard-full-canvas" in template
    assert "[data-dashboard-full-canvas]" in css
