from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MARKET_TEMPLATE = ROOT / "mccain_capital/templates/core/market_pulse.html"
DASHBOARD_TEMPLATE = ROOT / "mccain_capital/templates/dashboard.html"
MARKET_STYLES = ROOT / "static/css/market_pulse.css"
DASHBOARD_STYLES = ROOT / "static/css/app.css"
BASE_TEMPLATE = ROOT / "mccain_capital/templates/base.html"


def test_market_pulse_keeps_authoritative_refresh_and_chart_contracts():
    template = MARKET_TEMPLATE.read_text(encoding="utf-8")

    for contract in (
        'id="marketPulseStatusBar"',
        'id="marketPulseDecisionNarrative"',
        'id="marketPulseDataLockDiagnostics"',
        'id="marketPulseGammaCockpit"',
        'id="spxExecutionHeroChartCanvas"',
        "data-market-pulse-context-refresh",
        "data-playbook-pin-toggle",
    ):
        assert contract in template


def test_market_pulse_command_hierarchy_is_page_scoped():
    styles = MARKET_STYLES.read_text(encoding="utf-8")

    assert "body.page-market-pulse{" in styles
    assert "--mp-surface-primary:" in styles
    assert "body.page-market-pulse .marketPulseExecutionStrip" in styles
    assert "body.page-market-pulse .marketPulseStatusMetric strong" in styles
    assert "line-height:1.8;" in styles


def test_dashboard_performance_hub_leads_with_business_and_account_context():
    template = DASHBOARD_TEMPLATE.read_text(encoding="utf-8")
    styles = (ROOT / "static/css/command_surfaces.css").read_text(encoding="utf-8")

    assert 'id="dashboardCommandDeck"' in template
    assert 'id="dashboard-account-switcher"' in template
    assert "Trading Business Dashboard" in template
    assert "Daily realized P&amp;L" in template
    assert "body.page-dashboard .dashboardPerformanceHub" in styles
    assert "body.page-dashboard .dashboardPerformanceOutcome" in styles


def test_shared_tokens_are_limited_to_target_pages():
    market_styles = MARKET_STYLES.read_text(encoding="utf-8")
    dashboard_styles = DASHBOARD_STYLES.read_text(encoding="utf-8")

    assert "--mp-surface-primary:" in market_styles
    assert "--dash-surface-primary:" in dashboard_styles
    assert ":root{\n  --mp-surface-primary" not in market_styles
    assert ":root{\n  --dash-surface-primary" not in dashboard_styles


def test_shared_command_styles_load_for_both_receiving_pages():
    base = BASE_TEMPLATE.read_text(encoding="utf-8")
    market_conditional_end = base.index("{% endif %}", base.index("market_pulse.css"))
    command_link = base.index("command_surfaces.css")

    assert command_link > market_conditional_end
