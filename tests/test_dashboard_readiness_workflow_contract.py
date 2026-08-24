"""Contracts for the simplified Dashboard readiness workflow."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _dashboard(client) -> str:
    response = client.get("/dashboard", follow_redirects=True)
    assert response.status_code == 200
    return response.get_data(as_text=True)


def test_dashboard_uses_canonical_workflow_labels(client):
    body = _dashboard(client)
    nav = body[body.index('class="dashboardWorkflowNav"') : body.index('id="dashboardOperationStatus"')]

    for label in ("Performance", "Quality", "Trends", "Review"):
        assert f">{label}</a>" in nav
    assert "Decision Tools" not in nav
    assert 'class="dashboardStepLabel">Plan</div>' not in body
    assert 'class="dashboardStepLabel">Monitor</div>' not in body


def test_dashboard_consolidates_session_readiness(client):
    body = _dashboard(client)
    prepare = body[
        body.index('id="dashboardPrepareSection"') : body.index('id="dashboardPlanningSection"')
    ]

    assert prepare.count("Session Readiness") == 1
    assert 'data-dashboard-readiness' in prepare
    assert 'data-readiness-blockers' in prepare
    assert "Priority" in prepare
    assert '<details class="dashboardPreparationGuide"' in prepare
    assert "Preparation rules" in prepare
    assert "Checklist &amp; import" in prepare
    assert '<details class="dashboardCommandFold">' in prepare
    assert 'data-dashboard-live-sync' in prepare


def test_dashboard_ops_band_is_attention_aware(client):
    body = _dashboard(client)
    assert 'class="dashboardOperationalBand card is-quiet"' in body
    assert 'data-ops-attention="false"' in body
    assert "Open this only when sync status" not in body

    base_template = (ROOT / "mccain_capital/templates/base.html").read_text(encoding="utf-8")
    for attention_state in ("failed", "error", "blocked", "stale", "unavailable"):
        assert attention_state in base_template
    assert 'data-ops-attention="{{ \'true\' if ops_attention else \'false\' }}"' in base_template


def test_readiness_hooks_remain_available_for_live_updates(client):
    body = _dashboard(client)
    script = (ROOT / "static/js/dashboard_command_center.js").read_text(encoding="utf-8")

    for hook in (
        "data-readiness-pct-label",
        "data-readiness-meter",
        "data-readiness-count",
        "data-readiness-state",
        "data-readiness-detail",
        "data-readiness-command-summary",
        "data-readiness-item",
    ):
        assert hook in body
    assert 'document.addEventListener("dashboard:import-readiness", syncReadiness)' in script
