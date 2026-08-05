"""Functional-parity anchors for shared internal-page modernization."""

import pytest


INTERNAL_MENU_DESTINATIONS = (
    "/executive",
    "/dashboard",
    "/the-plan",
    "/market-pulse",
    "/candle-opens",
    "/strat",
    "/playbook",
    "/strategies",
    "/trades",
    "/journal",
    "/journal/life",
    "/life-alignment",
    "/forward-pace",
    "/self-control",
    "/analytics",
    "/calendar",
    "/calculator",
    "/trades/upload/statement",
    "/ops/alerts",
    "/profile",
    "/setup",
    "/ops/backups",
    "/admin/restore",
    "/auth/passkeys",
    "/books",
    "/payouts",
    "/goals",
)


def test_shared_navigation_preserves_internal_destinations(client):
    response = client.get("/executive", follow_redirects=True)
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    for destination in INTERNAL_MENU_DESTINATIONS:
        assert f'href="{destination}"' in body

    for external in (
        "https://www.tradingview.com/chart/",
        "https://x.com/",
        "https://trade.vanquishtrader.com/",
    ):
        assert f'href="{external}"' in body
    assert 'target="_blank" rel="noopener"' in body


@pytest.mark.parametrize(
    ("route", "page_class", "anchor"),
    (
        ("/executive", "page-executive", "executiveMonthWorkspaceHead"),
        ("/the-plan", "page-the-plan", "thePlan"),
        ("/market-pulse", "page-market-pulse", "marketPulse"),
        ("/candle-opens", "page-candle-opens", "candleHero"),
        ("/trades", "page-trades", "Execution Console"),
        ("/journal", "page-journal", "Journal"),
        ("/life-alignment", "page-life-alignment", "lifeAlignmentApp"),
        ("/analytics", "page-analytics", "analyticsDashboardApp"),
        ("/calendar", "page-calendar", "commandCalendarHero"),
        ("/calculator", "page-calc", "plannerHero"),
        ("/forward-pace", "page-forward-pace", "forwardPaceApp"),
        ("/payouts", "page-payouts", "Payout Planner"),
        ("/goals", "page-goals", "Goals Workspace"),
        ("/ops/alerts", "page-ops", "Ops Alerts"),
        ("/ops/backups", "page-ops", "opsBackupsHero"),
        ("/profile", "page-profile", "profilePage"),
        ("/self-control", "page-self-control", "Self-Control"),
        ("/books", "page-books", "Books Desk"),
    ),
)
def test_internal_page_families_render_with_modernization_scope(
    client, route: str, page_class: str, anchor: str
):
    response = client.get(route, follow_redirects=True)
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert page_class in body
    assert "appModernizedPage" in body
    assert "app_modern_pages.css" in body
    assert anchor in body


def test_dashboard_remains_outside_shared_modernization_scope(client):
    response = client.get("/dashboard", follow_redirects=True)
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "page-dashboard" in body
    assert "appModernizedPage" not in body
    assert "app_modern_pages.css" not in body


def test_high_risk_forms_preserve_methods_actions_and_csrf(client):
    backup = client.get("/ops/backups", follow_redirects=True).get_data(as_text=True)
    upload = client.get("/trades/upload/statement", follow_redirects=True).get_data(as_text=True)
    profile = client.get("/profile", follow_redirects=True).get_data(as_text=True)
    setup = client.get("/setup", follow_redirects=True).get_data(as_text=True)

    assert 'method="post"' in backup
    assert "/admin/restore" in backup
    assert 'enctype="multipart/form-data"' in upload
    assert 'action="/profile/details"' in profile
    assert 'type="password"' in setup
    for body in (backup, upload, profile, setup):
        assert 'name="csrf_token"' in body
