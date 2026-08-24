"""Behavioral contracts for adaptive application shell widths."""

from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("route", "mode", "page_class"),
    (
        ("/dashboard", "wide", "page-dashboard"),
        ("/executive", "wide", "page-executive"),
        ("/market-pulse", "wide", "page-market-pulse"),
        ("/candle-opens", "wide", "page-candle-opens"),
        ("/analytics", "wide", "page-analytics"),
        ("/journal", "standard", "page-journal"),
        ("/profile", "standard", "page-profile"),
        ("/calculator", "standard", "page-calc"),
    ),
)
def test_routes_render_with_explicit_shell_mode(client, route: str, mode: str, page_class: str):
    response = client.get(route, follow_redirects=True)
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert page_class in body
    assert f"shellMode-{mode}" in body
    assert f'data-shell-mode="{mode}"' in body
    assert "adaptive_shell.css" in body


def test_adaptive_shell_defines_one_authoritative_width_contract():
    styles = (ROOT / "static/css/adaptive_shell.css").read_text(encoding="utf-8")

    for token in (
        "--shell-chrome-max:1440px",
        "--shell-standard-max:1280px",
        "--shell-wide-max:1440px",
        "--shell-dense-max:1600px",
    ):
        assert token in styles

    assert "body.shellMode .topbar" in styles
    assert "body.shellMode #pageShell" in styles
    assert "max-width:72ch" in styles
    assert "@media (max-width:900px)" in styles


def test_primary_desktop_pages_use_centered_page_appropriate_reading_canvases():
    styles = (ROOT / "static/css/command_surfaces.css").read_text(encoding="utf-8")

    for selector in (
        "body.page-dashboard .wrap",
        "body.page-market-pulse .wrap",
        "body.page-executive .wrap",
    ):
        assert selector in styles
    assert "--primary-reading-canvas:1180px" in styles
    assert "body.page-market-pulse{" in styles
    assert "--primary-reading-canvas:1360px" in styles
    assert "width:min(calc(100% - 40px),var(--primary-reading-canvas)) !important" in styles
    assert "max-width:var(--primary-reading-canvas) !important" in styles


def test_executive_is_a_first_class_primary_navigation_item(client):
    body = client.get("/dashboard", follow_redirects=True).get_data(as_text=True)
    primary_nav = body.split('<div class="navGroup">', 1)[1].split("</div>", 1)[0]

    assert 'href="/executive"' in primary_nav
    assert primary_nav.index('href="/executive"') < primary_nav.index('href="/dashboard"')


def test_budget_redirect_and_dense_mode_contract_are_preserved(client):
    response = client.get("/budget", follow_redirects=False)
    template = (ROOT / "mccain_capital/templates/base.html").read_text(encoding="utf-8")

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/executive")
    assert "'dense' if active == 'budget'" in template


@pytest.mark.parametrize(
    ("route", "required_hook"),
    (
        ("/dashboard", 'href="/market-pulse"'),
        ("/market-pulse", 'id="marketPulseGammaLadderCard"'),
        ("/journal", 'href="/journal/new"'),
    ),
)
def test_shell_modes_preserve_representative_functionality(client, route: str, required_hook: str):
    body = client.get(route, follow_redirects=True).get_data(as_text=True)

    assert required_hook in body
    assert 'id="notifBellButton"' in body
    assert 'id="pageShell"' in body
