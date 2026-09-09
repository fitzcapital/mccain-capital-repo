"""Regression contract for the calendar-first Candle Opens workflow."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STYLES = ROOT / "static" / "css" / "app.css"
TEMPLATE = ROOT / "mccain_capital" / "templates" / "core" / "candle_opens.html"
BASE_TEMPLATE = ROOT / "mccain_capital" / "templates" / "base.html"


def test_candle_opens_renders_calendar_first_workflow_contract(client):
    response = client.get("/candle-opens?y=2026&m=2", follow_redirects=True)
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "data-candle-workflow" in body
    for region in (
        "command",
        "today",
        "calendar",
        "catalysts",
        "timing-reference",
        "macro-agenda",
    ):
        assert f'data-candle-workflow-region="{region}"' in body

    assert 'aria-label="Candle Opens month navigation"' in body
    assert 'aria-label="Previous month"' in body
    assert 'href="/candle-opens?y=2026&m=1"' in body
    assert 'href="/candle-opens"' in body
    assert 'aria-label="Next month"' in body
    assert 'href="/candle-opens?y=2026&m=3"' in body
    assert "data-candle-select-today" in body
    assert "data-candle-catalyst-card" in body
    assert "Timing Reference" in body


def test_candle_opens_preserves_calendar_and_day_profile_hooks(client):
    response = client.get("/candle-opens?y=2026&m=2", follow_redirects=True)
    body = response.get_data(as_text=True)

    for hook in (
        "data-candle-day",
        "data-candle-detail-date",
        "data-candle-detail-status",
        "data-candle-detail-importance",
        "data-candle-detail-resets",
        "data-candle-detail-macro-count",
        "data-candle-detail-tags",
        "data-candle-detail-day",
        "data-candle-detail-week",
        "data-candle-detail-month",
        "data-candle-detail-macro",
        'class="mobileCandleWeek"',
        'data-mobile-default="closed"',
    ):
        assert hook in body

    assert 'role="button" tabindex="0"' in body
    template = TEMPLATE.read_text(encoding="utf-8")
    assert 'href="#news-day-{{ cell.iso }}"' in template
    assert 'id="news-day-{{ news_day.iso }}"' in template


def test_candle_opens_workflow_styles_define_hierarchy_and_accessibility():
    styles = STYLES.read_text(encoding="utf-8")

    assert "/* Candle Opens calendar-first workflow modernization. */" in styles
    assert "/* Match the density of the main command pages without sacrificing calendar width. */" in styles
    assert "body.page-candle-opens .candleCell{ min-height:92px" in styles
    assert "min-height:56px; padding:7px 10px" in styles
    assert '[data-candle-workflow-region="calendar"]{ order:3; }' in styles
    assert '[data-candle-workflow-region="catalysts"]{ order:4; }' in styles
    assert '[data-candle-workflow-region="macro-agenda"]{ order:6; }' in styles
    assert "body.page-candle-opens .candleDayDetailPanel{" in styles
    assert "@media (max-width:720px)" in styles
    assert "@media (prefers-reduced-motion:reduce)" in styles
    assert "body.page-candle-opens *::after" in styles


def test_candle_opens_uses_standard_shell_width():
    template = BASE_TEMPLATE.read_text(encoding="utf-8")
    wide_pages = template.split("{% set wide_shell_pages =", 1)[1].split("%}", 1)[0]
    assert "'candle-opens'" not in wide_pages
    assert "--shell-standard-max:1280px" in (ROOT / "static/css/adaptive_shell.css").read_text(
        encoding="utf-8"
    )


def test_candle_opens_is_primary_desktop_navigation_without_tools_duplication():
    template = BASE_TEMPLATE.read_text(encoding="utf-8")
    primary_nav = template.split('<div class="navGroup">', 1)[1].split("</div>", 1)[0]
    tools_menu = template.split('<div id="moreMenu"', 1)[1].split(
        '<div class="menuTitle">Review</div>', 1
    )[0]

    assert primary_nav.index("Market Pulse") < primary_nav.index("Candle Opens")
    assert primary_nav.index("Candle Opens") < primary_nav.index("Trades")
    assert 'href="/candle-opens"' not in tools_menu
    assert template.count('href="/candle-opens"') >= 3
