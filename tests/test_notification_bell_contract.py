"""Regression anchors for the shared tweet notification bell."""

from pathlib import Path

from mccain_capital.services import core as core_svc


ROOT = Path(__file__).resolve().parents[1]


def test_notification_bell_recovers_when_unread_count_has_no_cached_tweets(client):
    body = client.get("/candle-opens", follow_redirects=True).get_data(as_text=True)

    assert 'id="notifBellButton"' in body
    assert 'id="notifDropdown"' in body
    assert 'popovertarget="notifDropdown"' in body
    assert 'popover="auto"' in body
    assert 'dropdownCount.textContent = "Loading tweets"' in body
    assert 'loading.textContent = "Loading latest tweets..."' in body
    assert "void seedFromFeed(false, false);" in body


def test_notification_bell_does_not_mark_an_empty_cache_as_read():
    template = (ROOT / "mccain_capital/templates/base.html").read_text(encoding="utf-8")
    empty_cache_recovery = template.index("if (!storedItems.length)")
    mark_read = template.index("const timestamp = writeLastReadAt", empty_cache_recovery)

    assert template.index("void seedFromFeed(false, false);", empty_cache_recovery) < mark_read
    assert template.index("return;", empty_cache_recovery) < mark_read


def test_notification_dropdown_is_portaled_above_page_stacking_contexts():
    template = (ROOT / "mccain_capital/templates/base.html").read_text(encoding="utf-8")
    styles = (ROOT / "static/css/app.css").read_text(encoding="utf-8")

    assert 'document.body.appendChild(dropdown)' in template
    assert 'dropdown.classList.add("is-portaled", "is-open")' in template
    assert 'dropdownHost.appendChild(dropdown)' in template
    assert 'window.addEventListener("resize", positionMenu)' in template
    assert ".notifDropdown.is-portaled{" in styles
    assert "z-index:10000" in styles[styles.index(".notifDropdown.is-portaled{"):]
    assert ".notifDropdown:popover-open{" in styles


def test_notification_bell_uses_one_shared_market_pulse_feed_on_every_page():
    template = (ROOT / "mccain_capital/templates/base.html").read_text(encoding="utf-8")

    assert 'const FEED_PAGE = "market-pulse";' in template
    assert 'onMarketPulsePage ? "market-pulse" : "dashboard"' not in template


def test_notification_last_read_formatter_is_hoisted_before_initialization():
    template = (ROOT / "mccain_capital/templates/base.html").read_text(encoding="utf-8")

    assert "function formatEt(iso)" in template
    assert "const formatEt = (iso)" not in template
    assert template.index("(function initNotifications(){") < template.index(
        "function formatEt(iso)"
    )


def test_notification_bell_hydrates_missing_tweet_cards_before_opening():
    template = (ROOT / "mccain_capital/templates/base.html").read_text(encoding="utf-8")
    refresh_handler = template.index('if (refreshBtn) {')
    background_polling_note = template.index(
        "// Tweet-backed notifications are manual-only.", refresh_handler
    )
    initialization = template[refresh_handler:background_polling_note]

    assert "if (!readStoredItems().length)" in initialization
    assert "void seedFromFeed(false, false);" in initialization


def test_dashboard_server_renders_shared_tweet_cards(client, monkeypatch):
    monkeypatch.setattr(
        core_svc,
        "_market_news_snapshot",
        lambda **_kwargs: {
            "pulse_feed_items": [
                {
                    "headline": "Dashboard shared tweet",
                    "summary": "Visible before client hydration runs.",
                    "age_label": "2m ago",
                    "published_et_label": "09:35 AM ET",
                    "url": "https://x.com/example/status/1",
                    "impact": "high",
                }
            ]
        },
    )

    body = client.get("/dashboard", follow_redirects=True).get_data(as_text=True)

    assert "Dashboard shared tweet" in body
    assert "Visible before client hydration runs." in body
    assert "2m ago · 09:35 AM ET" in body
    assert 'class="notifItem is-high"' in body


def test_market_pulse_initial_feed_populates_notification_cards():
    template = (ROOT / "mccain_capital/templates/core/market_pulse.html").read_text(
        encoding="utf-8"
    )
    initialization = template[template.index("const initialItems = currentFeedItems;"):]

    assert "renderFeedLayout(initialItems);" in initialization
    assert "syncNotifications(initialItems);" in initialization
    assert "storeNotificationOverrides(initialItems);" in initialization


def test_notification_refresh_exposes_visible_loading_feedback():
    template = (ROOT / "mccain_capital/templates/base.html").read_text(encoding="utf-8")
    styles = (ROOT / "static/css/app.css").read_text(encoding="utf-8")

    assert 'dropdown?.classList.toggle("is-refreshing", !!isLoading)' in template
    assert 'dropdownCount.textContent = "Refreshing tweets"' in template
    assert '!dropdown?.classList.contains("is-refreshing")' in template
    assert 'isLoading ? "Refreshing notifications" : "Refresh notifications"' in template
    assert "@keyframes notifRefreshSpin" in styles
    assert ".notifDropdown.is-refreshing::before{" in styles


def test_notification_footer_icons_resist_mobile_button_padding():
    styles = (ROOT / "static/css/adaptive_shell.css").read_text(encoding="utf-8")

    assert "body.shellMode .notifDropdownFooter .notifFooterIconBtn{" in styles
    assert "flex:0 0 42px" in styles
    assert "padding:0 !important" in styles
    assert "body.shellMode .notifDropdownFooter .notifFooterIconBtn svg{" in styles
    assert "flex:0 0 20px" in styles
    assert "width:20px !important" in styles
