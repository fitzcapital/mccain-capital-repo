"""Visual smoke guardrail with desktop + iOS-like viewport/state coverage."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

BASE_URL = os.environ.get("VISUAL_BASE_URL", "http://127.0.0.1:5001")
OUT_DIR = Path(os.environ.get("VISUAL_OUT_DIR", "artifacts/visual"))
LOGIN_PATH = os.environ.get("VISUAL_SMOKE_LOGIN_PATH", "/login")
LOGIN_USERNAME = str(os.environ.get("VISUAL_SMOKE_USERNAME") or "").strip()
LOGIN_PASSWORD = os.environ.get("VISUAL_SMOKE_PASSWORD") or ""
SETUP_PATH = os.environ.get("VISUAL_SMOKE_SETUP_PATH", "/setup")
BOOTSTRAP_USERNAME = str(os.environ.get("VISUAL_SMOKE_BOOTSTRAP_USERNAME") or "").strip()
BOOTSTRAP_PASSWORD = os.environ.get("VISUAL_SMOKE_BOOTSTRAP_PASSWORD") or ""
REQUIRE_AUTH = os.environ.get("VISUAL_SMOKE_REQUIRE_AUTH", "0") == "1"
SESSION_COOKIE = os.environ.get("VISUAL_SMOKE_SESSION_COOKIE") or ""
SESSION_COOKIE_NAME = os.environ.get("VISUAL_SMOKE_SESSION_COOKIE_NAME") or "session"
FONT_MODE = os.environ.get("VISUAL_SMOKE_FONT_MODE") or ""

SCENARIOS = [
    ("desktop-dashboard", "/dashboard", {"width": 1600, "height": 1000}, None),
    ("desktop-dashboard-menu", "/dashboard", {"width": 1600, "height": 1000}, ".moreBtn"),
    (
        "desktop-candle-opens-macro",
        "/candle-opens",
        {"width": 1600, "height": 1100},
        ".candleMacroSummary",
    ),
    ("desktop-market-pulse", "/market-pulse?refresh=1", {"width": 1600, "height": 1100}, None),
    ("desktop-trades", "/trades", {"width": 1600, "height": 1100}, None),
    (
        "desktop-statement-reconcile",
        "/trades/upload/statement?ws=reconcile",
        {"width": 1600, "height": 1100},
        None,
    ),
    ("desktop-journal", "/journal", {"width": 1600, "height": 1000}, None),
    ("desktop-weekly-review", "/journal/review/weekly", {"width": 1600, "height": 1000}, None),
    ("desktop-calculator", "/calculator", {"width": 1600, "height": 1000}, None),
    ("desktop-analytics", "/analytics?tab=performance", {"width": 1600, "height": 1100}, None),
    ("mobile-dashboard-390x844", "/dashboard", {"width": 390, "height": 844}, None),
    (
        "mobile-candle-opens-macro-390x844",
        "/candle-opens",
        {"width": 390, "height": 844},
        ".candleMacroSummary",
    ),
    (
        "mobile-dashboard-menu-390x844",
        "/dashboard",
        {"width": 390, "height": 844},
        "#mobileDockMenuBtn",
    ),
    ("mobile-market-pulse-390x844", "/market-pulse?refresh=1", {"width": 390, "height": 844}, None),
    ("mobile-trades-390x844", "/trades", {"width": 390, "height": 844}, None),
    (
        "mobile-statement-reconcile-390x844",
        "/trades/upload/statement?ws=reconcile",
        {"width": 390, "height": 844},
        None,
    ),
    ("mobile-journal-390x844", "/journal", {"width": 390, "height": 844}, None),
    ("mobile-weekly-review-390x844", "/journal/review/weekly", {"width": 390, "height": 844}, None),
    ("mobile-calculator-390x844", "/calculator", {"width": 390, "height": 844}, None),
    ("mobile-analytics-390x844", "/analytics?tab=performance", {"width": 390, "height": 844}, None),
    ("mobile-calendar-393x852", "/calendar", {"width": 393, "height": 852}, None),
    (
        "mobile-calendar-preview-393x852",
        "/calendar",
        {"width": 393, "height": 852},
        ".dayPreviewButton",
    ),
    ("mobile-calendar-390x780", "/calendar", {"width": 390, "height": 780}, None),
    ("mobile-calendar-375x667", "/calendar", {"width": 375, "height": 667}, None),
    ("mobile-payouts-390x844", "/payouts", {"width": 390, "height": 844}, None),
    ("mobile-payouts-390x780", "/payouts", {"width": 390, "height": 780}, None),
]


def _assert_tape_visuals(page, name: str) -> None:
    if "dashboard" not in name and "market-pulse" not in name:
        return
    if "menu" in name:
        return
    result = page.evaluate(
        """
        () => {
          const cards = Array.from(document.querySelectorAll(
            '.marketPulseTapeCard, .dashboardTapeAssetCard'
          ));
          const bodies = cards.flatMap((card) => Array.from(card.querySelectorAll(
            '.marketMiniSparkBody'
          )));
          const wicks = cards.flatMap((card) => Array.from(card.querySelectorAll(
            '.marketMiniSparkWick'
          )));
          const points = cards.flatMap((card) => Array.from(card.querySelectorAll(
            '.marketMiniSparkPoint'
          )));
          const guides = cards.flatMap((card) => Array.from(card.querySelectorAll(
            '.marketMiniSparkGuide'
          )));
          const visibleBodies = bodies.filter((body) => {
            const width = Number(body.getAttribute('width') || 0);
            const height = Number(body.getAttribute('height') || 0);
            return width > 0 && height > 0;
          }).length;
          return {
            cards: cards.length,
            bodies: bodies.length,
            wicks: wicks.length,
            points: points.length,
            guides: guides.length,
            visibleBodies,
          };
        }
        """
    )
    if result["bodies"] and (
        result["visibleBodies"] != result["bodies"]
        or result["wicks"] < result["bodies"]
        or result["points"] < max(1, result["cards"])
        or result["guides"] < max(1, result["cards"] * 2)
    ):
        raise RuntimeError(f"{name} tape candles lack visible detail: {result}")


def _assert_topbar_menu_visuals(page, name: str) -> None:
    if name != "desktop-dashboard-menu":
        return
    result = page.evaluate(
        """
        () => {
          const menu = document.querySelector('.moreMenu');
          if (!menu) return { missing: true };
          const styles = getComputedStyle(menu);
          const before = getComputedStyle(menu, '::before');
          const after = getComputedStyle(menu, '::after');
          return {
            missing: false,
            visible: styles.display !== 'none' && styles.visibility !== 'hidden',
            borderColor: styles.borderColor,
            boxShadow: styles.boxShadow,
            beforeOpacity: before.opacity,
            beforeBackground: before.backgroundImage,
            afterOpacity: after.opacity,
            afterBackground: after.backgroundImage,
          };
        }
        """
    )
    if (
        result.get("missing")
        or not result.get("visible")
        or "31, 79, 179" not in result.get("borderColor", "")
        or "31, 79, 179" not in result.get("boxShadow", "")
        or result.get("beforeOpacity") != "0"
        or result.get("beforeBackground") != "none"
        or result.get("afterOpacity") != "0"
        or result.get("afterBackground") != "none"
    ):
        raise RuntimeError(f"{name} topbar menu visual contract failed: {result}")


def _assert_candle_macro_visuals(page, name: str) -> None:
    if "candle-opens-macro" not in name:
        return
    result = page.evaluate(
        """
        () => {
          const fold = document.querySelector('.candleMacroFold');
          const markerGrid = document.querySelector('.candleNewsDayGrid');
          const cards = Array.from(document.querySelectorAll('.candleNewsDayCard'));
          const onCandlePage = document.body.classList.contains('page-candle-opens');
          if (!fold) return { missingFold: true, onCandlePage };
          const styles = getComputedStyle(fold);
          const rect = fold.getBoundingClientRect();
          const lastCard = cards.length ? cards[cards.length - 1].getBoundingClientRect() : null;
          return {
            missingFold: false,
            open: fold.hasAttribute('open'),
            maxHeight: styles.maxHeight,
            overflow: styles.overflow,
            markerGrid: Boolean(markerGrid),
            cards: cards.length,
            foldHeight: rect.height,
            lastCardBottom: lastCard ? lastCard.bottom : 0,
            foldBottom: rect.bottom,
          };
        }
        """
    )
    if result.get("missingFold") and not result.get("onCandlePage"):
        return
    if (
        (result.get("missingFold") and result.get("onCandlePage"))
        or not result.get("open")
        or result.get("maxHeight") != "none"
        or result.get("overflow") != "visible"
    ):
        raise RuntimeError(f"{name} macro accordion open-state contract failed: {result}")
    if result.get("markerGrid") and result.get("cards", 0) > 1:
        # The last marker card should be inside the open fold flow, not clipped by a fixed height.
        if result.get("lastCardBottom", 0) > result.get("foldBottom", 0) + 2:
            raise RuntimeError(f"{name} macro markers appear clipped: {result}")


def _assert_execution_rail_visuals(page, name: str) -> None:
    if "market-pulse" not in name:
        return
    result = page.evaluate(
        """
        () => {
          const rail = document.querySelector('.marketPulseExecutionHeroLevelRail');
          const items = Array.from(document.querySelectorAll('.marketPulseExecutionHeroLevelRailItem'));
          if (!rail || !items.length) return { present: false };
          const railRect = rail.getBoundingClientRect();
          const spills = items.map((item) => {
            const rect = item.getBoundingClientRect();
            const label = item.querySelector('span')?.getBoundingClientRect();
            const value = item.querySelector('strong')?.getBoundingClientRect();
            return {
              text: item.textContent.trim(),
              left: rect.left,
              right: rect.right,
              railLeft: railRect.left,
              railRight: railRect.right,
              labelRight: label ? label.right : 0,
              valueRight: value ? value.right : 0,
            };
          }).filter((row) => (
            row.left < row.railLeft - 20
            || row.right > row.railRight + 1
            || row.valueRight > row.right + 1
            || row.labelRight > row.right + 1
          ));
          return { present: true, items: items.length, spills };
        }
        """
    )
    if result.get("present") and result.get("spills"):
        raise RuntimeError(f"{name} execution rail spills outside its container: {result}")


def _assert_hero_chart_controls(page, name: str) -> None:
    if "market-pulse" not in name:
        return
    result = page.evaluate(
        """
        () => {
          const status = document.querySelector('#marketPulseHeroPollStatus');
          const host = document.querySelector('#spxExecutionHeroChart');
          const toggles = {
            markers: document.querySelector('#marketPulseHeroToggleMarkers'),
            levels: document.querySelector('#marketPulseHeroToggleLevels'),
            day: document.querySelector('#marketPulseHeroToggleDayLevels'),
          };
          if (!host) {
            return {
              hasHost: false,
              authGate: Boolean(document.querySelector('input[type="password"]')),
              statusText: '',
              toggleCount: 0,
            };
          }
          const railItems = () => Array.from(
            document.querySelectorAll('.marketPulseExecutionHeroLevelRailItem')
          ).map((item) => Array.from(item.classList).join(' '));
          const click = (node) => node && node.click();
          const before = railItems();
          click(toggles.day);
          const dayOff = railItems();
          click(toggles.levels);
          const levelsOffDayOff = railItems();
          click(toggles.levels);
          click(toggles.day);
          const restored = railItems();
          const hasDayRows = (rows) => rows.some((text) => /is-(pdh|pdl|cdh|cdl)/.test(text));
          const hasLevelRows = (rows) => rows.some((text) => /is-(main|spot|call|call-next|local|put|put-next)/.test(text));
          return {
            hasHost: Boolean(host),
            statusText: status ? status.textContent.trim() : '',
            toggleCount: Object.values(toggles).filter(Boolean).length,
            beforeCount: before.length,
            restoredCount: restored.length,
            dayRowsBefore: hasDayRows(before),
            levelRowsBefore: hasLevelRows(before),
            dayRowsAfterDayOff: hasDayRows(dayOff),
            levelRowsAfterLevelsOff: hasLevelRows(levelsOffDayOff),
            hasSessionBreakWhenExpected: !host?.classList.contains('has-session-break')
              || !document.querySelector('#spxExecutionHeroSessionBreak')?.hidden,
          };
        }
        """
    )
    if result.get("authGate") and not result.get("hasHost"):
        return
    empty_state = "Bars empty" in result.get("statusText", "")
    if (
        (not result.get("hasHost") and not result.get("authGate"))
        or result.get("toggleCount") != 3
        or "Bars" not in result.get("statusText", "")
        or "Quote" not in result.get("statusText", "")
        or "Levels" not in result.get("statusText", "")
        or "Levels pending" in result.get("statusText", "")
        or "Levels error" in result.get("statusText", "")
        or (not empty_state and result.get("beforeCount", 0) <= 0)
        or (not empty_state and result.get("restoredCount", 0) <= 0)
        or (not empty_state and not result.get("levelRowsBefore"))
        or (result.get("dayRowsBefore") and result.get("dayRowsAfterDayOff"))
        or result.get("levelRowsAfterLevelsOff")
        or not result.get("hasSessionBreakWhenExpected")
    ):
        raise RuntimeError(f"{name} hero chart controls contract failed: {result}")


def _capture(page, name: str, path: str, tap_selector: str | None = None) -> None:
    url = f"{BASE_URL}{path}"
    page.goto(url, wait_until="domcontentloaded", timeout=45000)
    page.wait_for_timeout(600)
    try:
        page.wait_for_load_state("networkidle", timeout=2500)
    except PlaywrightTimeoutError:
        pass
    if tap_selector:
        locator = page.locator(tap_selector).first
        try:
            locator.wait_for(state="visible", timeout=5000)
            locator.click(timeout=5000)
            page.wait_for_timeout(200)
        except PlaywrightTimeoutError:
            print(f"[visual_smoke] optional tap skipped for {name}: {tap_selector}")
    _assert_tape_visuals(page, name)
    _assert_topbar_menu_visuals(page, name)
    _assert_candle_macro_visuals(page, name)
    _assert_execution_rail_visuals(page, name)
    _assert_hero_chart_controls(page, name)
    page.screenshot(path=str(OUT_DIR / f"{name}.png"), full_page=True)


def _auth_username() -> str:
    return LOGIN_USERNAME or BOOTSTRAP_USERNAME


def _auth_password() -> str:
    return LOGIN_PASSWORD or BOOTSTRAP_PASSWORD


def _bootstrap_auth(context) -> None:
    username_value = BOOTSTRAP_USERNAME or LOGIN_USERNAME
    password_value = BOOTSTRAP_PASSWORD or LOGIN_PASSWORD
    if not username_value or not password_value:
        return

    page = context.new_page()
    try:
        page.goto(f"{BASE_URL}{SETUP_PATH}", wait_until="domcontentloaded", timeout=45000)
        page.wait_for_timeout(300)
        confirm = page.locator('input[name="confirm_password"]').first
        try:
            confirm.wait_for(state="visible", timeout=3000)
        except PlaywrightTimeoutError:
            return
        page.locator('input[name="username"]').first.fill(username_value)
        page.locator('input[name="password"]').first.fill(password_value)
        confirm.fill(password_value)
        page.locator('button[type="submit"]').first.click(timeout=5000)
        page.wait_for_load_state("domcontentloaded", timeout=45000)
        page.wait_for_timeout(500)
        if "/setup" in page.url:
            raise RuntimeError(f"Bootstrap auth did not complete; still on {page.url}")
    finally:
        page.close()


def _authenticate_context(context, *, name: str) -> None:
    if SESSION_COOKIE:
        return

    username_value = _auth_username()
    password_value = _auth_password()
    if not username_value or not password_value:
        if REQUIRE_AUTH:
            raise RuntimeError(
                "VISUAL_SMOKE_REQUIRE_AUTH=1 but no visual smoke auth credentials are set"
            )
        return

    page = context.new_page()
    try:
        page.goto(f"{BASE_URL}{LOGIN_PATH}", wait_until="domcontentloaded", timeout=45000)
        page.wait_for_timeout(300)
        if "/login" not in page.url:
            return
        username = page.locator('input[name="username"]').first
        password = page.locator('input[name="password"]').first
        username.wait_for(state="visible", timeout=8000)
        password.wait_for(state="visible", timeout=8000)
        username.fill(username_value)
        password.fill(password_value)
        page.locator('button[type="submit"]').first.click(timeout=5000)
        page.wait_for_load_state("domcontentloaded", timeout=45000)
        page.wait_for_timeout(600)
        if "/login" in page.url:
            raise RuntimeError(f"{name} login did not complete; still on {page.url}")
    finally:
        page.close()


def _add_session_cookie(context) -> None:
    if not SESSION_COOKIE:
        return

    host = urlparse(BASE_URL).hostname or "127.0.0.1"
    context.add_cookies(
        [
            {
                "name": SESSION_COOKIE_NAME,
                "value": SESSION_COOKIE,
                "domain": host,
                "path": "/",
                "httpOnly": True,
                "sameSite": "Lax",
            }
        ]
    )


def _add_font_mode(context) -> None:
    if FONT_MODE not in {"clean", "hand"}:
        return
    context.add_init_script(
        f"""
        (() => {{
          const mode = {FONT_MODE!r};
          try {{
            window.localStorage.setItem("mc_font_mode", mode);
            document.documentElement.setAttribute("data-font-mode", mode);
          }} catch (_err) {{}}
        }})();
        """,
    )


def _expected_screenshots() -> list[Path]:
    return [OUT_DIR / f"{name}.png" for name, *_ in SCENARIOS]


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for path in _expected_screenshots():
        path.unlink(missing_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        desktop = browser.new_context(viewport={"width": 1600, "height": 1100})
        mobile = browser.new_context(
            viewport={"width": 390, "height": 844},
            is_mobile=True,
            has_touch=True,
            user_agent=(
                "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 "
                "Mobile/15E148 Safari/604.1"
            ),
        )
        _add_session_cookie(desktop)
        _add_session_cookie(mobile)
        _add_font_mode(desktop)
        _add_font_mode(mobile)

        if not SESSION_COOKIE:
            _bootstrap_auth(desktop)
        _authenticate_context(desktop, name="desktop")
        _authenticate_context(mobile, name="mobile")

        for name, route, viewport, tap_selector in SCENARIOS:
            ctx = mobile if viewport["width"] <= 430 else desktop
            page = ctx.new_page()
            page.set_viewport_size(viewport)
            _capture(page, name=name, path=route, tap_selector=tap_selector)
            page.close()

        browser.close()

    created = _expected_screenshots()
    missing = [p.name for p in created if not p.exists()]
    if missing:
        raise RuntimeError(f"Missing expected screenshots: {', '.join(missing)}")
    too_small = [p.name for p in created if p.stat().st_size < 15_000]
    if too_small:
        raise RuntimeError(f"Screenshots unexpectedly small: {', '.join(too_small)}")

    print(f"Captured {len(created)} screenshots to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
