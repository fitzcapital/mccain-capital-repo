"""Visual smoke guardrail with desktop + iOS-like viewport/state coverage."""

from __future__ import annotations

import os
from pathlib import Path

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

SCENARIOS = [
    ("desktop-dashboard", "/dashboard", {"width": 1600, "height": 1000}, None),
    ("desktop-market-pulse", "/market-pulse?refresh=1", {"width": 1600, "height": 1100}, None),
    ("desktop-trades", "/trades", {"width": 1600, "height": 1100}, None),
    ("desktop-journal", "/journal", {"width": 1600, "height": 1000}, None),
    ("desktop-calculator", "/calculator", {"width": 1600, "height": 1000}, None),
    ("desktop-analytics", "/analytics?tab=performance", {"width": 1600, "height": 1100}, None),
    ("mobile-dashboard-390x844", "/dashboard", {"width": 390, "height": 844}, None),
    (
        "mobile-dashboard-menu-390x844",
        "/dashboard",
        {"width": 390, "height": 844},
        "#mobileDockMenuBtn",
    ),
    ("mobile-market-pulse-390x844", "/market-pulse?refresh=1", {"width": 390, "height": 844}, None),
    ("mobile-trades-390x844", "/trades", {"width": 390, "height": 844}, None),
    ("mobile-journal-390x844", "/journal", {"width": 390, "height": 844}, None),
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


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
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

    created = sorted(OUT_DIR.glob("*.png"))
    if len(created) != len(SCENARIOS):
        raise RuntimeError(f"Expected {len(SCENARIOS)} screenshots, found {len(created)}")
    too_small = [p.name for p in created if p.stat().st_size < 15_000]
    if too_small:
        raise RuntimeError(f"Screenshots unexpectedly small: {', '.join(too_small)}")

    print(f"Captured {len(created)} screenshots to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
