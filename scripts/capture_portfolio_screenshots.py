"""Capture authenticated desktop/mobile screenshots for README and portfolio."""

from __future__ import annotations

import os
from pathlib import Path

from playwright.sync_api import BrowserContext, sync_playwright

BASE_URL = os.environ.get("VISUAL_BASE_URL", "http://127.0.0.1:5001").rstrip("/")
OUT_DIR = Path(os.environ.get("VISUAL_OUT_DIR", "docs/images"))
USERNAME = os.environ.get("APP_USERNAME", "fitz")
PASSWORD = os.environ.get("APP_PASSWORD", "fitzfitz")

SCENARIOS = [
    ("desktop-dashboard", "/dashboard", {"width": 1600, "height": 1000}),
    ("desktop-market-pulse", "/market-pulse?refresh=1", {"width": 1600, "height": 1200}),
    ("desktop-trades", "/trades", {"width": 1600, "height": 1100}),
    ("desktop-analytics", "/analytics?tab=performance", {"width": 1600, "height": 1100}),
    ("mobile-dashboard", "/dashboard", {"width": 430, "height": 932}),
    ("mobile-market-pulse", "/market-pulse?refresh=1", {"width": 430, "height": 932}),
    ("mobile-trades", "/trades", {"width": 430, "height": 932}),
    ("mobile-analytics", "/analytics?tab=performance", {"width": 430, "height": 932}),
]


def _login(ctx: BrowserContext) -> None:
    page = ctx.new_page()
    page.goto(f"{BASE_URL}/login", wait_until="networkidle", timeout=45000)
    page.fill("input[name='username']", USERNAME)
    page.fill("input[name='password']", PASSWORD)
    page.click("button[type='submit']")
    page.wait_for_url(f"{BASE_URL}/**", timeout=45000)
    page.close()


def _capture(ctx: BrowserContext, name: str, route: str) -> None:
    page = ctx.new_page()
    page.goto(f"{BASE_URL}{route}", wait_until="networkidle", timeout=45000)
    page.screenshot(path=str(OUT_DIR / f"{name}.png"), full_page=True)
    page.close()


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        desktop = browser.new_context(viewport={"width": 1600, "height": 1100})
        mobile = browser.new_context(
            viewport={"width": 430, "height": 932},
            is_mobile=True,
            has_touch=True,
            user_agent=(
                "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 "
                "Mobile/15E148 Safari/604.1"
            ),
        )
        _login(desktop)
        _login(mobile)

        for name, route, viewport in SCENARIOS:
            ctx = mobile if viewport["width"] <= 430 else desktop
            _capture(ctx, name, route)

        browser.close()
    print(f"Captured {len(SCENARIOS)} screenshots to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
