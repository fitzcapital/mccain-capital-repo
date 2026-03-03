"""Visual smoke guardrail with desktop + iOS-like viewport/state coverage."""

from __future__ import annotations

import os
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE_URL = os.environ.get("VISUAL_BASE_URL", "http://127.0.0.1:5001")
OUT_DIR = Path(os.environ.get("VISUAL_OUT_DIR", "artifacts/visual"))

SCENARIOS = [
    ("desktop-dashboard", "/dashboard", {"width": 1600, "height": 1000}, None),
    ("desktop-trades", "/trades", {"width": 1600, "height": 1100}, None),
    ("desktop-journal", "/journal", {"width": 1600, "height": 1000}, None),
    ("desktop-calculator", "/calculator", {"width": 1600, "height": 1000}, None),
    ("desktop-analytics", "/analytics?tab=performance", {"width": 1600, "height": 1100}, None),
    ("mobile-dashboard-390x844", "/dashboard", {"width": 390, "height": 844}, None),
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
    page.goto(url, wait_until="networkidle", timeout=45000)
    if tap_selector:
        page.locator(tap_selector).first.click(timeout=5000)
        page.wait_for_timeout(200)
    page.screenshot(path=str(OUT_DIR / f"{name}.png"), full_page=True)


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
