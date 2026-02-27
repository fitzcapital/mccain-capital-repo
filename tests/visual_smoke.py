"""Visual smoke guardrail: capture key routes on desktop/mobile and fail if capture fails."""

from __future__ import annotations

import os
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE_URL = os.environ.get("VISUAL_BASE_URL", "http://127.0.0.1:5001")
OUT_DIR = Path(os.environ.get("VISUAL_OUT_DIR", "artifacts/visual"))

SCENARIOS = [
    ("desktop-dashboard", "/dashboard", {"width": 1600, "height": 1000}),
    ("desktop-trades", "/trades", {"width": 1600, "height": 1100}),
    ("desktop-journal", "/journal", {"width": 1600, "height": 1000}),
    ("desktop-calculator", "/calculator", {"width": 1600, "height": 1000}),
    ("desktop-analytics", "/analytics?tab=performance", {"width": 1600, "height": 1100}),
    ("mobile-dashboard", "/dashboard", {"width": 390, "height": 844}),
    ("mobile-trades", "/trades", {"width": 390, "height": 844}),
    ("mobile-journal", "/journal", {"width": 390, "height": 844}),
    ("mobile-calculator", "/calculator", {"width": 390, "height": 844}),
    ("mobile-analytics", "/analytics?tab=performance", {"width": 390, "height": 844}),
]


def _capture(page, name: str, path: str) -> None:
    url = f"{BASE_URL}{path}"
    page.goto(url, wait_until="networkidle", timeout=45000)
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

        for name, route, viewport in SCENARIOS:
            ctx = mobile if viewport["width"] <= 430 else desktop
            page = ctx.new_page()
            _capture(page, name=name, path=route)
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
