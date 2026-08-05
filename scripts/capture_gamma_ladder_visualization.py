"""Capture deterministic Gamma Ladder states without reading or writing runtime market data."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from playwright.sync_api import Route, sync_playwright

from capture_dashboard_redesign import _start_test_server


OUT_DIR = Path("artifacts/gamma-ladder-depth-visualization/final")
VIEWPORTS = {
    "mobile-390": {"width": 390, "height": 844},
    "tablet-768": {"width": 768, "height": 1024},
    "laptop-1440": {"width": 1440, "height": 1000},
    "wide-1920": {"width": 1920, "height": 1080},
}


def _rows() -> list[dict[str, Any]]:
    values = [
        (7600, 52, -18, 34),
        (7580, 45, -20, 25),
        (7560, 31, -24, 7),
        (7540, 28, -29, -1),
        (7520, 22, -33, -11),
        (7500, 18, -58, -40),
        (7480, 20, -42, -22),
        (7460, 24, -34, -10),
        (7440, 30, -27, 3),
    ]
    return [
        {
            "strike": strike,
            "call_gex": call * 1_000_000,
            "put_gex": put * 1_000_000,
            "net_gex": net * 1_000_000,
            "is_flip": strike == 7540,
            "is_strongest": strike == 7500,
            "is_spot_nearest": strike == 7560,
        }
        for strike, call, put, net in values
    ]


def _payload(state: str) -> tuple[int, dict[str, Any]]:
    if state == "error":
        return 503, {"ok": False, "symbol": "SPY", "message": "Options chain unavailable."}
    rows = _rows()
    if state == "partial":
        rows = rows[2:7]
    if state == "empty":
        rows = []
    updated_at = "2026-07-14T13:59:00-04:00"
    updated_label = "1:59 PM ET"
    if state == "stale":
        updated_at = "2026-07-14T09:30:00-04:00"
        updated_label = "9:30 AM ET"
    return 200, {
        "ok": True,
        "symbol": "SPY",
        "spot": 7553.28,
        "previous_spot": 7551.80,
        "expiration": "2026-07-17",
        "expiration_label": "3DTE",
        "regime": "negative_gamma",
        "regime_label": "Negative Gamma Regime",
        "updated_at": updated_at,
        "updated_label": updated_label,
        "rows_total": 96,
        "rows_visible": len(rows),
        "window_preset": "standard",
        "dte_preset": "0",
        "available_dte_options": ["0", "3", "7", "14", "all"],
        "rows": rows,
    }


def _fulfill(route: Route, state: str) -> None:
    status, payload = _payload(state)
    route.fulfill(status=status, content_type="application/json", body=json.dumps(payload))


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {}
    server, temp_dir = _start_test_server()
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            for state in ("complete", "partial", "stale", "empty", "error"):
                report[state] = {}
                for viewport_name, viewport in VIEWPORTS.items():
                    context = browser.new_context(viewport=viewport)
                    page = context.new_page()
                    page.route(
                        "**/api/gamma-ladder**",
                        lambda route, _request, selected_state=state: _fulfill(
                            route,
                            selected_state,
                        ),
                    )
                    page.goto("http://127.0.0.1:5016/market-pulse", wait_until="domcontentloaded")
                    page.wait_for_timeout(700)
                    page.add_style_tag(
                        content="""
                        #mobileMenuSheet, #mobileMenuOverlay, .mobileDock,
                        .mobileMarketStatus, .floatingActionDock { display:none !important; }
                        """
                    )
                    page.evaluate(
                        """
                        () => {
                          if (window.closeMobileMenu) window.closeMobileMenu();
                          document.querySelector('#marketPulseGammaLadderCard')?.scrollIntoView();
                        }
                        """
                    )
                    page.wait_for_timeout(200)
                    if state in {"complete", "partial", "stale"}:
                        first_row = page.locator("[data-gamma-row]").first
                        if first_row.count():
                            first_row.click()
                    metrics = page.evaluate(
                        """
                        () => ({
                          viewportWidth: innerWidth,
                          documentWidth: document.documentElement.scrollWidth,
                          horizontalOverflow: document.documentElement.scrollWidth > innerWidth + 1,
                          rows: document.querySelectorAll('[data-gamma-row]').length,
                          selectedRows: document.querySelectorAll('[data-gamma-row][aria-pressed="true"]').length,
                          inspector: document.querySelector('[data-gamma-selected-strike]')?.textContent.trim(),
                          guideExpanded: document.querySelector('#gammaLadderGuide')?.open,
                          error: document.querySelector('[data-gamma-error]')?.textContent.trim(),
                        })
                        """
                    )
                    report[state][viewport_name] = metrics
                    page.locator("#marketPulseGammaLadderCard").screenshot(
                        path=str(OUT_DIR / f"{state}--{viewport_name}.png"),
                    )
                    context.close()
            pin_context = browser.new_context(viewport=VIEWPORTS["laptop-1440"])
            pin_page = pin_context.new_page()
            pin_page.route(
                "**/api/gamma-ladder**",
                lambda route, _request: _fulfill(route, "complete"),
            )
            pin_page.goto("http://127.0.0.1:5016/market-pulse", wait_until="domcontentloaded")
            pin_page.wait_for_timeout(500)
            pin_toggle = pin_page.locator("[data-playbook-pin-toggle]")
            report["pinControl"] = {
                "default": pin_page.evaluate(
                    """
                    () => ({
                      bodyPinned: document.body.classList.contains('is-playbook-pinned'),
                      pressed: document.querySelector('[data-playbook-pin-toggle]')?.getAttribute('aria-pressed'),
                      position: getComputedStyle(document.querySelector('#marketPulseStatusBar')).position,
                    })
                    """
                )
            }
            pin_page.locator("#marketPulseStatusBar").screenshot(
                path=str(OUT_DIR / "playbook-header--default.png")
            )
            pin_toggle.click()
            report["pinControl"]["enabled"] = pin_page.evaluate(
                """
                () => ({
                  bodyPinned: document.body.classList.contains('is-playbook-pinned'),
                  pressed: document.querySelector('[data-playbook-pin-toggle]')?.getAttribute('aria-pressed'),
                  position: getComputedStyle(document.querySelector('#marketPulseStatusBar')).position,
                })
                """
            )
            pin_page.reload(wait_until="domcontentloaded")
            pin_page.wait_for_timeout(300)
            report["pinControl"]["restored"] = pin_page.evaluate(
                """
                () => ({
                  bodyPinned: document.body.classList.contains('is-playbook-pinned'),
                  pressed: document.querySelector('[data-playbook-pin-toggle]')?.getAttribute('aria-pressed'),
                  position: getComputedStyle(document.querySelector('#marketPulseStatusBar')).position,
                })
                """
            )
            pin_page.locator("[data-playbook-pin-toggle]").click()
            ticker_search_toggle = pin_page.locator("[data-playbook-symbol-search-control] [data-symbol-search-toggle]")
            ticker_search_toggle.click()
            report["playbookSearch"] = pin_page.evaluate(
                """
                () => {
                  const header = document.querySelector('#marketPulseStatusBar');
                  const toggle = document.querySelector('[data-playbook-symbol-search-control] [data-symbol-search-toggle]');
                  const popover = document.querySelector('[data-playbook-symbol-search-control] [data-symbol-search-popover]');
                  const headerRect = header?.getBoundingClientRect();
                  const toggleRect = toggle?.getBoundingClientRect();
                  const popoverRect = popover?.getBoundingClientRect();
                  const clippingAncestors = [];
                  for (let node = popover?.parentElement; node && node !== document.body; node = node.parentElement) {
                    const style = getComputedStyle(node);
                    if (![style.overflow, style.overflowX, style.overflowY].every((value) => value === 'visible')) {
                      clippingAncestors.push(node.id || node.className || node.tagName);
                    }
                  }
                  return {
                    expanded: toggle?.getAttribute('aria-expanded'),
                    popoverVisible: Boolean(popover && !popover.hidden && popoverRect?.height),
                    popoverBelowTrigger: Boolean(popoverRect && toggleRect && popoverRect.top >= toggleRect.bottom),
                    popoverWithinViewport: Boolean(popoverRect && popoverRect.left >= 0 && popoverRect.right <= innerWidth),
                    headerHeight: Math.round(headerRect?.height || 0),
                    clippingAncestors,
                  };
                }
                """
            )
            pin_page.screenshot(path=str(OUT_DIR / "playbook-header--search-open.png"))
            pin_page.keyboard.press("Escape")
            settings_toggle = pin_page.locator("[data-gamma-settings-toggle]")
            report["commandBar"] = {
                "closed": pin_page.evaluate(
                    """
                    () => ({
                      expanded: document.querySelector('[data-gamma-settings-toggle]')?.getAttribute('aria-expanded'),
                      label: document.querySelector('[data-gamma-settings-label]')?.textContent.trim(),
                      popoverHidden: document.querySelector('[data-gamma-settings-popover]')?.hidden,
                      visibleButtons: [...document.querySelectorAll('#gammaLadderStatus button')]
                        .filter((button) => button.offsetParent !== null).length,
                    })
                    """
                )
            }
            pin_page.locator("#gammaLadderStatus").screenshot(
                path=str(OUT_DIR / "gamma-command-bar--closed.png")
            )
            settings_toggle.click()
            report["commandBar"]["open"] = pin_page.evaluate(
                """
                () => ({
                  expanded: document.querySelector('[data-gamma-settings-toggle]')?.getAttribute('aria-expanded'),
                  popoverHidden: document.querySelector('[data-gamma-settings-popover]')?.hidden,
                  windowOptions: document.querySelectorAll('[data-gamma-settings-popover] [data-gamma-window-pill]').length,
                  dteOptions: document.querySelectorAll('[data-gamma-settings-popover] [data-gamma-dte-pill]').length,
                })
                """
            )
            pin_page.locator("#marketPulseGammaLadderCard").screenshot(
                path=str(OUT_DIR / "gamma-command-bar--open.png")
            )
            pin_page.keyboard.press("Escape")
            report["commandBar"]["dismissed"] = pin_page.evaluate(
                """
                () => ({
                  expanded: document.querySelector('[data-gamma-settings-toggle]')?.getAttribute('aria-expanded'),
                  popoverHidden: document.querySelector('[data-gamma-settings-popover]')?.hidden,
                  focusRestored: document.activeElement === document.querySelector('[data-gamma-settings-toggle]'),
                })
                """
            )
            pin_context.close()
            browser.close()
    finally:
        server.shutdown()
        temp_dir.cleanup()
    (OUT_DIR / "metrics.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print("Captured 20 deterministic Gamma Ladder states, Playbook pinning, and the command bar")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
