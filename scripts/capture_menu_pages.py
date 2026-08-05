"""Capture internal menu destinations against isolated application storage."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
from typing import Any

from playwright.sync_api import BrowserContext, Page, TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from capture_dashboard_redesign import _start_test_server

BASE_URL = os.environ.get("VISUAL_BASE_URL", "http://127.0.0.1:5016").rstrip("/")
OUT_ROOT = Path(os.environ.get("VISUAL_OUT_DIR", "artifacts/menu-page-modernization"))
PHASE = os.environ.get("MENU_CAPTURE_PHASE", "baseline").strip() or "baseline"
BASELINE_CSS_REF = os.environ.get("MENU_BASELINE_CSS_REF", "").strip()
ROUTE_FILTER = {
    value.strip() for value in os.environ.get("MENU_CAPTURE_ROUTES", "").split(",") if value.strip()
}

VIEWPORTS = {
    "mobile-390": {"width": 390, "height": 844},
    "tablet-768": {"width": 768, "height": 1024},
    "laptop-1440": {"width": 1440, "height": 1000},
    "wide-1920": {"width": 1920, "height": 1080},
}

ROUTES = {
    "executive": "/executive",
    "the-plan": "/the-plan",
    "trading-window": "/ops/trading-window",
    "strat": "/strat",
    "playbook": "/playbook",
    "strategies": "/strategies",
    "market-pulse": "/market-pulse",
    "market-feed": "/market-pulse/feed",
    "candle-opens": "/candle-opens",
    "trades": "/trades",
    "trade-new": "/trades/new",
    "open-positions": "/trades/open-positions",
    "live-upload": "/trades/upload/statement",
    "journal": "/journal",
    "life-journal": "/journal/life",
    "life-alignment": "/life-alignment",
    "analytics": "/analytics",
    "calendar": "/calendar",
    "planner": "/calculator",
    "forward-pace": "/forward-pace",
    "payouts": "/payouts",
    "goals": "/goals",
    "ops-alerts": "/ops/alerts",
    "backups": "/ops/backups",
    "profile": "/profile",
    "passkeys": "/auth/passkeys",
    "setup": "/setup",
    "self-control": "/self-control",
    "books": "/books",
    "restore": "/admin/restore",
}

# One or more representative pages per risk-aware family receive all four captures.
RESPONSIVE_REPRESENTATIVES = {
    "executive",
    "the-plan",
    "market-pulse",
    "candle-opens",
    "trades",
    "journal",
    "analytics",
    "forward-pace",
    "backups",
    "profile",
}


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")


def _accessible_name_script() -> str:
    return """
      (control) => control.getAttribute('aria-label')
        || control.getAttribute('title')
        || control.textContent.trim()
        || control.getAttribute('value')
        || control.getAttribute('placeholder')
        || (control.labels
          ? Array.from(control.labels).map((label) => label.textContent.trim()).join(' ')
          : '')
    """


def _page_metrics(page: Page, response_status: int | None) -> dict[str, Any]:
    return page.evaluate(
        """
        ({responseStatus, accessibleNameSource}) => {
          const accessibleName = eval(accessibleNameSource);
          const controls = Array.from(
            document.querySelectorAll('a, button, input, select, textarea, summary')
          );
          const visibleControls = controls.filter((control) => {
            const bounds = control.getBoundingClientRect();
            const style = window.getComputedStyle(control);
            return bounds.width > 0 && bounds.height > 0
              && style.visibility !== 'hidden' && style.display !== 'none';
          });
          return {
            requestedStatus: responseStatus,
            finalUrl: window.location.href,
            bodyClass: document.body.className,
            title: document.title,
            viewportWidth: window.innerWidth,
            documentWidth: document.documentElement.scrollWidth,
            documentHeight: document.documentElement.scrollHeight,
            horizontalOverflow: document.documentElement.scrollWidth > window.innerWidth + 1,
            controls: controls.length,
            visibleControls: visibleControls.length,
            unnamedVisibleControls: visibleControls.filter((control) => !accessibleName(control)).length,
            forms: Array.from(document.forms).map((form) => ({
              method: (form.getAttribute('method') || 'get').toLowerCase(),
              action: form.getAttribute('action') || window.location.pathname,
              fields: Array.from(form.elements).map((field) => field.getAttribute('name') || field.id || field.type),
            })),
            controlInventory: controls.map((control) => ({
              tag: control.tagName.toLowerCase(),
              id: control.id || '',
              name: accessibleName(control),
              href: control.getAttribute('href') || '',
              type: control.getAttribute('type') || '',
            })),
            dataHooks: Array.from(document.querySelectorAll('*')).flatMap((node) =>
              Array.from(node.attributes)
                .filter((attribute) => attribute.name.startsWith('data-'))
                .map((attribute) => attribute.name)
            ).filter((value, index, values) => values.indexOf(value) === index).sort(),
            scripts: Array.from(document.scripts).map((script) => script.getAttribute('src')).filter(Boolean),
          };
        }
        """,
        {
            "responseStatus": response_status,
            "accessibleNameSource": _accessible_name_script(),
        },
    )


def _capture(
    context: BrowserContext,
    name: str,
    route: str,
    viewport_name: str,
    viewport: dict[str, int],
    out_dir: Path,
    baseline_css: str | None,
) -> dict[str, Any]:
    page = context.new_page()
    try:
        if baseline_css is not None:
            page.route(
                "**/static/css/app.css*",
                lambda intercepted: intercepted.fulfill(
                    status=200,
                    content_type="text/css; charset=utf-8",
                    body=baseline_css,
                ),
            )
        page.set_viewport_size(viewport)
        response = page.goto(f"{BASE_URL}{route}", wait_until="domcontentloaded", timeout=45_000)
        try:
            page.wait_for_load_state("networkidle", timeout=1_500)
        except PlaywrightTimeoutError:
            pass
        page.wait_for_timeout(500)
        metrics = _page_metrics(page, response.status if response is not None else None)
        page.screenshot(
            path=str(out_dir / f"{_slug(name)}--{viewport_name}.png"),
            full_page=True,
        )
        return metrics
    finally:
        page.close()


def main() -> int:
    out_dir = OUT_ROOT / PHASE
    out_dir.mkdir(parents=True, exist_ok=True)
    baseline_css = None
    if BASELINE_CSS_REF:
        baseline_css = subprocess.run(
            ["git", "show", f"{BASELINE_CSS_REF}:static/css/app.css"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    report: dict[str, dict[str, Any]] = {}
    server, temp_dir = _start_test_server()
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context()
            for name, route in ROUTES.items():
                if ROUTE_FILTER and name not in ROUTE_FILTER:
                    continue
                route_report: dict[str, Any] = {}
                viewport_names = (
                    VIEWPORTS.keys() if name in RESPONSIVE_REPRESENTATIVES else ("laptop-1440",)
                )
                for viewport_name in viewport_names:
                    route_report[viewport_name] = _capture(
                        context,
                        name,
                        route,
                        viewport_name,
                        VIEWPORTS[viewport_name],
                        out_dir,
                        baseline_css,
                    )
                report[name] = {"route": route, "captures": route_report}
            browser.close()
    finally:
        server.shutdown()
        temp_dir.cleanup()

    (out_dir / "metrics.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Captured {sum(len(item['captures']) for item in report.values())} page views")
    print(f"Metrics: {out_dir / 'metrics.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
