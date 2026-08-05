"""Capture authenticated Dashboard baselines for modernization reviews."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
from threading import Thread
from typing import Any

from playwright.sync_api import BrowserContext, sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from werkzeug.serving import make_server

USE_TEST_APP = os.environ.get("VISUAL_USE_TEST_APP", "0") == "1"
BASE_URL = os.environ.get(
    "VISUAL_BASE_URL",
    "http://127.0.0.1:5016" if USE_TEST_APP else "http://127.0.0.1:5001",
).rstrip("/")
OUT_ROOT = Path(os.environ.get("VISUAL_OUT_DIR", "artifacts/dashboard-modernization"))
PHASE = os.environ.get("DASHBOARD_CAPTURE_PHASE", "baseline").strip() or "baseline"
USERNAME = os.environ.get("APP_USERNAME", "fitz")
PASSWORD = os.environ.get("APP_PASSWORD", "fitzfitz")

VIEWPORTS = {
    "mobile-390": {"width": 390, "height": 844},
    "tablet-768": {"width": 768, "height": 1024},
    "laptop-1440": {"width": 1440, "height": 1000},
    "wide-1920": {"width": 1920, "height": 1080},
}


def _start_test_server() -> tuple[Any, tempfile.TemporaryDirectory[str]]:
    """Start an auth-free app against isolated temporary storage."""
    from mccain_capital import app_core as core
    from mccain_capital import create_app, runtime
    from mccain_capital.repositories import journal as journal_repo
    from mccain_capital.repositories import self_control as self_control_repo

    temp_dir = tempfile.TemporaryDirectory(prefix="dashboard-redesign-")
    root = Path(temp_dir.name)
    db_path = root / "visual.db"
    uploads_dir = root / "uploads"
    books_dir = root / "books"
    uploads_dir.mkdir()
    books_dir.mkdir()

    core.DB_PATH = str(db_path)
    core.UPLOAD_DIR = str(uploads_dir)
    core.BOOKS_DIR = str(books_dir)
    core.APP_PASSWORD = ""
    core.APP_PASSWORD_HASH = ""
    runtime.DB_PATH = str(db_path)
    runtime.UPLOAD_DIR = str(uploads_dir)
    runtime.BOOKS_DIR = str(books_dir)
    journal_repo.DB_PATH = str(db_path)
    self_control_repo.DB_PATH = str(db_path)

    app = create_app()
    app.config.update(TESTING=True, CSRF_ENABLED=False)
    server = make_server("127.0.0.1", 5016, app, threaded=True)
    Thread(target=server.serve_forever, daemon=True).start()
    return server, temp_dir


def _login(context: BrowserContext) -> None:
    if USE_TEST_APP:
        return
    page = context.new_page()
    try:
        page.goto(f"{BASE_URL}/login", wait_until="domcontentloaded", timeout=45_000)
        if "/login" not in page.url:
            return
        page.fill("input[name='username']", USERNAME)
        page.fill("input[name='password']", PASSWORD)
        page.click("button.authSubmitBtn[type='submit']")
        page.wait_for_load_state("domcontentloaded", timeout=45_000)
        if "/login" in page.url:
            raise RuntimeError("Dashboard capture login failed")
    finally:
        page.close()


def _capture(
    context: BrowserContext,
    name: str,
    viewport: dict[str, int],
    out_dir: Path,
    baseline_css: str | None,
) -> dict:
    page = context.new_page()
    try:
        if baseline_css is not None:
            page.route(
                "**/static/css/app.css*",
                lambda route: route.fulfill(
                    status=200,
                    content_type="text/css; charset=utf-8",
                    body=baseline_css,
                ),
            )
        page.set_viewport_size(viewport)
        page.goto(f"{BASE_URL}/dashboard", wait_until="domcontentloaded", timeout=45_000)
        try:
            page.wait_for_load_state("networkidle", timeout=3_000)
        except PlaywrightTimeoutError:
            pass
        page.wait_for_timeout(1_200)
        metrics = page.evaluate(
            """
            () => {
              const controls = Array.from(
                document.querySelectorAll('a, button, input, select, textarea, summary')
              );
              const visibleControls = controls.filter((control) => {
                const bounds = control.getBoundingClientRect();
                const style = window.getComputedStyle(control);
                return bounds.width > 0 && bounds.height > 0 && style.visibility !== 'hidden';
              });
              const accessibleName = (control) =>
                control.getAttribute('aria-label')
                || control.getAttribute('title')
                || control.textContent.trim()
                || control.getAttribute('value')
                || control.getAttribute('placeholder')
                || (control.labels ? Array.from(control.labels).map((label) => label.textContent.trim()).join(' ') : '');
              const controlInventory = controls.map((control) => ({
                tag: control.tagName.toLowerCase(),
                id: control.id || '',
                name: accessibleName(control),
                href: control.getAttribute('href') || '',
                type: control.getAttribute('type') || '',
              }));
              return {
                viewportWidth: window.innerWidth,
                documentWidth: document.documentElement.scrollWidth,
                documentHeight: document.documentElement.scrollHeight,
                controls: controls.length,
                visibleControls: visibleControls.length,
                unnamedVisibleControls: visibleControls.filter((control) => !accessibleName(control)).length,
                controlInventory,
                sections: document.querySelectorAll('.dashboardStageSection').length,
                horizontalOverflow: document.documentElement.scrollWidth > window.innerWidth + 1,
              };
            }
            """
        )
        page.screenshot(path=str(out_dir / f"{name}.png"), full_page=True)
        return metrics
    finally:
        page.close()


def main() -> int:
    out_dir = OUT_ROOT / PHASE
    out_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, dict] = {}
    baseline_ref = os.environ.get("DASHBOARD_BASELINE_CSS_REF", "").strip()
    baseline_css = None
    if baseline_ref:
        baseline_css = subprocess.run(
            ["git", "show", f"{baseline_ref}:static/css/app.css"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    server = None
    temp_dir = None
    try:
        if USE_TEST_APP:
            server, temp_dir = _start_test_server()
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context()
            _login(context)
            for name, viewport in VIEWPORTS.items():
                report[name] = _capture(context, name, viewport, out_dir, baseline_css)
            browser.close()
    finally:
        if server is not None:
            server.shutdown()
        if temp_dir is not None:
            temp_dir.cleanup()

    (out_dir / "metrics.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Captured {len(report)} Dashboard screenshots to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
