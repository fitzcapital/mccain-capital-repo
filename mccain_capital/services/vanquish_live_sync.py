"""Vanquish live-login statement sync helpers.

This module automates broker login and statement generation using Playwright.
It intentionally keeps credential handling ephemeral: callers pass credentials
per request and decide whether to persist anything locally.
"""

from __future__ import annotations

import json
import os
import urllib.parse
from typing import Any, List, Optional, Tuple


def _contexts(page) -> List[Any]:
    # Includes main page and any iframes where hosted auth providers render forms.
    ctxs: List[Any] = [page]
    try:
        ctxs.extend(list(page.frames))
    except Exception:
        pass
    return ctxs


def _first_visible(page, selectors: List[str]):
    for ctx in _contexts(page):
        for selector in selectors:
            locator = ctx.locator(selector)
            try:
                if locator.count() > 0 and locator.first.is_visible():
                    return locator.first
            except Exception:
                continue
    return None


def _debug_write(debug_dir: Optional[str], name: str, content: str) -> Optional[str]:
    if not debug_dir:
        return None
    os.makedirs(debug_dir, exist_ok=True)
    path = os.path.join(debug_dir, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


def _debug_shot(page, debug_dir: Optional[str], name: str) -> Optional[str]:
    if not debug_dir:
        return None
    os.makedirs(debug_dir, exist_ok=True)
    path = os.path.join(debug_dir, name)
    page.screenshot(path=path, full_page=True)
    return path


def fetch_statement_html_via_login(
    *,
    base_origin: str,
    username: str,
    password: str,
    from_date: str,
    to_date: str,
    account: str,
    wl: str = "vanquishtrader",
    time_zone: str = "America/New_York",
    date_locale: str = "en-US",
    report_locale: str = "en",
    headless: bool = True,
    timeout_ms: int = 45000,
    debug_dir: Optional[str] = None,
) -> Tuple[str, List[str], List[str]]:
    warnings: List[str] = []
    artifacts: List[str] = []
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as e:  # pragma: no cover - dependency optional at runtime
        raise RuntimeError(
            "Playwright is required for live login sync. Install with "
            "`pip install playwright` then `playwright install chromium`."
        ) from e

    origin = (base_origin or "https://trade.vanquishtrader.com").rstrip("/")
    login_url = origin
    statement_path = "/account/statement/"
    statement_url = f"{origin}{statement_path}?" + urllib.parse.urlencode(
        {
            "wl": wl,
            "format": "html",
            "from": from_date,
            "to": to_date,
            "timeZone": time_zone,
            "account": account,
            "dateLocale": date_locale,
            "reportLocale": report_locale,
        }
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        if debug_dir:
            context.tracing.start(screenshots=True, snapshots=True, sources=True)
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        page.goto(login_url, wait_until="domcontentloaded")
        shot = _debug_shot(page, debug_dir, "01_open_login.png")
        if shot:
            artifacts.append(shot)

        user_input = _first_visible(
            page,
            [
                "input[name='username']",
                "input[name='userName']",
                "input[name='login']",
                "input[name='email']",
                "input[type='email']",
                "input[id*='user']",
                "input[id*='email']",
                "input[autocomplete='username']",
                "input[placeholder*='Email' i]",
                "input[placeholder*='Username' i]",
                "input[type='text']",
            ],
        )
        if not user_input:
            _debug_write(debug_dir, "01_login_dom.html", page.content())
            context.close()
            browser.close()
            raise RuntimeError("Could not locate username/email field on Vanquish page.")

        user_input.fill(username)
        shot = _debug_shot(page, debug_dir, "02_after_username.png")
        if shot:
            artifacts.append(shot)

        pass_input = _first_visible(
            page,
            [
                "input[name='password']",
                "input[type='password']",
                "input[id*='pass']",
                "input[autocomplete='current-password']",
                "input[placeholder*='Password' i]",
            ],
        )
        if not pass_input:
            next_btn = _first_visible(
                page,
                [
                    "button:has-text('Next')",
                    "button:has-text('Continue')",
                    "button:has-text('Proceed')",
                    "button[type='submit']",
                    "input[type='submit']",
                ],
            )
            if next_btn:
                next_btn.click()
                try:
                    page.wait_for_timeout(800)
                except Exception:
                    pass
            pass_input = _first_visible(
                page,
                [
                    "input[name='password']",
                    "input[type='password']",
                    "input[id*='pass']",
                    "input[autocomplete='current-password']",
                    "input[placeholder*='Password' i]",
                ],
            )
        if not pass_input:
            _debug_write(debug_dir, "02_username_step_dom.html", page.content())
            context.close()
            browser.close()
            raise RuntimeError("Could not locate password field after entering username/email.")

        pass_input.fill(password)

        submit_btn = _first_visible(
            page,
            [
                "button[type='submit']",
                "input[type='submit']",
                "button:has-text('Login')",
                "button:has-text('Sign in')",
            ],
        )
        if submit_btn:
            submit_btn.click()
        else:
            pass_input.press("Enter")
        shot = _debug_shot(page, debug_dir, "03_after_password_submit.png")
        if shot:
            artifacts.append(shot)

        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except PlaywrightTimeoutError:
            warnings.append("Login post-submit did not reach network idle; continuing.")

        if "login" in page.url.lower():
            _debug_write(debug_dir, "03_post_login_dom.html", page.content())
            context.close()
            browser.close()
            raise RuntimeError(
                "Still on login page after submit. Credentials may be invalid or MFA/CAPTCHA is required."
            )

        page.goto(statement_url, wait_until="domcontentloaded")
        shot = _debug_shot(page, debug_dir, "04_statement_page.png")
        if shot:
            artifacts.append(shot)
        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except PlaywrightTimeoutError:
            warnings.append("Statement page load did not reach network idle; continuing.")

        generate_btn = _first_visible(
            page,
            [
                "button:has-text('Generate Statement')",
                "button:has-text('Generate')",
                "input[value*='Generate']",
                "button:has-text('Run')",
            ],
        )
        if generate_btn:
            generate_btn.click()
            try:
                page.wait_for_load_state("networkidle", timeout=timeout_ms)
            except PlaywrightTimeoutError:
                warnings.append("Generate action completed without network-idle confirmation.")
        else:
            warnings.append("Generate button not found; captured current statement page.")

        shot = _debug_shot(page, debug_dir, "05_after_generate.png")
        if shot:
            artifacts.append(shot)

        html_text = page.content()
        html_path = _debug_write(debug_dir, "final_statement.html", html_text)
        if html_path:
            artifacts.append(html_path)
        debug_meta = {
            "login_url": login_url,
            "statement_url": statement_url,
            "final_url": page.url,
            "warnings": warnings,
        }
        meta_path = _debug_write(debug_dir, "debug_meta.json", json.dumps(debug_meta, indent=2))
        if meta_path:
            artifacts.append(meta_path)
        if debug_dir:
            trace_path = os.path.join(debug_dir, "trace.zip")
            context.tracing.stop(path=trace_path)
            artifacts.append(trace_path)
        context.close()
        browser.close()

    lowered = html_text.lower()
    if "<table" not in lowered:
        warnings.append("No HTML table detected in generated statement page.")
    if "password" in lowered and "login" in lowered:
        raise RuntimeError(
            "Received login page instead of statement HTML. Session may have expired."
        )
    return html_text, warnings, artifacts
