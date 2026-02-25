"""Vanquish live-login statement sync helpers.

This module automates broker login and statement generation using Playwright.
It intentionally keeps credential handling ephemeral: callers pass credentials
per request and decide whether to persist anything locally.
"""

from __future__ import annotations

import urllib.parse
from typing import List, Tuple


def _first_visible(page, selectors: List[str]):
    for selector in selectors:
        locator = page.locator(selector)
        try:
            if locator.count() > 0 and locator.first.is_visible():
                return locator.first
        except Exception:
            continue
    return None


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
) -> Tuple[str, List[str]]:
    warnings: List[str] = []
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
        page = browser.new_page()
        page.set_default_timeout(timeout_ms)
        page.goto(login_url, wait_until="domcontentloaded")

        user_input = _first_visible(
            page,
            [
                "input[name='username']",
                "input[name='email']",
                "input[type='email']",
                "input[id*='user']",
                "input[id*='email']",
                "input[type='text']",
            ],
        )
        pass_input = _first_visible(
            page,
            [
                "input[name='password']",
                "input[type='password']",
                "input[id*='pass']",
            ],
        )
        if not user_input or not pass_input:
            browser.close()
            raise RuntimeError("Could not locate login fields on Vanquish page.")

        user_input.fill(username)
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

        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except PlaywrightTimeoutError:
            warnings.append("Login post-submit did not reach network idle; continuing.")

        if "login" in page.url.lower():
            browser.close()
            raise RuntimeError(
                "Still on login page after submit. Credentials may be invalid or MFA/CAPTCHA is required."
            )

        page.goto(statement_url, wait_until="domcontentloaded")
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

        html_text = page.content()
        browser.close()

    lowered = html_text.lower()
    if "<table" not in lowered:
        warnings.append("No HTML table detected in generated statement page.")
    if "password" in lowered and "login" in lowered:
        raise RuntimeError(
            "Received login page instead of statement HTML. Session may have expired."
        )
    return html_text, warnings
