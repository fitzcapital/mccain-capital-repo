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


def _wait_until_enabled(locator, timeout_ms: int = 6000) -> bool:
    import time

    deadline = time.time() + (timeout_ms / 1000.0)
    while time.time() < deadline:
        try:
            disabled = locator.get_attribute("disabled")
            aria_disabled = locator.get_attribute("aria-disabled")
            if disabled is None and str(aria_disabled).lower() not in {"true", "1"}:
                return True
        except Exception:
            pass
        time.sleep(0.15)
    return False


def _click_first(page, selectors: List[str], timeout_ms: int = 4000) -> bool:
    for selector in selectors:
        try:
            loc = page.locator(selector)
            if loc.count() > 0 and loc.first.is_visible():
                loc.first.click(timeout=timeout_ms)
                return True
        except Exception:
            continue
    return False


def _set_statement_period_fields(page, from_date: str, to_date: str) -> bool:
    js = """
    (payload) => {
      const roots = Array.from(document.querySelectorAll('div,section,dialog'));
      const root = roots.find(el => (el.innerText || '').includes('Account Statement'));
      if (!root) return false;
      const inputs = Array.from(root.querySelectorAll('input'));
      const dateLike = inputs.filter(i => {
        const p = (i.getAttribute('placeholder') || '').toLowerCase();
        const v = (i.value || '').trim();
        return /\\d{2}\\/\\d{2}\\/\\d{4}/.test(v) || p.includes('mm') || p.includes('date');
      });
      if (dateLike.length < 2) return false;
      const setVal = (el, value) => {
        el.focus();
        el.value = value;
        el.dispatchEvent(new Event('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
        el.blur();
      };
      setVal(dateLike[0], payload.fromUS);
      setVal(dateLike[1], payload.toUS);
      return true;
    }
    """
    from_us = f"{from_date[5:7]}/{from_date[8:10]}/{from_date[0:4]}"
    to_us = f"{to_date[5:7]}/{to_date[8:10]}/{to_date[0:4]}"
    try:
        return bool(page.evaluate(js, {"fromUS": from_us, "toUS": to_us}))
    except Exception:
        return False


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

    raw = (base_origin or "https://trade.vanquishtrader.com").strip()
    parsed = urllib.parse.urlparse(raw if "://" in raw else f"https://{raw}")
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc or parsed.path
    if not netloc:
        raise RuntimeError("Invalid Base Origin. Expected host like trade.vanquishtrader.com")
    origin = f"{scheme}://{netloc}".rstrip("/")
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
        launch_args = ["--start-maximized", "--window-size=1920,1080"]
        browser = p.chromium.launch(headless=headless, args=launch_args)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            screen={"width": 1920, "height": 1080},
        )
        if debug_dir:
            context.tracing.start(screenshots=True, snapshots=True, sources=True)
        page = context.new_page()
        page.set_default_timeout(timeout_ms)
        page.goto(login_url, wait_until="domcontentloaded")
        # Vanquish login UI can finish client-side hydration after initial paint.
        try:
            page.wait_for_load_state("networkidle", timeout=12000)
        except Exception:
            pass
        page.wait_for_timeout(1200)
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
        # Some broker UIs only enable "Log In" after blur/input events settle.
        try:
            pass_input.press("Tab")
            page.wait_for_timeout(500)
        except Exception:
            pass

        submit_btn = _first_visible(
            page,
            [
                "button[type='submit']",
                "input[type='submit']",
                "button:has-text('Login')",
                "button:has-text('Log In')",
                "button:has-text('Sign in')",
            ],
        )
        if submit_btn:
            enabled = _wait_until_enabled(submit_btn, timeout_ms=20000)
            if not enabled:
                # Trigger another round of form validation for delayed client scripts.
                try:
                    user_input.focus()
                    user_input.press("Tab")
                    pass_input.focus()
                    pass_input.press("Tab")
                    page.wait_for_timeout(1200)
                except Exception:
                    pass
                enabled = _wait_until_enabled(submit_btn, timeout_ms=10000)
            try:
                submit_btn.click(timeout=3000)
            except Exception:
                # Fallback for JS-controlled forms that listen to Enter key.
                pass_input.press("Enter")
        else:
            pass_input.press("Enter")
        shot = _debug_shot(page, debug_dir, "03_after_password_submit.png")
        if shot:
            artifacts.append(shot)

        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except PlaywrightTimeoutError:
            warnings.append("Login post-submit did not reach network idle; continuing.")
        # Post-login workspace can hydrate slowly; allow extra settle time.
        try:
            page.wait_for_timeout(3500)
            page.wait_for_selector(
                "button.button.button-appMenu.button-icon, button[class*='button-appMenu']",
                timeout=20000,
            )
            page.wait_for_timeout(800)
        except Exception:
            warnings.append(
                "Post-login app menu did not become ready in time; continuing with fallbacks."
            )

        if "login" in page.url.lower():
            _debug_write(debug_dir, "03_post_login_dom.html", page.content())
            context.close()
            browser.close()
            raise RuntimeError(
                "Still on login page after submit. Credentials may be invalid or MFA/CAPTCHA is required."
            )

        # Preferred flow: hamburger menu -> Account Statement -> Generate Statement.
        statement_page = page
        menu_clicked = _click_first(
            page,
            [
                "button.button.button-appMenu.button-icon",
                "button[class*='button-appMenu']",
                "button[aria-label*='menu' i]",
                "button[title*='menu' i]",
                "button:has-text('≡')",
                "button:has-text('☰')",
                "div[role='button'][aria-label*='menu' i]",
                "div[role='button']:has-text('≡')",
            ],
        )
        if not menu_clicked:
            warnings.append("Could not click hamburger menu; using statement URL fallback.")
            page.goto(statement_url, wait_until="domcontentloaded")
        else:
            page.wait_for_timeout(600)
            statement_clicked = _click_first(
                page,
                [
                    "text=Account Statement",
                    "a:has-text('Account Statement')",
                    "button:has-text('Account Statement')",
                    "div[role='menuitem']:has-text('Account Statement')",
                ],
            )
            if not statement_clicked:
                warnings.append("Could not open Account Statement from menu; using URL fallback.")
                page.goto(statement_url, wait_until="domcontentloaded")
            else:
                page.wait_for_timeout(900)
                if not _set_statement_period_fields(page, from_date, to_date):
                    warnings.append(
                        "Could not set custom From/To in dialog; using visible defaults."
                    )
                _click_first(page, ["label:has-text('HTML')", "text=HTML"])
                generate_btn = _first_visible(
                    page,
                    [
                        "button:has-text('Generate Statement')",
                        "button:has-text('Generate')",
                        "input[value*='Generate']",
                    ],
                )
                if not generate_btn:
                    warnings.append("Generate Statement button not found; using URL fallback.")
                    page.goto(statement_url, wait_until="domcontentloaded")
                else:
                    try:
                        popup_page = None
                        with context.expect_page(timeout=12000) as popup_info:
                            generate_btn.click(timeout=7000)
                        popup_page = popup_info.value
                        popup_page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
                        try:
                            popup_page.wait_for_load_state("networkidle", timeout=timeout_ms)
                        except PlaywrightTimeoutError:
                            warnings.append(
                                "Generated statement tab opened but did not reach network idle."
                            )
                        statement_page = popup_page
                        warnings.append("Captured statement from generated popup tab.")
                    except Exception:
                        # Some sessions render statement in same tab instead of popup.
                        try:
                            generate_btn.click(timeout=7000)
                            page.wait_for_url("**/account/statement/**", timeout=timeout_ms)
                            statement_page = page
                        except Exception:
                            try:
                                page.wait_for_load_state("networkidle", timeout=timeout_ms)
                                statement_page = page
                            except PlaywrightTimeoutError:
                                warnings.append(
                                    "Generate clicked but navigation confirmation timed out."
                                )
                        try:
                            page.wait_for_load_state("networkidle", timeout=timeout_ms)
                        except PlaywrightTimeoutError:
                            pass

        shot = _debug_shot(page, debug_dir, "04_statement_page.png")
        if shot:
            artifacts.append(shot)
        try:
            statement_page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except PlaywrightTimeoutError:
            warnings.append("Statement page load did not reach network idle; continuing.")

        if statement_page is not page:
            shot = _debug_shot(statement_page, debug_dir, "05_generated_tab.png")
            if shot:
                artifacts.append(shot)

        shot = _debug_shot(statement_page, debug_dir, "05_after_generate.png")
        if shot:
            artifacts.append(shot)

        html_text = statement_page.content()
        html_path = _debug_write(debug_dir, "final_statement.html", html_text)
        if html_path:
            artifacts.append(html_path)
        debug_meta = {
            "login_url": login_url,
            "statement_url": statement_url,
            "workspace_url": page.url,
            "final_url": statement_page.url,
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
