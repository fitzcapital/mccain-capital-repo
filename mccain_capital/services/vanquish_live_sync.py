"""Vanquish live-login statement sync helpers.

This module automates broker login and statement generation using Playwright.
It intentionally keeps credential handling ephemeral: callers pass credentials
per request and decide whether to persist anything locally.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import threading
import time
import urllib.parse
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    import fcntl
except Exception:  # pragma: no cover - non-POSIX fallback
    fcntl = None

SELECTOR_PROFILE_VERSION = "2026-05-27.v3"
_BROWSER_BOOT_LOCK = threading.Lock()
_BROWSER_BOOT_LOCK_PATH = os.path.join(tempfile.gettempdir(), "mccain_browser_boot.lock")
_RESOURCE_ERROR_MARKERS = (
    "resource temporarily unavailable",
    "can't start new thread",
    "cannot start new thread",
    "pthread_create",
    "[errno 11]",
)
SELECTOR_PROFILES: Dict[str, List[str]] = {
    "login_user": [
        "[data-testid='login_user_name']",
        "[data-test-id='login_user_name']",
        "[data-test='login_user_name']",
        "#login_user_name",
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
    "login_password": [
        "[data-testid='login_password']",
        "[data-test-id='login_password']",
        "[data-test='login_password']",
        "#login_password",
        "input[name='password']",
        "input[type='password']",
        "input[id*='pass']",
        "input[autocomplete='current-password']",
        "input[placeholder*='Password' i]",
    ],
    "login_submit": [
        "[data-testid='login_submit_button']",
        "[data-test-id='login_submit_button']",
        "[data-test='login_submit_button']",
        "#login_submit_button",
        "button[type='submit']",
        "input[type='submit']",
        "button:has-text('Login')",
        "button:has-text('Log In')",
        "button:has-text('Sign in')",
    ],
    "workspace_menu": [
        "button.button.button-appMenu.button-icon",
        "button[class*='button-appMenu']",
        "button[aria-label*='menu' i]",
        "button[title*='menu' i]",
        "button:has-text('≡')",
        "button:has-text('☰')",
        "div[role='button'][aria-label*='menu' i]",
        "div[role='button']:has-text('≡')",
    ],
    "account_statement_menu_item": [
        "text=Account Statement",
        "a:has-text('Account Statement')",
        "button:has-text('Account Statement')",
        "div[role='menuitem']:has-text('Account Statement')",
    ],
    "generate_statement_button": [
        "button:has-text('Generate Statement')",
        "button:has-text('Generate')",
        "input[value*='Generate']",
    ],
}


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


def _wait_for_first_visible(
    page,
    selectors: List[str],
    *,
    timeout_ms: int = 6000,
    poll_ms: int = 250,
):
    deadline = time.time() + (max(0, timeout_ms) / 1000.0)
    while time.time() <= deadline:
        found = _first_visible(page, selectors)
        if found:
            return found
        try:
            page.wait_for_timeout(max(50, poll_ms))
        except Exception:
            time.sleep(max(0.05, poll_ms / 1000.0))
    return None


def _wait_for_login_username(page, *, timeout_ms: int = 20000):
    user_input = _wait_for_first_visible(
        page,
        SELECTOR_PROFILES["login_user"],
        timeout_ms=timeout_ms,
        poll_ms=250,
    )
    if user_input:
        return user_input
    try:
        page.wait_for_selector("#loginFormContainer", timeout=5000)
        page.wait_for_timeout(1500)
    except Exception:
        pass
    return _first_visible(page, SELECTOR_PROFILES["login_user"])


def _selector_counts(page, selectors: List[str]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for selector in selectors:
        count = 0
        for ctx in _contexts(page):
            try:
                count += int(ctx.locator(selector).count())
            except Exception:
                continue
        out[selector] = count
    return out


def _control_summaries(page) -> Dict[str, List[Dict[str, str]]]:
    script = """
    () => {
      const pick = (el) => ({
        tag: (el.tagName || '').toLowerCase(),
        type: el.getAttribute('type') || '',
        id: el.id || '',
        name: el.getAttribute('name') || '',
        class: el.getAttribute('class') || '',
        placeholder: el.getAttribute('placeholder') || '',
        autocomplete: el.getAttribute('autocomplete') || '',
        testid:
          el.getAttribute('data-testid') ||
          el.getAttribute('data-test-id') ||
          el.getAttribute('data-test') ||
          '',
        ariaLabel: el.getAttribute('aria-label') || '',
        visible: !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length),
      });
      return {
        inputs: Array.from(document.querySelectorAll('input')).slice(0, 24).map(pick),
        buttons: Array.from(
          document.querySelectorAll('button,input[type="submit"]')
        ).slice(0, 24).map(pick),
      };
    }
    """
    try:
        out = page.evaluate(script)
        if isinstance(out, dict):
            return {
                "inputs": list(out.get("inputs") or []),
                "buttons": list(out.get("buttons") or []),
            }
    except Exception:
        pass
    return {"inputs": [], "buttons": []}


def _login_probe_payload(page) -> Dict[str, Any]:
    frame_urls: List[str] = []
    for ctx in _contexts(page):
        try:
            frame_urls.append(str(getattr(ctx, "url", "") or ""))
        except Exception:
            frame_urls.append("")
    controls = _control_summaries(page)
    return {
        "url": str(getattr(page, "url", "") or ""),
        "frame_urls": frame_urls,
        "login_form_container_count": _selector_counts(page, ["#loginFormContainer"]).get(
            "#loginFormContainer", 0
        ),
        "selector_counts": {
            "login_user": _selector_counts(page, SELECTOR_PROFILES["login_user"]),
            "login_password": _selector_counts(page, SELECTOR_PROFILES["login_password"]),
            "login_submit": _selector_counts(page, SELECTOR_PROFILES["login_submit"]),
        },
        "inputs": controls["inputs"],
        "buttons": controls["buttons"],
        "selector_profile_version": SELECTOR_PROFILE_VERSION,
    }


def _write_login_probe(page, debug_dir: Optional[str]) -> Optional[str]:
    return _debug_write(
        debug_dir,
        "login_probe.json",
        json.dumps(_login_probe_payload(page), indent=2),
    )


def _debug_write(debug_dir: Optional[str], name: str, content: str) -> Optional[str]:
    if not debug_dir:
        return None
    try:
        os.makedirs(debug_dir, exist_ok=True)
        path = os.path.join(debug_dir, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path
    except OSError:
        return None


def _debug_shot(page, debug_dir: Optional[str], name: str) -> Optional[str]:
    if not debug_dir:
        return None
    try:
        os.makedirs(debug_dir, exist_ok=True)
        path = os.path.join(debug_dir, name)
        page.screenshot(path=path, full_page=True)
        return path
    except OSError:
        return None


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


def _stage_error(stage: str, msg: str) -> RuntimeError:
    return RuntimeError(f"[stage:{stage}] {msg}")


def _safe_close(obj: Any) -> None:
    if obj is None:
        return
    try:
        obj.close()
    except Exception:
        pass


def _is_resource_error(message: str) -> bool:
    text = str(message or "").strip().lower()
    return any(marker in text for marker in _RESOURCE_ERROR_MARKERS)


def _validate_statement_html(
    html_text: str,
    *,
    final_url: str = "",
    status: Optional[int] = None,
    headers: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    text = html_text or ""
    lowered = text.lower()
    url_lower = (final_url or "").lower()
    content_type = str((headers or {}).get("content-type") or "").lower()
    has_table = "<table" in lowered
    has_html_shape = "<html" in lowered or has_table or "<!doctype html" in lowered
    has_login_url = "login" in url_lower and "/account/statement/" not in url_lower
    has_login_form = (
        "password" in lowered
        and "login" in lowered
        and ("<input" in lowered or "loginform" in lowered or "sign in" in lowered)
    )
    has_statement_marker = any(
        marker in lowered
        for marker in (
            "account statement",
            "/account/statement/",
            "transaction time",
            "ending balance",
            "net liquidating",
            "instrument",
        )
    )
    has_trade_marker = bool(
        re.search(
            r"\b(?:spx|ndx|qqq|spy|es|mes|nq|mnq)\s+[a-z]{3}/\d{1,2}/\d{2}\s+",
            lowered,
        )
    )
    has_balance_marker = bool(
        re.search(
            r"\b(?:ending\s+balance|net\s+liquidating\s+value|account\s+value|balance)\b",
            lowered,
        )
    )
    markers = {
        "has_table": has_table,
        "has_html_shape": has_html_shape,
        "has_login_url": has_login_url,
        "has_login_form": has_login_form,
        "has_statement_marker": has_statement_marker,
        "has_trade_marker": has_trade_marker,
        "has_balance_marker": has_balance_marker,
        "content_type": content_type,
    }

    if status is not None and int(status) != 200:
        return {
            "ok": False,
            "reason": f"Statement request returned HTTP {status}.",
            "markers": markers,
        }
    if not text.strip():
        return {"ok": False, "reason": "Statement HTML was empty.", "markers": markers}
    if has_login_url or has_login_form:
        return {
            "ok": False,
            "reason": "Captured broker login page instead of statement HTML.",
            "markers": markers,
        }
    if content_type and "html" not in content_type and not has_html_shape:
        return {
            "ok": False,
            "reason": f"Statement response was not HTML ({content_type}).",
            "markers": markers,
        }
    if not has_html_shape:
        return {
            "ok": False,
            "reason": "Statement response did not look like HTML.",
            "markers": markers,
        }
    if not (has_statement_marker or has_trade_marker or has_balance_marker):
        return {
            "ok": False,
            "reason": "Statement HTML lacked statement, trade, and balance markers.",
            "markers": markers,
        }
    if not (has_table or has_trade_marker or has_balance_marker):
        return {
            "ok": False,
            "reason": "Statement HTML had no table, trade rows, or balance text.",
            "markers": markers,
        }
    return {"ok": True, "reason": "Statement HTML validated.", "markers": markers}


def _capture_result(
    *,
    method: str,
    html_text: str,
    final_url: str,
    validation: Dict[str, Any],
    status: Optional[int] = None,
    headers: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "method": method,
        "html_text": html_text or "",
        "final_url": final_url or "",
        "status": status,
        "headers": headers or {},
        "validation": validation,
    }


def _write_capture_artifacts(
    debug_dir: Optional[str],
    artifacts: List[str],
    *,
    stem: str,
    capture: Dict[str, Any],
) -> None:
    html_path = _debug_write(debug_dir, f"{stem}.html", str(capture.get("html_text") or ""))
    if html_path:
        artifacts.append(html_path)
    meta_path = _debug_write(
        debug_dir,
        f"{stem}_meta.json",
        json.dumps(
            {
                "capture_method": capture.get("method"),
                "http_status": capture.get("status"),
                "final_url": capture.get("final_url"),
                "validation_result": capture.get("validation"),
                "headers": capture.get("headers") or {},
            },
            indent=2,
        ),
    )
    if meta_path:
        artifacts.append(meta_path)


def _capture_attempt_summary(capture: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "method": capture.get("method"),
        "http_status": capture.get("status"),
        "final_url": capture.get("final_url"),
        "validation_result": capture.get("validation"),
    }


def _capture_rejection_reason(capture: Dict[str, Any]) -> str:
    validation = capture.get("validation") if isinstance(capture, dict) else None
    if isinstance(validation, dict):
        return str(validation.get("reason") or "validation failed")
    return "validation failed"


def _capture_is_valid(capture: Dict[str, Any], warnings: List[str], prefix: str) -> bool:
    validation = capture.get("validation") if isinstance(capture, dict) else None
    if isinstance(validation, dict) and validation.get("ok"):
        return True
    warnings.append(f"{prefix}: {_capture_rejection_reason(capture)}")
    return False


def _fetch_statement_html_with_context_request(
    context: Any,
    statement_url: str,
    *,
    timeout_ms: int,
) -> Dict[str, Any]:
    response = context.request.get(
        statement_url,
        timeout=timeout_ms,
        headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
    )
    status = int(getattr(response, "status", 0) or 0)
    headers = dict(getattr(response, "headers", {}) or {})
    html_text = response.text()
    final_url = str(getattr(response, "url", "") or statement_url)
    validation = _validate_statement_html(
        html_text,
        final_url=final_url,
        status=status,
        headers=headers,
    )
    return _capture_result(
        method="authenticated_request",
        html_text=html_text,
        final_url=final_url,
        status=status,
        headers=headers,
        validation=validation,
    )


def _capture_statement_from_page(
    page: Any,
    *,
    method: str,
    status: Optional[int] = None,
) -> Dict[str, Any]:
    html_text = page.content()
    final_url = str(getattr(page, "url", "") or "")
    validation = _validate_statement_html(html_text, final_url=final_url, status=status)
    return _capture_result(
        method=method,
        html_text=html_text,
        final_url=final_url,
        status=status,
        validation=validation,
    )


@contextmanager
def _browser_boot_gate(timeout_s: float = 20.0):
    deadline = time.time() + timeout_s
    file_handle = None
    acquired_thread = False
    try:
        while time.time() < deadline:
            acquired_thread = _BROWSER_BOOT_LOCK.acquire(
                timeout=min(0.5, max(0.05, deadline - time.time()))
            )
            if not acquired_thread:
                continue
            if fcntl is None:
                yield
                return
            os.makedirs(os.path.dirname(_BROWSER_BOOT_LOCK_PATH), exist_ok=True)
            file_handle = open(_BROWSER_BOOT_LOCK_PATH, "a+", encoding="utf-8")
            try:
                fcntl.flock(file_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                yield
                return
            except BlockingIOError:
                file_handle.close()
                file_handle = None
                _BROWSER_BOOT_LOCK.release()
                acquired_thread = False
                time.sleep(0.15)
                continue
        raise _stage_error(
            "system_resource",
            "Chromium startup resources are busy. Another browser boot is still active.",
        )
    finally:
        if file_handle is not None:
            try:
                if fcntl is not None:
                    fcntl.flock(file_handle.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                file_handle.close()
            except Exception:
                pass
        if acquired_thread:
            _BROWSER_BOOT_LOCK.release()


def _bootstrap_browser_session(
    *,
    playwright,
    headless: bool,
    timeout_ms: int,
    debug_dir: Optional[str],
    warnings: List[str],
    mark: Callable[[str, str], None],
):
    launch_profiles = [
        {
            "label": "standard",
            "args": [
                "--start-maximized",
                "--window-size=1920,1080",
                "--disable-dev-shm-usage",
                "--no-first-run",
            ],
            "context_kwargs": {
                "viewport": {"width": 1920, "height": 1080},
                "screen": {"width": 1920, "height": 1080},
            },
        },
        {
            "label": "lean",
            "args": [
                "--window-size=1440,900",
                "--disable-dev-shm-usage",
                "--no-first-run",
                "--disable-gpu",
                "--disable-software-rasterizer",
                "--disable-features=VizDisplayCompositor",
            ],
            "context_kwargs": {
                "viewport": {"width": 1440, "height": 900},
                "screen": {"width": 1440, "height": 900},
            },
        },
    ]
    last_error = ""
    with _browser_boot_gate():
        for attempt, profile in enumerate(launch_profiles, start=1):
            browser = None
            context = None
            page = None
            tracing_enabled = False
            try:
                attempt_label = (
                    "Starting Chromium session."
                    if attempt == 1
                    else f"Retrying Chromium session with {profile['label']} profile."
                )
                mark("browser_boot", attempt_label)
                browser = playwright.chromium.launch(
                    headless=headless,
                    args=profile["args"],
                    chromium_sandbox=False,
                )
                context = browser.new_context(**profile["context_kwargs"])
                if debug_dir:
                    try:
                        os.makedirs(debug_dir, exist_ok=True)
                        context.tracing.start(screenshots=True, snapshots=True, sources=True)
                        tracing_enabled = True
                    except OSError:
                        warnings.append("Debug capture disabled: cannot write to debug directory.")
                        debug_dir = None
                page = context.new_page()
                page.set_default_timeout(timeout_ms)
                return browser, context, page, tracing_enabled, debug_dir
            except Exception as e:
                last_error = str(e).strip() or e.__class__.__name__
                warnings.append(
                    f"Browser bootstrap attempt {attempt} ({profile['label']}) failed: "
                    f"{last_error}."
                )
                _safe_close(context)
                _safe_close(browser)
                if attempt < len(launch_profiles):
                    if _is_resource_error(last_error):
                        mark(
                            "system_resource",
                            "Chromium launch hit resource pressure. Retrying with lean profile.",
                        )
                        time.sleep(1.0)
                    else:
                        time.sleep(0.5)
                    continue
    raise _stage_error(
        "system_resource" if _is_resource_error(last_error) else "browser_boot",
        f"Chromium session could not be created. {last_error}",
    )


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
    progress_cb: Optional[Callable[[str, str], None]] = None,
) -> Tuple[str, List[str], List[str], Dict[str, Any]]:
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

    current_stage = "init"
    login_urls = [f"{origin}/#/login", login_url]
    meta: Dict[str, Any] = {
        "selector_profile_version": SELECTOR_PROFILE_VERSION,
        "used_statement_url_fallback": False,
        "opened_statement_popup": False,
        "login_url_candidates": login_urls,
    }

    def mark(stage: str, message: str) -> None:
        nonlocal current_stage
        current_stage = stage
        if progress_cb:
            try:
                progress_cb(stage, message)
            except Exception:
                pass

    with sync_playwright() as p:
        browser, context, page, tracing_enabled, debug_dir = _bootstrap_browser_session(
            playwright=p,
            headless=headless,
            timeout_ms=timeout_ms,
            debug_dir=debug_dir,
            warnings=warnings,
            mark=mark,
        )
        mark("open_login", "Opening broker login page.")
        opened_login_url = login_url
        user_input = None
        for candidate_url in login_urls:
            opened_login_url = candidate_url
            page.goto(candidate_url, wait_until="domcontentloaded")
            # Vanquish login UI can finish client-side hydration after initial paint.
            try:
                page.wait_for_load_state("networkidle", timeout=12000)
            except Exception:
                pass
            page.wait_for_timeout(1200)
            user_input = _wait_for_login_username(page, timeout_ms=8000)
            if user_input:
                break
            warnings.append(f"Login form did not hydrate at {candidate_url}; trying fallback.")
        meta["opened_login_url"] = opened_login_url
        shot = _debug_shot(page, debug_dir, "01_open_login.png")
        if shot:
            artifacts.append(shot)

        mark("locate_username", "Finding username field.")
        if not user_input:
            user_input = _wait_for_login_username(page, timeout_ms=12000)
        if not user_input:
            warnings.append("Login form did not hydrate in time; reloading login page once.")
            try:
                page.goto(opened_login_url, wait_until="domcontentloaded")
                try:
                    page.wait_for_load_state("networkidle", timeout=12000)
                except Exception:
                    pass
                page.wait_for_timeout(1500)
            except Exception:
                pass
            retry_shot = _debug_shot(page, debug_dir, "01_open_login_retry.png")
            if retry_shot:
                artifacts.append(retry_shot)
            user_input = _wait_for_login_username(page, timeout_ms=12000)
        if not user_input:
            _debug_write(debug_dir, "01_login_dom.html", page.content())
            _write_login_probe(page, debug_dir)
            _safe_close(context)
            _safe_close(browser)
            raise _stage_error("locate_username", "Could not locate username/email field.")

        mark("fill_username", "Entering username.")
        user_input.fill(username)
        shot = _debug_shot(page, debug_dir, "02_after_username.png")
        if shot:
            artifacts.append(shot)

        mark("locate_password", "Finding password field.")
        pass_input = _first_visible(page, SELECTOR_PROFILES["login_password"])
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
            pass_input = _first_visible(page, SELECTOR_PROFILES["login_password"])
        if not pass_input:
            _debug_write(debug_dir, "02_username_step_dom.html", page.content())
            _safe_close(context)
            _safe_close(browser)
            raise _stage_error("locate_password", "Could not locate password field.")

        mark("submit_login", "Submitting broker login.")
        pass_input.fill(password)
        # Some broker UIs only enable "Log In" after blur/input events settle.
        try:
            pass_input.press("Tab")
            page.wait_for_timeout(500)
        except Exception:
            pass

        submit_btn = _first_visible(page, SELECTOR_PROFILES["login_submit"])
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
            _safe_close(context)
            _safe_close(browser)
            raise _stage_error(
                "submit_login",
                "Still on login page after submit. Credentials may be invalid or "
                "MFA/CAPTCHA is required.",
            )

        mark("capture_statement_request", "Fetching statement HTML from authenticated session.")
        capture: Optional[Dict[str, Any]] = None
        capture_attempts: List[Dict[str, Any]] = []
        try:
            request_capture = _fetch_statement_html_with_context_request(
                context,
                statement_url,
                timeout_ms=timeout_ms,
            )
            _write_capture_artifacts(
                debug_dir,
                artifacts,
                stem="statement_request",
                capture=request_capture,
            )
            capture_attempts.append(_capture_attempt_summary(request_capture))
            mark("validate_statement_html", "Validating statement HTML.")
            if _capture_is_valid(
                request_capture,
                warnings,
                "Authenticated statement request rejected",
            ):
                capture = request_capture
        except Exception as e:
            warnings.append(f"Authenticated statement request failed: {e}")

        if capture is None:
            mark("capture_statement_html", "Opening statement URL directly.")
            meta["used_statement_url_fallback"] = True
            try:
                response = page.goto(
                    statement_url,
                    wait_until="domcontentloaded",
                    timeout=timeout_ms,
                )
                try:
                    page.wait_for_load_state("networkidle", timeout=timeout_ms)
                except PlaywrightTimeoutError:
                    warnings.append("Direct statement page did not reach network idle; continuing.")
                status = int(getattr(response, "status", 0) or 0) if response else None
                page_capture = _capture_statement_from_page(
                    page,
                    method="direct_page_navigation",
                    status=status,
                )
                _write_capture_artifacts(
                    debug_dir,
                    artifacts,
                    stem="direct_statement_page",
                    capture=page_capture,
                )
                capture_attempts.append(_capture_attempt_summary(page_capture))
                mark("validate_statement_html", "Validating statement HTML.")
                if _capture_is_valid(page_capture, warnings, "Direct statement page rejected"):
                    capture = page_capture
            except Exception as e:
                warnings.append(f"Direct statement page capture failed: {e}")

        if capture is None:
            mark("ui_statement_fallback", "Using broker UI fallback.")
            meta["used_ui_fallback"] = True
            statement_page = page
            response_capture: Optional[Dict[str, Any]] = None

            def capture_statement_response(response) -> None:
                nonlocal response_capture
                if response_capture is not None:
                    return
                try:
                    response_url = str(getattr(response, "url", "") or "")
                    if "/account/statement/" not in response_url:
                        return
                    body = response.text()
                    status = int(getattr(response, "status", 0) or 0)
                    headers = dict(getattr(response, "headers", {}) or {})
                    validation = _validate_statement_html(
                        body,
                        final_url=response_url,
                        status=status,
                        headers=headers,
                    )
                    candidate = _capture_result(
                        method="ui_response_intercept",
                        html_text=body,
                        final_url=response_url,
                        status=status,
                        headers=headers,
                        validation=validation,
                    )
                    if validation.get("ok"):
                        response_capture = candidate
                except Exception:
                    return

            try:
                page.on("response", capture_statement_response)
            except Exception:
                warnings.append("Could not attach statement response interceptor.")

            try:
                if "/account/statement/" in str(getattr(page, "url", "") or ""):
                    page.goto(origin, wait_until="domcontentloaded", timeout=timeout_ms)
                    try:
                        page.wait_for_load_state("networkidle", timeout=12000)
                    except PlaywrightTimeoutError:
                        pass
                    page.wait_for_timeout(1200)
            except Exception:
                warnings.append("Could not return to workspace before UI fallback.")

            mark("open_workspace_menu", "Opening workspace menu.")
            menu_clicked = _click_first(page, SELECTOR_PROFILES["workspace_menu"])
            if not menu_clicked:
                warnings.append("Could not click hamburger menu during UI fallback.")
            else:
                page.wait_for_timeout(600)
                mark("open_statement_dialog", "Opening statement dialog.")
                statement_clicked = _click_first(
                    page,
                    SELECTOR_PROFILES["account_statement_menu_item"],
                )
                if not statement_clicked:
                    warnings.append(
                        "Could not open Account Statement from menu during UI fallback."
                    )
                else:
                    page.wait_for_timeout(900)
                    mark("configure_statement_period", "Setting statement date range.")
                    if not _set_statement_period_fields(page, from_date, to_date):
                        warnings.append(
                            "Could not set custom From/To in dialog; using visible defaults."
                        )
                    _click_first(page, ["label:has-text('HTML')", "text=HTML"])
                    generate_btn = _first_visible(
                        page,
                        SELECTOR_PROFILES["generate_statement_button"],
                    )
                    if not generate_btn:
                        warnings.append("Generate Statement button not found during UI fallback.")
                    else:
                        mark("generate_statement", "Generating statement HTML.")
                        try:
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
                            meta["opened_statement_popup"] = True
                            warnings.append("Captured statement from generated popup tab.")
                        except Exception:
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

            if response_capture is not None:
                _write_capture_artifacts(
                    debug_dir,
                    artifacts,
                    stem="ui_fallback_statement",
                    capture=response_capture,
                )
                capture = response_capture
                capture_attempts.append(_capture_attempt_summary(response_capture))
            else:
                page_capture = _capture_statement_from_page(
                    statement_page,
                    method="ui_page_content",
                )
                _write_capture_artifacts(
                    debug_dir,
                    artifacts,
                    stem="ui_fallback_statement",
                    capture=page_capture,
                )
                capture_attempts.append(_capture_attempt_summary(page_capture))
                if _capture_is_valid(page_capture, warnings, "UI statement page rejected"):
                    capture = page_capture

        if capture is None:
            meta["capture_attempts"] = capture_attempts
            raise _stage_error(
                "capture_statement_html",
                "Could not capture validated statement HTML from authenticated session "
                "or UI fallback.",
            )

        html_text = str(capture.get("html_text") or "")
        html_path = _debug_write(debug_dir, "final_statement.html", html_text)
        if html_path:
            artifacts.append(html_path)
        meta.update(
            {
                "capture_method": capture.get("method"),
                "http_status": capture.get("status"),
                "final_url": capture.get("final_url"),
                "statement_url": statement_url,
                "validation_result": capture.get("validation"),
                "used_ui_fallback": bool(meta.get("used_ui_fallback")),
                "capture_attempts": capture_attempts,
            }
        )
        debug_meta = {
            "login_url": meta.get("opened_login_url") or login_url,
            "statement_url": statement_url,
            "workspace_url": page.url,
            "final_url": capture.get("final_url"),
            "stage": current_stage,
            "selector_profile_version": SELECTOR_PROFILE_VERSION,
            "warnings": warnings,
            "meta": meta,
        }
        meta_path = _debug_write(debug_dir, "debug_meta.json", json.dumps(debug_meta, indent=2))
        if meta_path:
            artifacts.append(meta_path)
        if debug_dir and tracing_enabled:
            trace_path = os.path.join(debug_dir, "trace.zip")
            try:
                context.tracing.stop(path=trace_path)
                artifacts.append(trace_path)
            except OSError:
                warnings.append("Trace artifact skipped: debug directory is not writable.")
        _safe_close(context)
        _safe_close(browser)

    validation_result = meta.get("validation_result") if isinstance(meta, dict) else None
    if isinstance(validation_result, dict) and not validation_result.get("ok"):
        raise _stage_error(
            "capture_statement_html",
            str(validation_result.get("reason") or "Statement HTML validation failed."),
        )
    lowered = html_text.lower()
    if "password" in lowered and "login" in lowered:
        raise _stage_error(
            "capture_statement_html", "Received login page instead of statement HTML."
        )
    warnings.append(f"Selector profile: {SELECTOR_PROFILE_VERSION}")
    meta["stage"] = current_stage
    return html_text, warnings, artifacts, meta
