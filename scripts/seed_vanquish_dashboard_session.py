#!/usr/bin/env python3
"""Seed a portable Playwright storage state for Vanquish dashboard Google login."""

from __future__ import annotations

import os
from pathlib import Path


def main() -> int:
    account = os.environ.get("VANQUISH_DASHBOARD_ACCOUNT", "OEV0059123").strip()
    data_dir = Path(os.environ.get("PERSISTENT_DATA_DIR", "persistent-data"))
    state_path = Path(
        os.environ.get(
            "VANQUISH_DASHBOARD_STORAGE_STATE",
            str(data_dir / "vanquish-dashboard-storage-state.json"),
        )
    )
    profile_dir = Path(
        os.environ.get(
            "VANQUISH_DASHBOARD_PROFILE_DIR",
            str(data_dir / "vanquish-dashboard-profile"),
        )
    )
    state_path.parent.mkdir(parents=True, exist_ok=True)
    profile_dir.mkdir(parents=True, exist_ok=True)

    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise SystemExit(
            "Playwright is required. Install project dependencies and run "
            "`python -m playwright install chromium`."
        ) from exc

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            str(profile_dir),
            headless=False,
            args=["--window-size=1440,1000", "--no-first-run"],
            viewport={"width": 1440, "height": 1000},
            screen={"width": 1440, "height": 1000},
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.goto("https://www.vanquishtrader.com/dashboard/accounts", wait_until="domcontentloaded")
        print("Sign into Vanquish with Google in the opened browser.")
        print(f"Waiting until account {account} appears, then saving {state_path}.")
        try:
            page.get_by_text(account, exact=False).wait_for(timeout=10 * 60 * 1000)
            page.wait_for_timeout(1500)
        except Exception:
            input("Account was not detected automatically. Press Enter after login is complete...")
        context.storage_state(path=str(state_path))
        context.close()

    print(f"Saved Vanquish dashboard storage state: {state_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
