#!/usr/bin/env python3
"""Auto-block Vanquish broker domains based on app lock state.

Usage:
  sudo python scripts/vanquish_site_blocker.py daemon
  sudo python scripts/vanquish_site_blocker.py once
  sudo python scripts/vanquish_site_blocker.py clear
  sudo python scripts/vanquish_site_blocker.py status
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

DEFAULT_STATE_URL = "http://127.0.0.1:5001/ops/vanquish-lock-state"
DEFAULT_DOMAINS = (
    "trade.vanquishtrader.com",
    "app.vanquishtrader.com",
    "api.vanquishtrader.com",
    "www.vanquishtrader.com",
    "vanquishtrader.com",
)
DEFAULT_HOSTS_PATH = "/etc/hosts"
MARKER_BEGIN = "# BEGIN MCCAIN VANQUISH BLOCK"
MARKER_END = "# END MCCAIN VANQUISH BLOCK"
TZ = ZoneInfo("America/New_York")
DEFAULT_MANUAL_LOCK_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "persistent-data",
        "uploads",
        ".vanquish_manual_lock.json",
    )
)


@dataclass
class LockState:
    active: bool
    unlock_label: str
    reason: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Vanquish broker site blocker")
    p.add_argument("command", choices=["daemon", "once", "status", "clear"])
    p.add_argument("--state-url", default=DEFAULT_STATE_URL)
    p.add_argument("--poll-seconds", type=int, default=15)
    p.add_argument("--hosts-path", default=DEFAULT_HOSTS_PATH)
    p.add_argument("--redirect-ip", default="127.0.0.1")
    p.add_argument("--redirect-ipv6", default="::1")
    p.add_argument("--domains", default=",".join(DEFAULT_DOMAINS))
    p.add_argument("--manual-lock-path", default=DEFAULT_MANUAL_LOCK_PATH)
    p.add_argument("--no-notify", action="store_true")
    return p.parse_args()


def fetch_lock_state(url: str) -> LockState:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=3) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return LockState(
        active=bool(payload.get("active")),
        unlock_label=str(payload.get("unlock_label") or ""),
        reason=str(payload.get("reason") or ""),
    )


def _parse_iso_dt(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.astimezone(TZ)
    except ValueError:
        return None


def read_manual_lock_state(path: str) -> LockState:
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as exc:
        raise RuntimeError(f"manual lock file unavailable: {exc}") from exc
    until_dt = _parse_iso_dt(str(payload.get("until_at") or ""))
    now_et = datetime.now(TZ)
    active = bool(until_dt and until_dt > now_et)
    return LockState(
        active=active,
        unlock_label=until_dt.strftime("%b %d, %Y %I:%M %p ET") if until_dt else "",
        reason="manual_lock_file_fallback",
    )


def _read_hosts(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _write_hosts(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def _build_block_block(domains: list[str], redirect_ip: str, redirect_ipv6: str) -> str:
    rows = [MARKER_BEGIN]
    for d in domains:
        rows.append(f"{redirect_ip} {d}")
        rows.append(f"{redirect_ipv6} {d}")
    rows.append(MARKER_END)
    return "\n".join(rows) + "\n"


def _strip_marker_block(text: str) -> str:
    start = text.find(MARKER_BEGIN)
    if start < 0:
        return text
    end = text.find(MARKER_END, start)
    if end < 0:
        return text
    end = end + len(MARKER_END)
    if end < len(text) and text[end : end + 1] == "\n":
        end += 1
    return text[:start] + text[end:]


def hosts_has_block(path: str) -> bool:
    try:
        txt = _read_hosts(path)
    except OSError:
        return False
    return MARKER_BEGIN in txt and MARKER_END in txt


def apply_hosts_block(path: str, domains: list[str], redirect_ip: str, redirect_ipv6: str) -> bool:
    txt = _read_hosts(path)
    cleaned = _strip_marker_block(txt).rstrip() + "\n\n"
    block = _build_block_block(domains, redirect_ip, redirect_ipv6)
    new_txt = cleaned + block
    if new_txt == txt:
        return False
    _write_hosts(path, new_txt)
    return True


def clear_hosts_block(path: str) -> bool:
    txt = _read_hosts(path)
    new_txt = _strip_marker_block(txt)
    if new_txt == txt:
        return False
    _write_hosts(path, new_txt)
    return True


def flush_dns_cache() -> None:
    cmds = [
        ["dscacheutil", "-flushcache"],
        ["killall", "-HUP", "mDNSResponder"],
    ]
    for cmd in cmds:
        try:
            subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass


def notify(title: str, body: str) -> None:
    safe_title = title.replace('"', "'")
    safe_body = body.replace('"', "'")
    script = f'display notification "{safe_body}" with title "{safe_title}"'
    try:
        subprocess.run(["osascript", "-e", script], check=False)
    except Exception:
        pass


def ensure_root_writable(path: str) -> None:
    if os.access(path, os.W_OK):
        return
    print(f"error: need write access to {path}. Run with sudo.", file=sys.stderr)
    sys.exit(1)


def run_once(args: argparse.Namespace) -> int:
    ensure_root_writable(args.hosts_path)
    domains = [x.strip() for x in str(args.domains).split(",") if x.strip()]
    try:
        state = fetch_lock_state(args.state_url)
    except Exception as exc:
        try:
            state = read_manual_lock_state(args.manual_lock_path)
            print(f"warn: lock-state endpoint unavailable, using manual lock file fallback ({exc})")
        except Exception as fallback_exc:
            print(f"error: invalid lock state response: {exc}", file=sys.stderr)
            print(f"error: manual lock fallback failed: {fallback_exc}", file=sys.stderr)
            return 2

    if state.active:
        changed = apply_hosts_block(
            args.hosts_path, domains, args.redirect_ip, args.redirect_ipv6
        )
        if changed:
            flush_dns_cache()
            if not args.no_notify:
                notify("Vanquish Block Enabled", state.unlock_label or "Lock is active")
            print(f"blocked ({', '.join(domains)}) until {state.unlock_label or 'unlock'}")
        else:
            print("blocked (already active)")
    else:
        changed = clear_hosts_block(args.hosts_path)
        if changed:
            flush_dns_cache()
            if not args.no_notify:
                notify("Vanquish Block Cleared", "Lock is inactive")
            print("unblocked")
        else:
            print("unblocked (already clear)")
    return 0


def run_status(args: argparse.Namespace) -> int:
    blocked = hosts_has_block(args.hosts_path)
    print("hosts_block=on" if blocked else "hosts_block=off")
    try:
        state = fetch_lock_state(args.state_url)
        print(f"app_lock={'on' if state.active else 'off'}")
        if state.unlock_label:
            print(f"unlock={state.unlock_label}")
        if state.reason:
            print(f"reason={state.reason}")
    except Exception as exc:
        try:
            state = read_manual_lock_state(args.manual_lock_path)
            print("app_lock_source=manual_lock_file_fallback")
            print(f"app_lock={'on' if state.active else 'off'}")
            if state.unlock_label:
                print(f"unlock={state.unlock_label}")
            if state.reason:
                print(f"reason={state.reason}")
            return 0
        except Exception:
            print(f"app_lock=unknown ({exc})")
            return 2
    return 0


def run_daemon(args: argparse.Namespace) -> int:
    ensure_root_writable(args.hosts_path)
    print(f"watching {args.state_url} every {args.poll_seconds}s")
    last_effective: bool | None = None
    while True:
        try:
            state = fetch_lock_state(args.state_url)
            effective = bool(state.active)
        except Exception:
            try:
                state = read_manual_lock_state(args.manual_lock_path)
                effective = bool(state.active)
            except Exception:
                # fail-safe: keep current hosts state unchanged on fetch errors
                effective = last_effective if last_effective is not None else False
                state = LockState(active=effective, unlock_label="", reason="lock_state_unavailable")

        if effective:
            changed = apply_hosts_block(
                args.hosts_path,
                [x.strip() for x in str(args.domains).split(",") if x.strip()],
                args.redirect_ip,
                args.redirect_ipv6,
            )
            if changed:
                flush_dns_cache()
                if not args.no_notify:
                    notify("Vanquish Block Enabled", state.unlock_label or "Lock active")
                print(f"[on] {state.unlock_label}")
        else:
            changed = clear_hosts_block(args.hosts_path)
            if changed:
                flush_dns_cache()
                if not args.no_notify:
                    notify("Vanquish Block Cleared", "Lock inactive")
                print("[off]")

        last_effective = effective
        time.sleep(max(5, int(args.poll_seconds)))


def run_clear(args: argparse.Namespace) -> int:
    ensure_root_writable(args.hosts_path)
    changed = clear_hosts_block(args.hosts_path)
    if changed:
        flush_dns_cache()
    print("cleared" if changed else "already clear")
    return 0


def main() -> int:
    args = parse_args()
    if args.command == "once":
        return run_once(args)
    if args.command == "status":
        return run_status(args)
    if args.command == "clear":
        return run_clear(args)
    return run_daemon(args)


if __name__ == "__main__":
    raise SystemExit(main())
