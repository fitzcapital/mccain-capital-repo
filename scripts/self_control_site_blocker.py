#!/usr/bin/env python3
"""Hosts-based blocker for Self-Control Mode on macOS.

Usage:
  sudo python3 scripts/self_control_site_blocker.py daemon
  sudo python3 scripts/self_control_site_blocker.py once
  sudo python3 scripts/self_control_site_blocker.py status
  sudo python3 scripts/self_control_site_blocker.py clear
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/New_York")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
DEFAULT_STATE_PATH = os.path.join(REPO_ROOT, "persistent-data", ".self_control_enforcement_state.json")
DEFAULT_STATUS_PATH = os.path.join(REPO_ROOT, "persistent-data", ".self_control_enforcement_status.json")
DEFAULT_HOSTS_PATH = "/etc/hosts"
MARKER_BEGIN = "# BEGIN MCCAIN SELF CONTROL BLOCK"
MARKER_END = "# END MCCAIN SELF CONTROL BLOCK"


@dataclass
class EnforcementState:
    active: bool
    status: str
    session_id: int
    label: str
    strict_mode: bool
    planned_end_at: str
    unlock_requirement: str
    blocked_domains: list[str]
    blocked_categories: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Self-Control Mode hosts blocker")
    parser.add_argument("command", choices=["daemon", "once", "status", "clear"])
    parser.add_argument("--state-path", default=DEFAULT_STATE_PATH)
    parser.add_argument("--status-path", default=DEFAULT_STATUS_PATH)
    parser.add_argument("--hosts-path", default=DEFAULT_HOSTS_PATH)
    parser.add_argument("--redirect-ip", default="127.0.0.1")
    parser.add_argument("--redirect-ipv6", default="::1")
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--no-notify", action="store_true")
    return parser.parse_args()


def require_root() -> None:
    if os.geteuid() != 0:
        print("error: this command must run as root (sudo).", file=sys.stderr)
        sys.exit(1)


def _now_iso() -> str:
    return datetime.now(TZ).isoformat()


def _parse_state(path: str) -> EnforcementState:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RuntimeError("invalid state payload")
    domains = []
    for raw in list(payload.get("blocked_domains") or []):
        text = str(raw or "").strip().lower()
        if text and "." in text:
            domains.append(text)
    return EnforcementState(
        active=bool(payload.get("active")),
        status=str(payload.get("status") or "idle").strip(),
        session_id=int(payload.get("session_id") or 0),
        label=str(payload.get("label") or "").strip(),
        strict_mode=bool(payload.get("strict_mode")),
        planned_end_at=str(payload.get("planned_end_at") or "").strip(),
        unlock_requirement=str(payload.get("unlock_requirement") or "").strip(),
        blocked_domains=sorted(set(_expand_domains(domains))),
        blocked_categories=[
            str(item or "").strip() for item in list(payload.get("blocked_categories") or []) if str(item or "").strip()
        ],
    )


def _parse_iso_dt(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ)


def _effective_active(state: EnforcementState) -> bool:
    if not state.active:
        return False
    planned_end = _parse_iso_dt(state.planned_end_at)
    if not planned_end:
        return True
    if planned_end > datetime.now(TZ):
        return True
    return bool(state.unlock_requirement)


def _expand_domains(domains: list[str]) -> list[str]:
    expanded: set[str] = set()
    for domain in domains:
        expanded.add(domain)
        if not domain.startswith("www."):
            expanded.add(f"www.{domain}")
    return sorted(expanded)


def _read_hosts(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def _write_hosts(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)


def _strip_marker_block(content: str) -> str:
    start = content.find(MARKER_BEGIN)
    if start < 0:
        return content
    end = content.find(MARKER_END, start)
    if end < 0:
        return content
    end += len(MARKER_END)
    if end < len(content) and content[end : end + 1] == "\n":
        end += 1
    return content[:start] + content[end:]


def _build_block_block(domains: list[str], redirect_ip: str, redirect_ipv6: str) -> str:
    lines = [MARKER_BEGIN]
    for domain in domains:
        lines.append(f"{redirect_ip} {domain}")
        lines.append(f"{redirect_ipv6} {domain}")
    lines.append(MARKER_END)
    return "\n".join(lines) + "\n"


def hosts_has_block(path: str) -> bool:
    try:
        current = _read_hosts(path)
    except OSError:
        return False
    return MARKER_BEGIN in current and MARKER_END in current


def apply_hosts_block(path: str, domains: list[str], redirect_ip: str, redirect_ipv6: str) -> bool:
    current = _read_hosts(path)
    cleaned = _strip_marker_block(current).rstrip() + "\n\n"
    block = _build_block_block(domains, redirect_ip, redirect_ipv6)
    next_content = cleaned + block
    if next_content == current:
        return False
    _write_hosts(path, next_content)
    return True


def clear_hosts_block(path: str) -> bool:
    current = _read_hosts(path)
    next_content = _strip_marker_block(current)
    if next_content == current:
        return False
    _write_hosts(path, next_content)
    return True


def flush_dns_cache() -> None:
    commands = [
        ["dscacheutil", "-flushcache"],
        ["killall", "-HUP", "mDNSResponder"],
    ]
    for command in commands:
        try:
            subprocess.run(command, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
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


def write_status(
    path: str,
    *,
    installed: bool,
    active: bool,
    mode: str,
    managed_domains: list[str],
    session: EnforcementState | None,
    last_error: str = "",
) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    payload: dict[str, Any] = {
        "updated_at": _now_iso(),
        "installed": installed,
        "active": active,
        "mode": mode,
        "managed_domains": managed_domains,
        "managed_count": len(managed_domains),
        "session_id": int(session.session_id if session else 0),
        "session_label": str(session.label if session else ""),
        "planned_end_at": str(session.planned_end_at if session else ""),
        "last_checked_at": _now_iso(),
        "last_error": last_error,
    }
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def run_once(args: argparse.Namespace) -> int:
    require_root()
    try:
        state = _parse_state(args.state_path)
    except Exception as exc:
        write_status(
            args.status_path,
            installed=True,
            active=hosts_has_block(args.hosts_path),
            mode="hosts",
            managed_domains=[],
            session=None,
            last_error=f"state_read_failed: {exc}",
        )
        print(f"error: unable to read state file: {exc}", file=sys.stderr)
        return 2

    if _effective_active(state) and state.blocked_domains:
        changed = apply_hosts_block(
            args.hosts_path, state.blocked_domains, args.redirect_ip, args.redirect_ipv6
        )
        if changed:
            flush_dns_cache()
            if not args.no_notify:
                label = state.label or "Self-Control Mode"
                notify("Self-Control Block Enabled", label)
        write_status(
            args.status_path,
            installed=True,
            active=True,
            mode="hosts",
            managed_domains=state.blocked_domains,
            session=state,
        )
        print(f"blocked {len(state.blocked_domains)} domains")
        return 0

    changed = clear_hosts_block(args.hosts_path)
    if changed:
        flush_dns_cache()
        if not args.no_notify:
            notify("Self-Control Block Cleared", "Self-Control Mode is inactive")
    write_status(
        args.status_path,
        installed=True,
        active=False,
        mode="hosts",
        managed_domains=[],
        session=state,
    )
    print("unblocked")
    return 0


def run_status(args: argparse.Namespace) -> int:
    blocked = hosts_has_block(args.hosts_path)
    print("hosts_block=on" if blocked else "hosts_block=off")
    try:
        state = _parse_state(args.state_path)
        print(f"state_active={'on' if state.active else 'off'}")
        print(f"state_status={state.status}")
        print(f"managed_domains={len(state.blocked_domains)}")
    except Exception as exc:
        print(f"state_error={exc}")
        return 2
    return 0


def run_clear(args: argparse.Namespace) -> int:
    require_root()
    changed = clear_hosts_block(args.hosts_path)
    if changed:
        flush_dns_cache()
    write_status(
        args.status_path,
        installed=True,
        active=False,
        mode="hosts",
        managed_domains=[],
        session=None,
    )
    print("cleared" if changed else "already clear")
    return 0


def run_daemon(args: argparse.Namespace) -> int:
    require_root()
    print(f"watching {args.state_path} every {max(2, int(args.poll_seconds))}s")
    while True:
        exit_code = run_once(args)
        if exit_code not in {0, 2}:
            return exit_code
        time.sleep(max(2, int(args.poll_seconds)))


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
