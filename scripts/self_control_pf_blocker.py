#!/usr/bin/env python3
"""PF-based blocker for Self-Control Mode on macOS.

Usage:
  sudo python3 scripts/self_control_pf_blocker.py daemon
  sudo python3 scripts/self_control_pf_blocker.py once
  sudo python3 scripts/self_control_pf_blocker.py status
  sudo python3 scripts/self_control_pf_blocker.py clear
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/New_York")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
DEFAULT_STATE_PATH = os.path.join(REPO_ROOT, "persistent-data", ".self_control_enforcement_state.json")
DEFAULT_STATUS_PATH = os.path.join(REPO_ROOT, "persistent-data", ".self_control_enforcement_status.json")
DEFAULT_ANCHOR_NAME = "com.mccain.selfcontrol"
DEFAULT_ANCHOR_PATH = "/etc/pf.anchors/com.mccain.selfcontrol"
DEFAULT_PF_CONF_PATH = "/etc/pf.conf"
MARKER_BEGIN = "# BEGIN MCCAIN SELF CONTROL ANCHOR"
MARKER_END = "# END MCCAIN SELF CONTROL ANCHOR"


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Self-Control PF blocker")
    parser.add_argument("command", choices=["daemon", "once", "status", "clear"])
    parser.add_argument("--state-path", default=DEFAULT_STATE_PATH)
    parser.add_argument("--status-path", default=DEFAULT_STATUS_PATH)
    parser.add_argument("--poll-seconds", type=int, default=10)
    parser.add_argument("--anchor-name", default=DEFAULT_ANCHOR_NAME)
    parser.add_argument("--anchor-path", default=DEFAULT_ANCHOR_PATH)
    parser.add_argument("--pf-conf", default=DEFAULT_PF_CONF_PATH)
    parser.add_argument("--no-notify", action="store_true")
    return parser.parse_args()


def require_root() -> None:
    if os.geteuid() != 0:
        print("error: this command must run as root (sudo).", file=sys.stderr)
        sys.exit(1)


def _now_iso() -> str:
    return datetime.now(TZ).isoformat()


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
        blocked_domains=sorted(set(domains)),
    )


def _effective_active(state: EnforcementState) -> bool:
    if not state.active:
        return False
    planned_end = _parse_iso_dt(state.planned_end_at)
    if not planned_end:
        return True
    if planned_end > datetime.now(TZ):
        return True
    return bool(state.unlock_requirement)


def resolve_domains(domains: list[str]) -> tuple[list[str], list[str]]:
    v4: set[str] = set()
    v6: set[str] = set()
    for domain in domains:
        try:
            infos = socket.getaddrinfo(domain, None, proto=socket.IPPROTO_TCP)
        except socket.gaierror:
            continue
        for family, _socktype, _proto, _canonname, sockaddr in infos:
            ip = str(sockaddr[0])
            if family == socket.AF_INET:
                v4.add(ip)
            elif family == socket.AF_INET6:
                v6.add(ip)
    return sorted(v4), sorted(v6)


def _run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def notify(title: str, body: str) -> None:
    safe_title = title.replace('"', "'")
    safe_body = body.replace('"', "'")
    script = f'display notification "{safe_body}" with title "{safe_title}"'
    try:
        subprocess.run(["osascript", "-e", script], check=False)
    except Exception:
        pass


def ensure_anchor_registration(pf_conf_path: str, anchor_name: str, anchor_path: str) -> bool:
    conf_path = Path(pf_conf_path)
    current = conf_path.read_text(encoding="utf-8")
    anchor_line = f'anchor "{anchor_name}"'
    load_line = f'load anchor "{anchor_name}" from "{anchor_path}"'
    if anchor_line in current and load_line in current:
        return False
    block = f"\n{MARKER_BEGIN}\n{anchor_line}\n{load_line}\n{MARKER_END}\n"
    conf_path.write_text(current.rstrip() + block, encoding="utf-8")
    return True


def build_anchor_rules(domains: list[str], v4_ips: list[str], v6_ips: list[str]) -> str:
    lines = [
        "# Managed by mccain-capital scripts/self_control_pf_blocker.py",
        f"# Updated: {_now_iso()}",
        f"# Domains: {', '.join(domains)}",
    ]
    if v4_ips:
        lines.append(f"table <self_control_v4> persist {{ {', '.join(v4_ips)} }}")
        lines.append("block drop out quick inet to <self_control_v4>")
    if v6_ips:
        lines.append(f"table <self_control_v6> persist {{ {', '.join(v6_ips)} }}")
        lines.append("block drop out quick inet6 to <self_control_v6>")
    if not v4_ips and not v6_ips:
        lines.append("# no DNS targets resolved; keeping anchor loaded with no block rules")
    return "\n".join(lines) + "\n"


def write_text(path: str, content: str) -> bool:
    target = Path(path)
    previous = target.read_text(encoding="utf-8") if target.exists() else ""
    if previous == content:
        return False
    target.write_text(content, encoding="utf-8")
    return True


def apply_pf(pf_conf_path: str) -> None:
    _run(["pfctl", "-f", pf_conf_path], check=True)
    enabled = _run(["pfctl", "-s", "info"], check=False)
    text = (enabled.stdout or "") + (enabled.stderr or "")
    if "Status: Enabled" not in text:
        _run(["pfctl", "-e"], check=False)


def set_block(*, pf_conf_path: str, anchor_name: str, anchor_path: str, domains: list[str]) -> tuple[bool, int, int]:
    ensure_anchor_registration(pf_conf_path, anchor_name, anchor_path)
    v4_ips, v6_ips = resolve_domains(domains)
    changed = write_text(anchor_path, build_anchor_rules(domains, v4_ips, v6_ips))
    apply_pf(pf_conf_path)
    return changed, len(v4_ips), len(v6_ips)


def clear_block(*, pf_conf_path: str, anchor_name: str, anchor_path: str) -> bool:
    ensure_anchor_registration(pf_conf_path, anchor_name, anchor_path)
    changed = write_text(
        anchor_path,
        "# Managed by mccain-capital scripts/self_control_pf_blocker.py\n# block cleared\n",
    )
    apply_pf(pf_conf_path)
    return changed


def read_anchor(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8")
    except OSError:
        return ""


def write_status(
    path: str,
    *,
    installed: bool,
    active: bool,
    managed_domains: list[str],
    session: EnforcementState | None,
    last_error: str = "",
) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    payload: dict[str, Any] = {
        "updated_at": _now_iso(),
        "installed": installed,
        "active": active,
        "mode": "pf",
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
            active="block drop out quick" in read_anchor(args.anchor_path),
            managed_domains=[],
            session=None,
            last_error=f"state_read_failed: {exc}",
        )
        print(f"error: unable to read state file: {exc}", file=sys.stderr)
        return 2

    if _effective_active(state) and state.blocked_domains:
        changed, v4_count, v6_count = set_block(
            pf_conf_path=args.pf_conf,
            anchor_name=args.anchor_name,
            anchor_path=args.anchor_path,
            domains=state.blocked_domains,
        )
        if changed and not args.no_notify:
            notify("Self-Control PF Block Enabled", state.label or "Self-Control Mode")
        write_status(
            args.status_path,
            installed=True,
            active=True,
            managed_domains=state.blocked_domains,
            session=state,
        )
        print(f"blocked v4={v4_count} v6={v6_count}")
        return 0

    changed = clear_block(
        pf_conf_path=args.pf_conf,
        anchor_name=args.anchor_name,
        anchor_path=args.anchor_path,
    )
    if changed and not args.no_notify:
        notify("Self-Control PF Block Cleared", "Self-Control Mode is inactive")
    write_status(
        args.status_path,
        installed=True,
        active=False,
        managed_domains=[],
        session=state,
    )
    print("unblocked")
    return 0


def run_status(args: argparse.Namespace) -> int:
    anchor_text = read_anchor(args.anchor_path)
    has_block_rule = "block drop out quick" in anchor_text
    print("pf_anchor_block=on" if has_block_rule else "pf_anchor_block=off")
    try:
        state = _parse_state(args.state_path)
        print(f"state_active={'on' if _effective_active(state) else 'off'}")
        print(f"managed_domains={len(state.blocked_domains)}")
    except Exception as exc:
        print(f"state_error={exc}")
        return 2
    return 0


def run_clear(args: argparse.Namespace) -> int:
    require_root()
    changed = clear_block(
        pf_conf_path=args.pf_conf,
        anchor_name=args.anchor_name,
        anchor_path=args.anchor_path,
    )
    write_status(
        args.status_path,
        installed=True,
        active=False,
        managed_domains=[],
        session=None,
    )
    print("cleared" if changed else "already_clear")
    return 0


def run_daemon(args: argparse.Namespace) -> int:
    require_root()
    print(f"watching {args.state_path} every {max(5, int(args.poll_seconds))}s")
    while True:
        exit_code = run_once(args)
        if exit_code not in {0, 2}:
            return exit_code
        time.sleep(max(5, int(args.poll_seconds)))


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
