#!/usr/bin/env python3
"""PF-based blocker for Vanquish domains on macOS.

Usage:
  sudo python scripts/vanquish_pf_blocker.py daemon
  sudo python scripts/vanquish_pf_blocker.py once
  sudo python scripts/vanquish_pf_blocker.py status
  sudo python scripts/vanquish_pf_blocker.py clear
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

DEFAULT_STATE_URL = "http://127.0.0.1:5001/ops/vanquish-lock-state"
DEFAULT_POLL_SECONDS = 10
DEFAULT_ANCHOR_NAME = "com.mccain.vanquish"
DEFAULT_ANCHOR_PATH = "/etc/pf.anchors/com.mccain.vanquish"
DEFAULT_PF_CONF_PATH = "/etc/pf.conf"
MARKER_BEGIN = "# BEGIN MCCAIN VANQUISH ANCHOR"
MARKER_END = "# END MCCAIN VANQUISH ANCHOR"
TZ = ZoneInfo("America/New_York")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DOMAINS_FILE = os.path.join(SCRIPT_DIR, "vanquish_pf_domains.txt")
DEFAULT_MANUAL_LOCK_PATH = os.path.abspath(
    os.path.join(
        SCRIPT_DIR,
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
    source: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PF blocker for Vanquish broker domains")
    p.add_argument("command", choices=["daemon", "once", "status", "clear"])
    p.add_argument("--state-url", default=DEFAULT_STATE_URL)
    p.add_argument("--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS)
    p.add_argument("--anchor-name", default=DEFAULT_ANCHOR_NAME)
    p.add_argument("--anchor-path", default=DEFAULT_ANCHOR_PATH)
    p.add_argument("--pf-conf", default=DEFAULT_PF_CONF_PATH)
    p.add_argument("--domains-file", default=DEFAULT_DOMAINS_FILE)
    p.add_argument("--domains", default="")
    p.add_argument("--manual-lock-path", default=DEFAULT_MANUAL_LOCK_PATH)
    p.add_argument("--no-notify", action="store_true")
    return p.parse_args()


def require_root() -> None:
    if os.geteuid() != 0:
        print("error: this command must run as root (sudo).", file=sys.stderr)
        sys.exit(1)


def _parse_iso_dt(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.astimezone(TZ)
    except ValueError:
        return None


def fetch_lock_state(url: str) -> LockState:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=3) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return LockState(
        active=bool(payload.get("active")),
        unlock_label=str(payload.get("unlock_label") or ""),
        reason=str(payload.get("reason") or ""),
        source="api",
    )


def read_manual_lock_state(path: str) -> LockState:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    until_dt = _parse_iso_dt(str(payload.get("until_at") or ""))
    now_et = datetime.now(TZ)
    active = bool(until_dt and until_dt > now_et)
    return LockState(
        active=active,
        unlock_label=until_dt.strftime("%b %d, %Y %I:%M %p ET") if until_dt else "",
        reason="manual_lock_file_fallback",
        source="manual_lock_file",
    )


def effective_lock_state(*, state_url: str, manual_lock_path: str) -> LockState:
    try:
        return fetch_lock_state(state_url)
    except Exception:
        return read_manual_lock_state(manual_lock_path)


def load_domains(domains_file: str, domains_csv: str) -> list[str]:
    inline = [x.strip() for x in str(domains_csv or "").split(",") if x.strip()]
    if inline:
        return sorted(set(inline))
    out: list[str] = []
    with open(domains_file, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            out.append(line)
    unique = sorted(set(out))
    if not unique:
        raise RuntimeError("domains list is empty")
    return unique


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
    block = f"\n{MARKER_BEGIN}\n" f"{anchor_line}\n" f"{load_line}\n" f"{MARKER_END}\n"
    conf_path.write_text(current.rstrip() + block, encoding="utf-8")
    return True


def build_anchor_rules(domains: list[str], v4_ips: list[str], v6_ips: list[str]) -> str:
    lines = [
        "# Managed by mccain-capital scripts/vanquish_pf_blocker.py",
        f"# Updated: {datetime.now(TZ).isoformat()}",
        f"# Domains: {', '.join(domains)}",
    ]
    if v4_ips:
        lines.append(f"table <vanquish_v4> persist {{ {', '.join(v4_ips)} }}")
        lines.append("block drop out quick inet to <vanquish_v4>")
    if v6_ips:
        lines.append(f"table <vanquish_v6> persist {{ {', '.join(v6_ips)} }}")
        lines.append("block drop out quick inet6 to <vanquish_v6>")
    if not v4_ips and not v6_ips:
        lines.append("# no DNS targets resolved; keeping anchor loaded with no block rules")
    return "\n".join(lines) + "\n"


def write_text(path: str, content: str) -> bool:
    target = Path(path)
    prev = target.read_text(encoding="utf-8") if target.exists() else ""
    if prev == content:
        return False
    target.write_text(content, encoding="utf-8")
    return True


def apply_pf(pf_conf_path: str) -> None:
    _run(["pfctl", "-f", pf_conf_path], check=True)
    # If already enabled, pfctl -e exits non-zero; ignore that case.
    enabled = _run(["pfctl", "-s", "info"], check=False)
    text = (enabled.stdout or "") + (enabled.stderr or "")
    if "Status: Enabled" not in text:
        _run(["pfctl", "-e"], check=False)


def set_block(
    *,
    pf_conf_path: str,
    anchor_name: str,
    anchor_path: str,
    domains: list[str],
) -> tuple[bool, int, int]:
    ensure_anchor_registration(pf_conf_path, anchor_name, anchor_path)
    v4_ips, v6_ips = resolve_domains(domains)
    changed = write_text(anchor_path, build_anchor_rules(domains, v4_ips, v6_ips))
    apply_pf(pf_conf_path)
    return changed, len(v4_ips), len(v6_ips)


def clear_block(*, pf_conf_path: str, anchor_name: str, anchor_path: str) -> bool:
    ensure_anchor_registration(pf_conf_path, anchor_name, anchor_path)
    changed = write_text(
        anchor_path,
        "# Managed by mccain-capital scripts/vanquish_pf_blocker.py\n# block cleared\n",
    )
    apply_pf(pf_conf_path)
    return changed


def read_anchor(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8")
    except OSError:
        return ""


def run_once(args: argparse.Namespace) -> int:
    require_root()
    domains = load_domains(args.domains_file, args.domains)
    state = effective_lock_state(state_url=args.state_url, manual_lock_path=args.manual_lock_path)
    if state.active:
        changed, v4_count, v6_count = set_block(
            pf_conf_path=args.pf_conf,
            anchor_name=args.anchor_name,
            anchor_path=args.anchor_path,
            domains=domains,
        )
        if changed and not args.no_notify:
            notify("Vanquish PF Block Enabled", state.unlock_label or "Lock active")
        print(
            f"blocked source={state.source} v4={v4_count} v6={v6_count} "
            f"until={state.unlock_label or 'unknown'}"
        )
    else:
        changed = clear_block(
            pf_conf_path=args.pf_conf, anchor_name=args.anchor_name, anchor_path=args.anchor_path
        )
        if changed and not args.no_notify:
            notify("Vanquish PF Block Cleared", "Lock inactive")
        print(f"unblocked source={state.source}")
    return 0


def run_status(args: argparse.Namespace) -> int:
    anchor_text = read_anchor(args.anchor_path)
    has_block_rule = "block drop out quick" in anchor_text
    print("pf_anchor_block=on" if has_block_rule else "pf_anchor_block=off")
    try:
        state = effective_lock_state(
            state_url=args.state_url, manual_lock_path=args.manual_lock_path
        )
        print(f"app_lock={'on' if state.active else 'off'}")
        print(f"app_lock_source={state.source}")
        if state.unlock_label:
            print(f"unlock={state.unlock_label}")
        if state.reason:
            print(f"reason={state.reason}")
    except Exception as exc:
        print(f"app_lock=unknown ({exc})")
        return 2
    return 0


def run_clear(args: argparse.Namespace) -> int:
    require_root()
    changed = clear_block(
        pf_conf_path=args.pf_conf, anchor_name=args.anchor_name, anchor_path=args.anchor_path
    )
    print("cleared" if changed else "already_clear")
    return 0


def run_daemon(args: argparse.Namespace) -> int:
    require_root()
    domains = load_domains(args.domains_file, args.domains)
    last_active: bool | None = None
    print(f"watching lock state every {int(args.poll_seconds)}s")
    while True:
        try:
            state = effective_lock_state(
                state_url=args.state_url, manual_lock_path=args.manual_lock_path
            )
            active = bool(state.active)
        except Exception:
            active = last_active if last_active is not None else False
            state = LockState(active=active, unlock_label="", reason="state_error", source="none")

        if active:
            changed, v4_count, v6_count = set_block(
                pf_conf_path=args.pf_conf,
                anchor_name=args.anchor_name,
                anchor_path=args.anchor_path,
                domains=domains,
            )
            if changed and not args.no_notify:
                notify("Vanquish PF Block Enabled", state.unlock_label or "Lock active")
            print(f"[on] source={state.source} v4={v4_count} v6={v6_count}")
        else:
            changed = clear_block(
                pf_conf_path=args.pf_conf,
                anchor_name=args.anchor_name,
                anchor_path=args.anchor_path,
            )
            if changed and not args.no_notify:
                notify("Vanquish PF Block Cleared", "Lock inactive")
            print(f"[off] source={state.source}")

        last_active = active
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
