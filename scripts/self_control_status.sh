#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
STATE_PATH="${REPO_ROOT}/persistent-data/.self_control_enforcement_state.json"
STATUS_PATH="${REPO_ROOT}/persistent-data/.self_control_enforcement_status.json"

"${SCRIPT_DIR}/self_control_repair_state.sh" >/dev/null || true

echo "LaunchDaemon:"
if launchctl print system/com.mccain.selfcontrolhosts >/dev/null 2>&1; then
  echo "  com.mccain.selfcontrolhosts: running/loaded"
else
  echo "  com.mccain.selfcontrolhosts: not loaded"
fi

echo
echo "Enforcement status:"
if [[ -f "${STATUS_PATH}" ]]; then
  /usr/bin/python3 - "${STATUS_PATH}" <<'PY'
from __future__ import annotations

import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)

print(f"  installed: {payload.get('installed')}")
print(f"  active: {payload.get('active')}")
print(f"  mode: {payload.get('mode')}")
print(f"  session: {payload.get('session_id')} {payload.get('session_label') or ''}".rstrip())
print(f"  managed_count: {payload.get('managed_count')}")
print(f"  planned_end_at: {payload.get('planned_end_at') or ''}")
print(f"  last_error: {payload.get('last_error') or ''}")
PY
else
  echo "  missing: ${STATUS_PATH}"
fi

echo
echo "Requested state:"
if [[ -f "${STATE_PATH}" ]]; then
  /usr/bin/python3 - "${STATE_PATH}" <<'PY'
from __future__ import annotations

import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)

domains = payload.get("blocked_domains") or []
print(f"  active: {payload.get('active')}")
print(f"  status: {payload.get('status')}")
print(f"  session_id: {payload.get('session_id')}")
print(f"  label: {payload.get('label') or ''}")
print(f"  planned_end_at: {payload.get('planned_end_at') or ''}")
print(f"  blocked_domains: {len(domains)}")
PY
else
  echo "  missing: ${STATE_PATH}"
fi

echo
echo "/etc/hosts block:"
if rg -q "BEGIN MCCAIN SELF CONTROL BLOCK" /etc/hosts; then
  echo "  present"
else
  echo "  absent"
fi
