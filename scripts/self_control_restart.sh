#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINUTES="${1:-60}"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run with sudo/root:"
  echo "  sudo ${0} [minutes]"
  exit 1
fi

"${SCRIPT_DIR}/self_control_off.sh"
"${SCRIPT_DIR}/self_control_on.sh" "${MINUTES}"
