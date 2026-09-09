#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if ! command -v netdata >/dev/null 2>&1; then
  echo "Netdata is not installed. Run: brew install netdata"
  exit 1
fi

NETDATA_CONFIG_DIR="${NETDATA_CONFIG_DIR:-/opt/homebrew/etc/netdata}"
NETDATA_PLUGIN_DIR="${NETDATA_PLUGIN_DIR:-/opt/homebrew/opt/netdata/libexec/netdata/plugins.d}"
mkdir -p "$NETDATA_CONFIG_DIR/health.d" "$NETDATA_PLUGIN_DIR"
cp "$ROOT_DIR/monitoring/netdata/mccain_capital.plugin" \
  "$NETDATA_PLUGIN_DIR/mccain_capital.plugin"
chmod 755 "$NETDATA_PLUGIN_DIR/mccain_capital.plugin"
cp "$ROOT_DIR/monitoring/netdata/health.d/mccain_capital.conf" \
  "$NETDATA_CONFIG_DIR/health.d/mccain_capital.conf"
brew services restart netdata >/dev/null
echo "Netdata is starting: http://127.0.0.1:19999/"
