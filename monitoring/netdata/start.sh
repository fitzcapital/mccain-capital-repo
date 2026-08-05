#!/usr/bin/env bash
set -euo pipefail

if ! command -v netdata >/dev/null 2>&1; then
  echo "Netdata is not installed. Run: brew install netdata"
  exit 1
fi

brew services start netdata >/dev/null
echo "Netdata is starting: http://127.0.0.1:19999/"
