#!/usr/bin/env bash
set -euo pipefail

brew services stop netdata >/dev/null
echo "Local Netdata stopped. McCain Capital was not touched."
