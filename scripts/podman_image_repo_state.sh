#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
ROOT_DIR="$(cd "$(dirname "${SCRIPT_PATH}")/.." && pwd)"
DATA_DIR="${1:-${DATA_DIR:-$ROOT_DIR/persistent-data}}"
STAMP_FILE="${STAMP_FILE:-$DATA_DIR/.podman-image-repo-stamp}"
STAMP_SCRIPT="${STAMP_SCRIPT:-$ROOT_DIR/scripts/current_repo_stamp.sh}"

if [[ ! -x "$STAMP_SCRIPT" ]]; then
  echo "unknown"
  exit 0
fi

current_stamp="$("$STAMP_SCRIPT")"
saved_stamp=""
if [[ -f "$STAMP_FILE" ]]; then
  saved_stamp="$(tr -d '[:space:]' < "$STAMP_FILE")"
fi

if [[ -z "$saved_stamp" ]]; then
  echo "missing"
elif [[ "$current_stamp" != "$saved_stamp" ]]; then
  echo "stale"
else
  echo "fresh"
fi
