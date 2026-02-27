#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://127.0.0.1:5001}"
OUT_DIR="docs/images"

echo "Refreshing README screenshots from ${BASE_URL} -> ${OUT_DIR}"
mkdir -p "${OUT_DIR}"

VISUAL_BASE_URL="${BASE_URL}" \
VISUAL_OUT_DIR="${OUT_DIR}" \
python tests/visual_smoke.py

echo "Done. Updated screenshot files in ${OUT_DIR}."
