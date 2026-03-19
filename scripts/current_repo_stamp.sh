#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
ROOT_DIR="$(cd "$(dirname "${SCRIPT_PATH}")/.." && pwd)"
GIT_BIN="${GIT_BIN:-$(command -v git || echo /usr/bin/git)}"
SHASUM_BIN="${SHASUM_BIN:-$(command -v shasum || echo /usr/bin/shasum)}"

if [[ ! -x "$GIT_BIN" ]] || [[ ! -x "$SHASUM_BIN" ]]; then
  echo "missing-tools"
  exit 0
fi

cd "$ROOT_DIR"

tmp_manifest="$(mktemp)"
cleanup() {
  rm -f "$tmp_manifest"
}
trap cleanup EXIT

"$GIT_BIN" ls-files -z --cached --others --exclude-standard \
  -- ':!:artifacts' ':!:persistent-data' ':!:mccain_capital.egg-info' ':!:**/__pycache__' ':!:*.pyc' |
while IFS= read -r -d '' path; do
  [[ -f "$path" ]] || continue
  file_hash="$("$SHASUM_BIN" -a 256 "$path" | awk '{print $1}')"
  printf '%s %s\n' "$file_hash" "$path"
done > "$tmp_manifest"

"$SHASUM_BIN" -a 256 "$tmp_manifest" | awk '{print $1}'
