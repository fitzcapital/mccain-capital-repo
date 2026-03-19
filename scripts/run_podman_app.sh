#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
ROOT_DIR="$(cd "$(dirname "${SCRIPT_PATH}")/.." && pwd)"
IMAGE_NAME="${IMAGE_NAME:-localhost/mccain-capital-app:latest}"
CONTAINER_NAME="${CONTAINER_NAME:-mccain-capital-app}"
HOST_PORT="${HOST_PORT:-5001}"
DATA_DIR="${DATA_DIR:-$ROOT_DIR/persistent-data}"
PODMAN_BIN="${PODMAN_BIN:-$(command -v podman || echo /opt/homebrew/bin/podman)}"
RG_BIN="${RG_BIN:-$(command -v rg || echo /opt/homebrew/bin/rg)}"
CURL_BIN="${CURL_BIN:-$(command -v curl || echo /usr/bin/curl)}"
REPAIR_DB_SCRIPT="${REPAIR_DB_SCRIPT:-$ROOT_DIR/scripts/repair_sqlite_mount_db.sh}"
STAMP_SCRIPT="${STAMP_SCRIPT:-$ROOT_DIR/scripts/current_repo_stamp.sh}"
STAMP_FILE="${STAMP_FILE:-$DATA_DIR/.podman-image-repo-stamp}"

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

mkdir -p "$DATA_DIR/uploads" "$DATA_DIR/books"

if [[ -x "$REPAIR_DB_SCRIPT" ]]; then
  "$REPAIR_DB_SCRIPT" "$DATA_DIR" || true
fi

current_stamp=""
if [[ -x "$STAMP_SCRIPT" ]]; then
  current_stamp="$("$STAMP_SCRIPT")"
fi

cd "$ROOT_DIR"

if ! "$PODMAN_BIN" info >/dev/null 2>&1; then
  echo "[run_podman_app] starting podman machine"
  "$PODMAN_BIN" machine start >/dev/null 2>&1 || true
fi

echo "[run_podman_app] building $IMAGE_NAME"
"$PODMAN_BIN" build -t "$IMAGE_NAME" -f Containerfile .

if "$PODMAN_BIN" ps -a --format '{{.Names}}' | "$RG_BIN" -x "$CONTAINER_NAME" >/dev/null 2>&1; then
  echo "[run_podman_app] removing existing container $CONTAINER_NAME"
  "$PODMAN_BIN" rm -f "$CONTAINER_NAME" >/dev/null
fi

echo "[run_podman_app] starting $CONTAINER_NAME on port $HOST_PORT"
"$PODMAN_BIN" run -d \
  --name "$CONTAINER_NAME" \
  -p "$HOST_PORT:5001" \
  -v "$DATA_DIR:/data" \
  "$IMAGE_NAME" >/dev/null

echo "[run_podman_app] waiting for healthz"
for _ in $(seq 1 30); do
  if "$CURL_BIN" -sf "http://127.0.0.1:${HOST_PORT}/healthz" >/dev/null; then
    break
  fi
  sleep 1
done

"$CURL_BIN" -sf "http://127.0.0.1:${HOST_PORT}/healthz"
echo
if [[ -n "$current_stamp" ]]; then
  printf '%s\n' "$current_stamp" > "$STAMP_FILE"
fi
echo "[run_podman_app] container is up"
echo "[run_podman_app] local: http://127.0.0.1:${HOST_PORT}"
