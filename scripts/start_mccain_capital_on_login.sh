#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
ROOT_DIR="$(cd "$(dirname "${SCRIPT_PATH}")/.." && pwd)"
CONTAINER_NAME="${CONTAINER_NAME:-mccain-capital-app}"
HOST_PORT="${HOST_PORT:-5001}"
DATA_DIR="${DATA_DIR:-$ROOT_DIR/persistent-data}"
LOG_DIR="${LOG_DIR:-$DATA_DIR/logs}"
IMAGE_NAME="${IMAGE_NAME:-localhost/mccain-capital-app:latest}"
PODMAN_BIN="${PODMAN_BIN:-$(command -v podman || echo /opt/homebrew/bin/podman)}"
RG_BIN="${RG_BIN:-$(command -v rg || echo /opt/homebrew/bin/rg)}"
CURL_BIN="${CURL_BIN:-$(command -v curl || echo /usr/bin/curl)}"
REPAIR_DB_SCRIPT="${REPAIR_DB_SCRIPT:-$ROOT_DIR/scripts/repair_sqlite_mount_db.sh}"
RUN_SCRIPT="${RUN_SCRIPT:-$ROOT_DIR/scripts/run_podman_app.sh}"
REPO_STATE_SCRIPT="${REPO_STATE_SCRIPT:-$ROOT_DIR/scripts/podman_image_repo_state.sh}"

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

mkdir -p "$DATA_DIR/uploads" "$DATA_DIR/books" "$LOG_DIR"

if [[ -x "$REPAIR_DB_SCRIPT" ]]; then
  LOG_FILE="$LOG_DIR/launch-agent.log" "$REPAIR_DB_SCRIPT" "$DATA_DIR" || true
fi

echo "[login-start] $(date '+%Y-%m-%d %H:%M:%S') starting McCain Capital" >> "$LOG_DIR/launch-agent.log"

if ! "$PODMAN_BIN" info >/dev/null 2>&1; then
  echo "[login-start] starting podman machine" >> "$LOG_DIR/launch-agent.log"
  "$PODMAN_BIN" machine start >/dev/null 2>&1 || true
  sleep 3
fi

repo_state="unknown"
if [[ -x "$REPO_STATE_SCRIPT" ]]; then
  repo_state="$("$REPO_STATE_SCRIPT" "$DATA_DIR")"
fi

needs_rebuild=0
if [[ "$repo_state" != "fresh" ]]; then
  needs_rebuild=1
fi
if ! "$PODMAN_BIN" image exists "$IMAGE_NAME" >/dev/null 2>&1; then
  needs_rebuild=1
fi

if [[ "$needs_rebuild" -eq 1 && -x "$RUN_SCRIPT" ]]; then
  echo "[login-start] repo/image state is ${repo_state}; rebuilding image from source" >> "$LOG_DIR/launch-agent.log"
  "$RUN_SCRIPT" >> "$LOG_DIR/launchd.stdout.log" 2>> "$LOG_DIR/launchd.stderr.log"
  exit 0
fi

if "$CURL_BIN" -sf "http://127.0.0.1:${HOST_PORT}/healthz" >/dev/null 2>&1; then
  echo "[login-start] app already healthy on port ${HOST_PORT}" >> "$LOG_DIR/launch-agent.log"
  exit 0
fi

if "$PODMAN_BIN" ps --format '{{.Names}}' | "$RG_BIN" -x "$CONTAINER_NAME" >/dev/null 2>&1; then
  echo "[login-start] container already running; waiting for health" >> "$LOG_DIR/launch-agent.log"
elif "$PODMAN_BIN" ps -a --format '{{.Names}}' | "$RG_BIN" -x "$CONTAINER_NAME" >/dev/null 2>&1; then
  echo "[login-start] starting existing container ${CONTAINER_NAME}" >> "$LOG_DIR/launch-agent.log"
  "$PODMAN_BIN" start "$CONTAINER_NAME" >/dev/null
elif "$PODMAN_BIN" image exists "$IMAGE_NAME" >/dev/null 2>&1; then
  echo "[login-start] recreating container ${CONTAINER_NAME} from cached image ${IMAGE_NAME}" >> "$LOG_DIR/launch-agent.log"
  "$PODMAN_BIN" run -d \
    --name "$CONTAINER_NAME" \
    -p "$HOST_PORT:5001" \
    -v "$DATA_DIR:/data" \
    "$IMAGE_NAME" >/dev/null
else
  echo "[login-start] image ${IMAGE_NAME} missing and rebuild helper unavailable" >> "$LOG_DIR/launch-agent.log"
  exit 1
fi

for _ in $(seq 1 30); do
  if "$CURL_BIN" -sf "http://127.0.0.1:${HOST_PORT}/healthz" >/dev/null 2>&1; then
    echo "[login-start] app healthy on port ${HOST_PORT}" >> "$LOG_DIR/launch-agent.log"
    exit 0
  fi
  sleep 1
done

echo "[login-start] app failed to become healthy on port ${HOST_PORT}" >> "$LOG_DIR/launch-agent.log"
exit 1
