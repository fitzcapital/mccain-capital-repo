#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
ROOT_DIR="$(cd "$(dirname "${SCRIPT_PATH}")/.." && pwd)"
CONTAINER_NAME="${CONTAINER_NAME:-mccain-capital-app}"
HOST_PORT="${HOST_PORT:-5001}"
MAX_CLOCK_SKEW_SECONDS="${MAX_CLOCK_SKEW_SECONDS:-15}"
DATA_DIR="${DATA_DIR:-$ROOT_DIR/persistent-data}"
LOG_DIR="${LOG_DIR:-$DATA_DIR/logs}"
PODMAN_BIN="${PODMAN_BIN:-$(command -v podman || echo /opt/homebrew/bin/podman)}"
PODMAN_MACHINE_NAME="${PODMAN_MACHINE_NAME:-podman-machine-applehv}"
CURL_BIN="${CURL_BIN:-$(command -v curl || echo /usr/bin/curl)}"
START_SCRIPT="${START_SCRIPT:-$ROOT_DIR/scripts/start_mccain_capital_on_login.sh}"
LOCK_DIR="${LOCK_DIR:-/tmp/mccain-capital-runtime-watchdog.lock}"

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/runtime-watchdog.log"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  exit 0
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

log() {
  printf '[runtime-watchdog] %s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >> "$LOG_FILE"
}

recover_app() {
  log "$1"
  "$START_SCRIPT" >> "$LOG_DIR/launchd.stdout.log" 2>> "$LOG_DIR/launchd.stderr.log"
}

if ! "$PODMAN_BIN" info >/dev/null 2>&1; then
  recover_app "podman unavailable; starting runtime"
  exit 0
fi

if ! "$CURL_BIN" -sf "http://127.0.0.1:${HOST_PORT}/healthz" >/dev/null 2>&1; then
  recover_app "health check failed; recovering app"
  exit 0
fi

container_epoch="$($PODMAN_BIN exec "$CONTAINER_NAME" date +%s 2>/dev/null || true)"
host_epoch="$(date +%s)"
if [[ ! "$container_epoch" =~ ^[0-9]+$ ]]; then
  recover_app "container clock unavailable; recovering app"
  exit 0
fi

clock_skew=$((host_epoch - container_epoch))
if (( clock_skew < 0 )); then
  clock_skew=$((-clock_skew))
fi

if (( clock_skew > MAX_CLOCK_SKEW_SECONDS )); then
  log "clock skew ${clock_skew}s exceeds ${MAX_CLOCK_SKEW_SECONDS}s; synchronizing VM clock"
  sync_epoch="$(date +%s)"
  if ! "$PODMAN_BIN" machine ssh "$PODMAN_MACHINE_NAME" -- \
    sudo date -u -s "@$sync_epoch" >/dev/null 2>&1; then
    log "VM clock synchronization failed; leaving the healthy app running"
    exit 1
  fi

  corrected_epoch="$($PODMAN_BIN exec "$CONTAINER_NAME" date +%s 2>/dev/null || true)"
  if [[ ! "$corrected_epoch" =~ ^[0-9]+$ ]]; then
    log "VM clock synchronized but container clock could not be verified"
    exit 1
  fi

  corrected_skew=$((host_epoch - corrected_epoch))
  if (( corrected_skew < 0 )); then
    corrected_skew=$((-corrected_skew))
  fi
  if (( corrected_skew > MAX_CLOCK_SKEW_SECONDS )); then
    log "VM clock still differs by ${corrected_skew}s after synchronization"
    exit 1
  fi
  log "VM clock synchronized; corrected skew ${corrected_skew}s"
fi
