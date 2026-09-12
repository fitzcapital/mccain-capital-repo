#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-status}"
if [[ $# -gt 0 ]]; then
  shift
fi

PODMAN_BIN="${PODMAN_BIN:-$(command -v podman || echo /opt/homebrew/bin/podman)}"
RETENTION_HOURS="${PODMAN_IMAGE_RETENTION_HOURS:-0}"
LOCK_DIR="${PODMAN_STORAGE_LOCK_DIR:-/tmp/mccain-capital-podman-storage.lock}"
APPLY=0

usage() {
  cat <<'EOF'
Usage: ./scripts/manage_podman_storage.sh [status|cleanup|auto] [options]

Commands:
  status                 Show Podman storage and dangling images (default; read-only).
  cleanup                Preview conservative cleanup (read-only unless --apply is supplied).
  auto                   Apply conservative cleanup after a healthy deployment.

Options:
  --apply                Authorize cleanup deletion (required with cleanup).
  --retention-hours N    Keep dangling images newer than N hours (default: 0).
  -h, --help             Show this help.

Safety: cleanup targets dangling images only. It never removes containers, volumes,
tagged current/rollback images, or files in persistent-data/.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)
      APPLY=1
      ;;
    --retention-hours)
      if [[ $# -lt 2 ]]; then
        echo "[podman-storage] --retention-hours requires a value" >&2
        exit 2
      fi
      RETENTION_HOURS="$2"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[podman-storage] unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

case "$MODE" in
  status|cleanup|auto) ;;
  -h|--help)
    usage
    exit 0
    ;;
  *)
    echo "[podman-storage] unknown command: $MODE" >&2
    usage >&2
    exit 2
    ;;
esac

if ! [[ "$RETENTION_HOURS" =~ ^[0-9]+$ ]]; then
  echo "[podman-storage] retention hours must be a non-negative integer" >&2
  exit 2
fi

if ! "$PODMAN_BIN" info >/dev/null 2>&1; then
  echo "[podman-storage] Podman is unavailable; start or repair the Podman machine." >&2
  exit 1
fi

show_status() {
  echo "[podman-storage] storage summary"
  "$PODMAN_BIN" system df
  echo "[podman-storage] note: Podman's Images TOTAL includes shared build layers"
  echo
  echo "[podman-storage] dangling images (cleanup is limited to images older than ${RETENTION_HOURS}h)"
  dangling_images="$("$PODMAN_BIN" images --filter dangling=true \
    --format '{{.ID}}  {{.CreatedSince}}  {{.Size}}' || true)"
  if [[ -n "$dangling_images" ]]; then
    printf '%s\n' "$dangling_images"
  else
    echo "[podman-storage] none"
  fi
}

show_status

if [[ "$MODE" == "status" ]]; then
  exit 0
fi

echo
echo "[podman-storage] policy: dangling images older than ${RETENTION_HOURS}h only"
echo "[podman-storage] protected: running images, tagged images, containers, volumes, persistent data"

if [[ "$MODE" == "cleanup" && "$APPLY" -ne 1 ]]; then
  echo "[podman-storage] preview only; add --apply to perform this cleanup"
  exit 0
fi

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[podman-storage] maintenance is already running; no images were removed" >&2
  exit 1
fi
cleanup_lock() {
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup_lock EXIT INT TERM

echo "[podman-storage] applying conservative cleanup"
"$PODMAN_BIN" image prune --force \
  --filter dangling=true \
  --filter "until=${RETENTION_HOURS}h"

echo
echo "[podman-storage] storage summary after cleanup"
"$PODMAN_BIN" system df
