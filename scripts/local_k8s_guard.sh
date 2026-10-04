#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER_NAME:-mccain-capital}"
NODE_CONTAINER="${CLUSTER_NAME}-control-plane"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="${K8S_NAMESPACE:-mccain-capital}"
HEALTH_URL="${K8S_GUARD_HEALTH_URL:-http://127.0.0.1:5001/healthz}"
FAILURE_THRESHOLD="${K8S_GUARD_FAILURE_THRESHOLD:-3}"
COOLDOWN_SECONDS="${K8S_GUARD_COOLDOWN_SECONDS:-600}"
STATE_DIR="${K8S_GUARD_STATE_DIR:-${HOME}/Library/Application Support/McCain Capital/k8s-guard}"
FAILURE_FILE="${STATE_DIR}/failures"
RECOVERY_FILE="${STATE_DIR}/last-recovery"
LOG_FILE="${STATE_DIR}/guard.log"
LOCK_DIR="${STATE_DIR}/lock"
MODE="run"

case "${1:-}" in
  "") ;;
  --status) MODE="status" ;;
  --check) MODE="check" ;;
  *) echo "Usage: $0 [--status|--check]" >&2; exit 2 ;;
esac

mkdir -p "$STATE_DIR"
touch "$LOG_FILE"

read_number() {
  local path="$1" fallback="${2:-0}" value=""
  [[ -f "$path" ]] && value="$(<"$path")"
  [[ "$value" =~ ^[0-9]+$ ]] && printf '%s' "$value" || printf '%s' "$fallback"
}

log() {
  printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*" >> "$LOG_FILE"
}

health_ok() {
  curl -fsS --max-time 5 "$HEALTH_URL" >/dev/null 2>&1
}

if [[ "$MODE" == "status" ]]; then
  failures="$(read_number "$FAILURE_FILE")"
  last_recovery="$(read_number "$RECOVERY_FILE")"
  if health_ok; then state="healthy"; else state="unavailable"; fi
  printf 'Health: %s\nConsecutive failures: %s/%s\nLast recovery epoch: %s\nLog: %s\n' \
    "$state" "$failures" "$FAILURE_THRESHOLD" "${last_recovery:-0}" "$LOG_FILE"
  exit 0
fi

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  log "skip: guard already running"
  exit 0
fi
trap 'rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

if health_ok; then
  printf '0\n' > "$FAILURE_FILE"
  [[ "$MODE" == "check" ]] && echo "healthy"
  exit 0
fi

failures=$(( $(read_number "$FAILURE_FILE") + 1 ))
printf '%s\n' "$failures" > "$FAILURE_FILE"
log "health failure ${failures}/${FAILURE_THRESHOLD}"

if [[ "$MODE" == "check" || "$failures" -lt "$FAILURE_THRESHOLD" ]]; then
  echo "health unavailable; recovery not triggered (${failures}/${FAILURE_THRESHOLD})"
  exit 1
fi

now="$(date +%s)"
last_recovery="$(read_number "$RECOVERY_FILE")"
if (( now - last_recovery < COOLDOWN_SECONDS )); then
  log "skip: recovery cooldown active"
  exit 1
fi

if ! podman inspect "$NODE_CONTAINER" >/dev/null 2>&1; then
  log "skip: exact node container ${NODE_CONTAINER} not found"
  exit 1
fi

printf '%s\n' "$now" > "$RECOVERY_FILE"
if podman exec "$NODE_CONTAINER" true >/dev/null 2>&1; then
  log "action: restarting web deployment; node can still create processes"
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" rollout restart \
    deployment/mccain-capital-web >> "$LOG_FILE" 2>&1
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" rollout status \
    deployment/mccain-capital-web --timeout=180s >> "$LOG_FILE" 2>&1 || true
else
  log "action: restarting exact node ${NODE_CONTAINER}; node process creation failed"
  podman restart "$NODE_CONTAINER" >> "$LOG_FILE" 2>&1
fi

for _attempt in {1..36}; do
  if health_ok; then
    printf '0\n' > "$FAILURE_FILE"
    log "recovered: ${HEALTH_URL} is healthy"
    exit 0
  fi
  sleep 5
done

log "recovery incomplete: health endpoint still unavailable"
exit 1
