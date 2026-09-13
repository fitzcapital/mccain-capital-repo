#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="${KIND_CLUSTER_NAME:-mccain-capital}"
NAMESPACE="${K8S_NAMESPACE:-mccain-capital}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
WATCH=0
INTERVAL=30
AUTO_CLEAN=1
AUTO_CLEAN_MIN_DANGLING="${LAPTOP_AUTO_CLEAN_MIN_DANGLING:-3}"
AUTO_CLEAN_COOLDOWN_SECONDS="${LAPTOP_AUTO_CLEAN_COOLDOWN_SECONDS:-3600}"
LAST_CLEANUP_EPOCH=0
DISK_WARN_PERCENT="${LAPTOP_DISK_WARN_PERCENT:-85}"
MEMORY_WARN_FREE_PERCENT="${LAPTOP_MEMORY_WARN_FREE_PERCENT:-10}"

usage() {
  cat <<'EOF'
Usage: ./scripts/monitor_laptop_resources.sh [--watch] [--interval SECONDS] [--no-auto-clean]

  --watch              Refresh continuously; enables conservative auto-cleanup.
  --interval SECONDS   Refresh interval for watch mode (default: 30).
  --no-auto-clean      Disable automatic dangling-image cleanup.
  -h, --help           Show this help.

Auto-cleanup only removes dangling images when at least three accumulate, or when
disk use reaches its warning threshold. It never removes volumes, tagged images,
containers, or application data. One-shot mode is always read-only.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --watch) WATCH=1 ;;
    --no-auto-clean) AUTO_CLEAN=0 ;;
    --interval)
      [[ $# -ge 2 ]] || { echo "--interval requires seconds" >&2; exit 2; }
      INTERVAL="$2"
      shift
      ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

[[ "$INTERVAL" =~ ^[1-9][0-9]*$ ]] || { echo "Interval must be a positive integer" >&2; exit 2; }

warnings=()
cleanup_note=""
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  RESET=$'\033[0m'
  BOLD=$'\033[1m'
  DIM=$'\033[2m'
  CYAN=$'\033[38;5;51m'
  BLUE=$'\033[38;5;75m'
  GREEN=$'\033[38;5;48m'
  YELLOW=$'\033[38;5;220m'
  RED=$'\033[38;5;203m'
  PURPLE=$'\033[38;5;141m'
else
  RESET="" BOLD="" DIM="" CYAN="" BLUE="" GREEN="" YELLOW="" RED="" PURPLE=""
fi

section() {
  printf '\n%s%s%s\n' "$CYAN" "$1" "$RESET"
  printf '%s\n' "────────────────────────────────────────────────────────────────────────"
}

meter() {
  local value="$1" width=20 filled empty color="$GREEN"
  (( value >= 85 )) && color="$RED"
  (( value >= 65 && value < 85 )) && color="$YELLOW"
  filled=$((value * width / 100))
  empty=$((width - filled))
  printf '%s' "$color"
  printf '%*s' "$filled" '' | tr ' ' '█'
  printf '%s' "$DIM"
  printf '%*s' "$empty" '' | tr ' ' '░'
  printf '%s %3s%%%s' "$RESET" "$value" "$RESET"
}

render() {
  warnings=()
  cleanup_note=""
  printf '%s%s🚀 McCAIN CAPITAL · LIVE RESOURCE MONITOR%s\n' "$BOLD" "$PURPLE" "$RESET"
  if [[ "$WATCH" -eq 1 && "$AUTO_CLEAN" -eq 1 ]]; then
    monitor_mode="safe auto-clean armed"
  else
    monitor_mode="read-only"
  fi
  printf '%sUpdated %s · %s%s\n' "$DIM" "$(date '+%b %d, %Y  %I:%M:%S %p %Z')" "$monitor_mode" "$RESET"

  section "💾  LAPTOP DISK"
  disk_line="$(df -h / | awk 'NR == 2 {print}')"
  disk_used="$(df -Pk / | awk 'NR == 2 {gsub(/%/, "", $5); print $5}')"
  meter "${disk_used:-0}"
  printf '  %s\n' "$disk_line"
  if [[ "$disk_used" =~ ^[0-9]+$ ]] && (( disk_used >= DISK_WARN_PERCENT )); then
    warnings+=("Laptop disk is ${disk_used}% full (warning at ${DISK_WARN_PERCENT}%).")
  fi

  section "📦  McCAIN CAPITAL STORAGE"
  printf '%-18s %s\n' "TOTAL REPOSITORY" "$(du -sh "$REPO_ROOT" 2>/dev/null | awk '{print $1}')"
  for path in persistent-data artifacts uploads books; do
    if [[ -e "$REPO_ROOT/$path" ]]; then
      label="$(printf '%s' "$path" | tr '[:lower:]' '[:upper:]')"
      printf '%-18s %s\n' "$label" "$(du -sh "$REPO_ROOT/$path" 2>/dev/null | awk '{print $1}')"
    fi
  done

  section "🧠  MEMORY"
  if command -v memory_pressure >/dev/null 2>&1; then
    memory_line="$(memory_pressure 2>/dev/null | awk '/System-wide memory free percentage/ {print; exit}')"
    memory_free="$(printf '%s\n' "$memory_line" | awk '{gsub(/%/, "", $5); print $5}')"
    memory_used=$((100 - ${memory_free:-0}))
    meter "$memory_used"
    printf '  %s free\n' "${memory_free:-unknown}%"
    if [[ "$memory_free" =~ ^[0-9]+$ ]] && (( memory_free <= MEMORY_WARN_FREE_PERCENT )); then
      warnings+=("Free memory is ${memory_free}% (warning at ${MEMORY_WARN_FREE_PERCENT}%).")
    fi
  else
    vm_stat | head -6
  fi

  section "🐳  PODMAN STORAGE"
  if command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
    dangling_count="$(podman images --filter dangling=true --format '{{.ID}}' | awk 'NF {count++} END {print count+0}')"
    current_epoch="$(date +%s)"
    cleanup_due=0
    if [[ "$WATCH" -eq 1 && "$AUTO_CLEAN" -eq 1 && "$dangling_count" -gt 0 ]]; then
      if (( dangling_count >= AUTO_CLEAN_MIN_DANGLING || disk_used >= DISK_WARN_PERCENT )); then
        if (( current_epoch - LAST_CLEANUP_EPOCH >= AUTO_CLEAN_COOLDOWN_SECONDS )); then
          cleanup_due=1
        fi
      fi
    fi
    if [[ "$cleanup_due" -eq 1 ]]; then
      if "$REPO_ROOT/scripts/manage_podman_storage.sh" auto >/dev/null 2>&1; then
        LAST_CLEANUP_EPOCH="$current_epoch"
        cleanup_note="Removed ${dangling_count} dangling image(s) safely."
      else
        warnings+=("Automatic dangling-image cleanup could not complete.")
      fi
    fi
    podman system df
    if [[ "$WATCH" -eq 1 && "$AUTO_CLEAN" -eq 1 ]]; then
      printf '%s🧹 Auto-clean armed%s · trigger: %s dangling images or %s%% disk · 1h cooldown\n' \
        "$GREEN" "$RESET" "$AUTO_CLEAN_MIN_DANGLING" "$DISK_WARN_PERCENT"
    elif [[ "$WATCH" -eq 1 ]]; then
      printf '%sAuto-clean disabled%s\n' "$DIM" "$RESET"
    else
      printf '%sOne-shot mode · no cleanup performed%s\n' "$DIM" "$RESET"
    fi
    [[ -z "$cleanup_note" ]] || printf '%s✅ %s%s\n' "$GREEN" "$cleanup_note" "$RESET"
  else
    printf '%s● OFFLINE%s Podman is not running\n' "$YELLOW" "$RESET"
  fi

  section "☸️   KUBERNETES"
  if command -v kubectl >/dev/null 2>&1 && kubectl config get-contexts "$KUBE_CONTEXT" >/dev/null 2>&1; then
    pod_rows="$(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods --no-headers 2>/dev/null || true)"
    if [[ -z "$pod_rows" ]]; then
      warnings+=("No Kubernetes pods were found in namespace $NAMESPACE.")
    elif printf '%s\n' "$pod_rows" | awk '
      {split($2, ready, "/")}
      $3 == "Succeeded" || $3 == "Completed" {next}
      ready[1] != ready[2] || $3 != "Running" || $4 + 0 > 0 {bad=1}
      END {exit !bad}
    '; then
      warnings+=("One or more active Kubernetes pods are not ready, not running, or have restarted.")
    fi
    kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods \
      -o custom-columns='NAME:.metadata.name,READY:.status.containerStatuses[0].ready,STATUS:.status.phase,RESTARTS:.status.containerStatuses[0].restartCount'
    printf '\n%sRESOURCE BUDGETS%s\n' "$BLUE" "$RESET"
    kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get deployment \
      -o custom-columns='NAME:.metadata.name,CPU_REQ:.spec.template.spec.containers[0].resources.requests.cpu,CPU_LIMIT:.spec.template.spec.containers[0].resources.limits.cpu,MEM_REQ:.spec.template.spec.containers[0].resources.requests.memory,MEM_LIMIT:.spec.template.spec.containers[0].resources.limits.memory'
    printf '\n%sLIVE USAGE%s\n' "$BLUE" "$RESET"
    kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" top pods 2>/dev/null || \
      printf '%sℹ Metrics warming up · resource limits remain enforced%s\n' "$YELLOW" "$RESET"
  else
    printf '%s● OFFLINE%s Kubernetes context %s is unavailable\n' "$RED" "$RESET" "$KUBE_CONTEXT"
    warnings+=("Kubernetes context $KUBE_CONTEXT is unavailable.")
  fi

  section "🩺  SYSTEM VERDICT"
  if [[ ${#warnings[@]} -eq 0 ]]; then
    printf '%s%s✅ ALL SYSTEMS HEALTHY%s · no problems detected\n' "$BOLD" "$GREEN" "$RESET"
  else
    for warning in "${warnings[@]}"; do
      printf '%s⚠️  %s%s\n' "$RED" "$warning" "$RESET"
    done
  fi

  section "⚡  QUICK ACTIONS"
  printf '%s%-23s%s %s\n' "$BLUE" "Cleanup preview" "$RESET" "./scripts/manage_podman_storage.sh cleanup"
  printf '%s%-23s%s %s\n' "$BLUE" "Safe image cleanup" "$RESET" "./scripts/manage_podman_storage.sh cleanup --apply"
  printf '%s%-23s%s %s\n' "$BLUE" "Detailed K8s status" "$RESET" "./scripts/local_k8s_status.sh"
}

FRAME_FILE=""
if [[ "$WATCH" -eq 1 && -t 1 ]]; then
  FRAME_FILE="$(mktemp -t mccain-resource-monitor.XXXXXX)"
  printf '\033[?25l'
  trap 'rm -f "$FRAME_FILE"; printf "\033[?25h\n"' EXIT INT TERM
fi

while true; do
  if [[ "$WATCH" -eq 1 && -n "$FRAME_FILE" ]]; then
    render > "$FRAME_FILE"
    printf '\033[H\033[J'
    command cat "$FRAME_FILE"
  else
    render
  fi
  [[ "$WATCH" -eq 1 ]] || break
  for ((remaining=INTERVAL; remaining>0; remaining--)); do
    printf '\r%s⟳ Next refresh in %2ss · Ctrl-C to stop%s' "$DIM" "$remaining" "$RESET"
    sleep 1
  done
done
