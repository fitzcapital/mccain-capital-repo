#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="${KIND_CLUSTER_NAME:-mccain-capital}"
NAMESPACE="${K8S_NAMESPACE:-mccain-capital}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
WATCH=0
INTERVAL=10
DISK_WARN_PERCENT="${LAPTOP_DISK_WARN_PERCENT:-85}"
MEMORY_WARN_FREE_PERCENT="${LAPTOP_MEMORY_WARN_FREE_PERCENT:-10}"

usage() {
  cat <<'EOF'
Usage: ./scripts/monitor_laptop_resources.sh [--watch] [--interval SECONDS]

  --watch              Refresh continuously until Ctrl-C.
  --interval SECONDS   Refresh interval for watch mode (default: 10).
  -h, --help           Show this help.

This command is read-only. It never removes images, volumes, or application data.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --watch) WATCH=1 ;;
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

section() {
  printf '\n[%s]\n' "$1"
}

render() {
warnings=()
if [[ "$WATCH" -eq 1 ]]; then
  printf '\033[2J\033[H'
fi
printf 'McCain Capital resource monitor · %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')"

section "Laptop disk"
df -h / | awk 'NR == 1 || NR == 2 {print}'
disk_used="$(df -Pk / | awk 'NR == 2 {gsub(/%/, "", $5); print $5}')"
if [[ "$disk_used" =~ ^[0-9]+$ ]] && (( disk_used >= DISK_WARN_PERCENT )); then
  warnings+=("Laptop disk is ${disk_used}% full (warning at ${DISK_WARN_PERCENT}%).")
fi

section "McCain Capital disk"
du -sh "$REPO_ROOT" 2>/dev/null || true
for path in persistent-data artifacts uploads books; do
  if [[ -e "$REPO_ROOT/$path" ]]; then
    du -sh "$REPO_ROOT/$path" 2>/dev/null || true
  fi
done

section "Mac memory pressure"
if command -v memory_pressure >/dev/null 2>&1; then
  memory_line="$(memory_pressure 2>/dev/null | awk '/System-wide memory free percentage/ {print; exit}')"
  echo "${memory_line:-Memory pressure details unavailable}"
  memory_free="$(printf '%s\n' "$memory_line" | awk '{gsub(/%/, "", $5); print $5}')"
  if [[ "$memory_free" =~ ^[0-9]+$ ]] && (( memory_free <= MEMORY_WARN_FREE_PERCENT )); then
    warnings+=("Free memory is ${memory_free}% (warning at ${MEMORY_WARN_FREE_PERCENT}%).")
  fi
else
  vm_stat | head -6
fi

section "Podman storage"
if command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
  podman system df
else
  echo "Podman is not running"
fi

section "Kubernetes workloads"
if command -v kubectl >/dev/null 2>&1 && kubectl config get-contexts "$KUBE_CONTEXT" >/dev/null 2>&1; then
  pod_rows="$(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods --no-headers 2>/dev/null || true)"
  if [[ -z "$pod_rows" ]]; then
    warnings+=("No Kubernetes pods were found in namespace $NAMESPACE.")
  elif printf '%s\n' "$pod_rows" | awk '
    {split($2, ready, "/")}
    ready[1] != ready[2] || $3 != "Running" || $4 + 0 > 0 {bad=1}
    END {exit !bad}
  '; then
    warnings+=("One or more Kubernetes pods are not ready, not running, or have restarted.")
  fi
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pods \
    -o custom-columns='NAME:.metadata.name,READY:.status.containerStatuses[0].ready,STATUS:.status.phase,RESTARTS:.status.containerStatuses[0].restartCount'
  echo
  echo "Declared resource budgets"
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get deployment \
    -o custom-columns='NAME:.metadata.name,CPU_REQ:.spec.template.spec.containers[0].resources.requests.cpu,CPU_LIMIT:.spec.template.spec.containers[0].resources.limits.cpu,MEM_REQ:.spec.template.spec.containers[0].resources.requests.memory,MEM_LIMIT:.spec.template.spec.containers[0].resources.limits.memory'
  echo
  echo "Current pod usage"
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" top pods 2>/dev/null || \
    echo "Metrics are not ready; declared budgets above are still enforced"
else
  echo "Kubernetes context $KUBE_CONTEXT is unavailable"
  warnings+=("Kubernetes context $KUBE_CONTEXT is unavailable.")
fi

section "Potential problems"
if [[ ${#warnings[@]} -eq 0 ]]; then
  echo "OK · no threshold or workload problems detected"
else
  for warning in "${warnings[@]}"; do
    echo "WARNING · $warning"
  done
fi

section "Quick guidance"
echo "Disk cleanup preview: ./scripts/manage_podman_storage.sh cleanup"
echo "Safe image cleanup:   ./scripts/manage_podman_storage.sh cleanup --apply"
echo "Detailed K8s status:  ./scripts/local_k8s_status.sh"
}

while true; do
  render
  [[ "$WATCH" -eq 1 ]] || break
  printf '\nRefreshing in %ss · Ctrl-C to stop\n' "$INTERVAL"
  sleep "$INTERVAL"
done
