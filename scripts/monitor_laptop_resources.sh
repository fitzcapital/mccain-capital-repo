#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="${KIND_CLUSTER_NAME:-mccain-capital}"
NAMESPACE="${K8S_NAMESPACE:-mccain-capital}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"

section() {
  printf '\n[%s]\n' "$1"
}

section "Laptop disk"
df -h / | awk 'NR == 1 || NR == 2 {print}'

section "McCain Capital disk"
du -sh "$REPO_ROOT" 2>/dev/null || true
for path in persistent-data artifacts uploads books; do
  if [[ -e "$REPO_ROOT/$path" ]]; then
    du -sh "$REPO_ROOT/$path" 2>/dev/null || true
  fi
done

section "Mac memory pressure"
if command -v memory_pressure >/dev/null 2>&1; then
  memory_pressure 2>/dev/null | awk '/System-wide memory free percentage/ {print; found=1} END {if (!found) print "Memory pressure details unavailable"}'
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
fi

section "Quick guidance"
echo "Disk cleanup preview: ./scripts/manage_podman_storage.sh cleanup"
echo "Safe image cleanup:   ./scripts/manage_podman_storage.sh cleanup --apply"
echo "Detailed K8s status:  ./scripts/local_k8s_status.sh"
