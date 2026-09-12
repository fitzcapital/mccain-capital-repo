#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER_NAME:-mccain-capital}"
NAMESPACE="${K8S_NAMESPACE:-mccain-capital}"
KUBECTL_BIN="${KUBECTL_BIN:-$(command -v kubectl || true)}"
PODMAN_BIN="${PODMAN_BIN:-$(command -v podman || echo /opt/homebrew/bin/podman)}"

if [[ -z "$KUBECTL_BIN" ]]; then
  echo "[local-k8s] kubectl is not installed" >&2
  exit 1
fi

echo "[local-k8s] workloads"
"$KUBECTL_BIN" --context "kind-${CLUSTER_NAME}" -n "$NAMESPACE" get pods -o wide
echo
echo "[local-k8s] declared resources"
"$KUBECTL_BIN" --context "kind-${CLUSTER_NAME}" -n "$NAMESPACE" get deployment \
  -o custom-columns='NAME:.metadata.name,CPU_REQ:.spec.template.spec.containers[0].resources.requests.cpu,CPU_LIMIT:.spec.template.spec.containers[0].resources.limits.cpu,MEM_REQ:.spec.template.spec.containers[0].resources.requests.memory,MEM_LIMIT:.spec.template.spec.containers[0].resources.limits.memory,IMAGE:.spec.template.spec.containers[0].image'
echo
echo "[local-k8s] current usage (available after metrics-server is ready)"
"$KUBECTL_BIN" --context "kind-${CLUSTER_NAME}" -n "$NAMESPACE" top pods 2>/dev/null || \
  echo "metrics are not ready yet"
echo
echo "[local-k8s] storage maintenance"
"$KUBECTL_BIN" --context "kind-${CLUSTER_NAME}" -n "$NAMESPACE" get cronjob \
  mccain-capital-storage-maintenance \
  -o custom-columns='NAME:.metadata.name,SCHEDULE:.spec.schedule,LAST_RUN:.status.lastScheduleTime,ACTIVE:.status.active[*].name'
"$KUBECTL_BIN" --context "kind-${CLUSTER_NAME}" -n "$NAMESPACE" get jobs \
  -l app.kubernetes.io/component=storage-maintenance --sort-by=.metadata.creationTimestamp \
  -o custom-columns='NAME:.metadata.name,STATUS:.status.conditions[-1].type,STARTED:.status.startTime,FINISHED:.status.completionTime' | tail -4
echo "manual run: ./scripts/run_k8s_storage_maintenance.sh"
echo
echo "[local-k8s] worker heartbeat"
"$KUBECTL_BIN" --context "kind-${CLUSTER_NAME}" -n "$NAMESPACE" exec \
  deployment/mccain-capital-worker -- python -m mccain_capital.worker --check
echo "current"
echo
echo "[local-k8s] localhost health"
curl -sf http://127.0.0.1:5001/healthz
echo
echo
"$PODMAN_BIN" system df
