#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER_NAME:-mccain-capital}"
NAMESPACE="${K8S_NAMESPACE:-mccain-capital}"
KUBECTL_BIN="${KUBECTL_BIN:-$(command -v kubectl || true)}"
CONTEXT="kind-${CLUSTER_NAME}"
JOB_NAME="storage-maintenance-manual-$(date +%s)"

if [[ -z "$KUBECTL_BIN" ]]; then
  echo "[storage-maintenance] kubectl is not installed" >&2
  exit 1
fi

echo "[storage-maintenance] starting $JOB_NAME"
"$KUBECTL_BIN" --context "$CONTEXT" -n "$NAMESPACE" create job "$JOB_NAME" \
  --from=cronjob/mccain-capital-storage-maintenance
deadline=$((SECONDS + 180))
while (( SECONDS < deadline )); do
  succeeded="$("$KUBECTL_BIN" --context "$CONTEXT" -n "$NAMESPACE" get "job/$JOB_NAME" -o jsonpath='{.status.succeeded}')"
  failed="$("$KUBECTL_BIN" --context "$CONTEXT" -n "$NAMESPACE" get "job/$JOB_NAME" -o jsonpath='{.status.failed}')"
  if [[ "${succeeded:-0}" -ge 1 ]]; then
    break
  fi
  if [[ "${failed:-0}" -ge 1 ]]; then
    "$KUBECTL_BIN" --context "$CONTEXT" -n "$NAMESPACE" logs "job/$JOB_NAME" --all-containers || true
    echo "[storage-maintenance] job failed" >&2
    exit 1
  fi
  sleep 2
done
if [[ "${succeeded:-0}" -lt 1 ]]; then
  echo "[storage-maintenance] job timed out" >&2
  exit 1
fi
"$KUBECTL_BIN" --context "$CONTEXT" -n "$NAMESPACE" logs "job/$JOB_NAME"
