#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
ROOT_DIR="$(cd "$(dirname "$SCRIPT_PATH")/.." && pwd)"
CLUSTER_NAME="${KIND_CLUSTER_NAME:-mccain-capital}"
CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="${K8S_NAMESPACE:-mccain-capital}"
IMAGE_NAME="${K8S_IMAGE_NAME:-localhost/mccain-capital-app:k8s}"
CONTAINER_NAME="${CONTAINER_NAME:-mccain-capital-app}"
DATA_DIR="${DATA_DIR:-$ROOT_DIR/persistent-data}"
PODMAN_BIN="${PODMAN_BIN:-$(command -v podman || echo /opt/homebrew/bin/podman)}"
PODMAN_MACHINE_NAME="${PODMAN_MACHINE_NAME:-podman-machine-applehv}"
KUBECTL_BIN="${KUBECTL_BIN:-$(command -v kubectl || true)}"
KIND_BIN="${KIND_BIN:-$(command -v kind || true)}"
STORAGE_MANAGER="${STORAGE_MANAGER:-$ROOT_DIR/scripts/manage_podman_storage.sh}"
CREATED_CLUSTER=0
CUTOVER_STARTED=0

for value in "$KUBECTL_BIN" "$KIND_BIN"; do
  if [[ -z "$value" || ! -x "$value" ]]; then
    echo "[local-k8s] kubectl and kind are required; run scripts/install_local_k8s.sh" >&2
    exit 1
  fi
done

mkdir -p "$DATA_DIR/uploads" "$DATA_DIR/books"

rollback_on_error() {
  exit_code=$?
  if [[ "$exit_code" -eq 0 ]]; then
    return
  fi
  echo "[local-k8s] deployment failed" >&2
  if [[ "$CUTOVER_STARTED" -eq 1 ]]; then
    if [[ "$CREATED_CLUSTER" -eq 1 ]]; then
      KIND_EXPERIMENTAL_PROVIDER=podman "$KIND_BIN" delete cluster --name "$CLUSTER_NAME" || true
    fi
    echo "[local-k8s] restoring standalone Podman app" >&2
    "$ROOT_DIR/scripts/run_podman_app.sh" || true
  fi
  exit "$exit_code"
}
trap rollback_on_error EXIT

if ! "$PODMAN_BIN" info >/dev/null 2>&1; then
  "$PODMAN_BIN" machine start "$PODMAN_MACHINE_NAME"
fi

if ! KIND_EXPERIMENTAL_PROVIDER=podman "$KIND_BIN" get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "[local-k8s] stopping standalone container for localhost:5001 cutover"
  CUTOVER_STARTED=1
  "$PODMAN_BIN" stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
  # Removing the ephemeral container releases gvproxy's host-port reservation.
  # Persistent application data remains in the external DATA_DIR bind mount.
  "$PODMAN_BIN" rm "$CONTAINER_NAME" >/dev/null 2>&1 || true
  config_file="$(mktemp -t mccain-kind-config.XXXXXX)"
  sed "s|__PERSISTENT_DATA_PATH__|$DATA_DIR|g" \
    "$ROOT_DIR/k8s/kind-config.yaml.template" > "$config_file"
  echo "[local-k8s] creating cluster $CLUSTER_NAME"
  KIND_EXPERIMENTAL_PROVIDER=podman "$KIND_BIN" create cluster \
    --name "$CLUSTER_NAME" --config "$config_file" --wait 5m
  rm -f "$config_file"
  CREATED_CLUSTER=1
fi

"$KUBECTL_BIN" config use-context "$CONTEXT" >/dev/null

echo "[local-k8s] building one shared image: $IMAGE_NAME"
"$PODMAN_BIN" build -t "$IMAGE_NAME" -f "$ROOT_DIR/Containerfile" "$ROOT_DIR"
KIND_EXPERIMENTAL_PROVIDER=podman "$KIND_BIN" load docker-image \
  --name "$CLUSTER_NAME" "$IMAGE_NAME"

"$KUBECTL_BIN" create namespace "$NAMESPACE" --dry-run=client -o yaml | "$KUBECTL_BIN" apply -f -
if [[ -f "$ROOT_DIR/.env" ]]; then
  "$KUBECTL_BIN" -n "$NAMESPACE" create secret generic mccain-runtime-env \
    --from-env-file="$ROOT_DIR/.env" --dry-run=client -o yaml | "$KUBECTL_BIN" apply -f -
else
  "$KUBECTL_BIN" -n "$NAMESPACE" create secret generic mccain-runtime-env \
    --from-literal=MCCAIN_LOCAL_K8S=1 --dry-run=client -o yaml | "$KUBECTL_BIN" apply -f -
fi

"$KUBECTL_BIN" apply -f "$ROOT_DIR/k8s/base.yaml"
revision="$(date -u +%Y%m%dT%H%M%SZ)"
for deployment in mccain-capital-web mccain-capital-worker; do
  "$KUBECTL_BIN" -n "$NAMESPACE" patch deployment "$deployment" --type merge \
    -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"mccain.capital/revision\":\"$revision\"}}}}}"
  "$KUBECTL_BIN" -n "$NAMESPACE" rollout status "deployment/$deployment" --timeout=5m
done

curl -sf http://127.0.0.1:5001/healthz
echo
"$KUBECTL_BIN" -n "$NAMESPACE" exec deployment/mccain-capital-web -- \
  test -f /data/journal.db
"$KUBECTL_BIN" -n "$NAMESPACE" exec deployment/mccain-capital-worker -- \
  python -m mccain_capital.worker --check

"$PODMAN_BIN" rm "$CONTAINER_NAME" >/dev/null 2>&1 || true
"$PODMAN_BIN" exec "${CLUSTER_NAME}-control-plane" crictl rmi --prune >/dev/null 2>&1 || true
if [[ -x "$STORAGE_MANAGER" ]]; then
  PODMAN_BIN="$PODMAN_BIN" "$STORAGE_MANAGER" auto || true
fi

CUTOVER_STARTED=0
trap - EXIT
echo "[local-k8s] deployment healthy: http://localhost:5001"
echo "[local-k8s] context: $CONTEXT"
