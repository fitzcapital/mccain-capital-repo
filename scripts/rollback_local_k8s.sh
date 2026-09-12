#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
ROOT_DIR="$(cd "$(dirname "$SCRIPT_PATH")/.." && pwd)"
CLUSTER_NAME="${KIND_CLUSTER_NAME:-mccain-capital}"
KIND_BIN="${KIND_BIN:-$(command -v kind || true)}"

if [[ -n "$KIND_BIN" ]] && \
  KIND_EXPERIMENTAL_PROVIDER=podman "$KIND_BIN" get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "[local-k8s] deleting cluster $CLUSTER_NAME (persistent-data is outside the cluster)"
  KIND_EXPERIMENTAL_PROVIDER=podman "$KIND_BIN" delete cluster --name "$CLUSTER_NAME"
fi

"$ROOT_DIR/scripts/run_podman_app.sh"
echo "[local-k8s] standalone Podman runtime restored"
