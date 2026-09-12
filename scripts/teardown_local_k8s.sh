#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER_NAME:-mccain-capital}"
KIND_BIN="${KIND_BIN:-$(command -v kind || true)}"

if [[ -z "$KIND_BIN" ]]; then
  echo "[local-k8s] kind is not installed" >&2
  exit 1
fi

echo "[local-k8s] deleting cluster $CLUSTER_NAME"
echo "[local-k8s] repository persistent-data will not be removed"
KIND_EXPERIMENTAL_PROVIDER=podman "$KIND_BIN" delete cluster --name "$CLUSTER_NAME"
