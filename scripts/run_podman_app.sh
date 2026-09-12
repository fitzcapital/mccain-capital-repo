#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
ROOT_DIR="$(cd "$(dirname "${SCRIPT_PATH}")/.." && pwd)"
IMAGE_NAME="${IMAGE_NAME:-localhost/mccain-capital-app:latest}"
ROLLBACK_IMAGE_NAME="${ROLLBACK_IMAGE_NAME:-localhost/mccain-capital-app:rollback}"
CONTAINER_NAME="${CONTAINER_NAME:-mccain-capital-app}"
HOST_PORT="${HOST_PORT:-5001}"
DATA_DIR="${DATA_DIR:-$ROOT_DIR/persistent-data}"
PODMAN_BIN="${PODMAN_BIN:-$(command -v podman || echo /opt/homebrew/bin/podman)}"
RG_BIN="${RG_BIN:-$(command -v rg || echo /opt/homebrew/bin/rg)}"
CURL_BIN="${CURL_BIN:-$(command -v curl || echo /usr/bin/curl)}"
KIND_BIN="${KIND_BIN:-$(command -v kind || true)}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-mccain-capital}"
REPAIR_DB_SCRIPT="${REPAIR_DB_SCRIPT:-$ROOT_DIR/scripts/repair_sqlite_mount_db.sh}"
STAMP_SCRIPT="${STAMP_SCRIPT:-$ROOT_DIR/scripts/current_repo_stamp.sh}"
STAMP_FILE="${STAMP_FILE:-$DATA_DIR/.podman-image-repo-stamp}"
STORAGE_MANAGER="${STORAGE_MANAGER:-$ROOT_DIR/scripts/manage_podman_storage.sh}"
ENV_PASSTHROUGH_VARS=(X_BEARER_TOKEN X_API_BEARER_TOKEN TWITTER_BEARER_TOKEN X_API_BASE_URL)

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

if [[ -n "$KIND_BIN" ]] && \
  KIND_EXPERIMENTAL_PROVIDER=podman "$KIND_BIN" get clusters 2>/dev/null | \
    "$RG_BIN" -x "$KIND_CLUSTER_NAME" >/dev/null 2>&1; then
  echo "[run_podman_app] Kubernetes cluster $KIND_CLUSTER_NAME is active." >&2
  echo "[run_podman_app] Use ./scripts/deploy_local_k8s.sh for deployments." >&2
  echo "[run_podman_app] Use ./scripts/rollback_local_k8s.sh to intentionally restore standalone mode." >&2
  exit 2
fi

mkdir -p "$DATA_DIR/uploads" "$DATA_DIR/books"

if [[ -x "$REPAIR_DB_SCRIPT" ]]; then
  "$REPAIR_DB_SCRIPT" "$DATA_DIR" || true
fi

current_stamp=""
if [[ -x "$STAMP_SCRIPT" ]]; then
  current_stamp="$("$STAMP_SCRIPT")"
fi

cd "$ROOT_DIR"

if ! "$PODMAN_BIN" info >/dev/null 2>&1; then
  echo "[run_podman_app] starting podman machine"
  "$PODMAN_BIN" machine start >/dev/null 2>&1 || true
fi

if "$PODMAN_BIN" image exists "$IMAGE_NAME"; then
  echo "[run_podman_app] preserving previous image as $ROLLBACK_IMAGE_NAME"
  "$PODMAN_BIN" tag "$IMAGE_NAME" "$ROLLBACK_IMAGE_NAME"
fi

echo "[run_podman_app] building $IMAGE_NAME"
"$PODMAN_BIN" build -t "$IMAGE_NAME" -f Containerfile .

if "$PODMAN_BIN" ps -a --format '{{.Names}}' | "$RG_BIN" -x "$CONTAINER_NAME" >/dev/null 2>&1; then
  echo "[run_podman_app] removing existing container $CONTAINER_NAME"
  "$PODMAN_BIN" rm -f "$CONTAINER_NAME" >/dev/null
fi

echo "[run_podman_app] starting $CONTAINER_NAME on port $HOST_PORT"
declare -a RUN_ENV_ARGS=()
for env_name in "${ENV_PASSTHROUGH_VARS[@]}"; do
  if [[ -n "${!env_name:-}" ]]; then
    RUN_ENV_ARGS+=(-e "$env_name=${!env_name}")
  fi
done
if [[ ${#RUN_ENV_ARGS[@]} -gt 0 ]]; then
  "$PODMAN_BIN" run -d \
    --name "$CONTAINER_NAME" \
    -p "$HOST_PORT:5001" \
    -v "$DATA_DIR:/data" \
    "${RUN_ENV_ARGS[@]}" \
    "$IMAGE_NAME" >/dev/null
else
  "$PODMAN_BIN" run -d \
    --name "$CONTAINER_NAME" \
    -p "$HOST_PORT:5001" \
    -v "$DATA_DIR:/data" \
    "$IMAGE_NAME" >/dev/null
fi

echo "[run_podman_app] waiting for healthz"
healthy=0
for _ in $(seq 1 30); do
  if "$CURL_BIN" -sf "http://127.0.0.1:${HOST_PORT}/healthz" >/dev/null; then
    healthy=1
    break
  fi
  sleep 1
done

if [[ "$healthy" -ne 1 ]]; then
  echo "[run_podman_app] health check failed; image cleanup skipped" >&2
  exit 1
fi

"$CURL_BIN" -sf "http://127.0.0.1:${HOST_PORT}/healthz"
echo
if [[ -n "$current_stamp" ]]; then
  printf '%s\n' "$current_stamp" > "$STAMP_FILE"
fi
echo "[run_podman_app] container is up"
echo "[run_podman_app] local: http://localhost:${HOST_PORT}"

if [[ -x "$STORAGE_MANAGER" ]]; then
  echo "[run_podman_app] pruning retained dangling image layers"
  PODMAN_BIN="$PODMAN_BIN" "$STORAGE_MANAGER" auto || \
    echo "[run_podman_app] warning: image cleanup failed; the healthy container remains running" >&2
fi
