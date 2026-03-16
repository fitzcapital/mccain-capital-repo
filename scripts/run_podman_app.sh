#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_NAME="${IMAGE_NAME:-localhost/mccain-capital-app:latest}"
CONTAINER_NAME="${CONTAINER_NAME:-mccain-capital-app}"
HOST_PORT="${HOST_PORT:-5001}"
DATA_DIR="${DATA_DIR:-$ROOT_DIR/persistent-data}"

mkdir -p "$DATA_DIR/uploads" "$DATA_DIR/books"

cd "$ROOT_DIR"

echo "[run_podman_app] building $IMAGE_NAME"
podman build -t "$IMAGE_NAME" -f Containerfile .

if podman ps -a --format '{{.Names}}' | rg -x "$CONTAINER_NAME" >/dev/null 2>&1; then
  echo "[run_podman_app] removing existing container $CONTAINER_NAME"
  podman rm -f "$CONTAINER_NAME" >/dev/null
fi

echo "[run_podman_app] starting $CONTAINER_NAME on port $HOST_PORT"
podman run -d \
  --name "$CONTAINER_NAME" \
  -p "$HOST_PORT:5001" \
  -v "$DATA_DIR:/data" \
  "$IMAGE_NAME" >/dev/null

echo "[run_podman_app] waiting for healthz"
for _ in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:${HOST_PORT}/healthz" >/dev/null; then
    break
  fi
  sleep 1
done

curl -sf "http://127.0.0.1:${HOST_PORT}/healthz"
echo
echo "[run_podman_app] container is up"
echo "[run_podman_app] local: http://127.0.0.1:${HOST_PORT}"
