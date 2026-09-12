#!/usr/bin/env bash
set -euo pipefail

BREW_BIN="${BREW_BIN:-$(command -v brew || true)}"

if [[ -z "$BREW_BIN" ]]; then
  echo "[local-k8s] Homebrew is required: https://brew.sh" >&2
  exit 1
fi

for tool in kubectl kind; do
  if command -v "$tool" >/dev/null 2>&1; then
    echo "[local-k8s] $tool already installed"
  else
    echo "[local-k8s] installing $tool"
    "$BREW_BIN" install "$tool"
  fi
done

if [[ -d /Applications/Freelens.app || -d /Applications/FreeLens.app ]]; then
  echo "[local-k8s] Freelens already installed"
else
  echo "[local-k8s] installing Freelens"
  "$BREW_BIN" install --cask freelens
fi

kubectl version --client
kind version
echo "[local-k8s] tools ready"
