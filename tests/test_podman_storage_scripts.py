from __future__ import annotations

import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANAGER = ROOT / "scripts" / "manage_podman_storage.sh"
DEPLOY = ROOT / "scripts" / "run_podman_app.sh"
MONITOR = ROOT / "scripts" / "monitor_laptop_resources.sh"


def _fake_podman(tmp_path: Path) -> tuple[Path, Path]:
    command_log = tmp_path / "podman-commands.log"
    binary = tmp_path / "podman"
    binary.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> "$FAKE_PODMAN_LOG"
case "${1:-}" in
  info) exit "${FAKE_PODMAN_INFO_EXIT:-0}" ;;
  system)
    printf 'TYPE  TOTAL  ACTIVE  SIZE  RECLAIMABLE\\nImages  4  1  1GB  750MB\\n'
    ;;
  images)
    printf 'old-image  8 days ago  500MB\\nnew-image  2 hours ago  250MB\\n'
    ;;
  image)
    [[ "${2:-}" == "prune" ]] && printf 'Deleted: old-image\\n'
    ;;
esac
"""
    )
    binary.chmod(0o755)
    return binary, command_log


def _run_manager(
    tmp_path: Path, *args: str, info_exit: int = 0
) -> subprocess.CompletedProcess[str]:
    binary, command_log = _fake_podman(tmp_path)
    env = os.environ.copy()
    env.update(
        {
            "PODMAN_BIN": str(binary),
            "FAKE_PODMAN_LOG": str(command_log),
            "FAKE_PODMAN_INFO_EXIT": str(info_exit),
            "PODMAN_STORAGE_LOCK_DIR": str(tmp_path / "maintenance.lock"),
        }
    )
    return subprocess.run(
        [str(MANAGER), *args],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )


def _commands(tmp_path: Path) -> str:
    path = tmp_path / "podman-commands.log"
    return path.read_text() if path.exists() else ""


def test_status_is_default_and_read_only(tmp_path: Path) -> None:
    result = _run_manager(tmp_path)

    assert result.returncode == 0
    assert "storage summary" in result.stdout
    assert "Images TOTAL includes shared build layers" in result.stdout
    assert "dangling images" in result.stdout
    assert "image prune" not in _commands(tmp_path)


def test_cleanup_without_apply_is_preview_only(tmp_path: Path) -> None:
    result = _run_manager(tmp_path, "cleanup", "--retention-hours", "24")

    assert result.returncode == 0
    assert "preview only" in result.stdout
    assert "older than 24h" in result.stdout
    assert "image prune" not in _commands(tmp_path)


def test_applied_cleanup_uses_only_dangling_age_filters(tmp_path: Path) -> None:
    result = _run_manager(tmp_path, "cleanup", "--retention-hours", "48", "--apply")

    assert result.returncode == 0
    commands = _commands(tmp_path)
    assert "image prune --force --filter dangling=true --filter until=48h" in commands
    assert "system prune" not in commands
    assert "volume" not in commands
    assert result.stdout.count("storage summary") == 2


def test_applied_cleanup_fails_closed_when_lock_is_held(tmp_path: Path) -> None:
    (tmp_path / "maintenance.lock").mkdir()
    result = _run_manager(tmp_path, "cleanup", "--apply")

    assert result.returncode == 1
    assert "maintenance is already running" in result.stderr
    assert "image prune" not in _commands(tmp_path)


def test_unavailable_podman_is_concise_and_read_only(tmp_path: Path) -> None:
    result = _run_manager(tmp_path, info_exit=1)

    assert result.returncode == 1
    assert "Podman is unavailable" in result.stderr
    assert "image prune" not in _commands(tmp_path)


def test_deploy_preserves_rollback_and_cleans_only_after_health() -> None:
    script = DEPLOY.read_text()

    rollback = script.index('"$PODMAN_BIN" tag "$IMAGE_NAME" "$ROLLBACK_IMAGE_NAME"')
    build = script.index('"$PODMAN_BIN" build -t "$IMAGE_NAME"')
    health_guard = script.index('if [[ "$healthy" -ne 1 ]]')
    cleanup = script.index('"$STORAGE_MANAGER" auto')

    assert rollback < build < health_guard < cleanup
    assert "health check failed; image cleanup skipped" in script
    assert "the healthy container remains running" in script
    assert 'DATA_DIR="${DATA_DIR:-$ROOT_DIR/persistent-data}"' in script
    assert '-v "$DATA_DIR:/data"' in script


def test_live_monitor_auto_cleanup_is_conservative_and_throttled() -> None:
    script = MONITOR.read_text()

    assert "INTERVAL=30" in script
    assert "AUTO_CLEAN_MIN_DANGLING" in script
    assert "AUTO_CLEAN_COOLDOWN_SECONDS" in script
    assert '"$REPO_ROOT/scripts/manage_podman_storage.sh" auto' in script
    assert '[[ "$WATCH" -eq 1 && "$AUTO_CLEAN" -eq 1' in script
    assert "--no-auto-clean" in script
    assert "podman system prune" not in script
    assert "podman volume prune" not in script
    assert '$3 == "Succeeded" || $3 == "Completed" {next}' in script
    assert "active Kubernetes pods" in script
