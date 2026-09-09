from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_runtime_watchdog_recovers_clock_drift() -> None:
    script = (ROOT / "scripts" / "watch_mccain_capital_runtime.sh").read_text()

    assert 'MAX_CLOCK_SKEW_SECONDS="${MAX_CLOCK_SKEW_SECONDS:-15}"' in script
    assert 'PODMAN_MACHINE_NAME="${PODMAN_MACHINE_NAME:-podman-machine-applehv}"' in script
    assert 'container_epoch="$($PODMAN_BIN exec "$CONTAINER_NAME" date +%s' in script
    assert "clock_skew=$((host_epoch - container_epoch))" in script
    assert 'if (( clock_skew > MAX_CLOCK_SKEW_SECONDS )); then' in script
    assert 'machine ssh "$PODMAN_MACHINE_NAME"' in script
    assert 'sudo date -u -s "@$sync_epoch"' in script
    assert 'corrected_epoch="$($PODMAN_BIN exec "$CONTAINER_NAME" date +%s' in script
    assert 'VM clock synchronized; corrected skew' in script
    assert 'machine stop' not in script


def test_launch_agent_checks_runtime_every_minute() -> None:
    installer = (ROOT / "scripts" / "install_macos_launch_agent.sh").read_text()

    assert "watch_mccain_capital_runtime.sh" in installer
    assert "<key>StartInterval</key>" in installer
    assert "<integer>60</integer>" in installer
