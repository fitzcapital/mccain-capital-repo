from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
KIND_CONFIG = ROOT / "k8s" / "kind-config.yaml.template"
GUARD = ROOT / "scripts" / "local_k8s_guard.sh"
INSTALLER = ROOT / "scripts" / "install_local_k8s_guard_launch_agent.sh"
UNINSTALLER = ROOT / "scripts" / "uninstall_local_k8s_guard_launch_agent.sh"
MONITOR = ROOT / "scripts" / "monitor_laptop_resources.sh"


def test_kind_cluster_enforces_per_pod_pid_limit() -> None:
    config = KIND_CONFIG.read_text()

    assert "kind: KubeletConfiguration" in config
    assert "podPidsLimit: 512" in config


def test_guard_uses_threshold_cooldown_and_exact_recovery_targets() -> None:
    script = GUARD.read_text()

    assert 'FAILURE_THRESHOLD="${K8S_GUARD_FAILURE_THRESHOLD:-3}"' in script
    assert 'COOLDOWN_SECONDS="${K8S_GUARD_COOLDOWN_SECONDS:-600}"' in script
    assert 'NODE_CONTAINER="${CLUSTER_NAME}-control-plane"' in script
    assert 'podman inspect "$NODE_CONTAINER"' in script
    assert 'podman restart "$NODE_CONTAINER"' in script
    assert "deployment/mccain-capital-web" in script
    assert "--status" in script
    assert "--check" in script

    # Recovery must not delete the cluster, containers, volumes, or application data.
    assert "kind delete" not in script
    assert "podman rm" not in script
    assert "volume prune" not in script
    assert "persistent-data" not in script


def test_launch_agent_runs_guard_once_per_minute_and_is_recoverable() -> None:
    installer = INSTALLER.read_text()
    uninstaller = UNINSTALLER.read_text()

    assert "com.mccaincapital.local-k8s-guard" in installer
    assert "<key>StartInterval</key><integer>60</integer>" in installer
    assert "local_k8s_guard.sh" in installer
    assert "launchctl bootstrap" in installer
    assert "launchctl bootout" in uninstaller
    assert 'mv "$PLIST_PATH" "${HOME}/.Trash/' in uninstaller


def test_monitor_reports_kind_node_task_pressure_and_guard_status() -> None:
    script = MONITOR.read_text()

    assert 'K8S_NODE_PID_WARN="${K8S_NODE_PID_WARN:-900}"' in script
    assert 'K8S_NODE_PID_CRITICAL="${K8S_NODE_PID_CRITICAL:-1200}"' in script
    assert 'node_container="${CLUSTER_NAME}-control-plane"' in script
    assert "/proc/[0-9]*/task/[0-9]*" in script
    assert "local_k8s_guard.sh --status" in script
