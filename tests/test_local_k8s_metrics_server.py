from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "k8s" / "metrics-server.yaml"
DEPLOY = ROOT / "scripts" / "deploy_local_k8s.sh"
MONITOR = ROOT / "scripts" / "monitor_laptop_resources.sh"


def test_metrics_server_manifest_is_pinned_and_kind_compatible() -> None:
    manifest = MANIFEST.read_text()

    assert "registry.k8s.io/metrics-server/metrics-server:v0.9.0" in manifest
    assert "--kubelet-insecure-tls" in manifest
    assert "--kubelet-preferred-address-types=InternalIP,Hostname,ExternalIP" in manifest
    assert "--metric-resolution=15s" in manifest
    assert "cpu: 250m" in manifest
    assert "memory: 300Mi" in manifest
    assert "runAsNonRoot: true" in manifest
    assert "readOnlyRootFilesystem: true" in manifest


def test_local_deploy_loads_waits_for_and_verifies_metrics_server() -> None:
    script = DEPLOY.read_text()

    assert 'METRICS_SERVER_IMAGE="${METRICS_SERVER_IMAGE:-registry.k8s.io/' in script
    assert '"$PODMAN_BIN" image exists "$METRICS_SERVER_IMAGE"' in script
    assert '"$PODMAN_BIN" pull "$METRICS_SERVER_IMAGE"' in script
    assert '"$KUBECTL_BIN" apply -f "$METRICS_SERVER_MANIFEST"' in script
    assert "rollout status deployment/metrics-server --timeout=3m" in script
    assert '"$KUBECTL_BIN" -n "$NAMESPACE" top pods' in script
    assert "Metrics Server deployed but live pod metrics are unavailable" in script


def test_resource_monitor_distinguishes_missing_and_unready_metrics_api() -> None:
    script = MONITOR.read_text()

    assert "Metrics Server is not installed" in script
    assert "Metrics API is installed but not ready" in script
    assert "v1beta1.metrics.k8s.io" in script
