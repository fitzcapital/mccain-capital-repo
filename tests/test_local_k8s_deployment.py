from __future__ import annotations

import fcntl
from pathlib import Path
import re

import pytest

from mccain_capital import runtime_role
from mccain_capital import worker


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "k8s" / "base.yaml"
MANIFEST_TEXT = MANIFEST.read_text()


def _document(kind: str, name: str) -> str:
    for document in MANIFEST_TEXT.split("\n---\n"):
        if re.search(rf"(?m)^kind: {re.escape(kind)}$", document) and re.search(
            rf"(?m)^  name: {re.escape(name)}$", document
        ):
            return document
    raise AssertionError(f"missing {kind}/{name}")


@pytest.mark.parametrize("role", ["standalone", "web", "worker"])
def test_runtime_role_accepts_supported_values(monkeypatch: pytest.MonkeyPatch, role: str) -> None:
    monkeypatch.setenv("MCCAIN_RUNTIME_ROLE", role)
    assert runtime_role() == role


def test_runtime_role_rejects_unknown_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MCCAIN_RUNTIME_ROLE", "duplicate")
    with pytest.raises(RuntimeError, match="Unsupported MCCAIN_RUNTIME_ROLE"):
        runtime_role()


def test_worker_heartbeat_and_owner_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    heartbeat = tmp_path / "heartbeat"
    lock = tmp_path / "worker.lock"
    monkeypatch.setattr(worker, "DATA_DIR", tmp_path)
    monkeypatch.setattr(worker, "LOCK_PATH", lock)
    monkeypatch.setattr(worker, "HEARTBEAT_PATH", heartbeat)

    assert not worker.heartbeat_is_current()
    heartbeat.touch()
    assert worker.heartbeat_is_current()

    owner = worker._acquire_owner_lock()
    try:
        with pytest.raises(RuntimeError, match="already owns"):
            worker._acquire_owner_lock()
    finally:
        fcntl.flock(owner.fileno(), fcntl.LOCK_UN)
        owner.close()


def test_two_single_replica_workloads_share_one_image_and_pvc() -> None:
    web = _document("Deployment", "mccain-capital-web")
    background = _document("Deployment", "mccain-capital-worker")
    for deployment in (web, background):
        assert "replicas: 1" in deployment
        assert "type: Recreate" in deployment
        assert "image: localhost/mccain-capital-app:k8s" in deployment
        assert "claimName: mccain-capital-data" in deployment


def test_workload_roles_resources_and_probes_are_bounded() -> None:
    expected = {
        "mccain-capital-web": ("web", "cpu: 300m", "cpu: 1500m", "memory: 512Mi", "memory: 1Gi"),
        "mccain-capital-worker": (
            "worker",
            "cpu: 150m",
            "cpu: 750m",
            "memory: 256Mi",
            "memory: 768Mi",
        ),
    }
    for name, values in expected.items():
        deployment = _document("Deployment", name)
        role, cpu_request, cpu_limit, memory_request, memory_limit = values
        assert f"value: {role}" in deployment
        for value in (cpu_request, cpu_limit, memory_request, memory_limit):
            assert value in deployment
        for probe in ("startupProbe:", "readinessProbe:", "livenessProbe:"):
            assert probe in deployment


def test_storage_and_localhost_port_are_retained() -> None:
    volume = _document("PersistentVolume", "mccain-capital-data")
    service = _document("Service", "mccain-capital-web")
    template = (ROOT / "k8s" / "kind-config.yaml.template").read_text()

    assert "persistentVolumeReclaimPolicy: Retain" in volume
    assert "path: /mnt/mccain-capital-data" in volume
    assert "nodePort: 30001" in service
    assert "hostPort: 5001" in template
    assert "__PERSISTENT_DATA_PATH__" in template


def test_storage_maintenance_is_scheduled_bounded_and_allowlisted() -> None:
    cronjob = _document("CronJob", "mccain-capital-storage-maintenance")

    for contract in (
        'schedule: "*/30 * * * *"',
        "concurrencyPolicy: Forbid",
        "successfulJobsHistoryLimit: 2",
        "failedJobsHistoryLimit: 2",
        "activeDeadlineSeconds: 120",
        "cpu: 10m",
        "cpu: 50m",
        "memory: 32Mi",
        "memory: 64Mi",
        "value: tmp,cache",
        'value: "168"',
        "claimName: mccain-capital-data",
        "allowPrivilegeEscalation: false",
        'command: ["python", "/app/mccain_capital/storage_maintenance.py"]',
    ):
        assert contract in cronjob
    for protected in ("journal.db", "uploads", "books", "backups", "podman.sock"):
        assert protected not in cronjob


def test_storage_maintenance_is_visible_and_manually_runnable() -> None:
    status = (ROOT / "scripts" / "local_k8s_status.sh").read_text()
    manual = (ROOT / "scripts" / "run_k8s_storage_maintenance.sh").read_text()

    assert "get cronjob" in status
    assert "LAST_RUN" in status
    assert "get jobs" in status
    assert "--from=cronjob/mccain-capital-storage-maintenance" in manual
    assert "status.succeeded" in manual
    assert "status.failed" in manual
    assert 'logs "job/$JOB_NAME"' in manual


def test_lifecycle_scripts_preserve_data_and_gate_cutover() -> None:
    deploy = (ROOT / "scripts" / "deploy_local_k8s.sh").read_text()
    rollback = (ROOT / "scripts" / "rollback_local_k8s.sh").read_text()
    teardown = (ROOT / "scripts" / "teardown_local_k8s.sh").read_text()

    rollout = deploy.index("rollout status")
    health = deploy.index("curl -sf http://127.0.0.1:5001/healthz")
    release_port = deploy.index('"$PODMAN_BIN" rm "$CONTAINER_NAME"')
    create_cluster = deploy.index('"$KIND_BIN" create cluster')
    final_cleanup = deploy.rindex('"$PODMAN_BIN" rm "$CONTAINER_NAME"')
    assert release_port < create_cluster < rollout < health < final_cleanup
    assert "manage_podman_storage.sh" in deploy
    assert "persistent-data" in rollback
    assert "persistent-data will not be removed" in teardown
    assert "rm -rf" not in deploy + rollback + teardown


def test_web_role_gate_is_explicit_in_application_startup() -> None:
    source = (ROOT / "mccain_capital" / "__init__.py").read_text()
    assert 'if role == "standalone" and not getattr(' in source
    assert source.count('if role == "standalone":') >= 1


def test_standalone_launcher_refuses_to_compete_with_kubernetes() -> None:
    source = (ROOT / "scripts" / "run_podman_app.sh").read_text()
    assert "KIND_CLUSTER_NAME" in source
    assert "Kubernetes cluster" in source
    assert "deploy_local_k8s.sh" in source
    assert "rollback_local_k8s.sh" in source


def test_login_launcher_prefers_existing_kubernetes_cluster() -> None:
    source = (ROOT / "scripts" / "start_mccain_capital_on_login.sh").read_text()
    cluster_gate = source.index("Kubernetes is authoritative")
    standalone_state = source.index('repo_state="unknown"')
    assert cluster_gate < standalone_state
    assert '"$PODMAN_BIN" start "$K8S_NODE_NAME"' in source
    assert "Kubernetes app healthy" in source
