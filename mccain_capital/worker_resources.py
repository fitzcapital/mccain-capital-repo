"""Sanitized process-capacity diagnostics for the dedicated worker."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
from typing import Any, Iterator


STATE_PATH = Path(
    os.environ.get("MCCAIN_WORKER_RESOURCE_STATE_PATH") or "/tmp/mccain-worker-resources.json"
)
WARNING_TASKS = max(1, int(os.environ.get("MCCAIN_WORKER_TASK_WARNING") or "300"))
CRITICAL_TASKS = max(
    WARNING_TASKS + 1,
    int(os.environ.get("MCCAIN_WORKER_TASK_CRITICAL") or "400"),
)
CRITICAL_SAMPLES = max(
    1,
    int(os.environ.get("MCCAIN_WORKER_TASK_CRITICAL_SAMPLES") or "3"),
)

_LOCK = threading.Lock()
_BASELINE_TASKS: int | None = None
_BASELINE_FDS: int | None = None
_HIGH_WATER_TASKS = 0
_HIGH_WATER_FDS = 0
_CONSECUTIVE_CRITICAL = 0
_LAST_COMPONENT = "startup"
_LAST_COMPONENT_DELTA_TASKS = 0
_LAST_COMPONENT_DELTA_FDS = 0


def _directory_count(path: str, fallback: int = -1) -> int:
    try:
        return len(os.listdir(path))
    except OSError:
        return fallback


def process_task_count() -> int:
    return _directory_count("/proc/self/task", threading.active_count())


def process_fd_count() -> int:
    return _directory_count("/proc/self/fd")


def _read_cgroup_number(name: str) -> int | None:
    candidates = (f"/sys/fs/cgroup/{name}", f"/sys/fs/cgroup/pids/{name}")
    for candidate in candidates:
        try:
            raw = Path(candidate).read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if raw == "max":
            return None
        try:
            return int(raw)
        except ValueError:
            continue
    return None


def _write_state(payload: dict[str, Any]) -> None:
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        temporary = STATE_PATH.with_suffix(f"{STATE_PATH.suffix}.tmp")
        temporary.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        temporary.replace(STATE_PATH)
    except OSError:
        # Capacity protection must not fail because diagnostics cannot be written.
        pass


def sample(*, component: str = "heartbeat", persist: bool = True) -> dict[str, Any]:
    """Return counts only; no provider payloads, credentials, or request data."""

    global _BASELINE_TASKS, _BASELINE_FDS
    global _HIGH_WATER_TASKS, _HIGH_WATER_FDS, _CONSECUTIVE_CRITICAL

    tasks = process_task_count()
    fds = process_fd_count()
    with _LOCK:
        if _BASELINE_TASKS is None:
            _BASELINE_TASKS = tasks
            _BASELINE_FDS = fds
        _HIGH_WATER_TASKS = max(_HIGH_WATER_TASKS, tasks)
        _HIGH_WATER_FDS = max(_HIGH_WATER_FDS, fds)
        status = (
            "critical"
            if tasks >= CRITICAL_TASKS
            else ("warning" if tasks >= WARNING_TASKS else "healthy")
        )
        _CONSECUTIVE_CRITICAL = _CONSECUTIVE_CRITICAL + 1 if status == "critical" else 0
        payload = {
            "sampled_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "status": status,
            "reason": "task_capacity" if status != "healthy" else "",
            "component": str(component or "unknown")[:64],
            "tasks": tasks,
            "file_descriptors": fds,
            "baseline_tasks": _BASELINE_TASKS,
            "baseline_file_descriptors": _BASELINE_FDS,
            "high_water_tasks": _HIGH_WATER_TASKS,
            "high_water_file_descriptors": _HIGH_WATER_FDS,
            "warning_tasks": WARNING_TASKS,
            "critical_tasks": CRITICAL_TASKS,
            "critical_samples": _CONSECUTIVE_CRITICAL,
            "critical_samples_required": CRITICAL_SAMPLES,
            "cgroup_tasks": _read_cgroup_number("pids.current"),
            "cgroup_limit": _read_cgroup_number("pids.max"),
            "last_growth_component": _LAST_COMPONENT,
            "last_growth_tasks": _LAST_COMPONENT_DELTA_TASKS,
            "last_growth_file_descriptors": _LAST_COMPONENT_DELTA_FDS,
        }
    if persist:
        _write_state(payload)
    return payload


def capacity_is_safe() -> bool:
    payload = read_state()
    return str(payload.get("status") or "healthy") != "critical"


def read_state() -> dict[str, Any]:
    try:
        payload = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def critical_limit_reached(payload: dict[str, Any]) -> bool:
    return bool(
        payload.get("status") == "critical"
        and int(payload.get("critical_samples") or 0) >= CRITICAL_SAMPLES
    )


@contextmanager
def track_component(component: str) -> Iterator[None]:
    """Attribute positive process growth to a scheduled refresh boundary."""

    global _LAST_COMPONENT, _LAST_COMPONENT_DELTA_TASKS, _LAST_COMPONENT_DELTA_FDS
    before_tasks = process_task_count()
    before_fds = process_fd_count()
    try:
        yield
    finally:
        task_delta = process_task_count() - before_tasks
        fd_delta = process_fd_count() - before_fds
        if task_delta > 0 or fd_delta > 0:
            with _LOCK:
                _LAST_COMPONENT = str(component or "unknown")[:64]
                _LAST_COMPONENT_DELTA_TASKS = task_delta
                _LAST_COMPONENT_DELTA_FDS = fd_delta
        sample(component=component)


def reset_for_tests() -> None:
    global _BASELINE_TASKS, _BASELINE_FDS
    global _HIGH_WATER_TASKS, _HIGH_WATER_FDS, _CONSECUTIVE_CRITICAL
    global _LAST_COMPONENT, _LAST_COMPONENT_DELTA_TASKS, _LAST_COMPONENT_DELTA_FDS
    with _LOCK:
        _BASELINE_TASKS = None
        _BASELINE_FDS = None
        _HIGH_WATER_TASKS = 0
        _HIGH_WATER_FDS = 0
        _CONSECUTIVE_CRITICAL = 0
        _LAST_COMPONENT = "startup"
        _LAST_COMPONENT_DELTA_TASKS = 0
        _LAST_COMPONENT_DELTA_FDS = 0
    STATE_PATH.unlink(missing_ok=True)
