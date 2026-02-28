"""Background job persistence helpers."""

from __future__ import annotations

import json
import os
import threading
from typing import Any, Callable, Dict, List
from uuid import uuid4


class BackgroundJobStore:
    """Persist lightweight background job state in memory and on disk."""

    def __init__(
        self,
        job_dir: str,
        now_iso: Callable[[], str],
        *,
        max_jobs: int = 40,
    ) -> None:
        self._job_dir = job_dir
        self._now_iso = now_iso
        self._max_jobs = max_jobs
        self._lock = threading.Lock()
        self._jobs: Dict[str, Dict[str, Any]] = {}

    def create(self, kind: str, title: str, requested: Dict[str, Any]) -> Dict[str, Any]:
        stamp = self._now_iso()
        job = {
            "id": uuid4().hex,
            "kind": kind,
            "title": title,
            "status": "queued",
            "stage": "start",
            "message": "Queued and waiting to start.",
            "requested": requested,
            "created_at": stamp,
            "updated_at": stamp,
            "duration_sec": None,
            "summary": {},
        }
        with self._lock:
            self._jobs[job["id"]] = job
            self._write(job)
            self._trim_locked()
            return dict(job)

    def update(self, job_id: str, **updates: Any) -> Dict[str, Any]:
        with self._lock:
            existing = self._jobs.get(job_id) or self._read(job_id)
            if not existing:
                return {}
            job = dict(existing)
            job.update(updates)
            job["updated_at"] = self._now_iso()
            self._jobs[job_id] = job
            self._write(job)
            return dict(job)

    def get(self, job_id: str) -> Dict[str, Any]:
        key = str(job_id or "").strip()
        if not key:
            return {}
        with self._lock:
            cached = self._jobs.get(key)
            if cached:
                return dict(cached)
            disk = self._read(key)
            if disk:
                self._jobs[key] = disk
            return dict(disk)

    def _path(self, job_id: str) -> str:
        return os.path.join(self._job_dir, f"{job_id}.json")

    def _write(self, job: Dict[str, Any]) -> None:
        os.makedirs(self._job_dir, exist_ok=True)
        job_id = str(job.get("id") or "").strip()
        if not job_id:
            return
        path = self._path(job_id)
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(job, handle, indent=2)
        os.replace(tmp_path, path)

    def _read(self, job_id: str) -> Dict[str, Any]:
        path = self._path(job_id)
        try:
            with open(path, "r", encoding="utf-8") as handle:
                parsed = json.load(handle)
                return parsed if isinstance(parsed, dict) else {}
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}

    def _trim_locked(self) -> None:
        try:
            os.makedirs(self._job_dir, exist_ok=True)
            entries: List[tuple[str, float]] = []
            for name in os.listdir(self._job_dir):
                if not name.endswith(".json"):
                    continue
                full = os.path.join(self._job_dir, name)
                if not os.path.isfile(full):
                    continue
                entries.append((full, os.path.getmtime(full)))
            if len(entries) <= self._max_jobs:
                return
            entries.sort(key=lambda item: item[1])
            for full, _ in entries[: max(0, len(entries) - self._max_jobs)]:
                try:
                    os.unlink(full)
                except OSError:
                    pass
        except OSError:
            return
