from __future__ import annotations

import os

from mccain_capital.services.background_jobs import BackgroundJobStore


def test_background_job_store_update_uses_unique_temp_files(tmp_path, monkeypatch):
    store = BackgroundJobStore(str(tmp_path / ".bg_jobs"), lambda: "2026-05-21T10:00:00-04:00")
    job = store.create("sync", "Live Sync", {"account_id": 1})

    seen_tmp_paths: list[str] = []
    original_replace = os.replace

    def tracking_replace(src: str, dst: str):
        seen_tmp_paths.append(src)
        return original_replace(src, dst)

    monkeypatch.setattr(os, "replace", tracking_replace)
    store.update(job["id"], status="running")
    store.update(job["id"], status="failed")

    assert len(seen_tmp_paths) == 2
    assert seen_tmp_paths[0] != seen_tmp_paths[1]
    assert all(path.endswith(".tmp") for path in seen_tmp_paths)
