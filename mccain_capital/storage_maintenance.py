"""Bounded persistent-volume observation and scratch cleanup."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import stat
import time
from typing import Iterable


ALLOWED_SCRATCH_NAMES = frozenset({"tmp", "cache"})


def _validated_roots(data_root: Path, names: Iterable[str]) -> list[Path]:
    root = data_root.resolve(strict=True)
    selected: list[Path] = []
    for raw_name in names:
        name = str(raw_name).strip()
        if name not in ALLOWED_SCRATCH_NAMES:
            raise ValueError(f"unsafe cleanup root: {name or '<empty>'}")
        candidate = root / name
        if candidate.is_symlink():
            raise ValueError(f"scratch root cannot be a symlink: {candidate}")
        selected.append(candidate)
    return selected


def cleanup_scratch(
    data_root: Path,
    *,
    scratch_names: Iterable[str] = ("tmp", "cache"),
    retention_hours: int = 168,
    now_epoch: float | None = None,
) -> dict[str, int]:
    """Remove aged regular files from the two dedicated scratch roots only."""

    if retention_hours < 1:
        raise ValueError("retention_hours must be at least 1")
    roots = _validated_roots(Path(data_root), scratch_names)
    cutoff = (time.time() if now_epoch is None else now_epoch) - (retention_hours * 3600)
    removed_files = 0
    removed_dirs = 0
    reclaimed_bytes = 0

    for scratch_root in roots:
        if not scratch_root.exists():
            continue
        for current, directories, filenames in os.walk(
            scratch_root, topdown=True, followlinks=False
        ):
            current_path = Path(current)
            directories[:] = [
                name for name in directories if not (current_path / name).is_symlink()
            ]
            for filename in filenames:
                path = current_path / filename
                try:
                    metadata = path.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if stat.S_ISREG(metadata.st_mode) and metadata.st_mtime < cutoff:
                    path.unlink()
                    removed_files += 1
                    reclaimed_bytes += metadata.st_size
        for current, directories, _filenames in os.walk(
            scratch_root, topdown=False, followlinks=False
        ):
            current_path = Path(current)
            for directory in directories:
                path = current_path / directory
                if path.is_symlink():
                    continue
                try:
                    path.rmdir()
                    removed_dirs += 1
                except OSError:
                    pass

    return {
        "removed_files": removed_files,
        "removed_dirs": removed_dirs,
        "reclaimed_bytes": reclaimed_bytes,
    }


def _usage_line(path: Path) -> str:
    usage = shutil.disk_usage(path)
    used_percent = round((usage.used / usage.total) * 100, 1) if usage.total else 0.0
    return (
        f"total={usage.total} used={usage.used} available={usage.free} "
        f"used_percent={used_percent}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default=os.getenv("STORAGE_MAINTENANCE_DATA_ROOT", "/data"))
    parser.add_argument(
        "--scratch-roots",
        default=os.getenv("STORAGE_MAINTENANCE_SCRATCH_ROOTS", "tmp,cache"),
    )
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=int(os.getenv("STORAGE_MAINTENANCE_RETENTION_HOURS", "168")),
    )
    args = parser.parse_args()
    data_root = Path(args.data_root)
    names = [name.strip() for name in args.scratch_roots.split(",") if name.strip()]

    print(f"[storage-maintenance] before {_usage_line(data_root)}", flush=True)
    result = cleanup_scratch(
        data_root,
        scratch_names=names,
        retention_hours=args.retention_hours,
    )
    print(
        "[storage-maintenance] cleanup "
        f"removed_files={result['removed_files']} removed_dirs={result['removed_dirs']} "
        f"reclaimed_bytes={result['reclaimed_bytes']}",
        flush=True,
    )
    print(f"[storage-maintenance] after {_usage_line(data_root)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
