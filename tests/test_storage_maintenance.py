import os
from pathlib import Path

import pytest

from mccain_capital.storage_maintenance import cleanup_scratch


def test_cleanup_removes_only_aged_allowlisted_scratch_files(tmp_path: Path):
    old = tmp_path / "tmp" / "nested" / "old.bin"
    recent = tmp_path / "cache" / "recent.bin"
    protected = tmp_path / "uploads" / "evidence.json"
    for path in (old, recent, protected):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"evidence")
    now_epoch = 2_000_000_000.0
    os.utime(old, (now_epoch - (8 * 24 * 3600),) * 2)
    os.utime(recent, (now_epoch - 60,) * 2)

    result = cleanup_scratch(tmp_path, retention_hours=168, now_epoch=now_epoch)

    assert result["removed_files"] == 1
    assert not old.exists()
    assert recent.exists()
    assert protected.exists()


def test_cleanup_accepts_missing_scratch_roots(tmp_path: Path):
    assert cleanup_scratch(tmp_path, now_epoch=2_000_000_000.0)["removed_files"] == 0


@pytest.mark.parametrize("unsafe_name", ("uploads", "books", ".", "../tmp", "/data"))
def test_cleanup_rejects_non_scratch_roots_before_deleting(tmp_path: Path, unsafe_name: str):
    protected = tmp_path / "uploads" / "evidence.json"
    protected.parent.mkdir()
    protected.write_text("keep", encoding="utf-8")

    with pytest.raises(ValueError, match="unsafe cleanup root"):
        cleanup_scratch(tmp_path, scratch_names=("tmp", unsafe_name))

    assert protected.read_text(encoding="utf-8") == "keep"


def test_cleanup_rejects_symlinked_scratch_root(tmp_path: Path):
    protected = tmp_path / "uploads"
    protected.mkdir()
    (protected / "evidence.json").write_text("keep", encoding="utf-8")
    (tmp_path / "tmp").symlink_to(protected, target_is_directory=True)

    with pytest.raises(ValueError, match="cannot be a symlink"):
        cleanup_scratch(tmp_path, scratch_names=("tmp",))

    assert (protected / "evidence.json").exists()
