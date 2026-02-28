"""Shared pytest fixtures for app tests."""

from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mccain_capital import create_app  # noqa: E402
from mccain_capital import app_core as core  # noqa: E402


@pytest.fixture()
def app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Return a test app bound to temp storage paths."""
    db_path = tmp_path / "test.db"
    uploads_dir = tmp_path / "uploads"
    books_dir = tmp_path / "books"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    books_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(core, "DB_PATH", str(db_path))
    monkeypatch.setattr(core, "UPLOAD_DIR", str(uploads_dir))
    monkeypatch.setattr(core, "BOOKS_DIR", str(books_dir))
    monkeypatch.setattr(core, "APP_PASSWORD", "")
    monkeypatch.setattr(core, "APP_PASSWORD_HASH", "")

    flask_app = create_app()
    flask_app.config.update(TESTING=True)
    yield flask_app


@pytest.fixture()
def client(app):
    return app.test_client()
