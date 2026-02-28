"""Auth-gate behavior tests."""

from pathlib import Path

import pytest

from mccain_capital import create_app
from mccain_capital import app_core as core


def test_auth_gate_redirects_to_login_when_enabled(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "auth.db"
    uploads_dir = tmp_path / "uploads"
    books_dir = tmp_path / "books"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    books_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(core, "DB_PATH", str(db_path))
    monkeypatch.setattr(core, "UPLOAD_DIR", str(uploads_dir))
    monkeypatch.setattr(core, "BOOKS_DIR", str(books_dir))
    monkeypatch.setattr(core, "APP_USERNAME", "owner")
    monkeypatch.setattr(core, "APP_PASSWORD_HASH", "")
    monkeypatch.setattr(core, "APP_PASSWORD", "secret")

    app = create_app()
    app.config.update(TESTING=True)

    client = app.test_client()
    resp = client.get("/dashboard", follow_redirects=False)
    assert resp.status_code in {301, 302}
    assert "/login" in resp.headers["Location"]


def test_create_app_requires_secret_key_in_production(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "prod.db"
    uploads_dir = tmp_path / "uploads"
    books_dir = tmp_path / "books"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    books_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(core, "DB_PATH", str(db_path))
    monkeypatch.setattr(core, "UPLOAD_DIR", str(uploads_dir))
    monkeypatch.setattr(core, "BOOKS_DIR", str(books_dir))
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.delenv("SECRET_KEY", raising=False)

    with pytest.raises(RuntimeError, match="SECRET_KEY must be set"):
        create_app()


def test_create_app_skips_workers_when_safe_mode(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "safe.db"
    uploads_dir = tmp_path / "uploads"
    books_dir = tmp_path / "books"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    books_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(core, "DB_PATH", str(db_path))
    monkeypatch.setattr(core, "UPLOAD_DIR", str(uploads_dir))
    monkeypatch.setattr(core, "BOOKS_DIR", str(books_dir))
    monkeypatch.setattr(core.app, "_auto_sync_worker_started", False, raising=False)

    def boom():
        raise RuntimeError("db init failed")

    monkeypatch.setattr(core, "init_db", boom)

    from mccain_capital.services import trades as trades_service

    calls = {"count": 0}

    def fake_start(_app):
        calls["count"] += 1

    monkeypatch.setattr(trades_service, "ensure_auto_sync_worker_started", fake_start)

    app = create_app()

    assert app.config["SAFE_MODE"] is True
    assert calls["count"] == 0
    assert getattr(app, "_auto_sync_worker_started", False) is False
