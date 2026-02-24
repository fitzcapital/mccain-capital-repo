"""Auth-gate behavior tests."""

from pathlib import Path

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
