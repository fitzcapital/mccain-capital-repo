"""Auth-gate behavior tests."""

from pathlib import Path

from mccain_capital import create_app
from mccain_capital import app_core as core
from mccain_capital.runtime import set_setting_value


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


def test_create_app_persists_secret_key_in_production(tmp_path: Path, monkeypatch):
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
    monkeypatch.delenv("SECRET_KEY_FILE", raising=False)

    app = create_app()
    secret_path = tmp_path / ".secret_key"

    assert app.secret_key
    assert secret_path.exists()
    persisted = secret_path.read_text(encoding="utf-8").strip()
    assert persisted == app.secret_key

    app_again = create_app()
    assert app_again.secret_key == persisted


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

    from mccain_capital.services import trades_sync as trades_service

    calls = {"count": 0}

    def fake_start(_app):
        calls["count"] += 1

    monkeypatch.setattr(trades_service, "ensure_auto_sync_worker_started", fake_start)

    app = create_app()

    assert app.config["SAFE_MODE"] is True
    assert calls["count"] == 0
    assert getattr(app, "_auto_sync_worker_started", False) is False


def test_healthz_reports_safe_mode_as_unhealthy(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "safe.db"
    uploads_dir = tmp_path / "uploads"
    books_dir = tmp_path / "books"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    books_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(core, "DB_PATH", str(db_path))
    monkeypatch.setattr(core, "UPLOAD_DIR", str(uploads_dir))
    monkeypatch.setattr(core, "BOOKS_DIR", str(books_dir))

    def boom():
        raise RuntimeError("db init failed")

    monkeypatch.setattr(core, "init_db", boom)

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    resp = client.get("/healthz")
    assert resp.status_code == 503
    payload = resp.get_json()
    assert payload["status"] == "degraded"
    assert payload["safe_mode"] is True


def test_login_page_renders_and_accepts_csrf_token(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "auth.db"
    uploads_dir = tmp_path / "uploads"
    books_dir = tmp_path / "books"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    books_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(core, "DB_PATH", str(db_path))
    monkeypatch.setattr(core, "UPLOAD_DIR", str(uploads_dir))
    monkeypatch.setattr(core, "BOOKS_DIR", str(books_dir))
    monkeypatch.setattr(core, "APP_USERNAME", "")
    monkeypatch.setattr(core, "APP_PASSWORD_HASH", "")
    monkeypatch.setattr(core, "APP_PASSWORD", "")

    app = create_app()
    app.config.update(TESTING=True, CSRF_ENABLED=True)

    with app.app_context():
        set_setting_value("auth_username", "owner")
        from werkzeug.security import generate_password_hash

        set_setting_value("auth_password_hash", generate_password_hash("secret-pass-123"))

    client = app.test_client()
    resp = client.get("/login", follow_redirects=True)
    assert resp.status_code == 200
    assert b'name="csrf_token"' in resp.data

    with client.session_transaction() as sess:
        token = str(sess.get("_csrf_token") or "")

    login = client.post(
        "/login?next=/dashboard",
        data={
            "csrf_token": token,
            "username": "owner",
            "password": "secret-pass-123",
            "next": "/dashboard",
        },
        follow_redirects=False,
    )
    assert login.status_code in {301, 302}
    assert login.headers.get("Location", "").endswith("/dashboard")
