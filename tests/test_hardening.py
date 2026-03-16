"""Hardening regression tests for storage and request security."""

from pathlib import Path

from mccain_capital import app_core as core
from mccain_capital import create_app
from mccain_capital.runtime import db


def test_runtime_dependent_paths_follow_create_app_overrides(tmp_path: Path, monkeypatch):
    from mccain_capital.services import trades as trades_service
    from mccain_capital.services import ui as ui_service

    db_path = tmp_path / "app.db"
    uploads_dir = tmp_path / "uploads"
    books_dir = tmp_path / "books"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    books_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(core, "DB_PATH", str(db_path))
    monkeypatch.setattr(core, "UPLOAD_DIR", str(uploads_dir))
    monkeypatch.setattr(core, "BOOKS_DIR", str(books_dir))

    create_app()

    assert trades_service._auto_backup_config_path() == str(
        uploads_dir / ".auto_backup_config.json"
    )
    assert ui_service._forex_factory_cache_file() == str(
        uploads_dir / ".forex_factory_weekly_cache.json"
    )


def test_csrf_rejects_missing_token(client):
    client.application.config["CSRF_ENABLED"] = True

    resp = client.post(
        "/ops/trading-window",
        data={
            "tw_enabled": "1",
            "tw_start_et": "09:30",
            "tw_done_by_et": "11:30",
            "tw_hard_stop_et": "12:00",
            "next": "/dashboard",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 400


def test_csrf_accepts_valid_token(client):
    client.application.config["CSRF_ENABLED"] = True

    get_resp = client.get("/dashboard", follow_redirects=True)
    assert get_resp.status_code == 200

    with client.session_transaction() as sess:
        token = str(sess.get("_csrf_token") or "")

    resp = client.post(
        "/ops/trading-window",
        data={
            "csrf_token": token,
            "tw_enabled": "1",
            "tw_start_et": "09:30",
            "tw_done_by_et": "11:30",
            "tw_hard_stop_et": "12:00",
            "next": "/dashboard",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert resp.headers.get("Location", "").endswith("/dashboard?tw=settings")


def test_sqlite_connection_uses_wal_mode():
    with db() as conn:
        row = conn.execute("PRAGMA journal_mode").fetchone()
    assert str(row[0] or "").lower() == "wal"


def test_ops_backups_page_shows_persistence_runtime(client):
    resp = client.get("/ops/backups", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Persistence Runtime" in resp.data
    assert b"SQLite Mode" in resp.data
    assert b"Secret Key File" in resp.data
