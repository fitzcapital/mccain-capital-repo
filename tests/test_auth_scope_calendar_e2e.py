"""End-to-end regression checks for auth + scope + calendar flows."""

from pathlib import Path

from mccain_capital import app_core as core
from mccain_capital import create_app
from mccain_capital.runtime import db


def _insert_trade(*, trade_date: str, net_pl: float, balance: float) -> None:
    with db() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                trade_date,
                "09:30",
                "10:00",
                "SPX",
                "CALL",
                5100.0,
                2.0,
                2.3,
                1,
                200.0,
                1.0,
                float(net_pl) + 1.0,
                float(net_pl),
                0.0,
                float(balance),
                "manual",
                f"{trade_date}T10:00:00-05:00",
            ),
        )


def test_calendar_requires_auth_when_enabled(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "auth-calendar.db"
    uploads_dir = tmp_path / "uploads"
    books_dir = tmp_path / "books"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    books_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(core, "DB_PATH", str(db_path))
    monkeypatch.setattr(core, "UPLOAD_DIR", str(uploads_dir))
    monkeypatch.setattr(core, "BOOKS_DIR", str(books_dir))
    monkeypatch.setattr(core, "APP_USERNAME", "owner")
    monkeypatch.setattr(core, "APP_PASSWORD_HASH", "")
    monkeypatch.setattr(core, "APP_PASSWORD", "secret-pass")

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    resp = client.get("/calendar", follow_redirects=False)
    assert resp.status_code in {301, 302}
    assert "/login" in resp.headers["Location"]


def test_calendar_allows_authenticated_session_when_auth_enabled(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "auth-calendar-login.db"
    uploads_dir = tmp_path / "uploads"
    books_dir = tmp_path / "books"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    books_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(core, "DB_PATH", str(db_path))
    monkeypatch.setattr(core, "UPLOAD_DIR", str(uploads_dir))
    monkeypatch.setattr(core, "BOOKS_DIR", str(books_dir))
    monkeypatch.setattr(core, "APP_USERNAME", "owner")
    monkeypatch.setattr(core, "APP_PASSWORD_HASH", "")
    monkeypatch.setattr(core, "APP_PASSWORD", "secret-pass")

    app = create_app()
    app.config.update(TESTING=True)
    client = app.test_client()

    login_resp = client.post(
        "/login",
        data={"username": "owner", "password": "secret-pass"},
        follow_redirects=False,
    )
    assert login_resp.status_code in {301, 302}

    calendar_resp = client.get("/calendar", follow_redirects=True)
    assert calendar_resp.status_code == 200
    assert b'id="calendarPreview"' in calendar_resp.data
    assert b"dayPreviewButton" in calendar_resp.data


def test_payouts_scope_switch_changes_balance_output(client):
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("starting_balance", "50000"),
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("active_account_start_date", "2026-03-01"),
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("active_account_start_balance", "55000"),
        )

    _insert_trade(trade_date="2026-02-27", net_pl=1000.0, balance=51000.0)
    _insert_trade(trade_date="2026-03-02", net_pl=200.0, balance=51200.0)

    all_resp = client.get("/payouts?scope=all", follow_redirects=True)
    active_resp = client.get("/payouts?scope=active", follow_redirects=True)

    assert all_resp.status_code == 200
    assert active_resp.status_code == 200
    assert b"$51,200.00" in all_resp.data
    assert b"$55,200.00" in active_resp.data
    assert b"Active Account</a>" in active_resp.data
    assert b"All History</a>" in all_resp.data


def test_calendar_preview_backdrop_contract(client):
    resp = client.get("/calendar", follow_redirects=True)
    assert resp.status_code == 200
    assert b'id="calendarPreview"' in resp.data
    assert b"dayPreviewButton" in resp.data
