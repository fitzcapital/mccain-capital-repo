"""Tests for open positions and rebuild reviews feature upgrades."""

import json
import os
import time

from mccain_capital.runtime import db, now_iso


def _insert_trade(
    *,
    trade_date: str,
    ticker: str = "SPX",
    opt_type: str = "CALL",
    strike: float = 5000.0,
    entry_price: float = 1.0,
    exit_price=None,
    contracts: int = 1,
    total_spent: float = 100.0,
    comm: float = 1.0,
    gross_pl=None,
    net_pl=None,
    result_pct=None,
    entry_time: str = "9:35 AM",
    exit_time: str = "",
):
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
                entry_time,
                exit_time,
                ticker,
                opt_type,
                strike,
                entry_price,
                exit_price,
                contracts,
                total_spent,
                comm,
                gross_pl,
                net_pl,
                result_pct,
                50000.0,
                "seed",
                now_iso(),
            ),
        )
        row = conn.execute("SELECT last_insert_rowid() AS id").fetchone()
    return int(row["id"])


def test_open_positions_page_lists_incomplete_rows(client):
    _insert_trade(
        trade_date="2026-02-24",
        ticker="SPX",
        opt_type="CALL",
        strike=6000.0,
        exit_price=None,
        net_pl=None,
        contracts=2,
        total_spent=420.0,
        exit_time="",
    )
    _insert_trade(
        trade_date="2026-02-24",
        ticker="QQQ",
        opt_type="PUT",
        strike=500.0,
        exit_price=1.3,
        net_pl=30.0,
        contracts=1,
        total_spent=100.0,
        gross_pl=31.0,
        result_pct=30.0,
        exit_time="9:42 AM",
    )

    resp = client.get("/trades/open-positions", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Open Positions" in resp.data
    assert b"SPX CALL 6000" in resp.data
    assert b"QQQ PUT 500" not in resp.data


def test_rebuild_reviews_creates_missing_review(client):
    trade_id = _insert_trade(
        trade_date="2026-02-20",
        ticker="SPX",
        opt_type="PUT",
        strike=5900.0,
        entry_price=2.0,
        exit_price=2.8,
        contracts=1,
        total_spent=200.0,
        comm=1.0,
        gross_pl=81.0,
        net_pl=80.0,
        result_pct=40.0,
        entry_time="10:10 AM",
        exit_time="10:22 AM",
    )

    resp = client.post(
        "/trades/reviews/rebuild",
        data={
            "start_date": "2026-02-01",
            "end_date": "2026-02-28",
            "scope": "missing",
            "preserve_manual": "1",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"Rebuild Reviews" in resp.data

    with db() as conn:
        row = conn.execute(
            "SELECT setup_tag, checklist_score FROM trade_reviews WHERE trade_id = ?",
            (trade_id,),
        ).fetchone()
    assert row is not None
    assert row["setup_tag"] == "Statement Import"
    assert row["checklist_score"] is not None


def test_auto_sync_fallback_password_is_encrypted(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    cfg_path = tmp_path / ".vanquish_auto_sync.json"
    monkeypatch.setattr(trades_svc, "BROKER_AUTO_SYNC_CONFIG_PATH", str(cfg_path))
    monkeypatch.setattr(trades_svc, "AUTO_SYNC_PASSWORD_FALLBACK", True)
    monkeypatch.setattr(trades_svc, "_set_auto_sync_password", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(trades_svc, "_keyring_client", lambda: None)
    monkeypatch.setenv("SECRET_KEY", "unit-test-fallback-secret")

    resp = client.post(
        "/trades/sync/auto/config",
        data={
            "auto_enabled": "1",
            "auto_mode": "broker",
            "auto_username": "vanq-user",
            "auto_password": "super-secret-pass",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200

    with open(cfg_path, "r", encoding="utf-8") as f:
        saved = json.load(f)

    assert saved.get("password", "") == ""
    assert isinstance(saved.get("password_enc"), str)
    assert saved.get("password_enc")
    assert "super-secret-pass" not in saved.get("password_enc", "")
    assert trades_svc._get_auto_sync_password(saved) == "super-secret-pass"


def test_sync_reliability_summary_computes_metrics():
    from mccain_capital.services import trades as trades_svc

    history = [
        {
            "updated_at": now_iso(),
            "status": "success",
            "stage": "import_complete",
            "source": "scheduler",
            "duration_sec": 20.0,
        },
        {
            "updated_at": now_iso(),
            "status": "failed",
            "stage": "submit_login",
            "source": "manual_auto_run",
            "duration_sec": 30.0,
        },
        {
            "updated_at": now_iso(),
            "status": "failed",
            "stage": "submit_login",
            "source": "manual_auto_run",
            "duration_sec": 25.0,
        },
    ]
    out = trades_svc._sync_reliability_summary(history, days=30)
    assert out["attempts"] == 3
    assert out["success"] == 1
    assert out["failed"] == 2
    assert round(float(out["success_rate"]), 1) == 33.3
    assert out["top_failure_stage"] == "submit_login"
    assert out["avg_duration_sec"] is not None


def test_upload_statement_workspaces_render(client):
    resp = client.get("/trades/upload/statement", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Import Workspace" in resp.data
    assert b"Sync Reliability (30D)" in resp.data

    resp_live = client.get("/trades/upload/statement?ws=live", follow_redirects=True)
    assert resp_live.status_code == 200
    assert b"Sync Reliability (30D)" in resp_live.data

    resp_upload = client.get("/trades/upload/statement?ws=upload", follow_redirects=True)
    assert resp_upload.status_code == 200
    assert b"Upload Statement" in resp_upload.data

    resp_rec = client.get("/trades/upload/statement?ws=reconcile", follow_redirects=True)
    assert resp_rec.status_code == 200
    assert b"Reconcile Import Batches (30D)" in resp_rec.data
    assert b"Unresolved Batches" in resp_rec.data


def test_trades_balance_bases_section_renders(client):
    resp = client.get("/trades", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Balance Bases (History + Active Account)" in resp.data
    assert b"History Ledger Basis" in resp.data
    assert b"Active Account Basis" in resp.data


def test_trades_update_balance_bases_updates_history_and_scope(client):
    _insert_trade(trade_date="2026-03-02", net_pl=100.0)
    _insert_trade(trade_date="2026-03-04", net_pl=258.6)

    resp_history = client.post(
        "/trades/balance-bases?d=2026-03-04&q=SPX",
        data={"mode": "history", "history_starting_balance": "60000"},
        follow_redirects=True,
    )
    assert resp_history.status_code == 200
    assert b"$60,358.60" in resp_history.data

    with db() as conn:
        start_val = conn.execute(
            "SELECT value FROM settings WHERE key = 'starting_balance'"
        ).fetchone()
        latest_row = conn.execute(
            "SELECT balance FROM trades ORDER BY trade_date DESC, id DESC LIMIT 1"
        ).fetchone()
    assert start_val is not None
    assert float(start_val["value"]) == 60000.0
    assert latest_row is not None
    assert round(float(latest_row["balance"]), 2) == 60358.60

    resp_scope = client.post(
        "/trades/balance-bases?d=2026-03-04&q=SPX",
        data={
            "mode": "scope",
            "scope_enabled": "1",
            "scope_start_date": "2026-03-03",
            "scope_starting_balance": "50000",
            "scope_label": "Funded Account",
        },
        follow_redirects=True,
    )
    assert resp_scope.status_code == 200

    with db() as conn:
        scope_settings = {
            r["key"]: r["value"]
            for r in conn.execute(
                """
                SELECT key, value
                FROM settings
                WHERE key IN (
                  'active_account_start_date',
                  'active_account_start_balance',
                  'active_account_label'
                )
                """
            ).fetchall()
        }
    assert scope_settings.get("active_account_start_date") == "2026-03-03"
    assert float(scope_settings.get("active_account_start_balance") or 0.0) == 50000.0
    assert scope_settings.get("active_account_label") == "Funded Account"


def test_trades_page_data_trust_shows_sync_failure_next_action(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    status_path = tmp_path / "sync_status.json"
    monkeypatch.setattr(trades_svc, "BROKER_SYNC_STATUS_PATH", str(status_path))
    status_path.write_text(
        json.dumps(
            {
                "status": "failed",
                "stage": "reconcile_gate",
                "updated_at_human": "Feb 27, 2026 10:30 AM ET",
            }
        ),
        encoding="utf-8",
    )

    resp = client.get("/trades", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Data Trust" in resp.data
    assert b"Latest sync/import reported a failure or block." in resp.data
    assert b"/trades/upload/statement?ws=live" in resp.data


def test_reconcile_gate_result_blocks_unresolved_conditions():
    from mccain_capital.services import trades as trades_svc

    blocked = trades_svc._reconcile_gate_result(
        {"errors_count": 1, "open_contracts": 0, "balance_delta": 0.0}
    )
    assert blocked["blocked"] is True
    assert blocked["reasons"]

    blocked = trades_svc._reconcile_gate_result(
        {"errors_count": 0, "open_contracts": 2, "balance_delta": 0.0}
    )
    assert blocked["blocked"] is True

    blocked = trades_svc._reconcile_gate_result(
        {
            "errors_count": 0,
            "open_contracts": 0,
            "balance_delta": trades_svc.RECONCILE_GATE_MAX_DELTA + 5.0,
        }
    )
    assert blocked["blocked"] is True

    clean = trades_svc._reconcile_gate_result(
        {"errors_count": 0, "open_contracts": 0, "balance_delta": 0.5}
    )
    assert clean["blocked"] is False


def test_live_sync_skips_balance_reconcile_when_date_fallback_warning(monkeypatch):
    from mccain_capital.services import trades as trades_svc

    monkeypatch.setattr(
        trades_svc.vanquish_live_sync,
        "fetch_statement_html_via_login",
        lambda **_kwargs: (
            "<html><body>statement</body></html>",
            ["Could not set custom From/To in dialog; using visible defaults."],
            [],
            {},
        ),
    )
    monkeypatch.setattr(
        trades_svc.importing,
        "parse_statement_html_to_broker_paste",
        lambda _path: ("row1\nrow2", 54396.20, []),
    )

    seen = {"ending_balance": "unset"}

    def _fake_insert(text, ending_balance=None, commit=False, import_batch_id=""):
        seen["ending_balance"] = ending_balance
        if not commit:
            return (
                0,
                [],
                {
                    "errors_count": 0,
                    "open_contracts": 0,
                    "balance_delta": None,
                    "inserted_trades": 0,
                    "duplicates_skipped": 0,
                },
            )
        return (
            0,
            [],
            {
                "errors_count": 0,
                "open_contracts": 0,
                "balance_delta": None,
                "inserted_trades": 0,
                "duplicates_skipped": 0,
            },
        )

    monkeypatch.setattr(
        trades_svc.importing, "insert_trades_from_broker_paste_with_report", _fake_insert
    )

    out = trades_svc._run_live_sync_once(
        mode="broker",
        username="u",
        password="p",
        base_url="https://trade.vanquishtrader.com",
        account="default:OEV0035974",
        wl="vanquishtrader",
        time_zone="America/New_York",
        date_locale="en-US",
        report_locale="en",
        from_date="2026-02-27",
        to_date="2026-02-27",
        headless=True,
        debug_capture=False,
        debug_only=False,
        source_label="LIVE LOGIN HTML",
    )

    assert out.get("ok") is True
    assert seen["ending_balance"] is None
    assert any(
        "skipped ending-balance reconcile" in str(w).lower() for w in (out.get("warns") or [])
    )


def test_rollback_import_batch_deletes_only_target_batch(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    history_path = tmp_path / ".vanquish_import_history.json"
    monkeypatch.setattr(trades_svc, "BROKER_IMPORT_HISTORY_PATH", str(history_path))
    history_path.write_text(
        json.dumps(
            [
                {"batch_id": "imp_target", "rolled_back": False, "updated_at": now_iso()},
                {"batch_id": "imp_keep", "rolled_back": False, "updated_at": now_iso()},
            ]
        ),
        encoding="utf-8",
    )

    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("auth_username", "owner"),
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("auth_password_hash", "pbkdf2:sha256:1$stub$stub"),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at, import_batch_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-26",
                "9:35 AM",
                "9:45 AM",
                "SPX",
                "CALL",
                6900.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                99.0,
                99.0,
                99.0,
                50099.0,
                "seed target",
                now_iso(),
                "imp_target",
            ),
        )
        target_trade_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at, import_batch_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-26",
                "10:00 AM",
                "10:05 AM",
                "SPX",
                "PUT",
                6890.0,
                1.0,
                1.5,
                1,
                100.0,
                1.0,
                49.0,
                49.0,
                49.0,
                50148.0,
                "seed keep",
                now_iso(),
                "imp_keep",
            ),
        )
        conn.execute(
            """
            INSERT INTO trade_reviews (
                trade_id, setup_tag, session_tag, checklist_score, rule_break_tags, review_note, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (target_trade_id, "Setup", "AM", 80, "", "", now_iso(), now_iso()),
        )

    with client.session_transaction() as sess:
        sess["auth_ok"] = True
        sess["auth_user"] = "owner"

    resp = client.post(
        "/trades/import/rollback", data={"batch_id": "imp_target"}, follow_redirects=True
    )
    assert resp.status_code == 200

    with db() as conn:
        remaining = conn.execute("SELECT import_batch_id FROM trades ORDER BY id ASC").fetchall()
        review = conn.execute(
            "SELECT 1 FROM trade_reviews WHERE trade_id = ?", (target_trade_id,)
        ).fetchone()
    assert [str(r["import_batch_id"]) for r in remaining] == ["imp_keep"]
    assert review is None

    saved_history = json.loads(history_path.read_text(encoding="utf-8"))
    target_entry = next(e for e in saved_history if e.get("batch_id") == "imp_target")
    assert target_entry.get("rolled_back") is True


def test_sync_fail_streak_notification_emits_after_threshold(monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    status_path = tmp_path / ".vanquish_sync_last_run.json"
    history_path = tmp_path / ".vanquish_sync_history.json"
    notify_path = tmp_path / ".vanquish_notify_history.json"
    monkeypatch.setattr(trades_svc, "BROKER_SYNC_STATUS_PATH", str(status_path))
    monkeypatch.setattr(trades_svc, "BROKER_SYNC_HISTORY_PATH", str(history_path))
    monkeypatch.setattr(trades_svc, "BROKER_NOTIFY_HISTORY_PATH", str(notify_path))
    monkeypatch.setattr(trades_svc, "NOTIFY_FAIL_STREAK", 2)
    monkeypatch.setattr(trades_svc, "NOTIFY_WEBHOOK_URL", "")

    trades_svc._save_last_sync_status(
        {
            "status": "failed",
            "stage": "submit_login",
            "message": "failed one",
            "requested": {"source": "scheduler", "mode": "broker"},
            "updated_at": now_iso(),
        }
    )
    trades_svc._save_last_sync_status(
        {
            "status": "failed",
            "stage": "submit_login",
            "message": "failed two",
            "requested": {"source": "scheduler", "mode": "broker"},
            "updated_at": now_iso(),
        }
    )

    notify = json.loads(notify_path.read_text(encoding="utf-8"))
    sent = notify.get("sent", [])
    assert any(e.get("event_type") == "sync_fail_streak" for e in sent)


def test_emit_notification_dedupe_window_suppresses_repeat(monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    notify_path = tmp_path / ".vanquish_notify_history.json"
    monkeypatch.setattr(trades_svc, "BROKER_NOTIFY_HISTORY_PATH", str(notify_path))
    monkeypatch.setattr(trades_svc, "NOTIFY_WEBHOOK_URL", "")
    monkeypatch.setattr(trades_svc, "NOTIFY_DEFAULT_DEDUPE_SECONDS", 999)
    monkeypatch.setattr(trades_svc, "NOTIFY_DEDUPE_BY_EVENT", {"drift_recurrence": 999})

    trades_svc._emit_notification(
        "drift_recurrence", "Drift", "Recurring drift", {"hits": 2, "threshold": 1.0}
    )
    trades_svc._emit_notification(
        "drift_recurrence", "Drift", "Recurring drift", {"hits": 2, "threshold": 1.0}
    )

    notify = json.loads(notify_path.read_text(encoding="utf-8"))
    sent = notify.get("sent", [])
    assert len(sent) == 2
    assert sent[0].get("delivery", {}).get("status") == "local_only"
    assert sent[1].get("delivery", {}).get("status") == "skipped_dedupe"


def test_emit_notification_signs_and_retries_webhook(monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    notify_path = tmp_path / ".vanquish_notify_history.json"
    monkeypatch.setattr(trades_svc, "BROKER_NOTIFY_HISTORY_PATH", str(notify_path))
    monkeypatch.setattr(trades_svc, "NOTIFY_WEBHOOK_URL", "https://example.test/hook")
    monkeypatch.setattr(trades_svc, "NOTIFY_WEBHOOK_SECRET", "test-secret")
    monkeypatch.setattr(trades_svc, "NOTIFY_RETRY_ATTEMPTS", 3)
    monkeypatch.setattr(trades_svc, "NOTIFY_RETRY_BACKOFF_SEC", 0.0)
    monkeypatch.setattr(trades_svc, "NOTIFY_RETRY_BACKOFF_MULTIPLIER", 2.0)
    monkeypatch.setattr(trades_svc, "NOTIFY_DEFAULT_DEDUPE_SECONDS", 0)
    monkeypatch.setattr(trades_svc, "NOTIFY_DEDUPE_BY_EVENT", {})

    calls = {"n": 0, "last_headers": {}}

    class _Resp:
        def read(self):
            return b"ok"

    def _fake_urlopen(req, timeout=0):
        calls["n"] += 1
        calls["last_headers"] = dict(req.headers)
        if calls["n"] < 3:
            raise trades_svc.urllib.error.URLError("temporary")
        return _Resp()

    monkeypatch.setattr(trades_svc.urllib.request, "urlopen", _fake_urlopen)

    trades_svc._emit_notification("sync_fail_streak", "Streak", "Failed 3x", {"streak": 3})

    assert calls["n"] == 3
    sig = calls["last_headers"].get("X-mccain-signature") or calls["last_headers"].get(
        "X-McCain-Signature"
    )
    assert isinstance(sig, str) and sig.startswith("sha256=")

    notify = json.loads(notify_path.read_text(encoding="utf-8"))
    sent = notify.get("sent", [])
    assert sent
    assert sent[-1].get("delivery", {}).get("status") == "delivered"


def test_emit_notification_respects_event_mute(monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    notify_path = tmp_path / ".vanquish_notify_history.json"
    monkeypatch.setattr(trades_svc, "BROKER_NOTIFY_HISTORY_PATH", str(notify_path))
    monkeypatch.setattr(trades_svc, "NOTIFY_WEBHOOK_URL", "")
    notify_path.write_text(
        json.dumps({"muted_by_event": {"reconcile_gate_block": "2999-01-01T00:00:00-05:00"}}),
        encoding="utf-8",
    )

    trades_svc._emit_notification(
        "reconcile_gate_block", "Gate blocked", "blocked", {"batch_id": "b1"}
    )
    saved = json.loads(notify_path.read_text(encoding="utf-8"))
    sent = saved.get("sent", [])
    alerts = saved.get("alerts", [])
    assert sent and sent[-1].get("delivery", {}).get("status") == "muted"
    assert alerts and alerts[-1].get("status") == "muted"


def test_ops_alerts_ack_and_resolve(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    notify_path = tmp_path / ".vanquish_notify_history.json"
    monkeypatch.setattr(trades_svc, "BROKER_NOTIFY_HISTORY_PATH", str(notify_path))
    notify_path.write_text(
        json.dumps(
            {
                "alerts": [
                    {
                        "id": "al_1",
                        "event_type": "sync_fail_streak",
                        "title": "Sync fail streak",
                        "message": "failed 3x",
                        "status": "open",
                        "count": 1,
                        "first_seen_at": now_iso(),
                        "last_seen_at": now_iso(),
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("auth_username", "owner"),
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("auth_password_hash", "pbkdf2:sha256:1$stub$stub"),
        )

    with client.session_transaction() as sess:
        sess["auth_ok"] = True
        sess["auth_user"] = "owner"

    ack = client.post("/ops/alerts/ack", data={"alert_id": "al_1"}, follow_redirects=True)
    assert ack.status_code == 200
    resolve = client.post("/ops/alerts/resolve", data={"alert_id": "al_1"}, follow_redirects=True)
    assert resolve.status_code == 200

    saved = json.loads(notify_path.read_text(encoding="utf-8"))
    row = saved.get("alerts", [])[0]
    assert row.get("status") == "resolved"
    assert row.get("ack_by") == "owner"
    assert row.get("resolved_by") == "owner"


def test_ops_backups_config_and_run_now(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc
    from mccain_capital import runtime as app_runtime

    notify_path = tmp_path / ".vanquish_notify_history.json"
    backup_cfg = tmp_path / ".auto_backup_config.json"
    backup_dir = tmp_path / "backups"
    audit_path = tmp_path / ".admin_audit_log.json"
    monkeypatch.setattr(trades_svc, "BROKER_NOTIFY_HISTORY_PATH", str(notify_path))
    monkeypatch.setattr(trades_svc, "AUTO_BACKUP_CONFIG_PATH", str(backup_cfg))
    monkeypatch.setattr(trades_svc, "AUTO_BACKUP_DIR", str(backup_dir))
    monkeypatch.setattr(trades_svc, "ADMIN_AUDIT_LOG_PATH", str(audit_path))

    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("auth_username", "owner"),
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("auth_password_hash", "pbkdf2:sha256:1$stub$stub"),
        )

    with client.session_transaction() as sess:
        sess["auth_ok"] = True
        sess["auth_user"] = "owner"

    save_resp = client.post(
        "/ops/backups/config",
        data={
            "enabled": "1",
            "run_weekends": "1",
            "run_times_et": "16:30, 20:15",
            "frequency_hours": "12",
            "keep_count": "10",
        },
        follow_redirects=True,
    )
    assert save_resp.status_code == 200
    assert b"Auto Backup Center" in save_resp.data
    assert backup_cfg.exists()
    cfg = json.loads(backup_cfg.read_text(encoding="utf-8"))
    assert cfg.get("enabled") is True
    assert cfg.get("run_weekends") is True
    assert cfg.get("run_times_et") == ["16:30", "20:15"]
    assert int(cfg.get("frequency_hours") or 0) == 12
    assert int(cfg.get("keep_count") or 0) == 10

    marker = os.path.join(app_runtime.UPLOAD_DIR, "restore_marker.txt")
    with open(marker, "w", encoding="utf-8") as f:
        f.write("before-backup")

    run_resp = client.post("/ops/backups/run", follow_redirects=True)
    assert run_resp.status_code == 200
    assert os.path.isdir(backup_dir)
    names = [n for n in os.listdir(backup_dir) if n.endswith(".zip")]
    assert names
    name = names[0]

    page_resp = client.get("/ops/backups", follow_redirects=True)
    assert page_resp.status_code == 200
    assert b"Saved Backups" in page_resp.data
    assert b"System Activity History" in page_resp.data

    dl_resp = client.get(f"/ops/backups/download/{name}", follow_redirects=True)
    assert dl_resp.status_code == 200

    dry_redirect = client.post(
        "/ops/backups/restore-dry-run", data={"name": name}, follow_redirects=False
    )
    assert dry_redirect.status_code == 302
    assert "dry_run=" in (dry_redirect.headers.get("Location") or "")
    dry_page = client.get(f"/ops/backups?dry_run={name}", follow_redirects=True)
    assert dry_page.status_code == 200
    assert b"Restore Dry Run" in dry_page.data

    with open(marker, "w", encoding="utf-8") as f:
        f.write("after-backup")
    restore_resp = client.post("/ops/backups/restore", data={"name": name}, follow_redirects=True)
    assert restore_resp.status_code == 200
    with open(marker, "r", encoding="utf-8") as f:
        restored = f.read()
    assert restored == "before-backup"

    del_resp = client.post("/ops/backups/delete", data={"name": name}, follow_redirects=True)
    assert del_resp.status_code == 200
    assert not os.path.exists(os.path.join(backup_dir, name))

    audit_rows = json.loads(audit_path.read_text(encoding="utf-8"))
    actions = [str(r.get("action") or "") for r in audit_rows]
    assert "auto_backup_config_saved" in actions
    assert "backup_created" in actions
    assert "backup_restored_from_center" in actions


def test_ops_async_backup_job_status_returns_result_html(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    monkeypatch.setattr(trades_svc, "BG_JOB_DIR", str(tmp_path / ".bg_jobs"))
    monkeypatch.setattr(
        trades_svc,
        "_run_backup_once",
        lambda reason, actor: {
            "ok": True,
            "name": "test_backup.zip",
            "size_bytes": 321,
        },
    )

    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("auth_username", "owner"),
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("auth_password_hash", "pbkdf2:sha256:1$stub$stub"),
        )

    with client.session_transaction() as sess:
        sess["auth_ok"] = True
        sess["auth_user"] = "owner"

    start = client.post("/ops/backups/run?async=1")
    assert start.status_code == 200
    payload = start.get_json()
    assert payload["ok"] is True
    job_id = payload["job"]["id"]

    status = client.get(f"/ops/jobs/{job_id}")
    assert status.status_code == 200
    status_payload = status.get_json()
    assert status_payload["ok"] is True
    assert status_payload["job"]["kind"] == "backup"

    deadline = time.time() + 1.5
    job = status_payload["job"]
    while time.time() < deadline and job["status"] in {"queued", "running"}:
        time.sleep(0.05)
        job = client.get(f"/ops/jobs/{job_id}").get_json()["job"]
    assert job["status"] == "success"
    assert "Backup Created" in job["result_html"]


def test_ops_integrity_run_records_audit(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    audit_path = tmp_path / ".admin_audit_log.json"
    monkeypatch.setattr(trades_svc, "ADMIN_AUDIT_LOG_PATH", str(audit_path))

    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("auth_username", "owner"),
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("auth_password_hash", "pbkdf2:sha256:1$stub$stub"),
        )

    with client.session_transaction() as sess:
        sess["auth_ok"] = True
        sess["auth_user"] = "owner"

    resp = client.post("/ops/integrity/run", follow_redirects=True)
    assert resp.status_code == 200
    rows = json.loads(audit_path.read_text(encoding="utf-8"))
    actions = [str(r.get("action") or "") for r in rows]
    assert "integrity_check_run" in actions


def test_rebuild_reviews_supports_async_job_flow(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    monkeypatch.setattr(trades_svc, "BG_JOB_DIR", str(tmp_path / ".bg_jobs"))

    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("auth_username", "owner"),
        )
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("auth_password_hash", "pbkdf2:sha256:1$stub$stub"),
        )

    with client.session_transaction() as sess:
        sess["auth_ok"] = True
        sess["auth_user"] = "owner"

    resp = client.post(
        "/trades/reviews/rebuild?async=1",
        data={"scope": "missing", "preserve_manual": "1"},
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    job_id = payload["job"]["id"]

    deadline = time.time() + 1.5
    job = client.get(f"/ops/jobs/{job_id}").get_json()["job"]
    while time.time() < deadline and job["status"] in {"queued", "running"}:
        time.sleep(0.05)
        job = client.get(f"/ops/jobs/{job_id}").get_json()["job"]
    assert job["kind"] == "review_rebuild"
    assert job["status"] == "success"
    assert "Review Rebuild Complete" in job["result_html"]


def test_trades_playbook_page_renders(client):
    resp = client.get("/trades/playbook", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Playbook Engine" in resp.data


def test_playbook_blocks_manual_trade_when_score_below_min(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    cfg_path = tmp_path / ".playbook_rules.json"
    monkeypatch.setattr(trades_svc, "PLAYBOOK_CONFIG_PATH", str(cfg_path))
    cfg_path.write_text(
        json.dumps(
            {
                "enabled": True,
                "min_checklist_score": 80,
                "max_size_pct": 100.0,
                "blocked_time_blocks": [],
                "require_positive_setup_expectancy": False,
            }
        ),
        encoding="utf-8",
    )

    resp = client.post(
        "/trades/new",
        data={
            "trade_date": "2026-02-26",
            "entry_time": "10:00 AM",
            "exit_time": "10:10 AM",
            "ticker": "SPX",
            "opt_type": "CALL",
            "strike": "6900",
            "contracts": "1",
            "entry_price": "1.0",
            "exit_price": "1.2",
            "comm": "1.0",
            "setup_tag": "Test Setup",
            "session_tag": "AM",
            "checklist_score": "60",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"Playbook blocked trade" in resp.data


def test_playbook_blocks_manual_trade_when_critical_items_missing(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    cfg_path = tmp_path / ".playbook_rules.json"
    monkeypatch.setattr(trades_svc, "PLAYBOOK_CONFIG_PATH", str(cfg_path))
    cfg_path.write_text(
        json.dumps(
            {
                "enabled": True,
                "min_checklist_score": 0,
                "max_size_pct": 100.0,
                "blocked_time_blocks": [],
                "require_positive_setup_expectancy": False,
                "require_critical_checklist": True,
                "critical_items": ["Bias Confirmed", "Risk Defined", "Stop Planned"],
            }
        ),
        encoding="utf-8",
    )

    resp = client.post(
        "/trades/new",
        data={
            "trade_date": "2026-02-26",
            "entry_time": "10:00 AM",
            "exit_time": "10:10 AM",
            "ticker": "SPX",
            "opt_type": "CALL",
            "strike": "6900",
            "contracts": "1",
            "entry_price": "1.0",
            "exit_price": "1.2",
            "comm": "1.0",
            "setup_tag": "Test Setup",
            "session_tag": "AM",
            "checklist_score": "90",
            "critical_item": ["Bias Confirmed"],
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"Playbook blocked trade" in resp.data
    assert b"Missing critical checklist items" in resp.data


def test_anomaly_watch_scanner_emits_alert(monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    notify_path = tmp_path / ".vanquish_notify_history.json"
    monkeypatch.setattr(trades_svc, "BROKER_NOTIFY_HISTORY_PATH", str(notify_path))
    monkeypatch.setattr(trades_svc, "NOTIFY_WEBHOOK_URL", "")
    rows = []
    for i in range(1, 25):
        rows.append(
            {
                "id": i,
                "trade_date": "2026-02-26",
                "entry_time": f"09:{30 + min(i, 29):02d} AM",
                "total_spent": 100.0 if i <= 18 else 260.0,
                "net_pl": 20.0 if i % 3 else -10.0,
                "setup_tag": "ORB",
            }
        )
    monkeypatch.setattr(trades_svc.analytics_repo, "fetch_analytics_rows", lambda: rows)

    trades_svc._scan_anomaly_watch()

    notify = json.loads(notify_path.read_text(encoding="utf-8"))
    sent = notify.get("sent", [])
    event_types = {str(x.get("event_type") or "") for x in sent}
    assert "anomaly_size_spike" in event_types


def test_manual_trade_auto_adds_no_cut_20_loss_review_tag(client):
    resp = client.post(
        "/trades/new",
        data={
            "trade_date": "2026-02-26",
            "entry_time": "10:00 AM",
            "exit_time": "10:10 AM",
            "ticker": "SPX",
            "opt_type": "CALL",
            "strike": "6900",
            "contracts": "1",
            "entry_price": "10.0",
            "exit_price": "7.5",
            "comm": "1.0",
            "setup_tag": "Test Setup",
            "session_tag": "AM",
            "checklist_score": "70",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302

    with db() as conn:
        row = conn.execute(
            """
            SELECT tr.rule_break_tags
            FROM trade_reviews tr
            JOIN trades t ON t.id = tr.trade_id
            ORDER BY t.id DESC
            LIMIT 1
            """
        ).fetchone()
    assert row is not None
    assert "no-cut-20-loss" in str(row["rule_break_tags"] or "")
