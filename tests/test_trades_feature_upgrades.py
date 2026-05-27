"""Tests for open positions and rebuild reviews feature upgrades."""

import io
import json
import os
import time

from mccain_capital.repositories import trades as trades_repo
from mccain_capital.runtime import db, now_iso


class _FakeLocator:
    def __init__(self, page, selector):
        self.page = page
        self.selector = selector
        self.first = self

    def count(self):
        return int(self.page.selector_counts.get(self.selector, 0))

    def is_visible(self):
        return self.selector in self.page.visible_selectors


class _FakeFrame:
    def __init__(self, page, url="https://frame.example"):
        self.page = page
        self.url = url

    def locator(self, selector):
        return _FakeLocator(self.page, selector)


class _FakeLoginPage:
    def __init__(
        self,
        *,
        visible_selectors=None,
        selector_counts=None,
        appear_after_waits=0,
        appear_selector="",
    ):
        self.url = "https://trade.vanquishtrader.com"
        self.visible_selectors = set(visible_selectors or [])
        self.selector_counts = dict(selector_counts or {})
        for selector in self.visible_selectors:
            self.selector_counts.setdefault(selector, 1)
        self.frames = [_FakeFrame(self)]
        self.wait_count = 0
        self.appear_after_waits = int(appear_after_waits or 0)
        self.appear_selector = appear_selector

    def locator(self, selector):
        return _FakeLocator(self, selector)

    def wait_for_timeout(self, _timeout):
        self.wait_count += 1
        if self.appear_selector and self.wait_count >= self.appear_after_waits:
            self.visible_selectors.add(self.appear_selector)
            self.selector_counts[self.appear_selector] = 1

    def wait_for_selector(self, selector, timeout=0):
        if self.selector_counts.get(selector, 0):
            return _FakeLocator(self, selector)
        raise TimeoutError(selector)

    def evaluate(self, _script):
        return {
            "inputs": [
                {
                    "tag": "input",
                    "id": "login_user_name",
                    "testid": "login_user_name",
                    "visible": True,
                }
            ],
            "buttons": [{"tag": "button", "testid": "login_submit_button", "visible": True}],
        }


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
    assert b"Operator Deck" in resp_live.data
    assert b"Balanced Run" in resp_live.data
    assert b"Failure Guide" in resp_live.data
    assert b"Stored securely" in resp_live.data
    assert b"Save username/password securely" in resp_live.data
    assert b"Vanquish Dashboard" in resp_live.data

    resp_upload = client.get("/trades/upload/statement?ws=upload", follow_redirects=True)
    assert resp_upload.status_code == 200
    assert b"Upload Statement" in resp_upload.data

    resp_rec = client.get("/trades/upload/statement?ws=reconcile", follow_redirects=True)
    assert resp_rec.status_code == 200
    assert b"Reconcile Import Batches (30D)" in resp_rec.data
    assert b"Unresolved Batches" in resp_rec.data


def test_upload_statement_live_workspace_injects_csrf_into_all_sync_forms(client):
    resp = client.get("/trades/upload/statement?ws=live", follow_redirects=True)
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)

    for form_id in (
        'id="live-sync-form"',
        'id="auto-sync-config-form"',
        'id="auto-sync-run-form"',
    ):
        form_start = html.index(form_id)
        form_end = html.index("</form>", form_start)
        form_html = html[form_start:form_end]
        assert 'name="csrf_token"' in form_html


def test_live_sync_workspace_surfaces_account_and_credentials_actions(client):
    account_id = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Protect",
        broker_account_id="default:ACC123",
        account_size=50000.0,
        starting_balance=50000.0,
        max_drawdown=5000.0,
    )
    trades_repo.set_active_account(int(account_id))

    resp = client.get(
        "/trades/upload/statement?ws=live&account_id=all&account_editor=new&credentials=edit",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Edit Credentials" in body
    assert "Selected ledger only" in body
    assert "account-only dedupe" in body
    assert "Vanquish Account Number" in body
    assert "ACC123" in body
    assert "default:ACC123" not in body
    assert "Ready to Run" in body
    assert "Username Override" in body
    assert "Advanced Sync Options" in body
    assert 'name="selected_account_id" value="" form="live-account-form"' in body
    assert "New account mode is blank on purpose." in body
    assert "Cancel" in body


def test_archive_account_hides_it_and_falls_back_to_remaining_active_account(client):
    first_account_id = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Protect 50k",
        broker_account_id="default:OEV0052447",
        account_size=50000.0,
        starting_balance=50000.0,
        max_drawdown=5000.0,
    )
    second_account_id = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Protect 75k",
        broker_account_id="default:OEV0052555",
        account_size=75000.0,
        starting_balance=75000.0,
        max_drawdown=7500.0,
    )
    trades_repo.set_active_account(int(first_account_id))

    resp = client.post(
        "/trades/upload/statement?ws=live",
        data={
            "intent": "archive_account",
            "selected_account_id": str(first_account_id),
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Archived Protect 50k." in body
    assert "OEV0052555" in body
    assert "default:OEV0052555" not in body

    archived = next(
        row
        for row in trades_repo.list_accounts(include_archived=True)
        if int(row["id"]) == int(first_account_id)
    )
    assert int(archived["archived"]) == 1
    assert trades_repo.get_account(int(first_account_id)) is None
    snapshot = trades_repo.account_scope_snapshot()
    assert str(snapshot.get("account_id") or "") == str(second_account_id)


def test_live_sync_can_save_and_reuse_credentials(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    cfg_store = {
        "base_url": "https://trade.vanquishtrader.com",
        "wl": "vanquishtrader",
        "account": "default:TEST123",
        "time_zone": "America/New_York",
        "date_locale": "en-US",
        "report_locale": "en",
        "username": "",
    }
    saved_password = {"value": ""}

    monkeypatch.setattr(trades_svc, "AUTO_SYNC_PASSWORD_FALLBACK", True)
    monkeypatch.setattr(trades_svc, "_set_auto_sync_password", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(trades_svc, "_keyring_client", lambda: None)
    monkeypatch.setattr(trades_svc, "trade_lockout_state", lambda *_args, **_kwargs: {"locked": False})
    monkeypatch.setenv("SECRET_KEY", "unit-test-live-sync-secret")
    monkeypatch.setattr(trades_svc, "_load_broker_sync_config", lambda: dict(cfg_store))

    def _save_cfg(new_cfg):
        cfg_store.clear()
        cfg_store.update(dict(new_cfg))
        if cfg_store.get("password_enc"):
            saved_password["value"] = trades_svc._decrypt_fallback_password(cfg_store["password_enc"])

    monkeypatch.setattr(trades_svc, "_save_broker_sync_config", _save_cfg)
    monkeypatch.setattr(
        trades_svc,
        "_get_auto_sync_password",
        lambda cfg: saved_password["value"] if str(cfg.get("username") or "") == cfg_store.get("username") else "",
    )

    captured = {}

    def _fake_start_sync_job(**kwargs):
        captured.update(kwargs)
        return {"id": "job-live-1"}

    monkeypatch.setattr(trades_svc, "_start_sync_job", _fake_start_sync_job)

    resp = client.post(
        "/trades/sync/live",
        data={
            "mode": "broker",
            "username": "saved-user",
            "password": "saved-pass",
            "base_url": "https://trade.vanquishtrader.com",
            "account": "default:TEST123",
            "remember_credentials": "1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert captured["username"] == "saved-user"
    assert captured["password"] == "saved-pass"
    assert cfg_store["username"] == "saved-user"
    assert cfg_store["password"] == ""
    assert cfg_store.get("password_enc")

    captured.clear()
    resp = client.post(
        "/trades/sync/live",
        data={
            "mode": "broker",
            "username": "",
            "password": "",
            "base_url": "https://trade.vanquishtrader.com",
            "account": "default:TEST123",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert captured["username"] == "saved-user"
    assert captured["password"] == "saved-pass"


def test_live_sync_can_clear_saved_credentials(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    cfg_store = {
        "base_url": "https://trade.vanquishtrader.com",
        "wl": "vanquishtrader",
        "account": "default:TEST123",
        "time_zone": "America/New_York",
        "date_locale": "en-US",
        "report_locale": "en",
        "username": "saved-user",
        "password": "",
        "password_enc": "",
    }
    saved_password = {"value": "saved-pass"}

    monkeypatch.setattr(trades_svc, "AUTO_SYNC_PASSWORD_FALLBACK", True)
    monkeypatch.setattr(trades_svc, "_set_auto_sync_password", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(trades_svc, "_keyring_client", lambda: None)
    monkeypatch.setattr(trades_svc, "trade_lockout_state", lambda *_args, **_kwargs: {"locked": False})
    monkeypatch.setenv("SECRET_KEY", "unit-test-live-sync-secret")
    cfg_store["password_enc"] = trades_svc._encrypt_fallback_password("saved-pass")
    monkeypatch.setattr(trades_svc, "_load_broker_sync_config", lambda: dict(cfg_store))

    def _save_cfg(new_cfg):
        cfg_store.clear()
        cfg_store.update(dict(new_cfg))

    monkeypatch.setattr(trades_svc, "_save_broker_sync_config", _save_cfg)
    monkeypatch.setattr(
        trades_svc,
        "_get_auto_sync_password",
        lambda cfg: saved_password["value"] if str(cfg.get("username") or "") == "saved-user" else "",
    )
    monkeypatch.setattr(
        trades_svc,
        "_clear_auto_sync_password",
        lambda username: saved_password.update(value="") or True,
    )

    resp = client.post(
        "/trades/sync/live",
        data={
            "username": "saved-user",
            "clear_saved_credentials": "1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert cfg_store["username"] == ""
    assert cfg_store["password"] == ""
    assert cfg_store["password_enc"] == ""


def test_live_sync_force_reset_clears_running_lane(client, monkeypatch):
    from mccain_capital.services import trades as trades_svc

    job = {
        "id": "job-live-force",
        "kind": "sync",
        "status": "running",
        "stage": "submit_login",
        "message": "Submitting broker login.",
        "updated_at": now_iso(),
        "created_at": now_iso(),
        "summary": {},
    }

    monkeypatch.setattr(trades_svc, "_get_bg_job", lambda job_id: dict(job) if job_id == job["id"] else {})

    def _force_reset(job_id):
        assert job_id == job["id"]
        out = dict(job)
        out.update({"status": "cancelled", "stage": "reset_required"})
        return out

    monkeypatch.setattr(trades_svc, "_force_reset_sync_job", _force_reset)

    resp = client.post(f"/trades/sync/job/{job['id']}/force-reset", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith(f"/trades/upload/statement?ws=live&job={job['id']}")


def test_trades_page_source_uses_focus_fallback():
    src = open(
        "mccain_capital/services/trades_page.py",
        "r",
        encoding="utf-8",
    ).read()
    assert 'hero_title = "Focus"' in src


def test_trades_balance_bases_section_renders(client):
    resp = client.get("/trades", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Execution Admin and Balance Bases" in resp.data
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
        selected_account_id=None,
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


def test_vanquish_login_selector_prefers_stable_username_test_id():
    from mccain_capital.services import vanquish_live_sync as live_sync

    stable = "[data-testid='login_user_name']"
    generic = "input[name='username']"
    page = _FakeLoginPage(visible_selectors={stable, generic})

    locator = live_sync._first_visible(page, live_sync.SELECTOR_PROFILES["login_user"])

    assert locator.selector == stable


def test_vanquish_login_selector_keeps_generic_username_fallback():
    from mccain_capital.services import vanquish_live_sync as live_sync

    generic = "input[name='username']"
    page = _FakeLoginPage(visible_selectors={generic})

    locator = live_sync._wait_for_login_username(page, timeout_ms=10)

    assert locator.selector == generic


def test_vanquish_login_wait_handles_hydrated_username_field():
    from mccain_capital.services import vanquish_live_sync as live_sync

    stable = "[data-testid='login_user_name']"
    page = _FakeLoginPage(
        selector_counts={"#loginFormContainer": 1},
        appear_after_waits=2,
        appear_selector=stable,
    )

    locator = live_sync._wait_for_login_username(page, timeout_ms=1000)

    assert locator.selector == stable
    assert page.wait_count >= 2


def test_vanquish_login_probe_includes_selector_counts_and_controls():
    from mccain_capital.services import vanquish_live_sync as live_sync

    page = _FakeLoginPage(
        selector_counts={
            "#loginFormContainer": 1,
            "[data-testid='login_user_name']": 0,
            "[data-testid='login_password']": 0,
            "[data-testid='login_submit_button']": 0,
        }
    )

    payload = live_sync._login_probe_payload(page)

    assert payload["url"] == "https://trade.vanquishtrader.com"
    assert payload["login_form_container_count"] == 2
    assert payload["selector_counts"]["login_user"]["[data-testid='login_user_name']"] == 0
    assert payload["inputs"][0]["testid"] == "login_user_name"
    assert payload["buttons"][0]["testid"] == "login_submit_button"


def test_live_sync_reports_browser_boot_stage(monkeypatch):
    from mccain_capital.services import trades as trades_svc
    account_id = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Protect",
        broker_account_id="default:OEV0035974",
        account_size=50000.0,
        starting_balance=50000.0,
        max_drawdown=5000.0,
    )

    monkeypatch.setattr(
        trades_svc.vanquish_live_sync,
        "fetch_statement_html_via_login",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError(
                "[stage:browser_boot] Chromium session could not be created. Target page, context or browser has been closed"
            )
        ),
    )

    out = trades_svc._run_live_sync_once(
        mode="broker",
        username="u",
        password="p",
        base_url="https://trade.vanquishtrader.com",
        account="default:OEV0035974",
        selected_account_id=int(account_id),
        wl="vanquishtrader",
        time_zone="America/New_York",
        date_locale="en-US",
        report_locale="en",
        from_date="2026-03-18",
        to_date="2026-03-18",
        headless=True,
        debug_capture=False,
        debug_only=False,
        source_label="LIVE LOGIN HTML",
    )

    assert out.get("ok") is False
    assert out.get("stage") == "browser_boot"
    assert "Chromium session could not be created" in str(out.get("message") or "")
    assert "[stage:" not in str(out.get("message") or "")


def test_live_sync_reclassifies_resource_pressure_browser_boot(monkeypatch):
    from mccain_capital.services import trades as trades_svc

    account_id = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Protect",
        broker_account_id="default:OEV0035974",
        account_size=50000.0,
        starting_balance=50000.0,
        max_drawdown=5000.0,
    )

    monkeypatch.setattr(
        trades_svc.vanquish_live_sync,
        "fetch_statement_html_via_login",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError(
                "[stage:system_resource] Chromium session could not be created. pthread_create: Resource temporarily unavailable"
            )
        ),
    )

    out = trades_svc._run_live_sync_once(
        mode="broker",
        username="u",
        password="p",
        base_url="https://trade.vanquishtrader.com",
        account="default:OEV0035974",
        selected_account_id=int(account_id),
        wl="vanquishtrader",
        time_zone="America/New_York",
        date_locale="en-US",
        report_locale="en",
        from_date="2026-03-18",
        to_date="2026-03-18",
        headless=True,
        debug_capture=False,
        debug_only=False,
        source_label="LIVE LOGIN HTML",
    )

    assert out.get("ok") is False
    assert out.get("stage") == "system_resource"
    assert "Resource temporarily unavailable" in str(out.get("message") or "")


def test_live_sync_startup_stage_renders_dispatching_without_duplicate_live_surfaces(
    client, monkeypatch, tmp_path
):
    from mccain_capital.services import trades as trades_svc

    bg_dir = tmp_path / ".bg_jobs"
    monkeypatch.setattr(trades_svc, "BG_JOB_DIR", str(bg_dir))

    job = trades_svc._create_bg_job(
        "sync",
        "Live Sync",
        {"source": "manual_live", "from_date": "2026-03-18", "to_date": "2026-03-18"},
    )
    trades_svc._update_bg_job(
        job["id"],
        status="running",
        stage="queue_dispatch",
        message="Sync worker picked up the job.",
    )

    resp = client.get(f"/trades/upload/statement?ws=live&job={job['id']}", follow_redirects=True)

    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Dispatching" in html
    assert 'id="sync-control-deck" hidden' in html
    assert 'id="sync-job-details"' in html
    assert 'Open Run Diagnostics' in html
    assert 'id="sync-job-runway"' not in html


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


def test_ops_backups_config_accepts_localized_scope_date(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc
    from mccain_capital.repositories import trades as trades_repo

    backup_cfg = tmp_path / ".auto_backup_config.json"
    backup_dir = tmp_path / "backups"
    audit_path = tmp_path / ".admin_audit_log.json"
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
            "run_times_et": "16:30",
            "frequency_hours": "24",
            "keep_count": "21",
            "account_scope_enabled": "1",
            "account_scope_start": "03 / 12 / 2026",
            "account_scope_start_balance": "$50,000.00",
            "account_scope_label": "Follow your PLaN",
        },
        follow_redirects=True,
    )
    assert save_resp.status_code == 200

    scope = trades_repo.account_scope_snapshot()
    assert scope.get("enabled") is True
    assert scope.get("start_date") == "2026-03-12"
    assert float(scope.get("starting_balance") or 0.0) == 50000.0


def test_save_auto_backup_config_falls_back_when_primary_unwritable(monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    primary = tmp_path / "blocked" / ".auto_backup_config.json"
    fallback = tmp_path / "fallback" / ".auto_backup_config.json"
    monkeypatch.setattr(
        trades_svc,
        "_auto_backup_config_paths",
        lambda for_read=True: [str(primary), str(fallback)],
    )

    real_safe_write_json = trades_svc._safe_write_json

    def _fake_safe_write_json(path, payload):
        if os.path.abspath(str(path)) == os.path.abspath(str(primary)):
            raise PermissionError("permission denied (test)")
        return real_safe_write_json(path, payload)

    monkeypatch.setattr(trades_svc, "_safe_write_json", _fake_safe_write_json)

    ok = trades_svc._save_auto_backup_config({"enabled": True, "run_times_et": ["16:30"]})
    assert ok is True
    assert fallback.exists()
    saved = json.loads(fallback.read_text(encoding="utf-8"))
    assert saved.get("enabled") is True


def test_ops_backups_config_warns_when_no_persist_path(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    bad_path = tmp_path / "blocked" / ".auto_backup_config.json"
    monkeypatch.setattr(
        trades_svc, "_auto_backup_config_paths", lambda for_read=True: [str(bad_path)]
    )
    real_safe_write_json = trades_svc._safe_write_json

    def _fake_safe_write_json(path, payload):
        if os.path.abspath(str(path)) == os.path.abspath(str(bad_path)):
            raise PermissionError("permission denied (test)")
        return real_safe_write_json(path, payload)

    monkeypatch.setattr(trades_svc, "_safe_write_json", _fake_safe_write_json)

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
        "/ops/backups/config",
        data={
            "enabled": "1",
            "run_times_et": "16:30",
            "frequency_hours": "24",
            "keep_count": "21",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert not bad_path.exists()


def test_record_admin_audit_falls_back_when_primary_unwritable(monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    primary = tmp_path / "blocked" / ".admin_audit_log.json"
    fallback = tmp_path / "fallback" / ".admin_audit_log.json"
    monkeypatch.setattr(
        trades_svc,
        "_admin_audit_paths",
        lambda for_read=True: [str(primary), str(fallback)],
    )

    real_safe_write_json = trades_svc._safe_write_json

    def _fake_safe_write_json(path, payload):
        if os.path.abspath(str(path)) == os.path.abspath(str(primary)):
            raise PermissionError("permission denied (test)")
        return real_safe_write_json(path, payload)

    monkeypatch.setattr(trades_svc, "_safe_write_json", _fake_safe_write_json)

    trades_svc.record_admin_audit("auto_backup_config_saved", {"enabled": True}, actor="owner")

    assert fallback.exists()
    rows = json.loads(fallback.read_text(encoding="utf-8"))
    assert rows and rows[-1].get("action") == "auto_backup_config_saved"


def test_ops_alert_ack_falls_back_when_primary_notify_history_is_unwritable(
    client, monkeypatch, tmp_path
):
    from mccain_capital.services import trades as trades_svc

    primary = tmp_path / "blocked" / ".vanquish_notify_history.json"
    fallback = tmp_path / "fallback" / ".vanquish_notify_history.json"
    monkeypatch.setattr(
        trades_svc,
        "_notify_history_paths",
        lambda for_read=True: [str(primary), str(fallback)],
    )
    fallback.parent.mkdir(parents=True, exist_ok=True)
    fallback.write_text(
        json.dumps(
            {
                "alerts": [
                    {
                        "id": "al_perm",
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

    real_safe_write_json = trades_svc._safe_write_json

    def _fake_safe_write_json(path, payload):
        if os.path.abspath(str(path)) == os.path.abspath(str(primary)):
            raise PermissionError("permission denied (test)")
        return real_safe_write_json(path, payload)

    monkeypatch.setattr(trades_svc, "_safe_write_json", _fake_safe_write_json)

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

    ack = client.post("/ops/alerts/ack", data={"alert_id": "al_perm"}, follow_redirects=True)

    assert ack.status_code == 200
    saved = json.loads(fallback.read_text(encoding="utf-8"))
    row = saved.get("alerts", [])[0]
    assert row.get("status") == "acknowledged"
    assert row.get("ack_by") == "owner"


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
    assert b"Advanced Rule Controls" in resp.data
    assert b"Setup Expectancy Snapshot" in resp.data


def test_trades_paste_page_renders(client):
    resp = client.get("/trades/paste", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Paste Trades" in resp.data
    assert b"tabs please" in resp.data
    assert b"Paste your trade rows here" in resp.data


def test_trades_broker_paste_page_renders(client):
    resp = client.get("/trades/paste/broker", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Paste Broker Fills" in resp.data
    assert b"Convert + Import" in resp.data


def test_statement_html_import_renders_balance_snapshot_result(app, monkeypatch):
    from mccain_capital.services import trades as trades_svc

    account_id = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Protect",
        broker_account_id="default:ACC100",
        account_size=50000.0,
        starting_balance=50000.0,
        max_drawdown=2500.0,
    )
    account = trades_repo.get_account(int(account_id))

    monkeypatch.setattr(
        trades_svc.importing,
        "parse_statement_html_to_broker_paste",
        lambda path: ("", 50125.50, ["Ending balance only"]),
    )
    monkeypatch.setattr(
        trades_svc.importing,
        "insert_balance_snapshot",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(trades_svc, "latest_balance_overall", lambda **kwargs: 50125.50)
    monkeypatch.setattr(trades_svc, "_record_import_batch", lambda **kwargs: None)

    with app.test_request_context("/trades/upload/statement", method="POST"):
        body = trades_svc._handle_statement_html_import(
            "/tmp/fake.html",
            "broker",
            "Statement HTML",
            account=account,
            filename="fake.html",
        )

    assert "Balance Snapshot Imported" in body
    assert "Ending balance only" in body


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
            "gate_setup_type": "Test Setup",
            "gate_invalidation": "Below morning low",
            "gate_max_risk": "$100",
            "gate_market_ready": "1",
            "gate_macro_clear": "1",
            "gate_risk_confirmed": "1",
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
            "gate_setup_type": "Test Setup",
            "gate_invalidation": "Below morning low",
            "gate_max_risk": "$100",
            "gate_market_ready": "1",
            "gate_macro_clear": "1",
            "gate_risk_confirmed": "1",
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


def test_live_sync_job_can_be_cancelled(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    monkeypatch.setattr(trades_svc, "BG_JOB_DIR", str(tmp_path / ".bg_jobs"))
    job = trades_svc._create_bg_job(
        "sync",
        "Live Sync",
        {"source": "manual_live", "from_date": "2026-03-17", "to_date": "2026-03-17"},
    )
    trades_svc._update_bg_job(
        job["id"], status="running", stage="submit_login", message="Logging in."
    )

    resp = client.post(f"/trades/sync/job/{job['id']}/cancel", follow_redirects=False)

    assert resp.status_code == 302
    cancelled = trades_svc._get_bg_job(job["id"])
    assert cancelled["status"] == "cancelled"
    assert cancelled["stage"] == "cancelled"
    assert "ignored" in str(cancelled["message"]).lower()


def test_live_sync_async_start_returns_job_json(client, monkeypatch):
    from mccain_capital.services import trades as trades_svc

    monkeypatch.setattr(trades_svc, "trade_lockout_state", lambda _day: {"locked": False})
    monkeypatch.setattr(
        trades_svc,
        "_load_broker_sync_config",
        lambda: {
            "base_url": "https://trade.vanquishtrader.com",
            "account": "default:OEXXXXXXXX",
            "wl": "vanquishtrader",
            "time_zone": "America/New_York",
            "date_locale": "en-US",
            "report_locale": "en",
        },
    )
    monkeypatch.setattr(
        trades_svc,
        "_start_sync_job",
        lambda **_kwargs: {
            "id": "job-123",
            "kind": "sync",
            "title": "Live Sync",
            "status": "queued",
            "stage": "start",
            "message": "Queued and waiting to start.",
            "created_at": now_iso(),
            "updated_at": now_iso(),
            "summary": {},
            "requested": {"source": "manual_live"},
        },
    )

    resp = client.post(
        "/trades/sync/live?async=1",
        data={
            "mode": "broker",
            "from_date": "2026-03-18",
            "to_date": "2026-03-18",
            "username": "demo-user",
            "password": "demo-pass",
            "base_url": "https://trade.vanquishtrader.com",
            "account": "default:OEXXXXXXXX",
        },
        headers={"Accept": "application/json"},
        follow_redirects=False,
    )

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    assert payload["job"]["id"] == "job-123"
    assert payload["job"]["status"] == "queued"


def test_start_sync_job_launches_dedicated_worker_thread(client, monkeypatch):
    from mccain_capital.services import trades as trades_svc

    launched = {}
    monkeypatch.setattr(
        trades_svc,
        "_create_bg_job",
        lambda kind, title, requested: {
            "id": "job-123",
            "kind": kind,
            "title": title,
            "status": "queued",
            "stage": "start",
            "message": "Queued and waiting to start.",
            "requested": requested,
        },
    )
    monkeypatch.setattr(trades_svc, "_sync_cancel_event", lambda _job_id: object())
    monkeypatch.setattr(trades_svc, "ensure_sync_dispatcher_started", lambda _app: None)
    monkeypatch.setattr(
        trades_svc,
        "_start_sync_job_thread",
        lambda app, worker_payload: launched.update({"app": app, "worker_payload": worker_payload}),
    )

    app = client.application
    with app.app_context():
        job = trades_svc._start_sync_job(
            selected_account_id=7,
            title="Live Sync",
            source_label="LIVE LOGIN HTML",
            record_source="LIVE SYNC",
            mode="broker",
            username="demo-user",
            password="demo-pass",
            base_url="https://trade.vanquishtrader.com",
            account="default:OEXXXXXXXX",
            wl="vanquishtrader",
            time_zone="America/New_York",
            date_locale="en-US",
            report_locale="en",
            from_date="2026-03-18",
            to_date="2026-03-18",
            headless=True,
            debug_capture=False,
            debug_only=False,
            requested={"source": "manual_live"},
        )

    assert job["id"] == "job-123"
    assert launched["app"] is app
    assert launched["worker_payload"]["job"]["id"] == "job-123"
    assert launched["worker_payload"]["selected_account_id"] == 7
    assert launched["worker_payload"]["requested"] == {"source": "manual_live"}


def test_start_sync_job_runs_inline_when_worker_thread_start_is_resource_constrained(
    client, monkeypatch, tmp_path
):
    from mccain_capital.services import trades as trades_svc

    monkeypatch.setattr(trades_svc, "BG_JOB_DIR", str(tmp_path / ".bg_jobs"))
    monkeypatch.setattr(
        trades_svc,
        "_start_sync_job_thread",
        lambda _app, _payload: (_ for _ in ()).throw(RuntimeError("can't start new thread")),
    )

    executed = {}

    def _fake_execute_sync_job(*, app, job, **_kwargs):
        executed["app"] = app
        executed["job_id"] = job["id"]
        trades_svc._update_bg_job(
            job["id"],
            status="success",
            stage="import_complete",
            message="Inline live import finished.",
        )

    monkeypatch.setattr(trades_svc, "_execute_sync_job", _fake_execute_sync_job)

    app = client.application
    with app.app_context():
        job = trades_svc._start_sync_job(
            selected_account_id=7,
            title="Live Sync",
            source_label="LIVE LOGIN HTML",
            record_source="LIVE LOGIN HTML",
            mode="broker",
            username="demo-user",
            password="demo-pass",
            base_url="https://trade.vanquishtrader.com",
            account="default:OEXXXXXXXX",
            wl="vanquishtrader",
            time_zone="America/New_York",
            date_locale="en-US",
            report_locale="en",
            from_date="2026-03-18",
            to_date="2026-03-18",
            headless=True,
            debug_capture=False,
            debug_only=False,
            requested={"source": "manual_live"},
        )

    assert executed["app"] is app
    assert executed["job_id"] == job["id"]
    assert job["status"] == "success"
    assert job["stage"] == "import_complete"


def test_load_last_sync_status_reclassifies_thread_error(tmp_path, monkeypatch):
    from mccain_capital.services import trades as trades_svc

    status_path = tmp_path / ".vanquish_sync_last_run.json"
    monkeypatch.setattr(trades_svc, "BROKER_SYNC_STATUS_PATH", str(status_path))
    status_path.write_text(
        json.dumps(
            {
                "status": "failed",
                "stage": "unknown",
                "message": "can't start new thread",
                "updated_at": now_iso(),
            }
        ),
        encoding="utf-8",
    )

    status = trades_svc._load_last_sync_status()

    assert status["stage"] == "system_resource"
    assert status["stage_help"] == trades_svc.SYNC_STAGE_HELP["system_resource"]


def test_live_sync_async_start_ignores_dispatcher_boot_failures(
    client, monkeypatch, tmp_path
):
    from mccain_capital.services import trades as trades_svc

    monkeypatch.setattr(trades_svc, "BG_JOB_DIR", str(tmp_path / ".bg_jobs"))
    monkeypatch.setattr(
        trades_svc,
        "BROKER_SYNC_STATUS_PATH",
        str(tmp_path / ".vanquish_sync_last_run.json"),
    )
    monkeypatch.setattr(trades_svc, "trade_lockout_state", lambda _day: {"locked": False})
    monkeypatch.setattr(
        trades_svc,
        "_load_broker_sync_config",
        lambda: {
            "base_url": "https://trade.vanquishtrader.com",
            "account": "default:OEXXXXXXXX",
            "wl": "vanquishtrader",
            "time_zone": "America/New_York",
            "date_locale": "en-US",
            "report_locale": "en",
        },
    )
    monkeypatch.setattr(
        trades_svc,
        "ensure_sync_dispatcher_started",
        lambda _app: (_ for _ in ()).throw(RuntimeError("can't start new thread")),
    )
    monkeypatch.setattr(
        trades_svc,
        "_start_sync_job_thread",
        lambda app, worker_payload: worker_payload["job"],
    )

    resp = client.post(
        "/trades/sync/live?async=1",
        data={
            "mode": "broker",
            "from_date": "2026-03-18",
            "to_date": "2026-03-18",
            "username": "demo-user",
            "password": "demo-pass",
            "base_url": "https://trade.vanquishtrader.com",
            "account": "default:OEXXXXXXXX",
        },
        headers={"Accept": "application/json"},
        follow_redirects=False,
    )

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    assert payload["job"]["status"] == "queued"


def test_stale_sync_job_is_reconciled_when_polled(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    bg_dir = tmp_path / ".bg_jobs"
    status_path = tmp_path / ".vanquish_sync_last_run.json"
    monkeypatch.setattr(trades_svc, "BG_JOB_DIR", str(bg_dir))
    monkeypatch.setattr(trades_svc, "BROKER_SYNC_STATUS_PATH", str(status_path))
    monkeypatch.setattr(trades_svc, "SYNC_JOB_STALE_SECONDS", 1)

    job = trades_svc._create_bg_job(
        "sync",
        "Live Sync",
        {"source": "manual_live", "from_date": "2026-03-18", "to_date": "2026-03-18"},
    )
    stale_stamp = "2026-03-18T09:30:00-04:00"
    stale_job = {
        **job,
        "status": "running",
        "stage": "submit_login",
        "message": "Logging in.",
        "created_at": stale_stamp,
        "updated_at": stale_stamp,
        "summary": {},
    }
    bg_dir.mkdir(parents=True, exist_ok=True)
    with open(bg_dir / f"{job['id']}.json", "w", encoding="utf-8") as handle:
        json.dump(stale_job, handle, indent=2)
    with open(status_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "job_id": job["id"],
                "status": "running",
                "stage": "submit_login",
                "message": "Logging in.",
                "requested": {"source": "manual_live"},
                "updated_at": stale_stamp,
            },
            handle,
            indent=2,
        )
    trades_svc._BG_JOB_STORES.pop(str(bg_dir), None)

    resp = client.get(f"/trades/sync/job/{job['id']}", follow_redirects=False)

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    assert payload["job"]["status"] == "failed"
    assert payload["job"]["stage"] == "stale"
    assert "stale" in str(payload["job"]["message"]).lower()
    sync_status = trades_svc._load_last_sync_status()
    assert sync_status["status"] == "failed"
    assert sync_status["stage"] == "stale"


def test_sync_runtime_state_reconciles_stale_jobs_at_startup(monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    bg_dir = tmp_path / ".bg_jobs"
    status_path = tmp_path / ".vanquish_sync_last_run.json"
    monkeypatch.setattr(trades_svc, "BG_JOB_DIR", str(bg_dir))
    monkeypatch.setattr(trades_svc, "BROKER_SYNC_STATUS_PATH", str(status_path))
    monkeypatch.setattr(trades_svc, "SYNC_JOB_STALE_SECONDS", 1)

    job = trades_svc._create_bg_job(
        "sync",
        "Live Sync",
        {"source": "manual_live", "from_date": "2026-03-18", "to_date": "2026-03-18"},
    )
    stale_stamp = "2026-03-18T09:30:00-04:00"
    bg_dir.mkdir(parents=True, exist_ok=True)
    with open(bg_dir / f"{job['id']}.json", "w", encoding="utf-8") as handle:
        json.dump(
            {
                **job,
                "status": "running",
                "stage": "generate_statement",
                "message": "Generating statement.",
                "created_at": stale_stamp,
                "updated_at": stale_stamp,
            },
            handle,
            indent=2,
        )
    with open(status_path, "w", encoding="utf-8") as handle:
        json.dump(
            {
                "job_id": job["id"],
                "status": "running",
                "stage": "generate_statement",
                "message": "Generating statement.",
                "requested": {"source": "manual_live"},
                "updated_at": stale_stamp,
            },
            handle,
            indent=2,
        )
    trades_svc._BG_JOB_STORES.pop(str(bg_dir), None)

    snapshot = trades_svc._reconcile_sync_runtime_state()

    assert snapshot["reconciled_jobs"] >= 1
    assert snapshot["active_job"] == {}
    assert snapshot["last_status"]["status"] == "failed"
    assert snapshot["last_status"]["stage"] == "stale"


def test_live_workspace_shows_latest_active_sync_job_without_query_param(
    client, monkeypatch, tmp_path
):
    from mccain_capital.services import trades as trades_svc

    bg_dir = tmp_path / ".bg_jobs"
    monkeypatch.setattr(trades_svc, "BG_JOB_DIR", str(bg_dir))

    job = trades_svc._create_bg_job(
        "sync",
        "Live Sync",
        {"source": "manual_live", "from_date": "2026-03-18", "to_date": "2026-03-18"},
    )
    trades_svc._update_bg_job(
        job["id"], status="running", stage="generate_statement", message="Generating statement."
    )

    resp = client.get("/trades/upload/statement?ws=live", follow_redirects=True)

    assert resp.status_code == 200
    assert b"Sync Job Status" in resp.data
    assert b"Generating statement." in resp.data


def test_live_sync_rejects_overlap_with_existing_running_job(client, monkeypatch, tmp_path):
    from mccain_capital.services import trades as trades_svc

    bg_dir = tmp_path / ".bg_jobs"
    monkeypatch.setattr(trades_svc, "BG_JOB_DIR", str(bg_dir))

    active_job = trades_svc._create_bg_job(
        "sync",
        "Live Sync",
        {"source": "manual_live", "from_date": "2026-03-18", "to_date": "2026-03-18"},
    )
    trades_svc._update_bg_job(
        active_job["id"],
        status="running",
        stage="submit_login",
        message="Logging in.",
    )

    resp = client.post(
        "/trades/sync/live?async=1",
        data={
            "mode": "broker",
            "from_date": "2026-03-18",
            "to_date": "2026-03-18",
            "username": "demo-user",
            "password": "demo-pass",
            "base_url": "https://trade.vanquishtrader.com",
            "account": "default:OEXXXXXXXX",
        },
        headers={"Accept": "application/json"},
        follow_redirects=False,
    )

    assert resp.status_code == 409
    payload = resp.get_json()
    assert payload["ok"] is False
    assert payload["job"]["id"] == active_job["id"]
    assert "already active" in payload["message"].lower()


def test_manual_trade_auto_adds_no_cut_20_loss_review_tag(client):
    resp = client.post(
        "/trades/new",
        data={
            "trade_date": "2026-02-26",
            "gate_setup_type": "Test Setup",
            "gate_invalidation": "Below morning low",
            "gate_max_risk": "$100",
            "gate_market_ready": "1",
            "gate_macro_clear": "1",
            "gate_risk_confirmed": "1",
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


def test_manual_trade_first_trade_gate_blocks_missing_gate_fields(client):
    resp = client.post(
        "/trades/new",
        data={
            "trade_date": "2026-03-13",
            "entry_time": "10:00 AM",
            "exit_time": "10:10 AM",
            "ticker": "SPX",
            "opt_type": "CALL",
            "strike": "6900",
            "contracts": "1",
            "entry_price": "1.0",
            "exit_price": "1.2",
            "comm": "1.0",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"Trade gate blocked first trade" in resp.data
    assert b"Trade Gate" in resp.data


def test_manual_trade_entry_page_renders_gate_and_checklist(client):
    resp = client.get("/trades/new")
    assert resp.status_code == 200
    assert b"Manual Trade Entry" in resp.data
    assert b"Trade Gate" in resp.data
    assert b"Critical Checklist Gate" in resp.data


def test_manual_trade_first_trade_gate_saves_pass_and_allows_followup_without_gate_fields(client):
    first = client.post(
        "/trades/new",
        data={
            "trade_date": "2026-03-14",
            "gate_setup_type": "ORB",
            "gate_invalidation": "Below opening drive low",
            "gate_max_risk": "$125",
            "gate_market_ready": "1",
            "gate_macro_clear": "1",
            "gate_risk_confirmed": "1",
            "entry_time": "10:00 AM",
            "exit_time": "10:10 AM",
            "ticker": "SPX",
            "opt_type": "CALL",
            "strike": "6900",
            "contracts": "1",
            "entry_price": "1.0",
            "exit_price": "1.2",
            "comm": "1.0",
        },
        follow_redirects=False,
    )
    assert first.status_code == 302

    second = client.post(
        "/trades/new",
        data={
            "trade_date": "2026-03-14",
            "entry_time": "11:00 AM",
            "exit_time": "11:05 AM",
            "ticker": "SPX",
            "opt_type": "PUT",
            "strike": "6880",
            "contracts": "1",
            "entry_price": "1.4",
            "exit_price": "1.1",
            "comm": "1.0",
        },
        follow_redirects=False,
    )
    assert second.status_code == 302


def test_admin_restore_upload_runs_async_job(client, monkeypatch, tmp_path):
    from mccain_capital import runtime as app_runtime
    from mccain_capital.services import trades as trades_svc

    backup_dir = tmp_path / "backups"
    audit_path = tmp_path / ".admin_audit_log.json"
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

    marker = os.path.join(app_runtime.UPLOAD_DIR, "restore_marker_async.txt")
    with open(marker, "w", encoding="utf-8") as f:
        f.write("before-upload-restore")

    run_resp = client.post("/ops/backups/run", follow_redirects=True)
    assert run_resp.status_code == 200
    names = [n for n in os.listdir(backup_dir) if n.endswith(".zip")]
    assert names
    backup_name = names[0]
    backup_path = os.path.join(backup_dir, backup_name)

    with open(marker, "w", encoding="utf-8") as f:
        f.write("after-upload-restore")

    with open(backup_path, "rb") as fh:
        upload_resp = client.post(
            "/admin/restore?async=1",
            data={"backup_zip": (io.BytesIO(fh.read()), backup_name)},
            content_type="multipart/form-data",
        )
    assert upload_resp.status_code == 200
    payload = upload_resp.get_json()
    assert payload["ok"] is True
    job_id = str(payload["job"]["id"])

    final_payload = None
    for _ in range(100):
        status_resp = client.get(f"/ops/jobs/{job_id}")
        assert status_resp.status_code == 200
        final_payload = status_resp.get_json()
        assert final_payload["ok"] is True
        if str(final_payload["job"]["status"]) not in {"queued", "running"}:
            break
        time.sleep(0.05)

    assert final_payload is not None
    assert final_payload["job"]["status"] == "success"
    assert "Restore Complete" in str(final_payload["job"].get("result_html") or "")

    with open(marker, "r", encoding="utf-8") as f:
        restored = f.read()
    assert restored == "before-upload-restore"

    page_resp = client.get(f"/admin/restore?job={job_id}", follow_redirects=True)
    assert page_resp.status_code == 200
    assert b"Upload Restore Archive" in page_resp.data
    assert b"restore-upload-runway" in page_resp.data


def test_admin_restore_async_returns_json_error_when_missing_file(client):
    resp = client.post(
        "/admin/restore?async=1",
        data={},
        follow_redirects=False,
    )
    assert resp.status_code == 400
    payload = resp.get_json()
    assert payload["ok"] is False
    assert payload["error"] == "missing_backup_zip"
    assert "Please choose a backup zip file." in payload["message"]
