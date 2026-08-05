"""Core app behavior tests."""

from datetime import datetime, timedelta
import json
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from mccain_capital import app_core as core
from mccain_capital.repositories import trades as trades_repo
from mccain_capital.runtime import db, get_setting_value, now_iso, set_setting_value, today_iso
from mccain_capital.services import core as core_service
from mccain_capital.services import market_pulse_tape
from mccain_capital.services import ui as ui_service
from werkzeug.security import generate_password_hash


def test_healthz_returns_ok_payload(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["status"] == "ok"
    assert payload["app"] == "mccain-capital"


def test_security_headers_applied(client):
    resp = client.get("/healthz")
    assert resp.headers.get("X-Content-Type-Options") == "nosniff"
    assert resp.headers.get("X-Frame-Options") == "SAMEORIGIN"
    assert resp.headers.get("Referrer-Policy") == "strict-origin-when-cross-origin"
    assert "Content-Security-Policy" in resp.headers


def test_request_profiling_headers_applied(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert float(resp.headers["X-Request-Duration-Ms"]) >= 0.0
    assert float(resp.headers["X-SQLite-Duration-Ms"]) >= 0.0
    assert int(resp.headers["X-SQLite-Query-Count"]) >= 0
    assert "app;dur=" in resp.headers["Server-Timing"]
    assert "sqlite;dur=" in resp.headers["Server-Timing"]


def test_calc_consistency_next_trade_cap_for_positive_pnl():
    consistency = trades_repo.calc_consistency(
        [
            {"net_pl": 493.0},
            {"net_pl": 493.0},
            {"net_pl": 493.0},
            {"net_pl": 493.0},
            {"net_pl": 64.20},
        ]
    )

    assert consistency["ratio"] == 493.0 / 2036.20
    assert consistency["cap_available"] is True
    assert consistency["cap_status"] == "within"
    assert round(consistency["next_win_cap"], 2) == 872.66
    assert round(consistency["remaining_room"], 2) == 379.66


def test_calc_consistency_next_trade_cap_flags_over_threshold():
    consistency = trades_repo.calc_consistency(
        [
            {"net_pl": 800.0},
            {"net_pl": 700.0},
            {"net_pl": 500.0},
        ]
    )

    assert consistency["ratio"] == 0.4
    assert consistency["cap_available"] is True
    assert consistency["cap_status"] == "over"
    assert round(consistency["next_win_cap"], 2) == 857.14
    assert round(consistency["remaining_room"], 2) == 57.14


def test_calc_consistency_cap_unavailable_without_positive_denominator():
    empty = trades_repo.calc_consistency([])
    flat = trades_repo.calc_consistency([{"net_pl": 100.0}, {"net_pl": -100.0}])

    assert empty["cap_available"] is False
    assert empty["next_win_cap"] is None
    assert flat["cap_available"] is False
    assert flat["next_win_cap"] is None


def test_dashboard_renders_consistency_next_trade_cap(client):
    with db() as conn:
        created = now_iso()
        for raw_line, net_pl, balance in (
            ("consistency cap biggest", 493.0, 50493.0),
            ("consistency cap second", 493.0, 50986.0),
            ("consistency cap third", 493.0, 51479.0),
            ("consistency cap fourth", 493.0, 51972.0),
            ("consistency cap base", 64.20, 52036.20),
        ):
            conn.execute(
                """
                INSERT INTO trades (
                    trade_date, entry_time, exit_time, ticker, opt_type, strike,
                    entry_price, exit_price, contracts, total_spent, comm, gross_pl,
                    net_pl, result_pct, balance, raw_line, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    today_iso(),
                    "9:35 AM",
                    "9:48 AM",
                    "SPX",
                    "CALL",
                    5000.0,
                    1.0,
                    1.3,
                    1,
                    100.0,
                    1.0,
                    net_pl,
                    net_pl,
                    30.0,
                    balance,
                    raw_line,
                    created,
                ),
            )

    resp = client.get("/dashboard", follow_redirects=True)
    body = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert "Next cap: $872.66 max winner to stay ≤ 30%." in body
    assert "Use this as the per-trade ceiling across the next 10 trades." in body


def test_core_pages_are_reachable(client):
    for path in [
        "/",
        "/dashboard",
        "/candle-opens",
        "/trades",
        "/journal",
        "/journal/review/weekly",
        "/calculator",
        "/payouts",
        "/ops/system-check",
    ]:
        resp = client.get(path, follow_redirects=True)
        assert resp.status_code == 200, f"Expected 200 for {path}, got {resp.status_code}"


def test_primary_app_surfaces_are_reachable(client):
    for path in [
        "/market-pulse",
        "/trades",
        "/journal",
        "/journal/review/weekly",
        "/calendar",
        "/calculator",
        "/analytics?tab=performance",
        "/payouts",
        "/self-control",
        "/strategies",
        "/playbook",
        "/strat",
        "/ops/alerts",
    ]:
        resp = client.get(path, follow_redirects=True)
        assert resp.status_code == 200, f"Expected 200 for {path}, got {resp.status_code}"


def test_statement_workspace_preserves_active_lane_cta(client):
    resp = client.get("/trades/upload/statement?ws=reconcile", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'class="actionRow workspaceHeroActions" data-preserve-primary="true"' in body
    assert 'class="btn primary" href="/trades/upload/statement?ws=reconcile"' in body
    assert 'class="btn " href="/trades/upload/statement?ws=upload"' in body


def test_dashboard_account_snapshot_and_actions_link_to_scoped_live_upload(client):
    account_id = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Protect",
        broker_account_id="default:ACC999",
        account_size=75000.0,
        starting_balance=75000.0,
        max_drawdown=5000.0,
    )
    trades_repo.set_active_account(int(account_id))

    resp = client.get("/dashboard?scope=active", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "dashboardSnapshotCard-accountDropdown" in body
    assert "dashboardSnapshotDropdownChevron" in body
    assert 'name="account_ids"' in body
    assert "data-dashboard-account-check" in body
    assert "data-dashboard-account-select-all" in body
    assert "data-dashboard-account-clear" in body
    assert "data-dashboard-account-selected-count" in body
    assert 'value="bulk_archive_accounts"' in body
    assert "Archive selected" in body
    assert f"/trades/upload/statement?ws=live&account_id={account_id}" in body
    assert (
        f'<a class="btn dashboardSyncDetailsLink" '
        f'href="/trades/upload/statement?ws=live&account_id={account_id}">'
        "Full sync details</a>" in body
    )
    assert "/trades/upload/statement?ws=live&account_id=all&account_editor=new" in body
    assert "Archive selected" in body
    assert "ACC999" in body
    assert "default:ACC999" not in body


def test_dashboard_milestone_save_persists_settings_and_redirects(client, app):
    app.config["CSRF_ENABLED"] = True
    page = client.get("/dashboard", follow_redirects=True)
    assert page.status_code == 200
    body = page.get_data(as_text=True)
    match = re.search(
        r'<form method="post" action="/dashboard/milestone"[^>]*>.*?'
        r'<input type="hidden" name="csrf_token" value="([^"]+)"',
        body,
        re.S,
    )
    assert match

    resp = client.post(
        "/dashboard/milestone",
        data={
            "csrf_token": match.group(1),
            "milestone_name": "Profit Milestone",
            "milestone_profit_source": "ytd",
            "milestone_profit_goal": "7500",
            "milestone_target_balance": "0.00",
            "y": "2026",
            "m": "6",
            "scope": "all",
            "ticker": "QQQ",
            "pace_tf": "d",
        },
        follow_redirects=True,
    )

    assert resp.status_code == 200
    assert "Profit Milestone" in resp.get_data(as_text=True)
    assert get_setting_value("dashboard_milestone_name") == "Profit Milestone"
    assert get_setting_value("dashboard_milestone_profit_source") == "ytd"
    assert get_setting_value("dashboard_milestone_profit_goal") == "7500.00"
    assert get_setting_value("dashboard_milestone_target_balance") == "0.00"


def test_dashboard_add_account_link_carries_rollover_context(client):
    account_id = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Eval 50k",
        broker_account_id="default:OEV0059123",
        account_size=50000.0,
        starting_balance=50000.0,
        max_drawdown=2500.0,
    )
    trades_repo.set_active_account(int(account_id))

    resp = client.get("/dashboard?scope=active", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert (
        "/trades/upload/statement?ws=live&account_id=all&account_editor=new"
        f"&rollover_from={account_id}"
    ) in body


def test_saving_opa_account_links_prior_eval_for_continuity(client):
    eval_account_id = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Eval 50k",
        broker_account_id="default:OEV0059123",
        account_size=50000.0,
        starting_balance=50000.0,
        max_drawdown=2500.0,
    )
    trades_repo.set_active_account(int(eval_account_id))

    resp = client.post(
        "/trades/upload/statement?ws=live&account_id=all&account_editor=new",
        data={
            "intent": "save_account",
            "selected_account_id": "",
            "rollover_from_account_id": str(eval_account_id),
            "account_name": "Performance 50k",
            "broker_account_id": "OPA0003049",
            "account_size": "50000",
            "starting_balance": "50000",
            "max_drawdown": "2500",
        },
        follow_redirects=True,
    )

    assert resp.status_code == 200
    performance_account = trades_repo.find_account_by_broker_account_id("OPA0003049")
    assert performance_account is not None
    assert performance_account["starting_balance"] == 50000.0
    assert trades_repo.account_continuity_ids(int(performance_account["id"])) == [
        int(eval_account_id),
        int(performance_account["id"]),
    ]
    scope = trades_repo.account_scope_snapshot()
    assert int(scope["account_id"]) == int(performance_account["id"])


def test_dashboard_continuity_uses_prior_eval_for_ledger_not_broker(client):
    eval_account_id = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Eval 50k",
        broker_account_id="default:OEV0059123",
        account_size=50000.0,
        starting_balance=50000.0,
        max_drawdown=2500.0,
    )
    performance_account_id = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Performance 50k",
        broker_account_id="default:OPA0003049",
        account_size=50000.0,
        starting_balance=50000.0,
        max_drawdown=2500.0,
    )
    trades_repo.set_account_continuity(int(performance_account_id), [int(eval_account_id)])
    trades_repo.set_active_account(int(performance_account_id))
    trades_repo.update_account_broker_metrics(
        int(performance_account_id),
        broker_equity=50125.0,
        broker_equity_peak=50125.0,
        broker_remaining_drawdown=2400.0,
        broker_max_loss=47600.0,
        updated_at="2026-06-11T09:30:00-04:00",
    )
    with db() as conn:
        for account_id, raw_line, net_pl, balance in (
            (eval_account_id, "eval rollover trade", 250.0, 50250.0),
            (performance_account_id, "performance trade", 125.0, 50125.0),
        ):
            conn.execute(
                """
                INSERT INTO trades (
                    trade_date, entry_time, exit_time, ticker, opt_type, strike,
                    entry_price, exit_price, contracts, total_spent, comm,
                    gross_pl, net_pl, result_pct, balance, raw_line, created_at, account_id
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "2026-06-11",
                    "10:05 AM",
                    "10:20 AM",
                    "SPX",
                    "CALL",
                    6900.0,
                    1.0,
                    2.0,
                    1,
                    100.0,
                    1.0,
                    net_pl,
                    net_pl,
                    100.0,
                    balance,
                    raw_line,
                    now_iso(),
                    int(account_id),
                ),
            )

    resp = client.get(
        f"/dashboard?y=2026&m=6&scope=active&account_id={performance_account_id}",
        follow_redirects=True,
    )

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Broker Equity" in body
    assert "$50,125.00" in body
    assert "Ledger P&amp;L" in body
    assert "continuity" in body
    assert "$375.00" in body
    assert "OEV0059123 + OPA0003049" in body
    assert "Continuity Ledger" in body


def test_trades_page_uses_action_specific_hero_and_trust_badges(client):
    resp = client.get("/trades", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Start the Session Clean" in body
    assert "Confidence" in body
    assert "Execution + sync" in body
    assert "Execution live" in body


def test_journal_page_uses_review_focus_workflow_surface(client):
    resp = client.get("/journal", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Capture Today While It Is Fresh" in body
    assert "Review Focus" in body
    assert "Capture Pace" in body
    assert "Last Update" in body


def test_trades_empty_state_uses_consistent_start_here_copy(client):
    resp = client.get("/trades", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "No trades in this view yet." in body
    assert "Start by uploading a statement, then add setup, session, and review tags." in body
    assert "First Trade Setup" in body
    assert "Start here:" in body


def test_journal_empty_state_uses_consistent_capture_copy(client):
    resp = client.get("/journal", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "No journal entries in this view yet." in body
    assert "Quick Capture" in body


def test_life_journal_page_renders_personal_workspace(client):
    resp = client.get("/journal/life", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Life Journal" in body
    assert "Save Life Entry" in body
    assert "No life journal entries yet." in body


def test_life_journal_save_persists_summary(client):
    resp = client.post(
        "/journal/life",
        data={
            "entry_date": "2026-04-16",
            "life_title": "Family dinner",
            "life_category": "Family",
            "life_mood": "Grateful",
            "life_notes": "Had dinner with family after a long week. Good reset and better perspective.",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Life journal entry saved." in body
    assert "Family dinner" in body
    assert "Good reset and better perspective." in body


def test_life_journal_edit_page_and_update_flow(client):
    client.post(
        "/journal/life",
        data={
            "entry_date": "2026-04-16",
            "life_title": "Morning walk",
            "life_category": "Health",
            "life_mood": "Calm",
            "life_notes": "Went for a walk before work. Felt clearer after moving early.",
        },
        follow_redirects=True,
    )
    from mccain_capital.repositories import journal as journal_repo

    rows = [dict(r) for r in journal_repo.fetch_entries_by_type("life_note")]
    assert rows
    entry_id = int(rows[0]["id"])

    edit_page = client.get(f"/journal/life/edit/{entry_id}", follow_redirects=True)
    assert edit_page.status_code == 200
    assert "Update Life Entry" in edit_page.get_data(as_text=True)

    resp = client.post(
        f"/journal/life/edit/{entry_id}",
        data={
            "entry_date": "2026-04-16",
            "life_title": "Morning walk updated",
            "life_category": "Health",
            "life_mood": "Focused",
            "life_notes": "What happened: Went for a longer walk before work.\n\nHow I felt: More focused and less rushed.\n\nNext step: Keep the phone away for the first 30 minutes.",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Life journal entry updated." in body
    assert "Morning walk updated" in body
    assert "What Happened" in body
    assert "How I Felt" in body
    assert "Next Step" in body


def test_strategies_page_uses_playbook_workflow_surface(client):
    resp = client.get("/strategies", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Build the Playbook From Real Edge" in body
    assert "Trade Seats" in body
    assert "Playbook Rule" in body


def test_books_page_renders_empty_library_and_featured_shelf(client):
    empty_resp = client.get("/books", follow_redirects=True)
    assert empty_resp.status_code == 200
    empty_body = empty_resp.get_data(as_text=True)
    assert "Private Trading Library" in empty_body
    assert "No PDFs found yet." in empty_body
    assert "No PDFs found in" in empty_body

    books_root = Path(core.BOOKS_DIR)
    books_root.mkdir(parents=True, exist_ok=True)
    featured_path = books_root / "Trading in the Zone -  Mark Douglas.pdf"
    featured_path.write_bytes(b"%PDF-1.4\n% featured test pdf\n")

    featured_resp = client.get("/books", follow_redirects=True)
    assert featured_resp.status_code == 200
    featured_body = featured_resp.get_data(as_text=True)
    assert "Trading in the Zone" in featured_body
    assert "Mindset Pull" in featured_body
    assert "Think in terms of probabilities." in featured_body
    assert (
        '/books/open/Trading in the Zone -  Mark Douglas.pdf">Open Trading in the Zone<'
        in featured_body
    )


def test_playbook_page_renders_trading_doctrine_surface(client):
    resp = client.get("/playbook", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "The Fitz Capital Trading Doctrine" in body
    assert (
        "Mindset, execution, and market-reading principles built for repeatable performance."
        in body
    )
    assert "Five Non-Negotiables" in body
    assert "Before You Click" in body
    assert "Mindset Doctrine" in body
    assert "Execution Doctrine" in body
    assert "Market Reading Doctrine" in body


def test_strat_page_tracks_modules_in_learning_progress_and_includes_deeper_training_notes(client):
    resp = client.get("/strat", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "0 / 23 concepts" in body
    assert "Think in probabilities." in body
    assert "Effort vs result:" in body


def test_strategy_mutations_flash_feedback(client):
    invalid = client.post(
        "/strategies/new",
        data={"title": "", "body": ""},
        follow_redirects=True,
    )
    assert invalid.status_code == 200
    assert b"Title and body required." in invalid.data
    assert b"Keep it executable." in invalid.data

    created = client.post(
        "/strategies/new",
        data={"title": "ORB", "body": "Open range break with defined risk."},
        follow_redirects=True,
    )
    assert created.status_code == 200
    assert b"Strategy saved." in created.data

    with db() as conn:
        row = conn.execute("SELECT id FROM strategies WHERE title = ?", ("ORB",)).fetchone()
        assert row is not None
        sid = int(row["id"])

    updated = client.post(
        f"/strategies/edit/{sid}",
        data={"title": "ORB+", "body": "Open range break with stricter confirmation."},
        follow_redirects=True,
    )
    assert updated.status_code == 200
    assert b"Strategy updated." in updated.data

    deleted = client.post(f"/strategies/delete/{sid}", follow_redirects=True)
    assert deleted.status_code == 200
    assert b"Strategy deleted." in deleted.data


def test_payouts_page_uses_unlock_workflow_surface(client):
    resp = client.get("/payouts", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Build the Buffer Before You Pull Cash" in body
    assert "Payout Planner" in body
    assert "Confidence" in body
    assert "Planner Inputs" in body


def test_payout_readiness_planner_uses_short_lived_cache(monkeypatch):
    from mccain_capital.services import goals as goals_service

    calls = []

    def _fake_gauss(mu, sigma):
        calls.append((mu, sigma))
        return mu

    monkeypatch.setattr(goals_service.random, "gauss", _fake_gauss)
    monkeypatch.setattr(goals_service, "_PAYOUT_SIM_CACHE", {})

    first = goals_service._payout_readiness_planner(
        daily_vals=[100.0, 120.0, 80.0],
        balance=52000.0,
        safe_floor=50500.0,
        biweekly_goal=2000.0,
    )
    second = goals_service._payout_readiness_planner(
        daily_vals=[100.0, 120.0, 80.0],
        balance=52000.0,
        safe_floor=50500.0,
        biweekly_goal=2000.0,
    )

    assert first == second
    assert calls
    first_call_count = len(calls)
    assert first_call_count > 0
    third = goals_service._payout_readiness_planner(
        daily_vals=[100.0, 120.0, 80.0],
        balance=52000.0,
        safe_floor=50500.0,
        biweekly_goal=2500.0,
    )
    assert third
    assert len(calls) > first_call_count


def test_system_check_page_uses_ops_workflow_surface(client):
    resp = client.get("/ops/system-check", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Runtime Looks Healthy" in body or "Resolve Runtime Gaps Before They Compound" in body
    assert "Runtime Read" in body
    assert "Backup Center" in body


def test_login_page_includes_passkey_cta_when_passkeys_exist(client):
    from mccain_capital.runtime import set_setting_value

    set_setting_value("auth_username", "owner")
    set_setting_value("auth_password_hash", generate_password_hash("secret-pass-123"))
    set_setting_value(
        "auth_passkeys",
        json.dumps(
            [
                {
                    "credential_id": "cred_demo",
                    "public_key": "pub_demo",
                    "sign_count": 1,
                    "label": "MacBook Pro",
                    "added_at": "2026-03-14T10:00:00-04:00",
                }
            ]
        ),
    )

    resp = client.get("/login", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Use Passkey" in body
    assert "Passkey sign-in is ready" in body


def test_passkeys_page_renders_registered_devices_for_authenticated_user(client):
    from mccain_capital.runtime import set_setting_value

    set_setting_value("auth_username", "owner")
    set_setting_value("auth_password_hash", generate_password_hash("secret-pass-123"))
    set_setting_value(
        "auth_passkeys",
        json.dumps(
            [
                {
                    "credential_id": "cred_demo_12345678",
                    "public_key": "pub_demo",
                    "sign_count": 2,
                    "label": "MacBook Pro Face ID",
                    "added_at": "2026-03-14T10:00:00-04:00",
                    "last_used_at": "2026-03-14T11:00:00-04:00",
                }
            ]
        ),
    )
    with client.session_transaction() as sess:
        sess["auth_ok"] = True
        sess["auth_user"] = "owner"

    resp = client.get("/auth/passkeys", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Passkey Control" in body
    assert "MacBook Pro Face ID" in body
    assert "Register Passkey" in body


def test_passkeys_page_redirects_loopback_ip_to_localhost(client):
    from mccain_capital.runtime import set_setting_value

    set_setting_value("auth_username", "owner")
    set_setting_value("auth_password_hash", generate_password_hash("secret-pass-123"))
    with client.session_transaction(base_url="http://127.0.0.1:5001") as sess:
        sess["auth_ok"] = True
        sess["auth_user"] = "owner"

    resp = client.get("/auth/passkeys", base_url="http://127.0.0.1:5001")

    assert resp.status_code == 302
    assert resp.headers["Location"] == "http://localhost:5001/auth/passkeys"


def test_passkey_registration_options_use_localhost_rp_for_loopback_ip(client):
    from mccain_capital.runtime import set_setting_value

    set_setting_value("auth_username", "owner")
    set_setting_value("auth_password_hash", generate_password_hash("secret-pass-123"))
    with client.session_transaction(base_url="http://127.0.0.1:5001") as sess:
        sess["auth_ok"] = True
        sess["auth_user"] = "owner"

    resp = client.post(
        "/auth/passkeys/register/options",
        base_url="http://127.0.0.1:5001",
        headers={"Origin": "http://localhost:5001"},
        json={},
    )

    assert resp.status_code == 200
    assert resp.get_json()["publicKey"]["rp"]["id"] == "localhost"


def test_ops_alerts_page_uses_extracted_workflow_template(client):
    with client.session_transaction() as sess:
        sess["auth_ok"] = True
        sess["auth_user"] = "owner"

    resp = client.get("/ops/alerts", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Clear Reliability Risk Before It Pollutes Review" in body
    assert "Review Ops" in body
    assert "Admin Timeline" in body


def test_calculator_page_uses_plan_first_workflow_surface(client):
    resp = client.get("/calculator", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Plan the Trade Before It Plans You" in body
    assert "Decision Rule" in body
    assert "Next Move" in body


def test_base_shell_includes_market_pulse_transition_overlay(client):
    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"marketPulseLoadingOverlay" in resp.data
    assert b"showMarketPulseLoading" in resp.data


def test_base_shell_generalizes_transition_loader_for_internal_pages(client):
    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "window.navigateWithShellLoading" in body
    assert '"/analytics": { title: "Analytics"' in body
    assert '"/strat": { title: "The Strat"' in body


def test_market_pulse_page_uses_deferred_context_refresh_button(client):
    resp = client.get("/market-pulse", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'id="marketPulseContextRefreshBtn"' in body
    assert "/api/market-pulse/context" in body
    assert "if (!pageLoaded || !coreReady) return;" in body
    assert 'id="marketPulseFeedFold"' in body
    assert 'id="marketPulseFeedFold" open' not in body
    assert "Source standby" in body
    assert "Actionability" in body
    assert "Gamma Data" in body
    assert "Decision" in body


def test_market_pulse_context_api_returns_playbook_payload(client):
    resp = client.get("/api/market-pulse/context", follow_redirects=True)
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    assert "payload" in payload
    assert "gamma_snapshot" in payload["payload"]
    assert "execution_model" in payload["payload"]
    assert "market_structure_snapshot" in payload["payload"]


def test_dashboard_renders_daily_brief_card(client):
    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Daily Brief" in resp.data
    assert b"Building the next-session brief" in resp.data
    assert b"More Info" in resp.data
    assert b"Active Level" in resp.data
    assert b"Execution Triggers" in resp.data
    assert b"Do Not Do" in resp.data


def test_dashboard_renders_accountability_checklist(client):
    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Tools" in body
    assert "Permission to trade checklist" in body
    assert "Mindset anchored" in body
    assert "Post-session import" in body
    assert "Debrief before re-risk" in body
    assert "No debrief or quick capture logged for today yet." in body


def test_dashboard_renders_foundation_routine_and_reflection_layers(client):
    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Operating State" in body
    assert "Permission" in body
    assert "Next Step" in body


def test_dashboard_renders_support_health_fold_toggle_button(client):
    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert body.count('id="dashboardWakeLockBtn"') == 1
    assert 'id="dashboardHealthSurface"' in body
    assert 'aria-controls="dashboardHealthSurface"' in body
    assert "Keep Support &amp; Health open" in body
    assert "Keep this dashboard awake" not in body
    assert "dashboardCommandDeck" in body
    assert "dashboardLabelIcon" in body
    assert "dashboardTapeHeaderSubline" in body
    assert "5-Day Memory" in body
    assert "Alignment Before Action" in body
    assert "Scripture Anchor" in body
    assert "Be doers of the word, and not hearers only" in body
    assert "Daily Routine" in body
    assert "Urgency Check" in body
    assert "Doer Score" in body
    assert "Was I a doer today?" in body


def test_dashboard_reflection_update_saves_and_renders(client):
    day = today_iso()
    save_resp = client.post(
        "/api/dashboard/reflection",
        data={
            "reflection_day": day,
            "reflection_answer": "no",
            "reflection_break_alignment": "Forced a setup early.",
            "reflection_urgency_trigger": "Pressure after missing the first move.",
            "reflection_obey_tomorrow": "Wait for confirmation before risk.",
        },
        follow_redirects=False,
    )
    assert save_resp.status_code == 200
    payload = save_resp.get_json()
    assert payload["ok"] is True
    saved = json.loads(get_setting_value(f"dashboard_reflection::{day}", "{}"))
    assert saved["answer"] == "no"
    assert saved["break_alignment"] == "Forced a setup early."

    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Saved to day" in body
    assert "Forced a setup early." in body
    assert "Pressure after missing the first move." in body
    assert "Wait for confirmation before risk." in body


def test_dashboard_behavior_update_saves_daily_summary_and_trend(client):
    day = today_iso()
    resp = client.post(
        "/api/dashboard/behavior",
        data={
            "behavior_day": day,
            "discipline_state": "locked-in",
            "discipline_mode": "a-plus-only",
            "gate_count": "3",
            "routine_done": "12",
            "routine_total": "16",
            "alignment_pct": "88",
            "intention": "Only take confirmed setups.",
            "reflection_answer": "yes",
            "increment_urgency": "1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    saved = json.loads(get_setting_value(f"dashboard_behavior::{day}", "{}"))
    assert saved["alignment_pct"] == 88
    assert saved["gate_count"] == 3
    assert saved["urgency_count"] == 1
    assert payload["trend"]["doer_streak"] >= 1


def test_dashboard_pace_buffer_updates_projection_profit(client):
    resp = client.post(
        "/dashboard/pace",
        data={
            "dashboard_pace_daily": "750",
            "dashboard_pace_buffer": "5000",
            "dashboard_projection_target_date": "2026-05-21",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert get_setting_value("dashboard_pace_buffer", "") == "5000.00"

    dashboard = client.get("/dashboard", follow_redirects=True)
    assert dashboard.status_code == 200
    body = dashboard.get_data(as_text=True)
    assert "Pass buffer ($)" in body
    assert "Buffer $5,000.00 applied." in body


def test_dashboard_accountability_checklist_reflects_today_journal_capture(client):
    from mccain_capital.repositories import journal as journal_repo

    journal_repo.create_entry(
        {
            "entry_date": today_iso(),
            "market": "SPX",
            "setup": "Opening drive",
            "notes": "Logged while context was fresh.",
            "template_payload": {
                "capture_screenshot_path": "journal-captures/2026-03-17/test-shot.png",
            },
        }
    )

    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "1 entry logged" in body
    assert "1 capture attached." in body


def test_dashboard_brief_update_saves_daily_plan(client):
    resp = client.post(
        "/dashboard/brief",
        data={
            "brief_day": "2026-03-13",
            "brief_focus": "Protect A setups only.",
            "brief_plan_a": "Take continuation longs above flip.",
            "brief_plan_b": "Fade extremes only after rejection.",
            "brief_no_trade": "Stand down into CPI.",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert get_setting_value("dashboard_daily_brief::2026-03-13", "")


def test_dashboard_brief_shows_manual_state_and_can_reset(client):
    client.post(
        "/dashboard/brief",
        data={
            "brief_day": today_iso(),
            "brief_focus": "Trade only at A levels.",
            "brief_plan_a": "Continuation only.",
            "brief_plan_b": "Fade only after rejection.",
            "brief_no_trade": "Stand down into macro.",
        },
        follow_redirects=False,
    )

    tuned = client.get("/api/dashboard/planning", follow_redirects=True)
    assert tuned.status_code == 200
    assert b"Manually tuned" in tuned.data

    reset = client.post(
        "/dashboard/brief",
        data={"brief_day": today_iso(), "brief_reset": "1"},
        follow_redirects=False,
    )
    assert reset.status_code == 302

    refreshed = client.get("/api/dashboard/planning", follow_redirects=True)
    assert refreshed.status_code == 200
    assert b"Auto-generated" in refreshed.data


def test_dashboard_daily_brief_filters_stale_macro_events():
    now_et = datetime(2026, 3, 17, 15, 0, tzinfo=core_service.app_runtime.TZ)
    brief = core_service._dashboard_daily_brief_viewmodel(
        now_et=now_et,
        dashboard_spx={"price": 6725.0, "pct_change": 0.2, "day_open": 6718.0},
        dashboard_vix={"price": 19.5},
        gamma_snapshot={"gamma_flip": 6720.0, "call_wall": 6750.0, "put_wall": 6690.0},
        news_snapshot={
            "macro_events": [
                {
                    "headline": "Old CPI",
                    "published_label": "Mon, Mar 9",
                    "summary": "Old event",
                    "starts_at": "2026-03-09T08:30:00-04:00",
                },
                {
                    "headline": "FOMC",
                    "published_label": "Tue, Mar 17",
                    "summary": "Current-day afternoon event",
                    "starts_at": "2026-03-17T16:00:00-04:00",
                },
                {
                    "headline": "Fed Presser",
                    "published_label": "Wed, Mar 18",
                    "summary": "Next event",
                    "starts_at": "2026-03-18T14:00:00-04:00",
                },
            ]
        },
        today_count=0,
        today_net=0.0,
    )
    headlines = [row["headline"] for row in brief["macro_events"]]
    assert "Old CPI" not in headlines
    assert headlines == ["FOMC", "Fed Presser"]


def test_dashboard_daily_brief_anchors_to_nearest_actionable_level():
    now_et = datetime(2026, 3, 17, 13, 0, tzinfo=core_service.app_runtime.TZ)
    brief = core_service._dashboard_daily_brief_viewmodel(
        now_et=now_et,
        dashboard_spx={"price": 6596.0, "pct_change": 0.3, "day_open": 6588.0},
        dashboard_vix={"price": 18.0},
        gamma_snapshot={
            "gamma_flip": 6785.0,
            "local_flip": 6593.0,
            "call_wall": 6600.0,
            "put_wall": 6575.0,
            "next_call_wall": 6620.0,
        },
        market_structure_snapshot={
            "spot": 6596.0,
            "main_flip": 6785.0,
            "local_flip": 6593.0,
            "call_wall": 6600.0,
            "put_wall": 6575.0,
            "next_call_wall": 6620.0,
        },
        news_snapshot={"macro_events": []},
        today_count=0,
        today_net=0.0,
    )
    assert brief["active_level_name"] == "Local Flip"
    assert "Local Flip 6593" in brief["active_level_display"]
    assert "between Local Flip 6593 and Call Wall 6600" in brief["market_location"]
    assert "Hold above Local Flip 6593 supports longs" in brief["execution_triggers"]
    assert "Main Flip" not in brief["market_condition"]


def test_dashboard_brief_uses_true_intraday_day_open(client, monkeypatch):
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import market_data_service
    from mccain_capital.services import market_worker

    monkeypatch.setattr(market_worker, "start_market_worker_once", lambda: None)
    monkeypatch.setattr(
        market_worker,
        "get_market_snapshot",
        lambda: {
            "prices": {
                "SPX": {
                    "price": 6725.0,
                    "pct_change": 0.2,
                    "provider": "tradier",
                    "reason": "tradier_stream_trade",
                    "as_of": "2026-03-17T15:00:00-04:00",
                },
                "VIX": {
                    "price": 19.5,
                    "pct_change": -1.0,
                    "provider": "tradier",
                    "reason": "tradier_stream_trade",
                    "as_of": "2026-03-17T15:00:00-04:00",
                },
            },
            "series": {
                "SPX": [6719.0 + float(i) for i in range(50)],
                "VIX": [20.1 - (0.01 * float(i)) for i in range(50)],
            },
            "series_points": {
                "SPX": [
                    {"ts": f"2026-03-17T14:{30 + (i % 30):02d}:00+00:00", "v": 6719.0 + float(i)}
                    for i in range(50)
                ],
                "VIX": [
                    {
                        "ts": f"2026-03-17T14:{30 + (i % 30):02d}:00+00:00",
                        "v": 20.1 - (0.01 * float(i)),
                    }
                    for i in range(50)
                ],
            },
            "updated_at": "2026-03-17T15:00:00-04:00",
        },
    )
    monkeypatch.setattr(
        market_data_service,
        "get_intraday",
        lambda symbol: (
            [
                {
                    "ts": "2026-03-17T13:30:00+00:00",
                    "open": 6701.25,
                    "high": 6704.0,
                    "low": 6699.0,
                    "close": 6702.0,
                    "volume": 100.0,
                },
                {
                    "ts": "2026-03-17T14:00:00+00:00",
                    "open": 6702.0,
                    "high": 6728.0,
                    "low": 6700.5,
                    "close": 6725.0,
                    "volume": 100.0,
                },
            ]
            if symbol == "SPX"
            else [
                {
                    "ts": "2026-03-17T13:30:00+00:00",
                    "open": 20.0,
                    "high": 20.2,
                    "low": 19.4,
                    "close": 19.5,
                    "volume": 100.0,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        market_data_service,
        "get_prior_session_intraday",
        lambda _symbol, anchor_session_day=None: [],
    )
    monkeypatch.setattr(
        market_data_service,
        "get_watchlist_tradier",
        lambda _symbols: {},
    )
    monkeypatch.setattr(
        market_data_service,
        "get_watchlist",
        lambda _symbols, allow_yf_fallback=False: {},
    )
    monkeypatch.setattr(
        gamma_map_service,
        "get_gamma_snapshot",
        lambda: {"gamma_flip": 6720.0, "call_wall": 6750.0, "put_wall": 6690.0},
    )
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda: {"macro_events": []},
    )

    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Day Open 6701" in body


def test_dashboard_links_to_auto_debrief_draft_when_trades_exist(client):
    with db() as conn:
        created = now_iso()
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm, gross_pl,
                net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                today_iso(),
                "9:35 AM",
                "9:48 AM",
                "SPX",
                "CALL",
                5000.0,
                1.0,
                1.3,
                1,
                100.0,
                1.0,
                30.0,
                29.0,
                29.0,
                50029.0,
                "seed",
                created,
            ),
        )

    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Auto Debrief Draft" in body
    assert "auto_draft=1" in body


def test_dashboard_primary_decision_actions_link_to_market_pulse_trade_gate_and_calendar(client):
    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert 'href="/market-pulse?ticker=SPY"' in body
    assert 'href="/ops/trading-window"' in body
    assert 'href="/calendar"' in body


def test_home_redirects_to_executive_dashboard(client):
    resp = client.get("/", follow_redirects=False)

    assert resp.status_code in {301, 302, 303, 307, 308}
    assert resp.headers["Location"].endswith("/executive")


def test_executive_dashboard_renders_command_center(client):
    resp = client.get("/executive", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "CEO Command Center" in body
    assert "Is McCain Capital healthier than yesterday?" in body
    assert "BOA-Based Projection" in body
    assert "Projection Controls" in body
    assert "Enter BOA balance. The system calculates the rest from the operating plan." in body
    assert "data-exec-recalculate-status" in body
    assert 'aria-live="polite"' in body
    assert "Advanced assumptions hidden" in body
    assert "Show Advanced Assumptions" in body
    assert "Add Adjustment" in body
    assert "Projection Summary" in body
    assert "Visual Command Graphs" in body
    assert "Month Selector" in body
    assert "Monthly Operating Calendar" in body
    assert "Projection Ledger" in body
    assert "Month Projection Timeline" in body
    assert "Budget Details" in body
    assert "Trading Rules" in body
    assert "July 2026" in body
    assert '"id": "2026-09"' in body
    assert '"boaPaycheck2": 4700' in body
    assert '"currentPaycheck2": 4700' in body
    assert "July 2027" in body
    assert "CEO Weekly Scorecard" in body
    assert "BOA Treasury Growth" in body
    assert "Company Metrics" in body
    assert "2026 Annual Targets" in body
    assert "12-Month Roadmap" in body
    assert "McCain Capital Timeline" in body
    assert "Projected BOA Close" in body
    assert "BOA Current Balance" in body
    assert "Chase fixed payment" in body
    assert "Chase Paydown" not in body
    assert "Current State Inputs" not in body
    assert "Consumer cards are locked and being paid down only." in body
    assert "executive_command_center.js" in body
    assert "Connect Current balance" in body
    assert "Waiting for first tracked month" in body
    assert "Placeholder" not in body
    assert 'href="/executive"' in body
    assert "Trading Dashboard" in body


def test_executive_recalculate_reads_current_controls_and_confirms_update():
    script = Path("static/js/executive_command_center.js").read_text()

    for contract in (
        "function recalculateFromControls(button)",
        "nextInputs[key] = parsed",
        "state.inputs[month.id] = nextInputs",
        "Projected close ${formatMoney(projection.projectedBOAClose)}",
        'button.textContent = "Projection Updated"',
        "recalculateFromControls(event.target.closest",
    ):
        assert contract in script


def test_executive_august_guardrails_remain_independent(client):
    resp = client.get("/executive", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    match = re.search(
        r'<script id="executive-operating-months" type="application/json">(.*?)</script>',
        body,
        re.DOTALL,
    )
    assert match
    months = json.loads(match.group(1))
    august = next(month for month in months if month["id"] == "2026-08")

    assert august["redLine"] == 4000
    assert august["openingBOA"] == 4400
    assert august["protectedFloor"] == 5500
    assert august["targetCloseLow"] == 6500
    assert august["temporaryFloor"] == 4000
    assert august["permanentFloorGoal"] == 10000
    assert august["floorActivationMonth"] == "2026-09"
    assert august["deposits"]["currentPaycheck1"] == 1873.78
    assert august["deposits"]["boaPaycheck1"] == 1873.78
    for month in months:
        verizon_bills = [bill for bill in month["bills"] if bill["name"] == "Verizon"]
        assert len(verizon_bills) == 1
        assert verizon_bills[0]["amount"] == 267
        assert verizon_bills[0]["dueDay"] == 26
        assert all("catch-up" not in bill["name"].lower() for bill in month["bills"])
    assert august["paySchedule"]["anchorDate"] == "2026-07-31"
    assert august["paySchedule"]["cadenceDays"] == 14
    assert august["paySchedule"]["regularCurrent"] == 1873.78
    assert august["paySchedule"]["regularBOA"] == 1873.78
    food_bills = [bill for bill in august["bills"] if bill["name"] == "Food"]
    assert food_bills == [
        {"name": "Food", "account": "Current", "amount": 450, "timing": "Monthly"}
    ]
    assert all(bill["name"] != "Food / Dates" for bill in august["bills"])
    chase_bills = [bill for bill in august["bills"] if bill["name"] == "Chase fixed payment"]
    assert chase_bills == [
        {
            "name": "Chase fixed payment",
            "account": "BOA",
            "amount": 376,
            "timing": "Paycheck 1",
            "dueDay": 17,
        }
    ]
    september = next(month for month in months if month["id"] == "2026-09")
    assert september["paySchedule"]["exceptions"]["2026-09-25"] == {
        "boa": 4700,
        "current": 4700,
        "estimated": True,
    }


def test_executive_capital_flow_projection_contract():
    script = Path("static/js/executive_command_center.js").read_text()

    assert "boaObligations" in script
    assert "cycle.boaObligations.length === 1" in script
    assert "cycle.boaObligations.length > 1" in script

    for contract in (
        "const normalizedEntry = (month, entry, index)",
        ".sort((a, b) => a.dueDay - b.dueDay",
        "const dailyPath = [{ day: asOfDay, boa, current",
        "const projectedLow = lowPoint.boa",
        "const fundingGap = Math.max(0, activeHardFloor - projectedLow)",
        "const absorbableUnexpectedExpense = Math.max(0, monthPathCushion)",
        "const asOfDay = projectionAsOfDay(month)",
        'if (["Paid", "Skipped"].includes(saved.status)) return 0',
        "Math.max(asOfDay, entry.dueDay)",
        'floorStatus = "Temporary Floor at Risk"',
        'floorStatus = "$10K Floor Secured"',
        'floorStatus = "Rebuilding Secured Floor"',
        'let floorPhase = "build"',
        'floorPhase = "secured"',
        'floorPhase = "recovery"',
        "dailyPath.slice(index).every",
        'const activeHardFloor = floorPhase === "build" ? temporaryFloor : permanentFloorGoal',
        "state.permanentFloorSecured = true",
        "const fundingCycleForDate = (date)",
        "new Date(2026, 6, 31)",
        "entry.cycleSettled",
        "const buildRequiredFloatEntries = (month, inputs, entries)",
        "Required Current float · Day ${day}",
        "Settled cycle <strong>",
        "$9,400 estimated",
        "projectionLog: []",
        "Recent projection snapshots",
        "].slice(-24)",
        "septemberExceptionEstimated: true",
        "timingEstimated: !hasExplicitDay",
        "The previous projection was kept",
        "renderCapitalFlow(month, projection)",
        "Cycle settled",
    ):
        assert contract in script


def test_executive_capital_flow_receiving_surface(client):
    resp = client.get("/executive", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "data-exec-capital-flow" in body
    assert 'aria-label="Daily capital flow"' in body
    assert (
        "Active floor ${formatMoney(projection.activeHardFloor)}"
        in Path("static/js/executive_command_center.js").read_text()
    )


def test_executive_desktop_cleanup_contract(client):
    resp = client.get("/executive", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    script = Path("static/js/executive_command_center.js").read_text()
    assert 'aria-label="Executive decision band"' in body
    assert "12-month planning baseline" in body
    assert "Year View" not in body
    assert "Net Worth Tracker" not in body
    assert "Capital Allocation" not in body
    assert "selectedMonth: currentOperatingMonth.id" in script
    assert '["Current BOA", formatMoney(monthInputs(month).openingBOA)' in script
    assert "Absorbable Cushion" in script
    assert "Planning baseline, not bank sync" in script
    assert "projected: itemProjection.projectedBOAClose" in script
    assert "months.slice(startIndex, startIndex + 12)" in script
    assert "currentToBOATransfers" in script
    assert "Current Carryover" in script
    assert "Committed Current" in script
    assert "Free Current" in script
    assert "Next-Cycle Support" in script
    assert "Estimated paycheck · Sep 25 · $9,400" in script
    assert "const fundingCycleRows = (month, count = 5)" in script
    assert "const nextCycleCommitment = (month, projection)" in script
    assert "Paycheck-to-paycheck funding map" in script
    assert "Settled in as-of balances" in script
    assert "Current pays cycle obligations" in script
    assert "Avoid unplanned evaluation purchases" in body
    assert "Finish July without more eval purchases" not in body
    assert "Estimated paycheck · $9,400 combined" in script
    assert "Current shortfall" in script
    assert "const currentObligations = isSettled ? [] : entries" in script
    assert "Why BOA support is needed" in script
    assert "Largest cycle obligations" in script
    assert "Other cycle obligations" in script
    assert "otherObligationsTotal" in script
    assert "View all ${cycle.currentObligations.length} cycle bills" in script
    assert "Total Current bills" in script
    assert "Current bills" in script
    assert "BOA covers only the remaining gap" in script
    assert 'tabindex="0" role="group" aria-label="Explain BOA support of' in script
    assert "Remaining BOA path stays at or above $10K" in script
    assert "Estimated paychecks are planning placeholders" in script
    assert "Updates automatically when the local date changes." in script
    assert "const refreshForCalendarChange = () =>" in script
    assert 'document.addEventListener("visibilitychange"' in script
    assert script.count('class="executiveTableScroll"') == 2
    assert "September post-bill surplus sweep" not in script
    assert "CEO Score" not in script
    assert "Cash Runway" not in script
    assert (
        "entry.cycleSettled ? '<small class=\"executiveTimingEstimate\">Automatic</small>'"
        in script
    )
    assert "data-exec-quick-expense-description" in script
    assert "executiveReviewComparison" in script


def test_vanquish_blocklist_download_endpoint(client):
    resp = client.get("/ops/vanquish-blocklist")
    assert resp.status_code == 200
    assert "text/plain" in str(resp.content_type)
    body = resp.get_data(as_text=True)
    assert "trade.vanquishtrader.com" in body
    assert "www.vanquishtrader.com" in body


def test_vanquish_manual_lock_control_roundtrip(client):
    start = client.post(
        "/ops/vanquish-lock",
        data={"action": "start", "duration_minutes": "1", "next": "/dashboard"},
        follow_redirects=True,
    )
    assert start.status_code == 200
    assert b"Milestone" in start.data

    clear = client.post(
        "/ops/vanquish-lock",
        data={"action": "clear", "next": "/dashboard"},
        follow_redirects=True,
    )
    assert clear.status_code == 200
    assert b"Milestone" in clear.data


def test_vanquish_lock_state_endpoint(client):
    resp = client.get("/ops/vanquish-lock-state")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert isinstance(payload, dict)
    assert "active" in payload
    assert "goal" in payload
    assert "day_net" in payload
    assert "unlock_label" in payload


def test_dashboard_trading_window_status_pill_hidden_after_done_by(client):
    from mccain_capital.runtime import set_setting_value
    from mccain_capital.services import ui as ui_service

    set_setting_value("trading_window_enabled", "1")
    set_setting_value("trading_window_start_et", "09:30")
    set_setting_value("trading_window_done_by_et", "11:30")
    set_setting_value("trading_window_test_mode", "1")
    set_setting_value("trading_window_test_date", "2026-03-12")
    set_setting_value("trading_window_test_time_et", "12:45")

    state = ui_service.get_trading_window_state()
    assert state["state"] == "stop"
    assert state["show_banner"] is False

    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"tradingWindowPill" not in resp.data


def test_dashboard_trading_window_status_pill_shows_only_when_open_is_soon(client):
    from mccain_capital.runtime import set_setting_value
    from mccain_capital.services import ui as ui_service

    set_setting_value("trading_window_enabled", "1")
    set_setting_value("trading_window_start_et", "09:30")
    set_setting_value("trading_window_done_by_et", "11:30")
    set_setting_value("trading_window_test_mode", "1")
    set_setting_value("trading_window_test_date", "2026-03-12")
    set_setting_value("trading_window_test_time_et", "08:50")

    state = ui_service.get_trading_window_state()
    assert state["state"] == "upcoming"
    assert state["show_banner"] is True

    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"tradingWindowPill" in resp.data
    assert b"Starts 09:30 ET" in resp.data


def test_dashboard_trading_window_status_pill_hidden_when_open_is_not_soon(client):
    from mccain_capital.runtime import set_setting_value
    from mccain_capital.services import ui as ui_service

    set_setting_value("trading_window_enabled", "1")
    set_setting_value("trading_window_start_et", "09:30")
    set_setting_value("trading_window_done_by_et", "11:30")
    set_setting_value("trading_window_test_mode", "1")
    set_setting_value("trading_window_test_date", "2026-03-12")
    set_setting_value("trading_window_test_time_et", "07:15")

    state = ui_service.get_trading_window_state()
    assert state["state"] == "pending"
    assert state["show_banner"] is False

    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"tradingWindowPill" not in resp.data


def test_dashboard_trading_window_status_pill_hidden_on_weekend_without_test_mode(client):
    from mccain_capital.runtime import set_setting_value

    set_setting_value("trading_window_enabled", "1")
    set_setting_value("trading_window_start_et", "09:30")
    set_setting_value("trading_window_done_by_et", "11:30")
    set_setting_value("trading_window_test_mode", "0")

    from mccain_capital.services import ui as ui_service

    class WeekendDatetime(ui_service.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 3, 14, 10, 0, tzinfo=tz or ui_service.TZ)

    original_datetime = ui_service.datetime
    ui_service.datetime = WeekendDatetime
    try:
        state = ui_service.get_trading_window_state()
        assert state["show_banner"] is False

        resp = client.get("/dashboard", follow_redirects=True)
        assert resp.status_code == 200
        assert b"tradingWindowPill" not in resp.data
    finally:
        ui_service.datetime = original_datetime


def test_trading_window_config_endpoint_saves_times(client):
    resp = client.post(
        "/ops/trading-window",
        data={
            "tw_enabled": "1",
            "tw_start_et": "09:35",
            "tw_done_by_et": "11:20",
            "tw_upcoming_notice_minutes": "45",
            "tw_test_mode": "1",
            "tw_test_date": "2026-03-12",
            "tw_test_time_et": "10:15",
            "next": "/dashboard",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert resp.headers.get("Location", "").endswith("/dashboard")

    assert str(get_setting_value("trading_window_enabled", "")) == "1"
    assert str(get_setting_value("trading_window_start_et", "")) == "09:35"
    assert str(get_setting_value("trading_window_done_by_et", "")) == "11:20"
    assert str(get_setting_value("trading_window_hard_stop_et", "")) == "11:20"
    assert str(get_setting_value("trading_window_upcoming_notice_minutes", "")) == "45"
    assert str(get_setting_value("trading_window_test_mode", "")) == "1"
    assert str(get_setting_value("trading_window_test_date", "")) == "2026-03-12"
    assert str(get_setting_value("trading_window_test_time_et", "")) == "10:15"


def test_trading_window_config_follow_redirect_shows_success_feedback(client):
    resp = client.post(
        "/ops/trading-window",
        data={
            "tw_enabled": "1",
            "tw_start_et": "09:40",
            "tw_done_by_et": "11:10",
            "tw_upcoming_notice_minutes": "30",
            "next": "/ops/trading-window",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"Trading window saved." in resp.data
    assert b"Session Guardrail Settings" in resp.data
    assert b"Upcoming Notice" in resp.data


def test_trading_window_config_can_disable_feature(client):
    resp = client.post(
        "/ops/trading-window",
        data={
            "tw_start_et": "09:30",
            "tw_done_by_et": "10:30",
            "tw_upcoming_notice_minutes": "60",
            "next": "/ops/trading-window",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert str(get_setting_value("trading_window_enabled", "")) == "0"

    from mccain_capital.services import ui as ui_service

    state = ui_service.get_trading_window_state()
    assert state["enabled"] is False
    assert state["state"] == "off"
    assert state["show_banner"] is False
    assert b"TRADING WINDOW OFF" in resp.data


def test_trading_window_settings_page_renders_form(client):
    resp = client.get("/ops/trading-window", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Session Guardrail Settings" in resp.data
    assert b'name="tw_enabled"' in resp.data
    assert b"Turn off to remove the session guardrail window" in resp.data
    assert b'input type="time" name="tw_done_by_et"' in resp.data
    assert b"tw_hard_stop_et" not in resp.data


def test_candle_opens_news_includes_placeholder_weeks(monkeypatch):
    monkeypatch.setattr(
        core_service,
        "get_forex_factory_month_feed",
        lambda: [
            {
                "country": "USD",
                "impact": "High",
                "title": "NFP",
                "date": "2026-03-06T08:30:00-05:00",
            }
        ],
    )
    monkeypatch.setattr(core_service, "get_forex_factory_feed", lambda: [])
    monkeypatch.setattr(core_service, "get_forex_factory_next_week_feed", lambda: [])
    out = core_service._forex_factory_usd_window_events(
        core_service.date(2026, 3, 1), core_service.date(2026, 3, 31)
    )
    assert bool(out.get("available"))
    assert "2026-03-11" in set((out.get("events_by_day") or {}).keys())


def test_candle_opens_uses_titled_march_backup_events(monkeypatch):
    monkeypatch.setattr(core_service, "get_forex_factory_month_feed", lambda: [])
    monkeypatch.setattr(core_service, "get_forex_factory_feed", lambda: [])
    monkeypatch.setattr(core_service, "get_forex_factory_next_week_feed", lambda: [])
    out = core_service._forex_factory_usd_window_events(
        core_service.date(2026, 3, 1), core_service.date(2026, 3, 31)
    )
    assert bool(out.get("fallback_used"))
    assert int(out.get("fallback_count") or 0) >= 10
    assert "curated backup" in str(out.get("source_note") or "").lower()
    event_days = set((out.get("events_by_day") or {}).keys())
    for expected in (
        "2026-03-11",
        "2026-03-12",
        "2026-03-13",
        "2026-03-16",
        "2026-03-18",
        "2026-03-19",
        "2026-03-24",
        "2026-03-25",
        "2026-03-26",
        "2026-03-31",
    ):
        assert expected in event_days
    march_18_titles = [
        row["title"] for row in (out.get("events_by_day") or {}).get("2026-03-18", [])
    ]
    assert "FOMC Rate Decision" in march_18_titles
    assert "FOMC Press Conference" in march_18_titles


def test_candle_opens_uses_titled_april_next_week_backup_events(monkeypatch):
    monkeypatch.setattr(core_service, "get_forex_factory_month_feed", lambda: [])
    monkeypatch.setattr(core_service, "get_forex_factory_feed", lambda: [])
    monkeypatch.setattr(core_service, "get_forex_factory_next_week_feed", lambda: [])
    out = core_service._forex_factory_usd_window_events(
        core_service.date(2026, 4, 12), core_service.date(2026, 4, 18)
    )
    assert bool(out.get("fallback_used"))
    assert int(out.get("fallback_count") or 0) >= 10
    event_days = set((out.get("events_by_day") or {}).keys())
    for expected in ("2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16"):
        assert expected in event_days
    april_14_titles = [
        row["title"] for row in (out.get("events_by_day") or {}).get("2026-04-14", [])
    ]
    assert "Core PPI m/m" in april_14_titles
    assert "PPI m/m" in april_14_titles


def test_candle_opens_sorts_events_by_actual_timestamp(monkeypatch):
    monkeypatch.setattr(
        core_service,
        "get_forex_factory_month_feed",
        lambda: [
            {
                "country": "USD",
                "impact": "High",
                "title": "JOLTS Job Openings",
                "date": "2026-03-13T10:00:00-04:00",
            },
            {
                "country": "USD",
                "impact": "High",
                "title": "Core PCE Price Index m/m",
                "date": "2026-03-13T08:30:00-04:00",
            },
            {
                "country": "USD",
                "impact": "High",
                "title": "GDP (Second Estimate) q/q",
                "date": "2026-03-13T08:30:00-04:00",
            },
        ],
    )
    monkeypatch.setattr(core_service, "get_forex_factory_feed", lambda: [])
    monkeypatch.setattr(core_service, "get_forex_factory_next_week_feed", lambda: [])
    out = core_service._forex_factory_usd_window_events(
        core_service.date(2026, 3, 13), core_service.date(2026, 3, 13)
    )
    day_events = (out.get("events_by_day") or {}).get("2026-03-13", [])
    titles = [row["title"] for row in day_events]
    assert titles[:2] == ["Core PCE Price Index m/m", "GDP (Second Estimate) q/q"]
    assert [row["time_label"] for row in day_events[:2]] == ["8:30 AM ET", "8:30 AM ET"]
    assert "JOLTS Job Openings" in titles[2:]


def test_candle_opens_includes_low_impact_yellow_macro_folders(monkeypatch):
    monkeypatch.setattr(
        core_service,
        "get_forex_factory_month_feed",
        lambda: [
            {
                "country": "USD",
                "impact": "Low",
                "title": "Mortgage Delinquencies",
                "date": "2026-05-06T09:00:00-04:00",
            }
        ],
    )
    monkeypatch.setattr(core_service, "get_forex_factory_feed", lambda: [])
    monkeypatch.setattr(core_service, "get_forex_factory_next_week_feed", lambda: [])

    out = core_service._forex_factory_usd_window_events(
        core_service.date(2026, 5, 6), core_service.date(2026, 5, 6)
    )

    assert out["low_count"] == 1
    assert out["total"] == 1
    assert out["high_count"] == 0
    assert out["medium_count"] == 0
    assert "0 USD high/medium macro folders" in out["summary"]
    event = out["events"][0]
    assert event["impact"] == "Low"
    assert event["impact_class"] == "low"
    assert out["days"][0]["low_count"] == 1


def test_global_top_notice_counts_next_24h_and_prioritizes_red(monkeypatch):
    now_et = datetime(2026, 5, 6, 17, 45, tzinfo=ZoneInfo("America/New_York"))

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now_et if tz else now_et.replace(tzinfo=None)

    def row(hours, impact, title):
        starts_at = now_et + timedelta(hours=hours)
        return {
            "country": "USD",
            "impact": impact,
            "title": title,
            "date": starts_at.isoformat(),
        }

    monkeypatch.setattr(ui_service, "datetime", FixedDateTime)
    monkeypatch.setattr(
        ui_service,
        "get_forex_factory_feed",
        lambda: [
            row(1, "High", "Initial Jobless Claims"),
            row(2, "Medium", "Fed Balance Sheet"),
            row(3, "Low", "Low Impact Ignored"),
            row(25, "High", "Outside Window"),
        ],
    )
    monkeypatch.setattr(ui_service, "get_forex_factory_next_week_feed", lambda: [])

    notice = ui_service._global_top_notice()

    assert notice is not None
    assert notice["count"] == 2
    assert notice["level"] == "high"
    assert [event["title"] for event in notice["events"]] == [
        "Initial Jobless Claims",
        "Fed Balance Sheet",
    ]
    assert all(event["href"].startswith("/candle-opens?") for event in notice["events"])


def test_global_top_notice_ignores_low_only_events(monkeypatch):
    now_et = datetime(2026, 5, 6, 17, 45, tzinfo=ZoneInfo("America/New_York"))

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now_et if tz else now_et.replace(tzinfo=None)

    monkeypatch.setattr(ui_service, "datetime", FixedDateTime)
    monkeypatch.setattr(
        ui_service,
        "get_forex_factory_feed",
        lambda: [
            {
                "country": "USD",
                "impact": "Low",
                "title": "Low Impact Only",
                "date": (now_et + timedelta(hours=1)).isoformat(),
            }
        ],
    )
    monkeypatch.setattr(ui_service, "get_forex_factory_next_week_feed", lambda: [])

    assert ui_service._global_top_notice() is None


def test_candle_page_top_notice_counts_next_24h_and_prioritizes_red():
    now_et = datetime(2026, 5, 6, 17, 45, tzinfo=ZoneInfo("America/New_York"))

    def event(hours, impact_class, title):
        starts_at = now_et + timedelta(hours=hours)
        return {
            "title": title,
            "impact": impact_class.title(),
            "impact_class": impact_class,
            "starts_at": starts_at.isoformat(),
            "time_label": starts_at.strftime("%-I:%M %p ET"),
            "tooltip": f"{impact_class.title()} impact • {title}",
        }

    notice = core_service._candle_page_top_notice(
        now_et,
        [
            event(1, "high", "Challenger Job Cuts y/y"),
            event(3, "medium", "Natural Gas Storage"),
            event(4, "low", "Low Impact Ignored"),
            event(25, "high", "Outside Window"),
        ],
    )

    assert notice is not None
    assert notice["count"] == 2
    assert notice["level"] == "high"
    assert [event["title"] for event in notice["events"]] == [
        "Challenger Job Cuts y/y",
        "Natural Gas Storage",
    ]
    assert notice["href"] == "/candle-opens?y=2026&m=5#news-day-2026-05-06"


def test_candle_page_top_notice_uses_orange_for_medium_plus_low():
    now_et = datetime(2026, 5, 6, 17, 45, tzinfo=ZoneInfo("America/New_York"))

    def event(hours, impact_class, title):
        starts_at = now_et + timedelta(hours=hours)
        return {
            "title": title,
            "impact": impact_class.title(),
            "impact_class": impact_class,
            "starts_at": starts_at.isoformat(),
            "time_label": starts_at.strftime("%-I:%M %p ET"),
            "tooltip": f"{impact_class.title()} impact • {title}",
        }

    notice = core_service._candle_page_top_notice(
        now_et,
        [
            event(1, "medium", "Natural Gas Storage"),
            event(2, "low", "Low Impact Ignored"),
        ],
    )

    assert notice is not None
    assert notice["count"] == 1
    assert notice["level"] == "medium"
    assert [event["title"] for event in notice["events"]] == ["Natural Gas Storage"]


def test_candle_page_top_notice_keeps_single_impact_level():
    now_et = datetime(2026, 5, 6, 17, 45, tzinfo=ZoneInfo("America/New_York"))
    events = []
    for hours, title in ((1, "Initial Jobless Claims"), (2, "Continuing Claims")):
        starts_at = now_et + timedelta(hours=hours)
        events.append(
            {
                "title": title,
                "impact": "High",
                "impact_class": "high",
                "starts_at": starts_at.isoformat(),
                "time_label": starts_at.strftime("%-I:%M %p ET"),
                "tooltip": f"High impact • {title}",
            }
        )

    notice = core_service._candle_page_top_notice(now_et, events)

    assert notice is not None
    assert notice["count"] == 2
    assert notice["level"] == "high"


def test_candle_open_calendar_surfaces_key_macro_days(monkeypatch):
    monkeypatch.setattr(
        core_service,
        "get_forex_factory_month_feed",
        lambda: [
            {
                "country": "USD",
                "impact": "High",
                "title": "FOMC Rate Decision",
                "date": "2026-03-18T14:00:00-04:00",
            },
            {
                "country": "USD",
                "impact": "High",
                "title": "FOMC Press Conference",
                "date": "2026-03-18T14:30:00-04:00",
            },
            {
                "country": "USD",
                "impact": "High",
                "title": "CPI m/m",
                "date": "2026-03-11T08:30:00-04:00",
            },
            {
                "country": "USD",
                "impact": "High",
                "title": "Unemployment Claims",
                "date": "2026-03-12T08:30:00-04:00",
            },
        ],
    )
    monkeypatch.setattr(core_service, "get_forex_factory_feed", lambda: [])
    monkeypatch.setattr(core_service, "get_forex_factory_next_week_feed", lambda: [])
    calendar = core_service._build_candle_open_calendar(2026, 3)
    top_days = list(calendar.get("news_top_days") or [])
    assert top_days
    assert top_days[0]["iso"] == "2026-03-18"
    assert top_days[0]["focus_key"] == "federal"
    march_18 = next(
        cell for week in calendar["weeks"] for cell in week if cell.get("iso") == "2026-03-18"
    )
    assert bool(march_18["is_key_news_day"])
    assert march_18["news_focus_label"] == "Fed Day"


def test_market_pulse_includes_tesla_in_quotes_and_watchlist():
    labels = {item["label"] for item in core_service.MARKET_PULSE_SYMBOLS}
    assert "TSLA" in labels
    assert "TSLA" in set(core_service.MARKET_PULSE_WATCHLIST_NEWS_SYMBOLS)


def test_market_news_item_marks_stale_headlines_and_uses_relative_labels():
    now_et = datetime(2026, 3, 16, 10, 0, tzinfo=core_service.app_runtime.TZ)
    fresh = core_service._market_news_item(
        {
            "headline": "Treasury yields cool into the open",
            "summary": "Rates ease ahead of cash session.",
            "source": "Finnhub",
            "url": "https://example.com/fresh",
            "datetime": int(
                datetime(2026, 3, 16, 9, 15, tzinfo=core_service.app_runtime.TZ).timestamp()
            ),
            "related": "SPX",
        },
        now_et=now_et,
    )
    stale = core_service._market_news_item(
        {
            "headline": "Legacy index driver",
            "summary": "Older market context.",
            "source": "Yahoo Finance RSS",
            "url": "https://example.com/stale",
            "datetime": int(
                datetime(2026, 3, 14, 12, 0, tzinfo=core_service.app_runtime.TZ).timestamp()
            ),
            "related": "SPY",
        },
        now_et=now_et,
    )

    assert fresh["stale"] is False
    assert "ago" in fresh["published_label"] or "Today" in fresh["published_label"]
    assert stale["stale"] is True
    assert stale["absolute_label"]


def test_market_news_recent_filter_drops_old_rows():
    now_et = datetime(2026, 3, 16, 10, 0, tzinfo=core_service.app_runtime.TZ)
    fresh_stamp = int(datetime(2026, 3, 16, 7, 0, tzinfo=core_service.app_runtime.TZ).timestamp())
    old_stamp = int(datetime(2026, 3, 13, 7, 0, tzinfo=core_service.app_runtime.TZ).timestamp())

    assert core_service._market_news_is_recent(
        fresh_stamp, now_et, core_service.MARKET_NEWS_MAX_AGE_SECONDS
    )
    assert not core_service._market_news_is_recent(
        old_stamp, now_et, core_service.MARKET_NEWS_MAX_AGE_SECONDS
    )


def test_market_pulse_core_tape_renders_leader_tickers(client, monkeypatch):
    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda **_: {
            "available": True,
            "fetched_at": "Mar 2, 2026 09:45 AM ET",
            "source_label": "Yahoo Finance chart feed",
            "source_note": "",
            "quotes": [
                {
                    "label": "SPX",
                    "symbol": "SPX",
                    "group": "core",
                    "price": 5100.0,
                    "change": 10.0,
                    "change_pct": 0.2,
                    "market_state": "Regular",
                    "day_range": "5000.00 to 5150.00",
                    "provider": "tradier",
                    "data_reason": "tradier_live_quote",
                    "data_state": "live",
                    "data_status_label": "Live",
                    "source_badge_label": "Tradier Live Quote",
                },
                {
                    "label": "TSLA",
                    "symbol": "TSLA",
                    "group": "leaders",
                    "price": 210.0,
                    "change": 2.0,
                    "change_pct": 0.96,
                    "market_state": "Regular",
                    "day_range": "205.00 to 212.00",
                    "provider": "yfinance",
                    "data_reason": "yfinance_fallback",
                    "data_state": "delayed",
                    "data_status_label": "Delayed",
                    "source_badge_label": "Yahoo Fallback",
                },
            ],
        },
    )
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda **_: {
            "available": False,
            "source_note": "",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
        },
    )

    resp = client.get("/market-pulse", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Core Tape" in resp.data
    assert b"SPX" in resp.data
    assert b"TSLA" in resp.data
    assert b'"quotes_map"' in resp.data
    assert b"marketPulseStreamStatus" in resp.data
    assert b"Tradier Live Quote" in resp.data
    assert b"Yahoo Fallback" in resp.data
    assert b"Trade Read" in resp.data
    assert b"Live Data Status" in resp.data
    assert b"marketPulseSetupHeadline" in resp.data
    assert b"marketPulseTradeReadState" in resp.data
    assert b"marketPulseLoadingOverlay" in resp.data
    assert b"Loading Market Pulse" in resp.data
    assert b"autoRefreshToggle" not in resp.data


def test_gamma_ladder_api_defaults_to_spy(client, monkeypatch):
    from mccain_capital.services import gamma_map_service

    monkeypatch.setattr(
        gamma_map_service,
        "build_gamma_ladder",
        lambda symbol, window="standard", dte="0": {
            "ok": True,
            "symbol": symbol,
            "spot": 7415.22,
            "expiration": "2026-05-21",
            "expiration_label": "0DTE",
            "regime": "positive_gamma",
            "regime_label": "Positive Gamma Regime",
            "updated_at": now_iso(),
            "updated_label": "2:41 PM ET",
            "total_net_gamma": 123456789.0,
            "flip_strike": 7400.0,
            "strongest_level": 7425.0,
            "rows_total": 96,
            "rows_visible": 17,
            "window_min_strike": 7350.0,
            "window_max_strike": 7475.0,
            "window_mode": "spot_band",
            "window_preset": window,
            "dte_preset": dte,
            "rows": [{"strike": 7425.0, "call_gex": 12.0, "put_gex": -8.0, "net_gex": 4.0}],
        },
    )

    resp = client.get("/api/gamma-ladder", follow_redirects=True)

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    assert payload["symbol"] == "SPY"
    assert payload["rows_total"] == 96
    assert payload["rows_visible"] == 17
    assert payload["window_preset"] == "standard"
    assert payload["dte_preset"] == "0"
    assert payload["rows"]


def test_gamma_ladder_api_accepts_searched_symbols_and_normalizes_invalid(client, monkeypatch):
    from mccain_capital.services import gamma_map_service

    seen = []

    monkeypatch.setattr(
        gamma_map_service,
        "build_gamma_ladder",
        lambda symbol, window="standard", dte="0": seen.append((symbol, window, dte))
        or {
            "ok": True,
            "symbol": symbol,
            "spot": 532.14,
            "expiration": "2026-05-21",
            "expiration_label": "0DTE",
            "regime": "mixed_gamma",
            "regime_label": "Mixed Gamma Regime",
            "updated_at": now_iso(),
            "updated_label": "2:41 PM ET",
            "total_net_gamma": 1.0,
            "flip_strike": 530.0,
            "strongest_level": 535.0,
            "rows_total": 48,
            "rows_visible": 13,
            "window_min_strike": 520.0,
            "window_max_strike": 542.0,
            "window_mode": "spot_band",
            "window_preset": window,
            "dte_preset": dte,
            "rows": [{"strike": 535.0, "call_gex": 5.0, "put_gex": -2.0, "net_gex": 3.0}],
        },
    )

    spy_resp = client.get("/api/gamma-ladder?symbol=SPY&window=tight&dte=0", follow_redirects=True)
    nvda_resp = client.get("/api/gamma-ladder?symbol=NVDA&window=wide&dte=7", follow_redirects=True)
    bad_resp = client.get("/api/gamma-ladder?symbol=bad!&window=nope", follow_redirects=True)

    assert spy_resp.status_code == 200
    assert spy_resp.get_json()["symbol"] == "SPY"
    assert spy_resp.get_json()["window_preset"] == "tight"
    assert nvda_resp.status_code == 200
    assert nvda_resp.get_json()["symbol"] == "NVDA"
    assert nvda_resp.get_json()["window_preset"] == "wide"
    assert nvda_resp.get_json()["dte_preset"] == "7"
    assert bad_resp.status_code == 200
    assert bad_resp.get_json()["symbol"] == "SPY"
    assert bad_resp.get_json()["window_preset"] == "standard"
    assert bad_resp.get_json()["dte_preset"] == "0"
    assert ("NVDA", "wide", "7") in seen


def test_gamma_ladder_api_error_returns_requested_symbol(client, monkeypatch):
    from mccain_capital.services import gamma_map_service

    def _raise(symbol, window="standard", dte="0"):
        raise RuntimeError(f"{symbol} options chain unavailable.")

    monkeypatch.setattr(gamma_map_service, "build_gamma_ladder", _raise)

    resp = client.get("/api/gamma-ladder?symbol=NVDA&window=tight", follow_redirects=True)

    assert resp.status_code == 503
    payload = resp.get_json()
    assert payload["ok"] is False
    assert payload["symbol"] == "NVDA"
    assert payload["window_preset"] == "tight"
    assert "NVDA options chain unavailable" in payload["message"]


def test_market_pulse_renders_gamma_ladder_switcher(client):
    resp = client.get("/market-pulse", follow_redirects=True)
    body = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert 'id="marketPulseGammaLadderCard"' in body
    assert 'class="gamma-symbol-switcher"' in body
    assert 'data-gamma-symbol-pill="SPX"' in body
    assert 'data-gamma-symbol-pill="SPY"' in body
    assert 'data-gamma-symbol-pill="QQQ"' in body
    assert 'data-default-symbol="SPY"' in body
    assert 'data-symbol="SPY"' in body
    assert 'data-gamma-symbol-pill="SPY"' in body
    assert 'class="gamma-symbol-pill active"' in body
    assert "data-gamma-symbol-search" in body
    assert "data-gamma-symbol-input" in body
    assert 'data-gamma-window-pill="tight"' in body
    assert 'data-gamma-window-pill="standard"' in body
    assert 'data-gamma-window-pill="wide"' in body
    assert "data-gamma-summary" in body
    assert "data-gamma-loading" in body
    assert "data-gamma-board" in body
    assert 'class="gamma-ladder-boardShell"' in body


def test_market_pulse_news_surface_keeps_tape_drivers_and_removes_watchlist_block(
    client, monkeypatch
):
    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda **_: {
            "available": True,
            "fetched_at": "Mar 16, 2026 10:30 AM ET",
            "source_label": "Massive market feed",
            "source_note": "",
            "quotes": [],
            "integrity": {},
        },
    )
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda **_: {
            "available": True,
            "source_note": "Fresh Finnhub drivers plus Forex Factory macro triggers.",
            "macro_events": [
                {
                    "headline": "Core Retail Sales m/m",
                    "summary": "High impact scheduled event.",
                    "source": "Forex Factory",
                    "url": "/candle-opens",
                    "published_label": "Wed 8:30 AM ET",
                    "tag": "Macro",
                    "why": "Calendar event",
                }
            ],
            "market_items": [
                {
                    "headline": "Treasury yields ease before open",
                    "summary": "Rates cool into the session.",
                    "source": "Finnhub",
                    "url": "https://example.com/1",
                    "published_label": "12m ago",
                    "absolute_label": "Mar 16, 10:18 AM ET",
                    "tag": "Rates",
                    "why": "Rates / liquidity backdrop",
                    "stale": False,
                }
            ],
            "watchlist_items": [
                {
                    "headline": "Legacy TSLA headline",
                    "summary": "Older single-name item.",
                    "source": "Finnhub",
                    "url": "https://example.com/tsla",
                    "published_label": "Yesterday 3:10 PM ET",
                    "absolute_label": "Mar 15, 3:10 PM ET",
                    "tag": "TSLA",
                    "symbol": "TSLA",
                    "why": "Single-name context",
                    "stale": True,
                }
            ],
        },
    )

    resp = client.get("/market-pulse", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Tape Drivers" in resp.data
    assert b"Watchlist Headlines" not in resp.data
    assert b"Treasury yields ease before open" in resp.data


def test_market_pulse_refresh_query_forces_snapshot_refresh(client, monkeypatch):
    force_flags = []

    def _fake_snapshot(*, force_refresh=False):
        force_flags.append(bool(force_refresh))
        return {
            "available": True,
            "fetched_at": "Mar 2, 2026 10:30 AM ET",
            "source_label": "Yahoo Finance chart feed",
            "source_note": "",
            "quotes": [],
        }

    monkeypatch.setattr(core_service, "_market_pulse_snapshot", _fake_snapshot)
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda **_: {
            "available": False,
            "source_note": "",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
        },
    )

    resp = client.get("/market-pulse?refresh=1", follow_redirects=True)
    assert resp.status_code == 200
    assert force_flags == [True]
    assert b"/market-pulse?refresh=1" in resp.data
    assert b'url.searchParams.delete("refresh")' in resp.data


def test_market_pulse_source_is_normalized_to_yahoo():
    out = core_service._market_pulse_force_yahoo_source(
        {
            "available": True,
            "fetched_at": "Mar 2, 2026 10:16 AM ET",
            "source_label": "Finnhub market feed",
            "source_note": "Live quotes and SPX candles are being served by Finnhub.",
            "quotes": [],
        }
    )
    assert out["source_label"] == "Yahoo Finance chart feed"
    assert "finnhub" not in str(out["source_note"]).lower()


def test_market_pulse_range_payload_prefers_ohlc_rows():
    out = market_pulse_tape.range_payload(
        [
            {"high": 747.25, "low": 745.35, "close": 746.2},
            {"high": 746.97, "low": 745.8, "close": 746.74},
        ],
        source="current_session",
    )

    assert out["day_range"] == "745.35 to 747.25"
    assert out["day_range_compact"] == "745.35-747.25"
    assert out["range_display"] == "745.35-747.25"
    assert out["day_range_source"] == "current_session"


def test_market_pulse_range_payload_falls_back_to_replay_values():
    out = market_pulse_tape.range_payload(
        [{"v": 745.35}, {"v": 746.97}, {"v": 746.21}],
        source="cached_replay",
    )

    assert out["day_range"] == "745.35 to 746.97"
    assert out["day_range_compact"] == "745.35-746.97"
    assert out["day_range_source"] == "cached_replay"


def test_market_pulse_range_payload_empty_when_no_range_data():
    assert market_pulse_tape.range_payload([], source="cached_replay") == {}
    assert market_pulse_tape.range_payload([{"v": 745.35}], source="cached_replay") == {}


def test_market_pulse_tape_api_uses_prior_open_day_range(client, monkeypatch):
    from mccain_capital.services import market_data_service

    monkeypatch.setattr(
        core_service,
        "MARKET_PULSE_SYMBOLS",
        [{"symbol": "SPY", "label": "SPY", "group": "core", "focus": ""}],
    )
    monkeypatch.setattr(
        market_data_service,
        "get_watchlist",
        lambda _symbols, allow_yf_fallback=False, force_refresh=False: {
            "SPY": {
                "price": 746.74,
                "pct_change": 1.04,
                "provider": "tradier",
                "reason": "tradier_live_quote",
                "as_of": "2026-06-19T16:00:00-04:00",
            }
        },
    )
    monkeypatch.setattr(market_data_service, "get_watchlist_tradier", lambda _symbols: {})
    monkeypatch.setattr(market_data_service, "get_intraday", lambda _symbol: [])
    monkeypatch.setattr(
        market_data_service,
        "get_prior_session_intraday",
        lambda _symbol, anchor_session_day=None: [
            {
                "ts": "2026-06-18T13:30:00+00:00",
                "open": 745.80,
                "high": 746.10,
                "low": 745.35,
                "close": 745.90,
                "volume": 100,
            },
            {
                "ts": "2026-06-18T19:59:00+00:00",
                "open": 745.90,
                "high": 746.97,
                "low": 746.01,
                "close": 746.74,
                "volume": 100,
            },
        ],
    )

    resp = client.get("/api/market-pulse/tape?include_series=1")

    assert resp.status_code == 200
    payload = resp.get_json()["payload"]
    spy = payload["quotes_map"]["SPY"]
    assert spy["day_range"] == "745.35 to 746.97"
    assert spy["day_range_compact"] == "745.35-746.97"
    assert spy["range_display"] == "745.35-746.97"
    assert spy["day_range_source"] == "prior_session"


def test_market_pulse_cached_payload_is_expanded_to_current_symbol_set():
    old_payload = {
        "available": True,
        "fetched_at": "Mar 2, 2026 10:16 AM ET",
        "source_label": "Finnhub market feed",
        "source_note": "legacy cached snapshot",
        "quotes": [
            {
                "label": "SPX",
                "symbol": "^GSPC",
                "price": 6878.88,
                "group": "core",
                "focus": "",
                "yahoo_href": "",
                "change": 0.0,
                "change_pct": 0.0,
                "volume": 0,
                "avg_volume": 0,
                "market_state": "At Close",
                "day_range": "—",
                "name": "SPX",
            },
            {
                "label": "META",
                "symbol": "META",
                "price": 649.54,
                "group": "leaders",
                "focus": "",
                "yahoo_href": "",
                "change": 0.0,
                "change_pct": 0.0,
                "volume": 0,
                "avg_volume": 0,
                "market_state": "Live",
                "day_range": "—",
                "name": "META",
            },
        ],
    }
    out = core_service._market_pulse_force_symbol_set(old_payload)
    labels = {q["label"] for q in out["quotes"]}
    assert out["source_label"] == "Yahoo Finance chart feed"
    assert "TSLA" in labels
    assert "SPX" in labels
    assert len(out["quotes"]) == len(core_service.MARKET_PULSE_SYMBOLS)


def test_market_pulse_stale_transition_and_alert_escalation():
    now_et = core_service.app_runtime.now_et()
    now_epoch = int(now_et.timestamp())
    base = [
        {
            "label": "SPY",
            "data_state": "live",
            "asof_epoch": now_epoch - 20,
            "mini_series": [1, 2, 3],
        },
        {
            "label": "QQQ",
            "data_state": "live",
            "asof_epoch": now_epoch - 120,
            "mini_series": [3, 2, 1],
        },
        {
            "label": "TSLA",
            "data_state": "cached",
            "asof_epoch": now_epoch - 400,
            "mini_series": [2, 2, 2],
        },
    ]
    enriched = core_service._market_pulse_enrich_quotes(base, now_et)
    by_label = {q["label"]: q for q in enriched}
    assert by_label["SPY"]["freshness_band"] == "live"
    assert by_label["QQQ"]["freshness_band"] == "warn"
    assert by_label["TSLA"]["freshness_band"] == "critical"
    alert = core_service._market_pulse_alert(enriched)
    assert alert["show"] is True
    assert alert["tone"] == "critical"


def test_market_pulse_guardrail_activates_on_threshold():
    quotes = [
        {"label": "SPY", "freshness_band": "critical"},
        {"label": "QQQ", "freshness_band": "critical"},
        {"label": "IWM", "freshness_band": "warn"},
    ]
    guard = core_service._market_pulse_guardrail(quotes)
    assert guard["active"] is True
    assert guard["critical_count"] >= guard["threshold"]


def test_market_pulse_closed_session_quotes_do_not_trigger_unsafe_guardrail():
    now_et = datetime(2026, 3, 5, 19, 45, tzinfo=ZoneInfo("America/New_York"))
    now_epoch = int(now_et.timestamp())
    quotes = [
        {
            "label": "SPY",
            "data_state": "live",
            "asof_epoch": now_epoch - (5 * 3600),
            "mini_series": [710.0, 711.0, 710.5, 711.2],
        },
        {
            "label": "QQQ",
            "data_state": "cached",
            "asof_epoch": now_epoch - (5 * 3600),
            "mini_series": [657.0, 658.0, 657.5, 657.2],
        },
    ]

    enriched = core_service._market_pulse_enrich_quotes(quotes, now_et)
    guard = core_service._market_pulse_guardrail(enriched)
    alert = core_service._market_pulse_alert(enriched)

    assert {q["freshness_band"] for q in enriched} == {"warn"}
    assert {q["market_state"] for q in enriched} == {"Closed"}
    assert all(str(q["freshness_label"]).startswith("Closed ·") for q in enriched)
    assert guard["active"] is False
    assert alert["message"].startswith("Closed-session quotes loaded")


def test_market_pulse_sparkline_renders_guides_and_candles():
    svg = market_pulse_tape.sparkline_svg([10.0, 11.0, 10.5, 12.0, 11.75], "up")

    assert "marketMiniSparkGuide" in svg
    assert "marketMiniSparkBaseline" in svg
    assert "marketMiniSparkWick" in svg
    assert "marketMiniSparkBody" in svg
    assert "marketMiniSparkPoint" in svg
    assert svg.count("<rect") >= 2
    assert "10.0,11.0" not in svg


def test_market_pulse_tape_state_uses_consistent_thresholds():
    assert core_service._market_pulse_tape_state("SPX", -0.36)["label"] == "RISK-OFF"
    assert core_service._market_pulse_tape_state("SPY", -0.36)["label"] == "RISK-OFF"
    assert core_service._market_pulse_tape_state("QQQ", 0.36)["label"] == "RISK-ON"
    assert core_service._market_pulse_tape_state("VIX", 0.40)["label"] == "MIXED"
    assert core_service._market_pulse_tape_state("VIX", 0.80)["label"] == "STRONG"
    assert core_service._market_pulse_tape_state("AAPL", -0.80)["label"] == "WEAK"


def test_market_pulse_enrich_quotes_uses_replay_when_quote_series_is_sparse(monkeypatch):
    monkeypatch.setattr(
        core_service,
        "_market_pulse_cached_replay_series",
        lambda _symbol: (
            [{"v": value} for value in [10.0, 11.0, 10.5, 12.0, 11.75, 12.4, 12.1, 12.8]],
            "2026-03-05",
        ),
    )

    [quote] = core_service._market_pulse_enrich_quotes(
        [{"label": "SPY", "price": 101.0, "mini_series": [100.0, 101.0]}],
        core_service.app_runtime.now_et(),
    )

    assert len(quote["mini_series"]) >= 8
    assert len(quote["series"]) >= 8
    assert "marketMiniSparkBody" in quote["sparkline_svg"]


def test_dashboard_tape_refresh_returns_series_points(client, monkeypatch):
    from mccain_capital.services import market_worker

    def quote(symbol: str, price: float, pct: float, series: list[float]) -> dict:
        return {
            "symbol": symbol,
            "label": symbol,
            "price": price,
            "pct_change": pct,
            "as_of": "2026-03-05T15:55:00-05:00",
            "provider": "tradier",
            "reason": "tradier_live_quote",
            "mini_series": series,
        }

    monkeypatch.setattr(market_worker, "start_market_worker_once", lambda: None)
    monkeypatch.setattr(
        market_worker,
        "get_market_snapshot",
        lambda: {
            "updated_at": "2026-03-05T15:55:00-05:00",
            "prices": {
                "QQQ": quote("QQQ", 657.55, -1.01, [660.0, 659.2, 658.4, 658.0, 657.55]),
                "SPY": quote("SPY", 711.69, -0.49, [715.2, 714.1, 713.0, 712.2, 711.69]),
                "VIX": quote("VIX", 17.83, -1.06, [18.6, 18.4, 18.2, 18.0, 17.83]),
                "SPX": quote("SPX", 6780.25, -0.36, [6801.0, 6794.5, 6788.0, 6783.4, 6780.25]),
            },
        },
    )

    resp = client.get("/api/dashboard/tape?ticker=QQQ", follow_redirects=True)

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    assert sorted(payload["series_points"]) == ["QQQ", "SPX", "SPY", "VIX"]
    assert len(payload["series_points"]["SPY"]) == 5


def test_dashboard_tape_refresh_backfills_vix_intraday_curve(client, monkeypatch):
    from mccain_capital.services import market_data_service
    from mccain_capital.services import market_worker

    def quote(symbol: str, price: float, pct: float) -> dict:
        return {
            "symbol": symbol,
            "label": symbol,
            "price": price,
            "pct_change": pct,
            "as_of": "2026-03-05T15:55:00-05:00",
            "provider": "tradier",
            "reason": "tradier_live_quote",
        }

    monkeypatch.setattr(market_worker, "start_market_worker_once", lambda: None)
    monkeypatch.setattr(
        market_worker,
        "get_market_snapshot",
        lambda: {
            "updated_at": "2026-03-05T15:55:00-05:00",
            "prices": {
                "SPY": quote("SPY", 711.69, -0.49),
                "QQQ": quote("QQQ", 657.55, -1.01),
                "VIX": quote("VIX", 17.39, 0.06),
                "SPX": quote("SPX", 6780.25, -0.36),
            },
            "series_points": {},
            "series": {},
        },
    )
    monkeypatch.setattr(market_data_service, "get_watchlist_tradier", lambda _symbols: {})
    monkeypatch.setattr(
        market_data_service,
        "get_intraday",
        lambda symbol: (
            [{"close": value} for value in [17.10, 17.18, 17.09, 17.31, 17.24, 17.39]]
            if symbol == "VIX"
            else []
        ),
    )

    resp = client.get("/api/dashboard/tape?ticker=SPY", follow_redirects=True)

    assert resp.status_code == 200
    payload = resp.get_json()
    assert [row["v"] for row in payload["series_points"]["VIX"]] == [
        17.1,
        17.18,
        17.09,
        17.31,
        17.24,
        17.39,
    ]
    assert payload["quotes"]["VIX"]["mini_series"][-1] == 17.39


def test_dashboard_first_render_uses_detailed_tape_sparklines(client, monkeypatch):
    from mccain_capital.services import market_data_service
    from mccain_capital.services import market_worker

    def quote(symbol: str, price: float, pct: float) -> dict:
        return {
            "symbol": symbol,
            "label": symbol,
            "price": price,
            "pct_change": pct,
            "provider": "tradier",
            "reason": "tradier_stream_trade",
            "as_of": "2026-03-17T15:00:00-04:00",
        }

    series = {
        "QQQ": [664.28, 662.90, 661.30, 659.10, 657.55],
        "SPY": [715.23, 714.40, 713.35, 712.20, 711.69],
        "VIX": [18.67, 18.38, 18.12, 17.95, 17.83],
        "SPX": [6802.15, 6795.40, 6788.35, 6782.50, 6780.25],
    }
    prices = {
        "QQQ": quote("QQQ", 657.55, -1.01),
        "SPY": quote("SPY", 711.69, -0.49),
        "VIX": quote("VIX", 17.83, -1.06),
        "SPX": quote("SPX", 6780.25, -0.36),
    }
    monkeypatch.setattr(market_worker, "start_market_worker_once", lambda: None)
    monkeypatch.setattr(
        market_worker,
        "get_market_snapshot",
        lambda: {
            "prices": prices,
            "series": series,
            "series_points": {
                symbol: [{"v": value, "close": value} for value in values]
                for symbol, values in series.items()
            },
            "updated_at": "2026-03-17T15:00:00-04:00",
        },
    )
    monkeypatch.setattr(market_data_service, "get_intraday", lambda _symbol: [])
    monkeypatch.setattr(
        market_data_service, "get_prior_session_intraday", lambda *_args, **_kwargs: []
    )
    monkeypatch.setattr(market_data_service, "get_watchlist_tradier", lambda _symbols: {})
    monkeypatch.setattr(
        market_data_service,
        "get_watchlist",
        lambda _symbols, allow_yf_fallback=False: {},
    )

    resp = client.get("/dashboard", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert body.count("marketMiniSparkBody") >= 8
    assert body.count("marketMiniSparkWick") >= 8
    assert body.count("marketMiniSparkPoint") >= 4
    assert body.count("marketMiniSparkAmbientBand") >= 12
    assert body.count("marketMiniSparkCurrentGlow") >= 4
    assert body.count("marketMiniSparkBaseline") >= 4
    assert body.count("dashboardTapeFreshnessGlyph") >= 4
    assert body.count("data-freshness-label=") >= 4
    assert body.count('data-role="row-live"') >= 4
    assert "Broad tape is defensive" in body


def test_dashboard_vix_uses_quote_mini_series_for_range_and_sparkline(client, monkeypatch):
    from mccain_capital.services import market_data_service
    from mccain_capital.services import market_worker

    def quote(symbol: str, price: float, pct: float, extra: dict | None = None) -> dict:
        payload = {
            "symbol": symbol,
            "label": symbol,
            "price": price,
            "pct_change": pct,
            "provider": "tradier",
            "reason": "tradier_live_quote",
            "as_of": "2026-03-17T15:00:00-04:00",
        }
        if extra:
            payload.update(extra)
        return payload

    monkeypatch.setattr(market_worker, "start_market_worker_once", lambda: None)
    monkeypatch.setattr(
        market_worker,
        "get_market_snapshot",
        lambda: {
            "prices": {
                "SPX": quote("SPX", 6780.25, 0.12),
                "QQQ": quote("QQQ", 657.55, -0.10),
                "VIX": quote("VIX", 17.39, 0.06, {"mini_series": [17.10, 17.22, 17.50, 17.39]}),
                "SPY": quote("SPY", 711.69, 0.16),
            },
            "series_points": {},
            "series": {},
            "updated_at": "2026-03-17T15:00:00-04:00",
        },
    )
    monkeypatch.setattr(core_service, "_market_pulse_snapshot", lambda **_: {"quotes": []})
    monkeypatch.setattr(
        core_service, "_market_pulse_cached_replay_series", lambda _symbol: ([], None)
    )
    monkeypatch.setattr(market_data_service, "get_intraday", lambda _symbol: [])
    monkeypatch.setattr(
        market_data_service, "get_prior_session_intraday", lambda *_args, **_kwargs: []
    )
    monkeypatch.setattr(market_data_service, "get_watchlist_tradier", lambda _symbols: {})
    monkeypatch.setattr(market_data_service, "get_watchlist", lambda *_args, **_kwargs: {})

    resp = client.get("/dashboard", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "VIX" in body
    assert "17.10-17.50" in body
    assert "marketMiniSparkPoint" in body


def test_market_pulse_market_hours_defaults_execution_mode(client, monkeypatch):
    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda **_: {
            "available": True,
            "fetched_at": "Mar 2, 2026 10:30:00 AM ET",
            "source_label": "Yahoo Finance chart feed",
            "source_note": "",
            "quotes": [],
            "integrity": {},
        },
    )
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda **_: {
            "available": False,
            "source_note": "",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
        },
    )
    monkeypatch.setattr(core_service, "_market_pulse_market_hours", lambda now_et: True)

    resp = client.get("/market-pulse", follow_redirects=True)
    assert resp.status_code == 200
    assert b'data-market-hours="1"' in resp.data
    assert (
        b'let mode = storedMode || ((marketHours || mobileFoldQuery.matches) ? "execution" : "research");'
        in resp.data
    )


def test_market_pulse_empty_state_uses_consistent_feed_copy(client, monkeypatch):
    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda **_: {
            "available": True,
            "fetched_at": "Mar 16, 2026 9:29 AM ET",
            "source_label": "Fallback Snapshot",
            "source_note": "Cached pre-open snapshot",
            "quotes": [],
            "integrity": {
                "live_count": 0,
                "delayed_count": 0,
                "cached_count": 1,
                "missing_count": 4,
            },
        },
    )
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda **_: {
            "available": False,
            "source_note": "No major market drivers in the current refresh window. Monitoring tracked sources for fresh catalysts.",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
            "pulse_feed_available": False,
            "pulse_feed_source_note": "No major market drivers in the current refresh window. Monitoring tracked sources for fresh catalysts.",
            "pulse_feed_accounts": ["Reuters", "Federal Reserve", "Yahoo Finance", "MarketWatch"],
            "pulse_feed_items": [],
            "market_feed_snapshot": {
                "status": "quiet",
                "source_note": "No major market drivers in the current refresh window. Monitoring tracked sources for fresh catalysts.",
                "sources_monitored": ["Reuters", "Federal Reserve", "Yahoo Finance", "MarketWatch"],
                "now_summary": {
                    "spx_focus": "Watching SPX and leadership rotation.",
                    "leadership": "Mixed tape",
                    "weakness": "No clear laggard",
                    "feed_state": "Monitoring Reuters, Federal Reserve, Yahoo Finance, and MarketWatch",
                },
            },
        },
    )
    monkeypatch.setattr(core_service, "_market_pulse_market_hours", lambda now_et: False)

    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import options_panel_service

    monkeypatch.setattr(gamma_map_service, "start_gamma_worker_once", lambda: None)
    monkeypatch.setattr(options_panel_service, "start_options_worker_once", lambda: None)
    monkeypatch.setattr(
        gamma_map_service,
        "get_gamma_snapshot",
        lambda: {"asof": "", "regime": "unavailable", "bias": "insufficient_data"},
    )
    monkeypatch.setattr(
        options_panel_service,
        "get_options_snapshot",
        lambda: {"asof": "", "symbols": {"SPX": {"contracts": []}}},
    )

    resp = client.get("/market-pulse", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Market Feed" in body
    assert "Unusual Whales Flow Desk" in body
    assert "Refresh Feed" in body
    assert "https://twitter.com/unusual_whales" in body
    assert "No trend" in body
    assert "Awaiting contract rows." in body
    assert "Fallback Snapshot" in body


def test_market_pulse_renders_three_high_impact_feed_items(client, monkeypatch):
    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda **_: {
            "available": True,
            "fetched_at": "Apr 5, 2026 9:45 AM ET",
            "source_label": "Yahoo Finance chart feed",
            "source_note": "Live snapshot",
            "quotes": [],
            "integrity": {},
        },
    )
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda **_: {
            "available": True,
            "source_note": "RSS feed live",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
            "pulse_feed_available": True,
            "pulse_feed_source_note": "RSS feed live",
            "pulse_feed_accounts": ["@unusual_whales"],
            "pulse_feed_items": [
                {
                    "handle": "@unusual_whales",
                    "headline": "FOMC minutes point to higher rates sensitivity",
                    "summary": "Macro desks are focused on yields and policy language.",
                    "source_label": "Reuters",
                    "url": "https://example.com/1",
                    "published_at": "2026-04-05T09:40:00-04:00",
                    "published_et_label": "Apr 05, 09:40 AM",
                    "published_label": "5m ago",
                    "impact": "high",
                    "impact_label": "HIGH",
                    "category": "macro",
                    "category_label": "Macro",
                },
                {
                    "handle": "@unusual_whales",
                    "headline": "Treasury yields climb as SPX futures fade",
                    "summary": "Rates pressure remains the lead market driver.",
                    "source_label": "MarketWatch",
                    "url": "https://example.com/2",
                    "published_at": "2026-04-05T09:36:00-04:00",
                    "published_et_label": "Apr 05, 09:36 AM",
                    "published_label": "9m ago",
                    "impact": "high",
                    "impact_label": "HIGH",
                    "category": "market",
                    "category_label": "Market",
                },
            ],
            "market_feed_snapshot": {
                "status": "live",
                "source_note": "RSS feed live",
                "sources_monitored": ["@unusual_whales"],
                "now_summary": {
                    "spx_focus": "SPX flat, QQQ slightly strong, small caps leading",
                    "leadership": "Broad risk-on",
                    "weakness": "TSLA -5.4%",
                    "feed_state": "Monitoring Reuters, Fed, Yahoo Finance, and MarketWatch",
                },
            },
        },
    )
    monkeypatch.setattr(core_service, "_market_pulse_market_hours", lambda now_et: True)

    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import options_panel_service

    monkeypatch.setattr(gamma_map_service, "start_gamma_worker_once", lambda: None)
    monkeypatch.setattr(options_panel_service, "start_options_worker_once", lambda: None)
    monkeypatch.setattr(
        gamma_map_service,
        "get_gamma_snapshot",
        lambda: {"asof": "", "regime": "unavailable", "bias": "insufficient_data"},
    )
    monkeypatch.setattr(
        options_panel_service,
        "get_options_snapshot",
        lambda: {"asof": "", "symbols": {"SPX": {"contracts": []}}},
    )

    resp = client.get("/market-pulse", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Market Feed" in body
    assert "Unusual Whales Flow Desk" in body
    assert "Options Flow / Positioning / Single-Name Tape" in body
    assert "FOMC minutes point to higher rates sensitivity" in body
    assert "Treasury yields climb as SPX futures fade" in body
    assert "Refresh Feed" in body
    assert "All posts" in body
    assert 'data-feed-published-at="2026-04-05T09:40:00-04:00"' in body
    assert 'data-feed-absolute-label="Apr 05, 09:40 AM"' in body
    assert "5m ago · Apr 05, 09:40 AM" in body
    assert "formatRelativeFeedAge" in body
    assert "refreshFeedTimeLabels" in body
    assert "Open on X" in body
    assert "https://twitter.com/unusual_whales" in body


def test_calculator_shows_projected_balances_for_stop_and_target(client):
    resp = client.post(
        "/calculator",
        data={
            "entry": "10",
            "contracts": "1",
            "stop_pct": "20",
            "target_pct": "30",
            "fee_per_contract": "0.70",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"Balance If Stop Hits" in resp.data
    assert b"Balance If Target Hits" in resp.data
    assert b"Consistency If Stop Hits" in resp.data
    assert b"Consistency If Target Hits" in resp.data
    assert b"$49,799.30" in resp.data
    assert b"$50,299.30" in resp.data


def test_calculator_supports_async_json_updates(client):
    resp = client.post(
        "/calculator",
        data={
            "entry": "10",
            "contracts": "1",
            "stop_pct": "20",
            "target_pct": "30",
            "fee_per_contract": "0.70",
        },
        headers={"X-Requested-With": "XMLHttpRequest", "Accept": "application/json"},
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is True
    assert payload["err"] is None
    assert "Plan updated" not in payload["results_html"]
    assert "Balance If Stop Hits" in payload["results_html"]
    assert "Consistency If Target Hits" in payload["results_html"]


def test_goals_and_payouts_render_new_planning_sections(client):
    goals_resp = client.get("/goals", follow_redirects=True)
    assert goals_resp.status_code == 200
    assert b"Goal-to-Execution Bridge" in goals_resp.data

    payouts_resp = client.get("/payouts", follow_redirects=True)
    assert payouts_resp.status_code == 200
    assert b"Payout Readiness Planner" in payouts_resp.data


def test_expected_endpoints_registered(app):
    endpoints = {rule.endpoint for rule in app.url_map.iter_rules()}
    expected = {
        "home",
        "healthz",
        "dashboard",
        "dashboard_recompute_balances",
        "stream_market",
        "stream_options_panel",
        "candle_opens_page",
        "trades_page",
        "journal_home",
        "calculator",
        "payouts_page",
        "books_page",
    }
    assert expected.issubset(endpoints)


def test_dashboard_renders_live_market_pulse_panel(client, monkeypatch):
    monkeypatch.setattr(
        core_service,
        "_load_dashboard_milestone_settings",
        lambda: {
            "name": "Profit Milestone",
            "profit_goal": 0.0,
            "target_balance": 0.0,
            "profit_source": "ytd",
        },
    )
    monkeypatch.setattr(
        core_service,
        "_dashboard_milestone_viewmodel",
        lambda *args, **kwargs: {
            "name": "Profit Milestone",
            "profit_source": "ytd",
            "profit_source_label": "YTD",
            "profit_current": 0.0,
            "profit_goal": 0.0,
            "profit_remaining": 0.0,
            "target_balance": 0.0,
            "balance_remaining": 0.0,
            "overall_progress_pct": 0.0,
            "profit_progress_pct": 0.0,
            "balance_progress_pct": 0.0,
            "profit_done": False,
            "balance_done": False,
            "has_profit_goal": False,
            "has_balance_goal": False,
            "avg_daily_profit": 0.0,
            "projected_days_profit": None,
            "projected_days_balance": None,
            "projected_days_overall": None,
        },
    )
    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Prime" in resp.data
    assert b"Milestone" in resp.data
    assert b"Live Market Pulse" not in resp.data


def test_dashboard_live_tape_compact_labels_and_guardrails(client, monkeypatch):
    from mccain_capital.services import market_worker
    from mccain_capital.services import market_data_service

    monkeypatch.setattr(market_worker, "start_market_worker_once", lambda: None)
    monkeypatch.setattr(
        market_worker,
        "get_market_snapshot",
        lambda: {
            "prices": {
                "SPX": {
                    "price": 6775.80,
                    "pct_change": -0.09,
                    "provider": "tradier",
                    "reason": "tradier_live_quote",
                    "as_of": "2026-03-05T12:00:30-05:00",
                },
                "VIX": {
                    "price": 24.23,
                    "pct_change": -2.81,
                    "provider": "tradier",
                    "reason": "tradier_live_quote",
                    "as_of": "2026-03-05T12:00:30-05:00",
                },
            },
            "series_points": {},
            "series": {},
            "updated_at": "2026-03-05T12:00:30-05:00",
        },
    )
    monkeypatch.setattr(
        market_data_service,
        "get_watchlist_tradier",
        lambda symbols: {
            "SPX": {
                "price": 6775.80,
                "pct_change": -0.09,
                "provider": "tradier",
                "reason": "tradier_live_quote",
                "as_of": "2026-03-05T12:00:30-05:00",
            },
            "VIX": {
                "price": 24.23,
                "pct_change": -2.81,
                "provider": "tradier",
                "reason": "tradier_live_quote",
                "as_of": "2026-03-05T12:00:30-05:00",
            },
        },
    )
    monkeypatch.setattr(market_data_service, "get_watchlist", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        market_data_service,
        "get_intraday",
        lambda symbol: (
            [{"close": 6773.42}, {"close": 6775.80}]
            if symbol == "SPX"
            else [{"close": 24.21}, {"close": 24.58}]
        ),
    )

    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"dashboardCoreTapeCard" in resp.data
    assert b"dashboardCoreTapeRow" in resp.data
    assert b"dashboardCoreTapeStat" in resp.data
    assert b"dashboardTapeStreamStatus" in resp.data
    assert b"Market Tape" in resp.data
    assert b"Live Tape" not in resp.data
    assert b"dashboardGapLine" in resp.data
    assert b"Gap O/N:" in resp.data
    assert b"Tradier Live Quote" in resp.data
    assert b"SPX cash" in resp.data
    assert b"6775.80" in resp.data
    assert b"-0.09%" in resp.data
    assert b"6773.42-6775.80" in resp.data
    assert b"VIX pulse" in resp.data
    assert b"Live \xc2\xb7" not in resp.data
    assert b"Delayed \xc2\xb7" not in resp.data
    assert b'data-role="market-state">Live</span>' not in resp.data
    assert b"Freshness" in resp.data
    assert b"dashboardTapeAssetStatus is-" in resp.data


def test_dashboard_tape_cached_rows_have_non_live_tone(client, monkeypatch):
    from mccain_capital.services import market_data_service
    from mccain_capital.services import market_worker

    now_iso = core_service.app_runtime.now_et().isoformat()
    monkeypatch.setattr(market_worker, "start_market_worker_once", lambda: None)
    monkeypatch.setattr(
        market_worker,
        "get_market_snapshot",
        lambda: {
            "prices": {
                "SPY": {
                    "price": 732.90,
                    "pct_change": 0.10,
                    "provider": "yfinance",
                    "reason": "cached_snapshot",
                    "as_of": now_iso,
                },
                "QQQ": {
                    "price": 695.77,
                    "pct_change": 0.10,
                    "provider": "yfinance",
                    "reason": "cached_snapshot",
                    "as_of": now_iso,
                },
                "SPX": {
                    "price": 6775.80,
                    "pct_change": 0.10,
                    "provider": "yfinance",
                    "reason": "cached_snapshot",
                    "as_of": now_iso,
                },
                "VIX": {
                    "price": None,
                    "pct_change": None,
                    "provider": "",
                    "reason": "unavailable",
                    "as_of": "",
                },
                "IWM": {
                    "price": 286.23,
                    "pct_change": 0.10,
                    "provider": "yfinance",
                    "reason": "cached_snapshot",
                    "as_of": now_iso,
                },
            },
            "series_points": {},
            "series": {},
            "updated_at": now_iso,
        },
    )
    monkeypatch.setattr(core_service, "_market_pulse_snapshot", lambda **_: {"quotes": []})
    monkeypatch.setattr(
        core_service, "_market_pulse_cached_replay_series", lambda _symbol: ([], None)
    )
    monkeypatch.setattr(market_data_service, "get_watchlist_tradier", lambda _symbols: {})
    monkeypatch.setattr(market_data_service, "get_watchlist", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(market_data_service, "get_intraday", lambda _symbol: [])

    resp = client.get("/dashboard", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "dashboardTapeAssetStatus is-delayed" in body
    assert "dashboardTapeAssetStatus is-missing" in body
    assert "Cached ·" not in body


def test_stream_market_sse_emits_json_payload(client, monkeypatch):
    from mccain_capital.services import market_worker
    from mccain_capital.services import options_panel_service
    from mccain_capital.services import gamma_map_service

    monkeypatch.setattr(market_worker, "start_market_worker_once", lambda: None)
    monkeypatch.setattr(options_panel_service, "start_options_worker_once", lambda: None)
    monkeypatch.setattr(gamma_map_service, "start_gamma_worker_once", lambda: None)
    monkeypatch.setattr(
        market_worker,
        "get_market_snapshot",
        lambda: {
            "prices": {
                "QQQ": {"price": 456.12, "pct_change": 0.42, "as_of": "2026-03-05T12:00:00"}
            },
            "alerts": ["QQQ crossed above 456.00 at 456.12"],
            "updated_at": "2026-03-05T12:00:00",
        },
    )
    monkeypatch.setattr(
        options_panel_service,
        "get_options_snapshot",
        lambda: {"asof": "2026-03-05T12:00:00-05:00", "symbols": {"SPX": {}}},
    )
    monkeypatch.setattr(
        gamma_map_service,
        "get_gamma_snapshot",
        lambda: {
            "asof": "2026-03-05T12:00:00-05:00",
            "spot": 5120.35,
            "regime": "positive",
            "net_gex": 2100000000.0,
            "gamma_flip": 5110.0,
            "call_wall": 5150.0,
            "put_wall": 5050.0,
            "gamma_walls_top3": [5150.0, 5125.0, 5100.0],
            "void_zone": {"start": 5060.0, "end": 5090.0},
            "bias": "buy_dips_above_flip",
            "paths": {
                "csv": "/app/persistent/uploads/gamma_data.csv",
                "png": "/app/persistent/uploads/gamma_map.png",
            },
        },
    )
    monkeypatch.setattr(core_service.time, "sleep", lambda _: None)

    resp = client.get("/stream/market", follow_redirects=True)
    assert resp.status_code == 200
    assert resp.headers.get("Content-Type", "").startswith("text/event-stream")
    assert resp.headers.get("X-Accel-Buffering") == "no"
    assert b"data: " in resp.data
    assert b"stream_ready" in resp.data
    assert b"QQQ" in resp.data
    assert b"options" in resp.data
    assert b"gamma_map" in resp.data


def test_stream_market_ws_requires_upgrade(client):
    resp = client.get("/ws/market", follow_redirects=True)
    assert resp.status_code in {400, 501}


def test_trades_sync_live_get_redirects_to_upload_workspace(client):
    resp = client.get("/trades/sync/live", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/trades/upload/statement")


def test_trades_sync_auto_config_get_redirects_to_upload_workspace(client):
    resp = client.get("/trades/sync/auto/config", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/trades/upload/statement")


def test_market_pulse_renders_spx_gamma_details(client, monkeypatch):
    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda **_: {
            "available": True,
            "fetched_at": "Mar 2, 2026 10:30 AM ET",
            "source_label": "Massive market feed",
            "source_note": "",
            "quotes": [
                {
                    "label": "SPX",
                    "symbol": "SPX",
                    "group": "core",
                    "focus": "index",
                    "price": 5120.35,
                    "change": 24.35,
                    "change_pct": 0.48,
                    "asof": "2026-03-05T12:00:00-05:00",
                    "asof_epoch": 1741194000,
                    "data_state": "live",
                    "data_reason": "tradier_live",
                    "provider": "tradier",
                    "mini_series": [5096.0, 5108.0, 5120.35],
                    "series": [],
                    "prior_day_low": 5064.25,
                    "prior_day_high": 5098.75,
                }
            ],
            "integrity": {},
        },
    )
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda: {
            "available": False,
            "source_note": "",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
        },
    )
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import market_data_service
    from mccain_capital.services import options_panel_service

    monkeypatch.setattr(gamma_map_service, "start_gamma_worker_once", lambda: None)
    monkeypatch.setattr(options_panel_service, "start_options_worker_once", lambda: None)
    monkeypatch.setattr(
        market_data_service,
        "get_intraday",
        lambda _symbol: [
            {
                "ts": "2026-03-05T14:30:00+00:00",
                "open": 5102.0,
                "high": 5124.5,
                "low": 5098.25,
                "close": 5111.0,
                "volume": 100.0,
            },
            {
                "ts": "2026-03-05T20:59:00+00:00",
                "open": 5111.0,
                "high": 5128.75,
                "low": 5097.5,
                "close": 5120.35,
                "volume": 100.0,
            },
        ],
    )
    monkeypatch.setattr(
        market_data_service,
        "get_prior_session_intraday",
        lambda _symbol, anchor_session_day=None: [
            {
                "ts": "2026-03-04T14:30:00+00:00",
                "open": 5078.0,
                "high": 5094.25,
                "low": 5066.5,
                "close": 5088.0,
                "volume": 100.0,
            },
            {
                "ts": "2026-03-04T20:59:00+00:00",
                "open": 5088.0,
                "high": 5098.75,
                "low": 5064.25,
                "close": 5092.0,
                "volume": 100.0,
            },
        ],
    )
    monkeypatch.setattr(
        gamma_map_service,
        "get_gamma_snapshot",
        lambda: {
            "asof": "2026-03-05T12:00:00-05:00",
            "spot": 5120.35,
            "spot_price_used": 5120.35,
            "spot_source_name": "tradier",
            "spot_source_timestamp": "2026-03-05T12:00:00-05:00",
            "regime": "positive",
            "net_gex": 2100000000.0,
            "net_gamma_label": "+2.10B",
            "gamma_flip": 5110.0,
            "call_wall": 5150.0,
            "put_wall": 5050.0,
            "gamma_range_estimate": 70.0,
            "gamma_range_high": 5190.35,
            "gamma_range_low": 5050.35,
            "call_wall_gamma_per_point": 245000000.0,
            "put_wall_gamma_per_point": 198000000.0,
            "gamma_walls_top3": [5150.0, 5125.0, 5100.0],
            "void_zone": {"start": 5060.0, "end": 5090.0},
            "bias": "buy_dips_above_flip",
        },
    )
    monkeypatch.setattr(
        options_panel_service,
        "get_options_snapshot",
        lambda: {
            "asof": "2026-03-05T12:00:00-05:00",
            "symbols": {"SPX": {"contracts": [{"label": "SPXW 2026-03-06 5125C", "liq": "Tight"}]}},
        },
    )

    resp = client.get("/market-pulse", follow_redirects=True)
    assert resp.status_code == 200
    assert b"SPX Priority" in resp.data
    assert b"Gamma Flip" in resp.data
    assert b"245.0 million" in resp.data
    assert b"198.0 million" in resp.data
    assert b"5064.25 - 5098.75" in resp.data
    assert b"Next walls: Inferred from strike ladder" in resp.data
    assert b"Gamma range estimate: Wall-based gamma range" in resp.data
    assert b"Best Contracts" in resp.data
    assert b"SPXW 2026-03-06 5125C" in resp.data


def test_market_pulse_gamma_quality_flags_stale_snapshots(client):
    quotes = [
        {
            "label": "SPX",
            "data_state": "live",
            "data_reason": "tradier_live",
        }
    ]
    gamma_snapshot = {
        "asof": "2026-03-16T10:25:00-04:00",
        "diagnostics": {"status": "ok", "contracts_used": 84},
    }
    now_et = datetime(2026, 3, 16, 10, 40, tzinfo=core_service.app_runtime.TZ)
    quality = core_service._gamma_data_quality(gamma_snapshot, quotes, now_et)
    assert quality["tone"] == "warn"
    assert quality["warning"] == "Gamma stale >5m"
    assert "fetch-time proxy" in quality["summary"]


def test_market_pulse_gamma_quality_flags_spot_value_mismatch():
    quotes = [
        {
            "label": "SPX",
            "price": 5125.0,
            "asof": "2026-03-16T10:39:00-04:00",
            "data_state": "live",
            "data_reason": "tradier_live",
        }
    ]
    gamma_snapshot = {
        "asof": "2026-03-16T10:39:00-04:00",
        "spot_price_used": 5110.0,
        "spot_source_timestamp": "2026-03-16T10:39:00-04:00",
        "diagnostics": {"status": "ok", "contracts_used": 84},
    }
    now_et = datetime(2026, 3, 16, 10, 40, tzinfo=core_service.app_runtime.TZ)
    quality = core_service._gamma_data_quality(gamma_snapshot, quotes, now_et)
    assert quality["tone"] == "critical"
    assert quality["warning"] == "Spot source mismatch"


def test_market_pulse_gamma_quality_flags_spot_timestamp_drift():
    quotes = [
        {
            "label": "SPX",
            "price": 5125.0,
            "asof": "2026-03-16T10:40:00-04:00",
            "data_state": "live",
            "data_reason": "tradier_live",
        }
    ]
    gamma_snapshot = {
        "asof": "2026-03-16T10:40:00-04:00",
        "spot_price_used": 5125.0,
        "spot_source_timestamp": "2026-03-16T10:35:00-04:00",
        "diagnostics": {"status": "ok", "contracts_used": 84},
    }
    now_et = datetime(2026, 3, 16, 10, 40, tzinfo=core_service.app_runtime.TZ)
    quality = core_service._gamma_data_quality(gamma_snapshot, quotes, now_et)
    assert quality["tone"] == "warn"
    assert quality["warning"] == "Spot timestamp drift"


def test_market_pulse_gamma_quality_flags_degraded_snapshot_state():
    quotes = [
        {
            "label": "SPX",
            "price": 5125.0,
            "asof": "2026-03-16T10:40:00-04:00",
            "data_state": "live",
            "data_reason": "tradier_live",
        }
    ]
    gamma_snapshot = {
        "snapshot_status": "degraded",
        "source_effective_timestamp": "2026-03-16T10:40:00-04:00",
        "exchange_timestamp_available": False,
        "spot_price_used": 5125.0,
        "spot_source_timestamp": "2026-03-16T10:40:00-04:00",
        "diagnostics": {"status": "ok", "contracts_used": 84},
    }
    now_et = datetime(2026, 3, 16, 10, 40, tzinfo=core_service.app_runtime.TZ)
    quality = core_service._gamma_data_quality(gamma_snapshot, quotes, now_et)
    assert quality["tone"] == "warn"
    assert quality["warning"] == "Degraded gamma basket"


def test_market_pulse_gamma_quality_flags_invalid_snapshot_state():
    quotes = [{"label": "SPX", "data_state": "live", "data_reason": "tradier_live"}]
    gamma_snapshot = {
        "snapshot_status": "invalid",
        "source_effective_timestamp": "2026-03-16T10:40:00-04:00",
        "exchange_timestamp_available": False,
        "diagnostics": {"status": "invalid", "contracts_used": 0},
    }
    now_et = datetime(2026, 3, 16, 10, 40, tzinfo=core_service.app_runtime.TZ)
    quality = core_service._gamma_data_quality(gamma_snapshot, quotes, now_et)
    assert quality["tone"] == "critical"
    assert quality["warning"] == "Invalid snapshot"


def test_market_pulse_renders_degraded_snapshot_banner(client, monkeypatch):
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import options_panel_service

    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda **_: {
            "available": True,
            "fetched_at": "Mar 20, 2026 09:30 AM ET",
            "source_label": "Tradier market feed",
            "source_note": "",
            "quotes": [],
            "integrity": {"live_count": 2, "delayed_count": 0, "missing_count": 0},
        },
    )
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda: {
            "available": True,
            "source_note": "",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
        },
    )
    monkeypatch.setattr(
        gamma_map_service,
        "get_gamma_snapshot",
        lambda: {
            "asof": "2026-03-20T09:30:00-04:00",
            "snapshot_status": "degraded",
            "snapshot_status_label": "Degraded Gamma Basket: 1 of 2 expiries available",
            "snapshot_status_detail": "One expiry is missing, so levels are computed from a partial combined basket.",
            "requested_expiries": ["2026-03-20", "2026-03-23"],
            "included_expiries": ["2026-03-20"],
            "exchange_timestamp_available": False,
            "source_fetch_timestamp": "2026-03-20T09:30:00-04:00",
            "source_effective_timestamp": "2026-03-20T09:30:00-04:00",
            "source_effective_timestamp_note": "Exchange-native chain timestamp unavailable; using fetch timestamp.",
            "spot": 5120.0,
            "spot_price_used": 5120.0,
            "spot_source_name": "tradier",
            "spot_source_timestamp": "2026-03-20T09:30:00-04:00",
            "regime": "negative",
            "net_gex": -120000000.0,
            "net_gamma_label": "-120.00M",
            "gamma_flip": 5140.0,
            "call_wall": 5160.0,
            "put_wall": 5090.0,
            "gamma_range_estimate": 40.0,
            "gamma_range_high": 5160.0,
            "gamma_range_low": 5080.0,
            "call_wall_gamma_per_point": 125000000.0,
            "put_wall_gamma_per_point": 114000000.0,
            "gamma_walls_top3": [5160.0],
            "warnings": ["Missing expiry data for 2026-03-23."],
            "stale_flags": ["missing_expiries"],
            "void_zone": {"start": None, "end": None},
            "bias": "sell_rips_below_flip",
            "diagnostics": {"status": "ok", "contracts_used": 42},
            "narrative": {"warning_badges": ["Missing next expiry"]},
        },
    )
    monkeypatch.setattr(
        options_panel_service,
        "get_options_snapshot",
        lambda: {"asof": "2026-03-20T09:30:00-04:00", "symbols": {"SPX": {"contracts": []}}},
    )

    resp = client.get("/market-pulse", follow_redirects=True)

    assert resp.status_code == 200
    assert b"Degraded Gamma Basket: 1 of 2 expiries available" in resp.data
    assert b"Fetch-time freshness proxy" in resp.data


def test_market_pulse_renders_invalid_snapshot_banner(client, monkeypatch):
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import options_panel_service

    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda **_: {
            "available": True,
            "fetched_at": "Mar 20, 2026 09:30 AM ET",
            "source_label": "Tradier market feed",
            "source_note": "",
            "quotes": [],
            "integrity": {"live_count": 2, "delayed_count": 0, "missing_count": 0},
        },
    )
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda: {
            "available": True,
            "source_note": "",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
        },
    )
    monkeypatch.setattr(
        gamma_map_service,
        "get_gamma_snapshot",
        lambda: {
            "asof": "2026-03-20T09:30:00-04:00",
            "snapshot_status": "invalid",
            "snapshot_status_label": "Invalid Snapshot: gamma levels unavailable",
            "snapshot_status_detail": "No trustworthy gamma calculation is available.",
            "requested_expiries": ["2026-03-20", "2026-03-23"],
            "included_expiries": [],
            "exchange_timestamp_available": False,
            "source_fetch_timestamp": "2026-03-20T09:30:00-04:00",
            "source_effective_timestamp": "2026-03-20T09:30:00-04:00",
            "source_effective_timestamp_note": "Exchange-native chain timestamp unavailable; using fetch timestamp.",
            "warnings": ["No gamma source rows were loaded."],
            "stale_flags": ["empty_source"],
            "regime": "unavailable",
            "net_gex": 0.0,
            "diagnostics": {"status": "invalid", "contracts_used": 0},
            "narrative": {"warning_badges": ["Invalid Snapshot"]},
        },
    )
    monkeypatch.setattr(
        options_panel_service,
        "get_options_snapshot",
        lambda: {"asof": "2026-03-20T09:30:00-04:00", "symbols": {"SPX": {"contracts": []}}},
    )

    resp = client.get("/market-pulse", follow_redirects=True)

    assert resp.status_code == 200
    assert b"Invalid Snapshot: gamma levels unavailable" in resp.data


def test_market_pulse_renders_stale_snapshot_banner(client, monkeypatch):
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import options_panel_service

    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda **_: {
            "available": True,
            "fetched_at": "Mar 20, 2026 09:30 AM ET",
            "source_label": "Tradier market feed",
            "source_note": "",
            "quotes": [],
            "integrity": {"live_count": 2, "delayed_count": 0, "missing_count": 0},
        },
    )
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda: {
            "available": True,
            "source_note": "",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
        },
    )
    monkeypatch.setattr(
        gamma_map_service,
        "get_gamma_snapshot",
        lambda: {
            "asof": "2026-03-20T09:15:00-04:00",
            "snapshot_status": "stale",
            "snapshot_status_label": "Stale Snapshot: showing last known good values",
            "snapshot_status_detail": "A fresh trusted basket is unavailable, so the last validated snapshot is being served.",
            "requested_expiries": ["2026-03-20", "2026-03-23"],
            "included_expiries": ["2026-03-20", "2026-03-23"],
            "exchange_timestamp_available": False,
            "source_fetch_timestamp": "2026-03-20T09:15:00-04:00",
            "source_effective_timestamp": "2026-03-20T09:15:00-04:00",
            "source_effective_timestamp_note": "Exchange-native chain timestamp unavailable; using fetch timestamp.",
            "spot": 5120.0,
            "spot_price_used": 5120.0,
            "spot_source_name": "tradier",
            "spot_source_timestamp": "2026-03-20T09:15:00-04:00",
            "regime": "positive",
            "net_gex": 120000000.0,
            "net_gamma_label": "+120.00M",
            "gamma_flip": 5110.0,
            "call_wall": 5150.0,
            "put_wall": 5090.0,
            "gamma_range_estimate": 30.0,
            "gamma_range_high": 5150.0,
            "gamma_range_low": 5090.0,
            "call_wall_gamma_per_point": 125000000.0,
            "put_wall_gamma_per_point": 114000000.0,
            "gamma_walls_top3": [5150.0],
            "warnings": [
                "Stale Snapshot: showing last known good values because the latest refresh failed."
            ],
            "stale_flags": ["stale_snapshot"],
            "void_zone": {"start": None, "end": None},
            "bias": "buy_dips_above_flip",
            "diagnostics": {"status": "stale", "contracts_used": 84},
            "narrative": {"warning_badges": ["Stale Snapshot"]},
        },
    )
    monkeypatch.setattr(
        options_panel_service,
        "get_options_snapshot",
        lambda: {"asof": "2026-03-20T09:30:00-04:00", "symbols": {"SPX": {"contracts": []}}},
    )

    resp = client.get("/market-pulse", follow_redirects=True)

    assert resp.status_code == 200
    assert b"Stale Snapshot: showing last known good values" in resp.data


def test_market_pulse_renders_source_health_and_degraded_banner(client, monkeypatch):
    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda **_: {
            "available": True,
            "fetched_at": "Mar 7, 2026 10:30 AM ET",
            "source_label": "Tradier market feed",
            "source_note": "cached fallback",
            "quotes": [],
            "integrity": {"live_count": 0, "delayed_count": 2, "missing_count": 3},
        },
    )
    monkeypatch.setattr(
        core_service,
        "_market_news_snapshot",
        lambda: {
            "available": True,
            "source_note": "Live + cached merge",
            "macro_events": [],
            "market_items": [],
            "watchlist_items": [],
        },
    )
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import options_panel_service

    monkeypatch.setattr(gamma_map_service, "start_gamma_worker_once", lambda: None)
    monkeypatch.setattr(options_panel_service, "start_options_worker_once", lambda: None)
    monkeypatch.setattr(
        gamma_map_service,
        "get_gamma_snapshot",
        lambda: {"asof": "", "regime": "unavailable", "bias": "insufficient_data"},
    )
    monkeypatch.setattr(
        options_panel_service,
        "get_options_snapshot",
        lambda: {"asof": "", "symbols": {"SPX": {"contracts": []}}},
    )

    resp = client.get("/market-pulse", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Source Health" in resp.data
    assert b"Live Data Status" in resp.data


def test_stream_options_panel_sse_emits_json_payload(client, monkeypatch):
    from mccain_capital.services import options_panel_service

    monkeypatch.setattr(options_panel_service, "start_options_worker_once", lambda: None)
    monkeypatch.setattr(
        options_panel_service,
        "get_options_snapshot",
        lambda: {
            "asof": "2026-03-05T12:00:00-05:00",
            "symbols": {
                "SPX": {
                    "underlying": {"price": 5120.35, "change_pct": 0.42, "source": "massive"},
                    "contracts": [
                        {
                            "label": "SPXW 2026-03-06 5125C",
                            "mid": 24.10,
                            "delta": 0.47,
                            "vol": 9200,
                            "oi": 18400,
                            "spread": 0.60,
                            "liq": "Tight",
                        }
                    ],
                    "gamma": {
                        "gamma_flip": 5110.0,
                        "call_wall": 5150.0,
                        "put_wall": 5050.0,
                        "net_gamma": "+2.1B",
                    },
                }
            },
        },
    )
    monkeypatch.setattr(core_service.time, "sleep", lambda _: None)

    resp = client.get("/stream/options_panel", follow_redirects=True)
    assert resp.status_code == 200
    assert resp.headers.get("Content-Type", "").startswith("text/event-stream")
    assert b"data: " in resp.data
    assert b"SPXW 2026-03-06 5125C" in resp.data
    assert b"gamma_flip" in resp.data


def test_candle_opens_page_renders_monthly_market_calendar(client):
    resp = client.get("/candle-opens?y=2026&m=2", follow_redirects=True)
    assert resp.status_code == 200
    assert b"February 2026 Candle Opens" in resp.data
    assert b"Presidents Day" in resp.data
    assert b"2D" in resp.data
    assert b"Trading Days" in resp.data
    assert b"Day reset" in resp.data
    assert b"candleWeekdayInline" in resp.data
    assert b"candleFocusStrip" in resp.data
    assert b"Next Reset Cluster" in resp.data


def test_trades_page_uses_derived_running_balance(client):
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("starting_balance", "50000"),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-24",
                "9:35 AM",
                "10:00 AM",
                "SPX",
                "CALL",
                6925.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                399.0,
                399.0,
                399.0,
                50445.10,  # intentionally stale/incorrect row balance
                "seed 1",
                now_iso(),
            ),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-25",
                "9:40 AM",
                "10:05 AM",
                "SPX",
                "CALL",
                6920.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                3000.0,
                3000.0,
                3000.0,
                50434.40,  # intentionally stale/incorrect row balance
                "seed 2",
                now_iso(),
            ),
        )

    resp = client.get("/trades", follow_redirects=True)
    assert resp.status_code == 200
    # 50,000 + (399 + 3,000) = 53,399
    assert b"$53,399.00" in resp.data


def test_trade_mutations_flash_feedback(client):
    with db() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm, gross_pl,
                net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                today_iso(),
                "9:35 AM",
                "9:40 AM",
                "SPX",
                "CALL",
                5000.0,
                1.0,
                1.2,
                1,
                100.0,
                1.0,
                19.0,
                19.0,
                19.0,
                50019.0,
                "seed",
                now_iso(),
            ),
        )
        trade_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

    reviewed = client.post(
        f"/trades/review/{trade_id}?d={today_iso()}",
        data={
            "strategy_label": "ORB",
            "session_tag": "Open",
            "checklist_score": "82",
            "rule_break_tags": "",
            "review_note": "Good process.",
        },
        follow_redirects=True,
    )
    assert reviewed.status_code == 200
    assert b"Trade review saved." in reviewed.data
    assert b"Review Completion" in reviewed.data
    assert b"All core review checks logged." not in reviewed.data

    review_page = client.get(f"/trades/review/{trade_id}?d={today_iso()}", follow_redirects=True)
    assert review_page.status_code == 200
    assert b"Review Completion" in review_page.data

    trades_page = client.get(f"/trades?d={today_iso()}", follow_redirects=True)
    assert trades_page.status_code == 200
    assert b"tradeReviewMeta-missing" in trades_page.data

    edited = client.post(
        f"/trades/edit/{trade_id}?d={today_iso()}",
        data={
            "trade_date": today_iso(),
            "entry_time": "9:35 AM",
            "exit_time": "9:42 AM",
            "ticker": "SPX",
            "opt_type": "CALL",
            "strike": "5000",
            "contracts": "1",
            "entry_price": "1.00",
            "exit_price": "1.30",
            "comm": "1.00",
        },
        follow_redirects=True,
    )
    assert edited.status_code == 200
    assert b"Trade updated." in edited.data

    risk = client.post(
        "/trades/risk-controls",
        data={"daily_max_loss": "250", "enforce_lockout": "1"},
        follow_redirects=True,
    )
    assert risk.status_code == 200
    assert b"Risk controls saved." in risk.data


def test_manual_trade_save_flashes_and_redirects(client):
    resp = client.post(
        "/trades/new",
        data={
            "trade_date": today_iso(),
            "entry_time": "9:35 AM",
            "exit_time": "9:40 AM",
            "ticker": "SPX",
            "opt_type": "CALL",
            "strike": "5000",
            "contracts": "1",
            "entry_price": "1.00",
            "exit_price": "1.20",
            "comm": "1.00",
            "strategy_label": "",
            "session_tag": "",
            "checklist_score": "",
            "gate_setup_type": "Opening drive",
            "gate_invalidation": "Lose opening range low",
            "gate_max_risk": "100",
            "gate_focus": "Take one clean A setup",
            "gate_market_ready": "1",
            "gate_macro_clear": "1",
            "gate_risk_confirmed": "1",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"Trade saved." in resp.data


def test_trade_edit_page_renders_form(client):
    created = client.post(
        "/trades/new",
        data={
            "trade_date": today_iso(),
            "entry_time": "9:35 AM",
            "exit_time": "9:40 AM",
            "ticker": "SPX",
            "opt_type": "CALL",
            "strike": "5000",
            "contracts": "1",
            "entry_price": "1.00",
            "exit_price": "1.20",
            "comm": "1.00",
            "gate_setup_type": "Opening drive",
            "gate_invalidation": "Lose opening range low",
            "gate_max_risk": "100",
            "gate_focus": "Take one clean A setup",
            "gate_market_ready": "1",
            "gate_macro_clear": "1",
            "gate_risk_confirmed": "1",
        },
        follow_redirects=True,
    )
    assert created.status_code == 200

    with db() as conn:
        trade_id = conn.execute("SELECT MAX(id) FROM trades").fetchone()[0]

    resp = client.get(f"/trades/edit/{trade_id}?d={today_iso()}", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Edit Trade" in resp.data
    assert b"Fees (total)" in resp.data


def test_risk_controls_page_renders_form(client):
    resp = client.get("/trades/risk-controls", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Risk Controls" in resp.data
    assert b"Daily Max Loss" in resp.data


def test_dashboard_shows_balance_basis_and_drift_signal(client):
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("starting_balance", "50000"),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-26",
                "9:45 AM",
                "10:00 AM",
                "SPX",
                "CALL",
                6930.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                600.0,
                600.0,
                600.0,
                50434.40,  # stale row balance to trigger drift signal
                "seed drift",
                now_iso(),
            ),
        )

    resp = client.get("/dashboard", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Start $50,000.00" in resp.data
    assert b"Ledger drift detected" in resp.data
    assert b"Daily P/L Calendar" in resp.data
    assert b"/ops/alerts" in resp.data


def test_dashboard_renders_calendar_week_cards_and_preview_metadata(client):
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("starting_balance", "50000"),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-24",
                "9:35 AM",
                "9:50 AM",
                "SPX",
                "CALL",
                6900.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                250.0,
                250.0,
                250.0,
                50250.0,
                "win 1",
                now_iso(),
            ),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-24",
                "10:05 AM",
                "10:20 AM",
                "SPX",
                "PUT",
                6895.0,
                1.5,
                1.0,
                1,
                150.0,
                1.0,
                -80.0,
                -80.0,
                -53.3,
                50170.0,
                "loss 1",
                now_iso(),
            ),
        )

    resp = client.get("/dashboard?y=2026&m=2", follow_redirects=True)
    assert resp.status_code == 200
    assert b"weekCardTitle" in resp.data
    assert b"2T" in resp.data
    assert b'data-wins="1"' in resp.data
    assert b'data-losses="1"' in resp.data
    assert b"calendarPreview" in resp.data
    assert b'aria-label="Preview 2026-02-24"' in resp.data


def test_dashboard_active_scope_aligns_balance_card_and_calendar(client):
    set_setting_value("starting_balance", "50000")
    set_setting_value("active_account_start_date", "2026-02-20")
    set_setting_value("active_account_start_balance", "52000")
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
                "2026-02-18",
                "9:35 AM",
                "9:50 AM",
                "SPX",
                "CALL",
                6900.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                4321.0,
                4321.0,
                4321.0,
                54321.0,
                "old scope trade",
                now_iso(),
            ),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-24",
                "10:05 AM",
                "10:20 AM",
                "SPX",
                "PUT",
                6895.0,
                1.5,
                1.0,
                1,
                150.0,
                1.0,
                250.0,
                250.0,
                166.7,
                54571.0,
                "scoped trade",
                now_iso(),
            ),
        )

    resp = client.get("/dashboard?y=2026&m=2&scope=active", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Active Account Profit" in resp.data
    assert b"Funded $52,000.00" in resp.data
    assert b"Daily P/L calendar view for <strong>Active Account</strong>" in resp.data
    assert b'data-balance="$250.00"' in resp.data
    assert b'data-balance="$54,321.00"' not in resp.data


def test_dashboard_all_history_uses_profit_only_display(client):
    set_setting_value("starting_balance", "50000")
    account_id = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Protect",
        broker_account_id="default:acct-1",
        account_size=50000.0,
        starting_balance=50000.0,
        max_drawdown=2500.0,
    )
    trades_repo.set_active_account(int(account_id))
    with db() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at, account_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-24",
                "10:05 AM",
                "10:20 AM",
                "SPX",
                "PUT",
                6895.0,
                1.5,
                1.0,
                1,
                150.0,
                1.0,
                250.0,
                250.0,
                166.7,
                50250.0,
                "all history trade",
                now_iso(),
                int(account_id),
            ),
        )

    resp = client.get("/dashboard?y=2026&m=2&scope=all", follow_redirects=True)
    assert resp.status_code == 200
    assert b"All History Profit" in resp.data
    assert b'data-balance="$250.00"' in resp.data
    assert b'data-balance="$50,250.00"' not in resp.data


def test_dashboard_recompute_balances_endpoint_updates_stored_rows(client):
    set_setting_value("auth_username", "owner")
    set_setting_value("auth_password_hash", generate_password_hash("pass123"))
    set_setting_value("starting_balance", "50000")
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
                "2026-02-24",
                "9:35 AM",
                "10:00 AM",
                "SPX",
                "CALL",
                6925.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                399.0,
                399.0,
                399.0,
                50000.0,  # stale/incorrect
                "seed 1",
                now_iso(),
            ),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-25",
                "9:40 AM",
                "10:05 AM",
                "SPX",
                "CALL",
                6920.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                3000.0,
                3000.0,
                3000.0,
                50000.0,  # stale/incorrect
                "seed 2",
                now_iso(),
            ),
        )

    with client.session_transaction() as sess:
        sess["auth_ok"] = True
        sess["auth_user"] = "owner"

    resp = client.post("/dashboard/recompute-balances", follow_redirects=True)
    assert resp.status_code == 200

    with db() as conn:
        rows = conn.execute("SELECT balance FROM trades ORDER BY trade_date ASC, id ASC").fetchall()
    assert len(rows) == 2
    assert float(rows[0]["balance"]) == 50399.0
    assert float(rows[1]["balance"]) == 53399.0


def test_dashboard_recompute_balances_endpoint_updates_active_account_rows(client):
    set_setting_value("auth_username", "owner")
    set_setting_value("auth_password_hash", generate_password_hash("pass123"))
    set_setting_value("starting_balance", "50000")

    account_a = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Protect",
        broker_account_id="default:ACC111",
        account_size=50000.0,
        starting_balance=50000.0,
    )
    account_b = trades_repo.create_account(
        prop_firm="Vanquish",
        account_name="Growth",
        broker_account_id="default:ACC222",
        account_size=30000.0,
        starting_balance=30000.0,
    )
    trades_repo.set_active_account(int(account_a))

    with db() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at, account_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-24",
                "9:35 AM",
                "10:00 AM",
                "SPX",
                "CALL",
                6925.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                399.0,
                399.0,
                399.0,
                50000.0,
                "account a trade 1",
                now_iso(),
                int(account_a),
            ),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at, account_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-25",
                "9:40 AM",
                "10:05 AM",
                "SPX",
                "CALL",
                6920.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                3000.0,
                3000.0,
                3000.0,
                50000.0,
                "account a trade 2",
                now_iso(),
                int(account_a),
            ),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at, account_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-24",
                "11:15 AM",
                "11:30 AM",
                "QQQ",
                "PUT",
                500.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                249.0,
                249.0,
                249.0,
                30000.0,
                "account b trade 1",
                now_iso(),
                int(account_b),
            ),
        )

    with client.session_transaction() as sess:
        sess["auth_ok"] = True
        sess["auth_user"] = "owner"

    resp = client.post(
        "/dashboard/recompute-balances",
        data={"scope": "active"},
        follow_redirects=True,
    )
    assert resp.status_code == 200

    with db() as conn:
        rows_a = conn.execute(
            "SELECT balance FROM trades WHERE account_id = ? ORDER BY trade_date ASC, id ASC",
            (int(account_a),),
        ).fetchall()
        rows_b = conn.execute(
            "SELECT balance FROM trades WHERE account_id = ? ORDER BY trade_date ASC, id ASC",
            (int(account_b),),
        ).fetchall()
    assert len(rows_a) == 2
    assert float(rows_a[0]["balance"]) == 50399.0
    assert float(rows_a[1]["balance"]) == 53648.0
    assert len(rows_b) == 1
    assert float(rows_b[0]["balance"]) == 50648.0


def test_dashboard_recompute_balances_requires_auth(client):
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("starting_balance", "50000"),
        )
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-24",
                "9:35 AM",
                "10:00 AM",
                "SPX",
                "CALL",
                6925.0,
                1.0,
                2.0,
                1,
                100.0,
                1.0,
                399.0,
                399.0,
                399.0,
                50000.0,
                "seed",
                now_iso(),
            ),
        )

    resp = client.post("/dashboard/recompute-balances", follow_redirects=True)
    assert resp.status_code == 200

    with db() as conn:
        row = conn.execute("SELECT balance FROM trades LIMIT 1").fetchone()
    assert float(row["balance"]) == 50000.0
