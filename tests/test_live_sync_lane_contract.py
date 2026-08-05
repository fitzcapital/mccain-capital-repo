"""Contract tests for canonical Live Sync state and safe Dashboard execution."""

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from mccain_capital.services import trades as trades_svc
from mccain_capital.services import trades_sync


def _preflight(*, can_run: bool = True) -> dict:
    return {
        "can_run": can_run,
        "disabled_reason": "" if can_run else "Missing setup",
        "credentials_ready": can_run,
    }


def test_cancelled_attempt_never_completes_today():
    now = datetime(2026, 8, 5, 12, 0, tzinfo=ZoneInfo("America/New_York"))
    state = trades_sync._canonical_live_sync_state(
        last_status={
            "status": "cancelled",
            "stage": "cancelled",
            "updated_at": "2026-08-05T14:00:00+00:00",
            "message": "Cancelled by operator.",
        },
        active_job={},
        history=[],
        auto_cfg={"enabled": True, "run_time_et": "16:25"},
        preflight=_preflight(),
        now_et=now,
    )

    assert state["outcome"] == "cancelled"
    assert state["attempted_today"] is True
    assert state["import_completed_today"] is False
    assert state["today_status"] == "cancelled"


def test_diagnostic_success_is_not_an_import():
    now = datetime(2026, 8, 5, 12, 0, tzinfo=ZoneInfo("America/New_York"))
    state = trades_sync._canonical_live_sync_state(
        last_status={
            "status": "debug_only",
            "stage": "capture_statement_html",
            "updated_at": "2026-08-05T14:00:00+00:00",
            "requested": {"debug_only": True},
        },
        active_job={},
        history=[],
        auto_cfg={},
        preflight=_preflight(),
        now_et=now,
    )

    assert state["outcome"] == "diagnostic_only"
    assert state["import_completed_today"] is False
    assert state["today_status"] == "diagnostic"


def test_no_new_trades_completes_today():
    now = datetime(2026, 8, 5, 12, 0, tzinfo=ZoneInfo("America/New_York"))
    last_status = {
        "status": "success",
        "stage": "import_complete",
        "inserted": 0,
        "updated_at": "2026-08-05T14:00:00+00:00",
        "requested": {"debug_only": False},
    }
    state = trades_sync._canonical_live_sync_state(
        last_status=last_status,
        active_job={},
        history=[],
        auto_cfg={},
        preflight=_preflight(),
        now_et=now,
    )

    assert state["outcome"] == "no_new_trades"
    assert state["import_completed_today"] is True
    assert state["today_status"] == "completed"


def test_earlier_success_is_preserved_after_failure():
    now = datetime(2026, 8, 5, 12, 0, tzinfo=ZoneInfo("America/New_York"))
    state = trades_sync._canonical_live_sync_state(
        last_status={
            "status": "failed",
            "stage": "submit_login",
            "updated_at": "2026-08-05T15:00:00+00:00",
        },
        active_job={},
        history=[
            {
                "status": "success",
                "stage": "import_complete",
                "updated_at": "2026-08-05T13:00:00+00:00",
            }
        ],
        auto_cfg={},
        preflight=_preflight(),
        now_et=now,
    )

    assert state["outcome"] == "failed"
    assert state["import_completed_today"] is True
    assert state["last_successful_import_at"] == "2026-08-05T13:00:00+00:00"


def test_reliability_excludes_cancelled_and_diagnostic_from_import_denominator():
    history = [
        {"updated_at": trades_svc.now_iso(), "status": "cancelled", "source": "manual"},
        {"updated_at": trades_svc.now_iso(), "status": "debug_only", "source": "manual"},
    ]

    summary = trades_svc._sync_reliability_summary(history, days=30)

    assert summary["attempts"] == 2
    assert summary["import_attempts"] == 0
    assert summary["cancelled"] == 1
    assert summary["diagnostic_only"] == 1
    assert summary["success_rate"] is None


def test_sync_today_preflight_uses_current_et_day_and_selected_broker(monkeypatch):
    selected = {
        "id": 42,
        "account_name": "75k",
        "broker_account_id": "default:OEV0073921",
    }
    monkeypatch.setattr(
        trades_sync,
        "_broker_credential_context",
        lambda username="", cfg=None: (dict(cfg or {}), "saved-user", "saved-password"),
    )

    preflight = trades_sync._build_sync_today_preflight(
        selected_account=selected,
        cfg={"base_url": "https://trade.example"},
        requested={
            "from_date": "2026-07-01",
            "to_date": "2026-07-31",
            "account": "default:OLD0001",
            "debug_only": True,
        },
        today_et="2026-08-05",
    )

    assert preflight["can_run"] is True
    assert preflight["from_date"] == "2026-08-05"
    assert preflight["to_date"] == "2026-08-05"
    assert preflight["broker_account"] == "default:OEV0073921"
    assert preflight["debug_only"] is False
    assert preflight["intent_label"] == "Normal import"


def test_sync_today_preflight_fails_closed_without_broker_id(monkeypatch):
    monkeypatch.setattr(
        trades_sync,
        "_broker_credential_context",
        lambda username="", cfg=None: (dict(cfg or {}), "saved-user", "saved-password"),
    )

    preflight = trades_sync._build_sync_today_preflight(
        selected_account={"id": 42, "account_name": "75k", "broker_account_id": ""},
        cfg={"base_url": "https://trade.example"},
        today_et="2026-08-05",
    )

    assert preflight["can_run"] is False
    assert "broker account ID" in preflight["disabled_reason"]


def test_dashboard_sync_today_rebuilds_safe_request(client, monkeypatch):
    selected = {
        "id": 42,
        "account_name": "75k",
        "broker_account_id": "default:OEV0073921",
    }
    old_request = {
        "source": "manual_live",
        "mode": "balance",
        "from_date": "2026-07-01",
        "to_date": "2026-07-31",
        "account": "default:OLD0001",
        "debug_only": True,
        "username": "saved-user",
    }
    captured = {}
    monkeypatch.setattr(trades_svc, "_latest_active_sync_job", lambda: {})
    monkeypatch.setattr(trades_svc, "trade_lockout_state", lambda _day: {"locked": False})
    monkeypatch.setattr(trades_svc, "_selected_account", lambda: selected)
    monkeypatch.setattr(
        trades_svc,
        "_load_last_sync_status",
        lambda: {"status": "debug_only", "requested": old_request},
    )
    monkeypatch.setattr(trades_svc, "_load_sync_history", lambda: [])
    monkeypatch.setattr(trades_svc, "_load_auto_sync_config", lambda: {"enabled": False})
    monkeypatch.setattr(
        trades_svc,
        "_load_broker_sync_config",
        lambda: {
            "username": "saved-user",
            "base_url": "https://trade.example",
            "headless": True,
            "debug_capture": True,
        },
    )
    monkeypatch.setattr(trades_svc, "_get_auto_sync_password", lambda _cfg: "saved-password")

    def fake_start(**kwargs):
        captured.update(kwargs)
        return {
            "id": "sync-today-1",
            "kind": "sync",
            "title": "Live Sync",
            "status": "queued",
            "stage": "start",
            "message": "Queued.",
            "created_at": trades_svc.now_iso(),
            "updated_at": trades_svc.now_iso(),
            "summary": {},
            "requested": kwargs["requested"],
        }

    monkeypatch.setattr(trades_svc, "_start_sync_job", fake_start)

    response = client.post(
        "/trades/sync/live/last-run?async=1",
        headers={"Accept": "application/json"},
    )

    assert response.status_code == 200
    assert captured["mode"] == "broker"
    assert captured["from_date"] == trades_svc.today_iso()
    assert captured["to_date"] == trades_svc.today_iso()
    assert captured["account"] == "default:OEV0073921"
    assert captured["debug_only"] is False
    assert captured["record_source"] == "DASHBOARD SYNC TODAY"


def test_dashboard_client_does_not_reclassify_idle_timestamp_as_completion():
    source = Path("static/js/dashboard_command_center.js").read_text(encoding="utf-8")

    initialization = source[source.index("if (activeJobId)") : source.index(
        "(function () {\n  const mindsetItem"
    )]
    assert "applyIdleState();" not in initialization
    assert "applyCanonicalState(payload.sync)" in source


def test_live_sync_templates_surface_safe_desktop_language():
    dashboard = Path("mccain_capital/templates/dashboard.html").read_text(encoding="utf-8")
    lane = Path("mccain_capital/templates/trades/upload_statement.html").read_text(
        encoding="utf-8"
    )

    assert "Sync Today preflight" in dashboard
    assert ">Sync Today<" in dashboard
    assert "Normal Import" in lane
    assert "Diagnostic Test — No Import" in lane
    assert "No import attempts" in lane


def test_live_sync_template_has_three_region_desktop_hierarchy():
    lane = Path("mccain_capital/templates/trades/upload_statement.html").read_text(
        encoding="utf-8"
    )

    assert lane.count('data-live-sync-region="') == 3
    assert 'data-live-sync-region="status"' in lane
    assert 'data-live-sync-region="primary"' in lane
    assert 'data-live-sync-region="advanced"' in lane
    advanced_start = lane.index('data-live-sync-region="advanced"')
    advanced_end = lane.rindex("{% if ws == 'reconcile' %}")
    advanced_markup = lane[advanced_start:advanced_end]
    assert "Operator Deck" in advanced_markup
    assert "Run Feedback" in advanced_markup
    assert "Failure Guide" in advanced_markup
    assert "Sync Reliability (30D)" in advanced_markup
    assert "After-Market Auto Sync" in advanced_markup
    assert "<details class=\"card liveSyncAdvancedRegion\"" in lane


def test_dashboard_sync_is_a_compact_readiness_control():
    dashboard = Path("mccain_capital/templates/dashboard.html").read_text(encoding="utf-8")
    start = dashboard.index('class="dashboardCommandRail dashboardSyncReadiness"')
    end = dashboard.index("</details>", start)
    sync_markup = dashboard[start:end]

    assert "Import readiness" in sync_markup
    assert "Full sync details" in sync_markup
    assert sync_markup.count("data-dashboard-sync-run") == 2
    assert "Active Account" not in sync_markup
    assert "All History" not in sync_markup
    assert ">Scope<" not in sync_markup
    assert "Auto sync:" not in sync_markup
    assert 'hidden class="dashboardSyncPreflight"' in sync_markup


def test_monitoring_projection_is_redacted_and_passive(monkeypatch):
    calls = {"reads": 0}

    def fake_state():
        calls["reads"] += 1
        return {
            "outcome": "cancelled",
            "stage": "cancelled",
            "attempted_today": True,
            "import_completed_today": False,
            "last_attempt_at": "2026-08-05T14:00:00+00:00",
            "last_successful_import_at": "",
            "automation_enabled": True,
            "automation_next_run_at": "2026-08-05T16:25:00-04:00",
            "preflight": {
                "broker_account": "default:OEV0073921",
                "username": "private-user",
            },
        }

    monkeypatch.setattr(trades_sync, "dashboard_live_sync_state", fake_state)

    result = trades_sync.live_sync_monitoring_state()

    assert calls == {"reads": 1}
    assert result["outcome"] == "cancelled"
    assert "preflight" not in result
    assert "username" not in result
    assert "broker_account" not in result
