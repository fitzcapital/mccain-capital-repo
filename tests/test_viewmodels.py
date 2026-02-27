"""Unit tests for shared UI viewmodel builders."""

from mccain_capital.services.viewmodels import (
    analytics_data_trust,
    dashboard_data_trust,
    trades_data_trust,
)


def test_dashboard_data_trust_prefers_drift_message():
    vm = dashboard_data_trust(
        {"last_sync_status": "success", "last_sync_stage": "", "last_sync_updated_human": "now"},
        {"has_drift": True, "delta": 839.0},
    )
    assert vm.tone == "critical"
    assert "Ledger drift detected" in vm.message
    assert vm.primary_href == "/trades/upload/statement?ws=reconcile"


def test_trades_data_trust_prefers_guardrail_lock():
    vm = trades_data_trust(
        {"status": "failed", "stage": "reconcile_gate", "updated_at_human": "now"},
        guardrail_locked=True,
        active_day="2026-02-27",
    )
    assert vm.tone == "critical"
    assert "Guardrail is locked for 2026-02-27" in vm.message
    assert vm.primary_href == "/trades/risk-controls"


def test_analytics_data_trust_sync_fail_action():
    vm = analytics_data_trust(
        {"last_sync_status": "failed", "last_sync_stage": "submit_login"},
        integrity_issue_count=0,
    )
    assert vm.tone == "critical"
    assert "Sync reliability is degraded" in vm.message
    assert vm.primary_href == "/trades/upload/statement?ws=live"
