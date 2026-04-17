from datetime import timedelta
import json

import pytest

from mccain_capital import app_core as core
from mccain_capital import runtime as app_runtime
from mccain_capital.repositories import journal as journal_repo
from mccain_capital.repositories import self_control as repo
from scripts import self_control_pf_blocker


@pytest.fixture(autouse=True)
def _bind_repo_db_paths(monkeypatch):
    monkeypatch.setattr(repo, "DB_PATH", core.DB_PATH)
    monkeypatch.setattr(journal_repo, "DB_PATH", core.DB_PATH)


def test_self_control_page_renders_with_seeded_console(client):
    response = client.get("/self-control")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "Self-Control Mode" in body
    assert "Blocked Sites" in body
    assert "Market Open Lock" in body
    assert "Post-Loss Cooldown" in body
    assert "Trading" in body
    assert "trade.vanquishtrader.com" in body
    assert "www.tradingview.com" in body
    assert "tradingview.com" in body
    assert "Browser Extension" in body


def test_self_control_site_add_toggle_delete(client):
    add_response = client.post(
        "/self-control/sites/add",
        data={"domain": "example.com", "category": "Random distractions"},
        follow_redirects=True,
    )

    assert add_response.status_code == 200
    site = next((item for item in repo.list_blocked_sites() if item["domain"] == "example.com"), None)
    assert site is not None
    assert int(site["enabled"]) == 1

    client.post(f"/self-control/sites/{int(site['id'])}/toggle", follow_redirects=True)
    site = repo.get_blocked_site(int(site["id"]))
    assert int(site["enabled"]) == 0

    client.post(f"/self-control/sites/{int(site['id'])}/delete", follow_redirects=True)
    assert repo.get_blocked_site(int(site["id"])) is None


def test_self_control_session_start_and_state_api(client):
    response = client.post(
        "/self-control/session/start",
        data={
            "start_confirmed": "1",
            "label": "Execution Lock",
            "duration_minutes": "30",
            "intent_note": "No revenge trades",
            "blocked_categories": ["Social", "News / Doomscroll"],
            "strict_mode": "1",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    session = repo.get_active_session()
    assert session is not None
    assert session["label"] == "Execution Lock"
    assert bool(session["strict_mode"]) is True

    state = client.get("/api/self-control/state")
    payload = state.get_json()
    assert payload["ok"] is True
    assert payload["session"]["label"] == "Execution Lock"
    assert payload["session"]["status"] == "active"


def test_self_control_strict_session_cannot_cancel(client):
    client.post(
        "/self-control/presets/market-open-lock/start",
        data={"start_confirmed": "1"},
        follow_redirects=True,
    )

    response = client.post(
        "/self-control/session/cancel",
        data={"cancel_reason": "Trying to exit"},
        follow_redirects=True,
    )

    assert response.status_code == 200
    session = repo.get_active_session()
    assert session is not None
    assert session["status"] == "active"
    assert bool(session["strict_mode"]) is True


def test_self_control_non_strict_cancel_requires_reason(client):
    client.post(
        "/self-control/presets/midday-reset/start",
        data={"start_confirmed": "1"},
        follow_redirects=True,
    )

    response = client.post("/self-control/session/cancel", data={}, follow_redirects=True)
    assert response.status_code == 200
    session = repo.get_active_session()
    assert session is not None
    assert session["status"] == "active"

    client.post(
        "/self-control/session/cancel",
        data={"cancel_reason": "Breaking focus to reset the plan."},
        follow_redirects=True,
    )
    recent = repo.list_recent_sessions(1)[0]
    assert recent["status"] == "cancelled"
    assert "reset the plan" in recent["cancel_reason"]


def test_self_control_stale_session_auto_recovers_on_state_api(client):
    now_et = app_runtime.now_et()
    repo.create_session(
        {
            "label": "Expired lock",
            "status": "active",
            "strict_mode": True,
            "started_at": (now_et - timedelta(minutes=40)).isoformat(),
            "planned_end_at": (now_et - timedelta(minutes=10)).isoformat(),
            "planned_minutes": 30,
            "completed_minutes": 0,
            "blocked_categories": ["Social"],
            "blocked_domains": ["x.com"],
        }
    )

    payload = client.get("/api/self-control/state").get_json()
    assert payload["ok"] is True
    assert payload["session"] is None
    recent = repo.list_recent_sessions(1)[0]
    assert recent["status"] == "completed"
    assert recent["completed_minutes"] == 30


def test_self_control_journal_unlock_gate_requires_trade_debrief(client):
    now_et = app_runtime.now_et()
    repo.create_session(
        {
            "label": "Debrief Gate",
            "status": "active",
            "strict_mode": True,
            "started_at": (now_et - timedelta(minutes=20)).isoformat(),
            "planned_end_at": (now_et - timedelta(minutes=5)).isoformat(),
            "planned_minutes": 15,
            "completed_minutes": 0,
            "blocked_categories": ["Social"],
            "blocked_domains": ["reddit.com"],
            "unlock_requirement": "trade_debrief_today_after_session_start",
        }
    )

    first_payload = client.get("/api/self-control/state").get_json()
    assert first_payload["session"]["status"] == "awaiting_journal_unlock"
    assert first_payload["unlock_ready"] is False

    journal_repo.create_entry(
        {
            "entry_date": now_et.date().isoformat(),
            "entry_type": "trade_debrief",
            "setup": "Post-loss reset",
            "notes": "Logged the debrief after the cooldown completed.",
        }
    )

    second_payload = client.get("/api/self-control/state").get_json()
    assert second_payload["session"] is None
    recent = repo.list_recent_sessions(1)[0]
    assert recent["status"] == "completed"
    assert str(recent["unlock_satisfied_at"] or "").strip()


def test_self_control_custom_domain_is_reflected_in_matching_preset_scope(client):
    client.post(
        "/self-control/sites/add",
        data={"domain": "news.ycombinator.com", "category": "News / Doomscroll"},
        follow_redirects=True,
    )

    response = client.get("/self-control")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "news.ycombinator.com" in body
    assert "No Scroll Until Close" in body


def test_self_control_page_exports_enforcement_state(client, tmp_path, monkeypatch):
    state_path = tmp_path / "self-control-state.json"
    monkeypatch.setattr("mccain_capital.services.self_control.SELF_CONTROL_STATE_PATH", str(state_path))

    client.post(
        "/self-control/presets/market-open-lock/start",
        data={"start_confirmed": "1"},
        follow_redirects=True,
    )
    client.get("/self-control")

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["active"] is True
    assert payload["status"] == "active"
    assert payload["label"] == "Market Open Lock"
    assert "x.com" in payload["blocked_domains"]
    assert "trade.vanquishtrader.com" in payload["blocked_domains"]
    assert "tradingview.com" in payload["blocked_domains"]
    assert "www.tradingview.com" in payload["blocked_domains"]


def test_self_control_pf_state_expands_www_domains(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "active": True,
                "status": "active",
                "session_id": 1,
                "label": "Trading Lock",
                "strict_mode": True,
                "planned_end_at": "",
                "unlock_requirement": "",
                "blocked_domains": ["tradingview.com", "trade.vanquishtrader.com"],
            }
        ),
        encoding="utf-8",
    )

    state = self_control_pf_blocker._parse_state(str(state_path))

    assert "tradingview.com" in state.blocked_domains
    assert "www.tradingview.com" in state.blocked_domains
    assert "trade.vanquishtrader.com" in state.blocked_domains
