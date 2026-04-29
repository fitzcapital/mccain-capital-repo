from mccain_capital import runtime
from mccain_capital.services import life_alignment as life_alignment_service


def test_life_alignment_page_renders_and_links(client, tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "PERSISTENT_DATA_DIR", str(tmp_path))

    resp = client.get("/life-alignment", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Life Alignment" in body
    assert "Save Today&#39;s Alignment" in body or "Save Today's Alignment" in body
    assert "/journal/life" in body

    journal_resp = client.get("/journal/life", follow_redirects=True)
    assert journal_resp.status_code == 200
    assert "/life-alignment" in journal_resp.get_data(as_text=True)


def test_life_alignment_save_persists_and_scores(client, tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "PERSISTENT_DATA_DIR", str(tmp_path))

    resp = client.post(
        "/api/life-alignment/today",
        json={
            "water_oz": 100,
            "water_goal_oz": 100,
            "workout_completed": True,
            "workout_type": "Full Body",
            "pushups": 50,
            "squats": 75,
            "walk_minutes": 25,
            "steps": 7200,
            "sleep_hours": 7.25,
            "devotion_completed": True,
            "journal_completed": True,
            "mood": "strong",
            "notes": "Clean day.",
        },
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["entry"]["discipline_score"] == 100
    assert payload["entry"]["locked"] is False
    assert payload["entry"]["followed_rules"] == "not_yet"
    assert payload["entry"]["daily_status"] == "COMPLETE"

    today = client.get("/api/life-alignment/today").get_json()["entry"]
    assert today["water_oz"] == 100
    assert today["workout_type"] == "Full Body"
    assert today["notes"] == "Clean day."


def test_life_alignment_analytics_updates(client, tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "PERSISTENT_DATA_DIR", str(tmp_path))
    client.post(
        "/api/life-alignment/today",
        json={
            "water_oz": 120,
            "water_goal_oz": 100,
            "workout_completed": True,
            "walk_minutes": 30,
            "sleep_hours": 8,
            "devotion_completed": True,
            "journal_completed": False,
        },
    )

    payload = client.get("/api/life-alignment/analytics").get_json()
    analytics = payload["analytics"]
    assert analytics["current_workout_streak"] == 1
    assert analytics["current_water_goal_streak"] == 1
    assert analytics["current_devotion_streak"] == 1
    assert analytics["current_journal_streak"] == 0
    assert analytics["average_discipline_score"] == 85
    assert analytics["best_workout_streak"] == 1
    assert analytics["weekly_habit_hit_rates"]["water"] >= 0
    assert analytics["accountability_insights"]
    assert analytics["accountability_insights"] == ["Not enough data yet. Stack 3 clean days."]


def test_life_alignment_rule_break_caps_score(client, tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "PERSISTENT_DATA_DIR", str(tmp_path))

    resp = client.post(
        "/api/life-alignment/today",
        json={
            "water_oz": 100,
            "water_goal_oz": 100,
            "workout_completed": True,
            "walk_minutes": 25,
            "sleep_hours": 8,
            "devotion_completed": True,
            "journal_completed": True,
            "followed_rules": "no",
        },
    )

    entry = resp.get_json()["entry"]
    assert entry["discipline_score"] == 69
    assert entry["score_label"] == "BUILDING"
    assert "Rule break logged" in entry["rule_message"]


def test_life_alignment_lock_and_unlock(client, tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "PERSISTENT_DATA_DIR", str(tmp_path))

    lock_resp = client.post(
        "/api/life-alignment/today",
        json={"water_oz": 10, "water_goal_oz": 100, "action": "lock"},
    )
    locked = lock_resp.get_json()["entry"]
    assert locked["locked"] is True
    assert locked["locked_at"]

    blocked = client.post("/api/life-alignment/today", json={"water_oz": 20})
    assert blocked.status_code == 409

    unlock_resp = client.post("/api/life-alignment/today", json={"action": "unlock"})
    unlocked = unlock_resp.get_json()["entry"]
    assert unlocked["locked"] is False
    assert unlocked["locked_at"] == ""


def test_life_alignment_insights_do_not_repeat_best_and_weakest(monkeypatch):
    monkeypatch.setattr(runtime, "today_iso", lambda: "2026-04-29")
    monkeypatch.setattr(life_alignment_service, "today_iso", lambda: "2026-04-29")
    entries = []
    for day in ["2026-04-27", "2026-04-28", "2026-04-29"]:
        entries.append(
            {
                "date": day,
                "water_oz": 100,
                "water_goal_oz": 100,
                "workout_completed": False,
                "walk_minutes": 20,
                "sleep_hours": 7,
                "devotion_completed": True,
                "journal_completed": False,
                "created_at": f"{day}T08:00:00-04:00",
            }
        )

    insights = life_alignment_service._build_analytics(entries)["accountability_insights"]

    assert "Best habit this week: Water" in insights
    assert "Weakest habit this week: Water" not in insights


def test_life_alignment_insights_use_flat_message_for_ties(monkeypatch):
    monkeypatch.setattr(runtime, "today_iso", lambda: "2026-04-29")
    monkeypatch.setattr(life_alignment_service, "today_iso", lambda: "2026-04-29")
    entries = [
        {
            "date": day,
            "water_oz": 0,
            "water_goal_oz": 100,
            "workout_completed": False,
            "walk_minutes": 0,
            "sleep_hours": 0,
            "devotion_completed": False,
            "journal_completed": False,
            "created_at": f"{day}T08:00:00-04:00",
        }
        for day in ["2026-04-27", "2026-04-28", "2026-04-29"]
    ]

    insights = life_alignment_service._build_analytics(entries)["accountability_insights"]

    assert insights[0] == "Habits are currently flat - build momentum."
