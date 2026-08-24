from mccain_capital.services.forward_pace import build_projection


def test_forward_pace_page_renders(client):
    resp = client.get("/forward-pace", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Profit & Payout Planner" in body
    assert "js/forward_pace.js" in body
    assert 'href="/forward-pace"' in body
    assert 'name="target_date"' in body
    assert 'name="account_phase"' in body
    assert 'name="daily_profit"' in body
    assert 'id="forwardPaceSubmit"' in body
    assert "Run Projection" in body
    assert "Performance Rules" in body
    assert 'id="forwardLifecycleNext"' in body
    assert 'id="forwardLifecycleSafety"' in body


def test_forward_pace_frontend_uses_explicit_update_contract():
    script = open("static/js/forward_pace.js", encoding="utf-8").read()

    assert 'form.addEventListener("submit"' in script
    assert 'form.addEventListener("input"' in script
    assert "Changes not applied" in script
    assert "Lifecycle updated" in script
    assert "renderTrajectory" in script
    assert "forwardLifecycleLevel" in script


def test_forward_pace_projection_calculates_tax_and_schedule(client):
    resp = client.post(
        "/api/forward-pace/projection",
        json={
            "base_balance": 50000,
            "gross_payout": 250,
            "payouts_per_week": 5,
            "weeks": 4,
            "fixed_buffer": 5000,
            "buffer_rate": 10,
            "state": "GA",
            "filing_status": "single",
            "start_date": "2026-05-02",
        },
    )

    assert resp.status_code == 200
    projection = resp.get_json()["projection"]
    assert projection["weekly"]["gross"] == 1250
    assert projection["weekly"]["net"] > 0
    assert projection["tax"]["federal_annual"] > 0
    assert projection["tax"]["state_rate"] == 5.19
    assert len(projection["schedule"]) == 4
    assert projection["schedule"][0]["start"] == "2026-05-02"
    assert projection["window"]["sessions"] == 20
    assert projection["scenarios"][1]["projected_balance"] == projection["totals"][
        "projected_balance"
    ]


def test_forward_pace_pdf_download(client):
    resp = client.post(
        "/forward-pace/pdf",
        json={"base_balance": 10000, "gross_payout": 500, "weeks": 2, "state": "TX"},
    )

    assert resp.status_code == 200
    assert resp.mimetype == "application/pdf"
    assert resp.data.startswith(b"%PDF")
    assert "attachment" in resp.headers["Content-Disposition"]


def test_forward_pace_build_projection_clamps_inputs():
    projection = build_projection({"weeks": 500, "payouts_per_week": 0, "buffer_rate": 500})

    assert projection["inputs"]["weeks"] == 104
    assert projection["inputs"]["payouts_per_week"] == 1
    assert projection["inputs"]["buffer_rate"] == 90


def test_forward_pace_date_window_prorates_partial_weeks():
    projection = build_projection(
        {
            "horizon_mode": "date",
            "start_date": "2026-08-13",
            "target_date": "2026-08-19",
            "gross_payout": 250,
            "payouts_per_week": 5,
            "state": "TX",
            "buffer_rate": 0,
        }
    )

    assert projection["window"]["calendar_days"] == 7
    assert projection["window"]["sessions"] == 5
    assert projection["window"]["equivalent_weeks"] == 1
    assert [row["sessions"] for row in projection["schedule"]] == [2, 3]
    assert all(row["partial"] for row in projection["schedule"])
    assert projection["totals"]["gross"] == projection["weekly"]["gross"]


def test_forward_pace_date_target_reports_required_pace_and_scenarios():
    projection = build_projection(
        {
            "horizon_mode": "date",
            "start_date": "2026-08-03",
            "target_date": "2026-08-14",
            "base_balance": 50000,
            "target_balance": 52000,
            "gross_payout": 250,
            "payouts_per_week": 5,
        }
    )

    assert projection["window"]["sessions"] == 10
    assert projection["target"]["active"] is True
    assert projection["target"]["required_daily"] == 200
    assert projection["target"]["required_weekly"] == 1000
    assert [row["multiplier"] for row in projection["scenarios"]] == [0.75, 1.0, 1.25]
    assert projection["scenarios"][1]["projected_balance"] == projection["totals"][
        "projected_balance"
    ]
    assert projection["target"]["projected_gap"] == round(
        projection["totals"]["projected_balance"] - 52000, 2
    )
    assert projection["target"]["weekly_adjustment"] == round(
        projection["target"]["required_weekly"] - projection["weekly"]["net"], 2
    )
    assert all(row["completion_date"] for row in projection["scenarios"])


def test_forward_pace_api_rejects_reversed_date_window(client):
    resp = client.post(
        "/api/forward-pace/projection",
        json={
            "horizon_mode": "date",
            "start_date": "2026-08-14",
            "target_date": "2026-08-03",
        },
    )

    assert resp.status_code == 400
    assert resp.get_json() == {
        "ok": False,
        "error": "Target date must be on or after the start date.",
    }


def test_forward_pace_date_window_rejects_weekend_only_range(client):
    resp = client.post(
        "/api/forward-pace/projection",
        json={
            "horizon_mode": "date",
            "start_date": "2026-08-15",
            "target_date": "2026-08-16",
        },
    )

    assert resp.status_code == 400
    assert "weekday" in resp.get_json()["error"]


def test_evaluation_lifecycle_uses_ten_percent_target():
    projection = build_projection(
        {
            "account_phase": "evaluation",
            "base_balance": 50000,
            "current_balance": 51500,
            "daily_profit": 250,
            "start_date": "2026-08-03",
            "target_date": "2026-08-28",
            "horizon_mode": "date",
        }
    )

    lifecycle = projection["lifecycle"]
    assert lifecycle["evaluation_target"] == 55000
    assert lifecycle["evaluation_remaining"] == 3500
    assert lifecycle["next_label"] == "Pass evaluation"
    assert lifecycle["next_date"] == "2026-08-21"
    assert lifecycle["recommendation"]["action"] == "continue"
    assert lifecycle["recommendation"]["required_balance"] == 55000
    assert lifecycle["recommendation"]["sessions_to_ready"] == 15
    assert [row["key"] for row in lifecycle["milestones"]] == ["start", "evaluation"]
    assert [row["label"] for row in lifecycle["steps"]] == [
        "Evaluation Started",
        "Current Balance",
        "Pass Evaluation",
    ]
    assert not {"buffer", "loss", "protected"} & {
        row["key"] for row in lifecycle["milestones"]
    }


def test_performance_lifecycle_locks_limit_and_protects_payout():
    projection = build_projection(
        {
            "account_phase": "performance",
            "base_balance": 50000,
            "current_balance": 55000,
            "daily_profit": 250,
            "performance_buffer": 52875,
            "current_loss_limit": 48500,
            "fixed_loss_limit": 50375,
            "safety_cushion": 1000,
            "proposed_payout": 2500,
            "start_date": "2026-08-03",
            "target_date": "2026-08-28",
            "horizon_mode": "date",
        }
    )

    lifecycle = projection["lifecycle"]
    assert lifecycle["buffer_reached"] is True
    assert lifecycle["loss_limit_state"] == "Fixed"
    assert lifecycle["applicable_loss_limit"] == 50375
    assert lifecycle["theoretical_capacity"] == 4625
    assert lifecycle["protected_capacity"] == 3625
    assert lifecycle["post_payout_balance"] == 52500
    assert lifecycle["post_payout_cushion"] == 2125
    assert lifecycle["payout_safe"] is True
    assert lifecycle["recommendation"]["action"] == "withdraw"
    assert lifecycle["recommendation"]["decision_amount"] == 2500
    assert lifecycle["recommendation"]["post_action_balance"] == 52500
    assert [row["key"] for row in lifecycle["milestones"]] == ["buffer", "loss", "protected"]
    assert [row["state"] for row in lifecycle["steps"]] == [
        "complete",
        "complete",
        "complete",
        "active",
    ]


def test_performance_prebuffer_uses_current_loss_limit():
    projection = build_projection(
        {
            "account_phase": "performance",
            "base_balance": 50000,
            "current_balance": 51000,
            "performance_buffer": 52875,
            "current_loss_limit": 48500,
            "fixed_loss_limit": 50375,
            "safety_cushion": 1000,
            "proposed_payout": 2000,
        }
    )

    lifecycle = projection["lifecycle"]
    assert lifecycle["buffer_reached"] is False
    assert lifecycle["applicable_loss_limit"] == 48500
    assert lifecycle["loss_limit_state"] == "Trailing / current"
    assert lifecycle["payout_safe"] is False
    assert lifecycle["recommendation"]["action"] == "wait"
    assert lifecycle["recommendation"]["required_balance"] == 53375
    assert [row["state"] for row in lifecycle["steps"]] == [
        "complete",
        "active",
        "locked",
        "locked",
    ]


def test_performance_desired_payout_drives_required_work_and_date():
    projection = build_projection(
        {
            "account_phase": "performance",
            "base_balance": 50000,
            "current_balance": 50000,
            "daily_profit": 500,
            "performance_buffer": 52875,
            "current_loss_limit": 48500,
            "fixed_loss_limit": 50375,
            "safety_cushion": 1000,
            "proposed_payout": 10000,
            "start_date": "2026-09-07",
            "target_date": "2026-11-04",
            "horizon_mode": "date",
        }
    )

    recommendation = projection["lifecycle"]["recommendation"]
    assert recommendation["action"] == "wait"
    assert recommendation["decision_amount"] == 10000
    assert recommendation["required_balance"] == 61375
    assert recommendation["additional_profit"] == 11375
    assert recommendation["sessions_to_ready"] == 23
    assert recommendation["ready_date"] == "2026-10-07"
    assert recommendation["post_action_balance"] == 51375
