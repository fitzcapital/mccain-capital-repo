from mccain_capital.services.trade_review_scoring import compute_trade_review_foundation


def _base_row():
    return {
        "setup_tag": "Sweep Reversal",
        "session_tag": "Open",
        "checklist_score": 82,
        "thesis_note": "Sweep into prior high with reclaim.",
        "review_note": "Good process.",
        "improvement_note": "Hold for cleaner extension.",
        "planned_risk_dollars": 220,
        "reviewed_stop_price": 730.10,
        "reviewed_target_price": 733.50,
        "entry_quality_note": "Waited for reclaim.",
        "exit_quality_note": "Scaled into target.",
        "execution_grade": 91,
        "risk_grade": 90,
        "plan_grade": 94,
        "reviewed_sizing_quality": "Proper",
        "reviewed_stop_discipline": "Within Risk Plan",
        "reviewed_within_plan": 1,
        "manual_grade_letter": "A",
        "classification_override": "Good Win",
        "net_pl": 99.30,
        "entry_price": 731.0,
        "exit_price": 732.4,
        "balance": 50000,
    }


def test_review_completion_breakdown_fully_reviewed():
    review = compute_trade_review_foundation(_base_row())

    assert review["review_state"]["label"] == "Fully Reviewed"
    assert review["review_completion_pct"] == 100
    assert review["review_missing_summary"] == "All core review checks logged."
    assert all(item["done"] for item in review["review_completion_items"])


def test_review_completion_breakdown_partial_missing_narrative_and_levels():
    row = _base_row()
    row["thesis_note"] = ""
    row["review_note"] = ""
    row["improvement_note"] = ""
    row["reviewed_stop_price"] = ""
    row["reviewed_target_price"] = ""
    row["entry_quality_note"] = ""
    row["exit_quality_note"] = ""
    row["execution_grade"] = ""

    review = compute_trade_review_foundation(row)

    assert review["review_state"]["label"] == "Partially Reviewed"
    missing = {item["key"] for item in review["review_completion_items"] if not item["done"]}
    assert {"thesis", "reflection", "target", "execution"} <= missing
    assert review["review_missing_summary"].startswith("Missing thesis + review note")


def test_review_completion_breakdown_not_reviewed_with_setup_only():
    row = {
        "setup_tag": "Sweep Reversal",
        "session_tag": "",
        "net_pl": 0.0,
        "balance": 50000,
    }

    review = compute_trade_review_foundation(row)

    assert review["review_state"]["label"] == "Not Reviewed"
    assert review["review_completion_pct"] == 30
    assert review["review_missing_summary"].startswith("Missing thesis + review note")
