from mccain_capital import runtime
from mccain_capital.services import budget as budget_service


def test_budget_page_renders_and_nav_links(client, tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "PERSISTENT_DATA_DIR", str(tmp_path))

    resp = client.get("/budget", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Budget Command Center" in body
    assert "Month-by-month financial log" in body
    assert "Chase Credit Card — Locked / Paydown Only" in body
    assert "Paycheck 1: $188" in body
    assert "Consumer credit is not purchasing power. Treasury is purchasing power." in body
    assert "liabilities under management, not operating tools" in body
    assert "js/budget.js" in body
    assert 'href="/budget"' in body


def test_budget_profile_and_items_persist_and_summarize(client, tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "PERSISTENT_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(budget_service, "today_iso", lambda: "2026-04-29")

    profile = client.post(
        "/api/budget/profile",
        json={"monthly_take_home": 7200, "target_extra_monthly_income": 4000},
    )
    assert profile.status_code == 200

    income = client.post(
        "/api/budget/income",
        json={"name": "W-2 Paycheck", "type": "job", "amount": 3600, "frequency": "biweekly"},
    )
    assert income.status_code == 200

    bill = client.post(
        "/api/budget/bill",
        json={"name": "Rent", "category": "housing", "amount": 1800, "due_day": 1, "paid": True},
    )
    assert bill.status_code == 200

    charge = client.post(
        "/api/budget/charge",
        json={
            "date": "2026-04-29",
            "name": "Lunch",
            "category": "food",
            "amount": 25,
            "need_or_want": "want",
        },
    )
    assert charge.status_code == 200

    goal = client.post(
        "/api/budget/goal",
        json={"name": "Emergency Fund", "target_amount": 5000, "current_amount": 500, "monthly_contribution": 250},
    )
    assert goal.status_code == 200

    debt = client.post(
        "/api/budget/debt",
        json={"name": "Credit Card", "balance": 2500, "minimum_payment": 120, "interest_rate": 24},
    )
    assert debt.status_code == 200

    summary = client.get("/api/budget/summary").get_json()["summary"]
    assert summary["fixed_bills_total"] == 1800
    assert summary["variable_spending_total"] == 25
    assert summary["debt_minimums_total"] == 120
    assert summary["savings_goal_total"] == 250
    assert summary["top_spending_category"] == "food"
    assert summary["cashflow_health_score"] > 0

    data = client.get("/api/budget/data").get_json()["data"]
    assert data["income_sources"][0]["name"] == "W-2 Paycheck"
    assert data["charges"][0]["name"] == "Lunch"


def test_budget_delete_and_monthly_review(client, tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "PERSISTENT_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(budget_service, "today_iso", lambda: "2026-04-29")

    created = client.post("/api/budget/charge", json={"name": "Leak", "amount": 80, "need_or_want": "leak"})
    item_id = created.get_json()["item"]["id"]
    assert client.delete(f"/api/budget/charge/{item_id}").status_code == 200
    assert client.get("/api/budget/data").get_json()["data"]["charges"] == []

    review = client.post("/api/budget/monthly-review", json={})
    assert review.status_code == 200
    payload = review.get_json()
    assert payload["review"]["month"] == "2026-04"
    assert "cash_left" in payload["review"]


def test_budget_negative_cashflow_caps_score(client, tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "PERSISTENT_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(budget_service, "today_iso", lambda: "2026-04-29")
    client.post("/api/budget/profile", json={"monthly_take_home": 1000})
    client.post("/api/budget/bill", json={"name": "Rent", "amount": 1500, "due_day": 1})

    summary = client.get("/api/budget/summary").get_json()["summary"]

    assert summary["projected_cash_left"] < 0
    assert summary["cashflow_health_score"] <= 49


def test_budget_fitz_seed_math_and_paycheck_map(client, tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "PERSISTENT_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(budget_service, "today_iso", lambda: "2026-04-29")
    client.post(
        "/api/budget/profile",
        json={"monthly_take_home": 7269.66, "pay_frequency": "biweekly", "target_extra_monthly_income": 4000},
    )
    rows = [
        ("Rent 1st payment", 1320.00, 1, "housing", "bill", "any", ""),
        ("Rent 2nd payment", 1024.99, 15, "housing", "bill", "any", ""),
        ("Chase", 374.00, 29, "debt", "debt", "any", ""),
        ("AMEX", 172.00, 17, "debt", "debt", "any", ""),
        ("Power", 136.00, 2, "utilities", "bill", "any", ""),
        ("Verizon", 268.00, 26, "utilities", "bill", "any", ""),
        ("Concord", 60.00, 1, "debt", "debt", "any", ""),
        ("Capital One", 65.00, 12, "debt", "debt", "any", ""),
        ("Progressive", 208.00, 17, "insurance", "bill", "any", ""),
        ("ATT Internet", 66.00, 16, "utilities", "bill", "any", ""),
        ("Car note", 740.00, 23, "auto", "bill", "any", ""),
        ("Subscriptions", 120.00, None, "subscriptions", "subscription", "any", "monthly"),
        ("Food", 1200.00, None, "food", "food", "split", "$500/$500 set aside per pay period, plus buffer"),
        ("IRS", 380.00, 29, "debt", "debt", "any", ""),
        ("Renter Insurance", 30.99, 12, "insurance", "bill", "any", ""),
        ("Life Insurance", 14.00, 1, "insurance", "bill", "any", ""),
        ("Gas", 55.00, None, "gas", "gas", "any", "monthly"),
    ]
    for name, amount, due_day, category, item_type, allocation, notes in rows:
        response = client.post(
            "/api/budget/bill",
            json={
                "name": name,
                "amount": amount,
                "due_day": due_day,
                "category": category,
                "type": item_type,
                "paycheck_allocation": allocation,
                "notes": notes,
            },
        )
        assert response.status_code == 200

    duplicate = client.post(
        "/api/budget/bill",
        json={"name": "Gas", "amount": 55.00, "due_day": None, "category": "gas", "type": "gas", "notes": "monthly"},
    )
    assert duplicate.status_code == 200

    data = client.get("/api/budget/data").get_json()["data"]
    assert len(data["bills"]) == 17

    summary = client.get("/api/budget/summary").get_json()["summary"]
    assert summary["monthly_income"] == 7269.66
    assert summary["fixed_bills_total"] == 4978.98
    assert summary["planned_variable_total"] == 1255.00
    assert summary["total_planned_outflow"] == 6233.98
    assert summary["projected_cash_left"] == 1035.68
    assert summary["goal_allocation"] == {"buffer": 517.84, "debt_payoff": 310.7, "flex": 207.14}

    allocation = summary["paycheck_allocation"]
    first_names = {item["name"] for item in allocation["first_check"]["items"]}
    second_names = {item["name"] for item in allocation["second_check"]["items"]}
    flexible_names = {item["name"] for item in allocation["flexible"]["items"]}
    assert {"Rent 1st payment", "Power", "Concord", "Capital One", "Renter Insurance", "Life Insurance"} <= first_names
    assert {
        "Rent 2nd payment",
        "Chase",
        "AMEX",
        "Verizon",
        "Progressive",
        "ATT Internet",
        "Car note",
        "IRS",
    } <= second_names
    assert {"Subscriptions", "Food", "Gas"} <= flexible_names
    assert allocation["first_check"]["set_asides_total"] == 500
    assert allocation["second_check"]["set_asides_total"] == 500
