"""Budget Command Center route registrations."""

from mccain_capital.handlers import budget as h


def register(app):
    app.add_url_rule("/budget", endpoint="budget_page", view_func=h.budget_page)
    app.add_url_rule("/api/budget/summary", endpoint="budget_summary", view_func=h.api_summary)
    app.add_url_rule("/api/budget/data", endpoint="budget_data", view_func=h.api_data)
    app.add_url_rule("/api/budget/analytics", endpoint="budget_analytics", view_func=h.api_analytics)
    app.add_url_rule(
        "/api/budget/profile", endpoint="budget_profile", view_func=h.api_profile, methods=["POST"]
    )
    app.add_url_rule("/api/budget/income", endpoint="budget_income", view_func=h.api_income, methods=["POST"])
    app.add_url_rule(
        "/api/budget/income/<item_id>",
        endpoint="budget_income_delete",
        view_func=h.api_delete_income,
        methods=["DELETE"],
    )
    app.add_url_rule("/api/budget/bill", endpoint="budget_bill", view_func=h.api_bill, methods=["POST"])
    app.add_url_rule(
        "/api/budget/bill/<item_id>",
        endpoint="budget_bill_delete",
        view_func=h.api_delete_bill,
        methods=["DELETE"],
    )
    app.add_url_rule("/api/budget/charge", endpoint="budget_charge", view_func=h.api_charge, methods=["POST"])
    app.add_url_rule(
        "/api/budget/charge/<item_id>",
        endpoint="budget_charge_delete",
        view_func=h.api_delete_charge,
        methods=["DELETE"],
    )
    app.add_url_rule("/api/budget/debt", endpoint="budget_debt", view_func=h.api_debt, methods=["POST"])
    app.add_url_rule(
        "/api/budget/debt/<item_id>",
        endpoint="budget_debt_delete",
        view_func=h.api_delete_debt,
        methods=["DELETE"],
    )
    app.add_url_rule("/api/budget/goal", endpoint="budget_goal", view_func=h.api_goal, methods=["POST"])
    app.add_url_rule(
        "/api/budget/goal/<item_id>",
        endpoint="budget_goal_delete",
        view_func=h.api_delete_goal,
        methods=["DELETE"],
    )
    app.add_url_rule(
        "/api/budget/monthly-review",
        endpoint="budget_monthly_review",
        view_func=h.api_monthly_review,
        methods=["POST"],
    )
