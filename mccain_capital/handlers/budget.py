"""Budget Command Center endpoint handlers."""

from mccain_capital.services import budget as svc


def budget_page():
    return svc.budget_page()


def api_summary():
    return svc.api_summary()


def api_data():
    return svc.api_data()


def api_profile():
    return svc.api_profile()


def api_income():
    return svc.api_upsert_income()


def api_delete_income(item_id):
    return svc.api_delete_income(item_id)


def api_bill():
    return svc.api_upsert_bill()


def api_delete_bill(item_id):
    return svc.api_delete_bill(item_id)


def api_charge():
    return svc.api_upsert_charge()


def api_delete_charge(item_id):
    return svc.api_delete_charge(item_id)


def api_debt():
    return svc.api_upsert_debt()


def api_delete_debt(item_id):
    return svc.api_delete_debt(item_id)


def api_goal():
    return svc.api_upsert_goal()


def api_delete_goal(item_id):
    return svc.api_delete_goal(item_id)


def api_analytics():
    return svc.api_analytics()


def api_monthly_review():
    return svc.api_monthly_review()
