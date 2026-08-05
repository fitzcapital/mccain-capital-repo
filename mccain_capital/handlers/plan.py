"""The Plan endpoint handlers."""

from mccain_capital.services import plan as svc


def the_plan_page():
    return svc.the_plan_page()
