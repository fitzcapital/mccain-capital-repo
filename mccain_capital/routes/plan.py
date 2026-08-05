"""The Plan route registrations."""

from mccain_capital.handlers import plan as h


def register(app):
    app.add_url_rule("/the-plan", endpoint="the_plan_page", view_func=h.the_plan_page)
