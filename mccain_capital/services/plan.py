"""The Plan poster page."""

from __future__ import annotations

from flask import render_template

from mccain_capital.services.ui import render_page


def the_plan_page():
    content = render_template(
        "plan.html",
        account_goal={"start": "$50,000", "target": "$60,000"},
        account_objectives=[
            "Funded account: $50,000",
            "Max drawdown: 5% ($2,500)",
            "Static drawdown at: $52,850",
            "Goal: reach $60,000",
            "Position size: $1,000 until balance clears $49,800",
            "Max trades per day: 2",
            "Stop loss: 20% ($200)",
            "Take profit: 25% max ($250)",
            "Mindset: protect capital, execute process, stay consistent",
        ],
        risk_management={
            "per_trade": [("Risk", "$200 (20%)"), ("Reward", "$250 (25%)")],
            "weekly_limit": "-$800 to -$1,000",
            "defense": [
                ("Above $50,800", "Full size ($1,000)"),
                ("$49,800 - $50,800", "Caution (optional reduce / 1 trade)"),
                ("Below $49,800", "Defense mode (reduce size / stop)"),
            ],
        },
        setup_checklist=[
            "Liquidity sweep (high/low)",
            "Reclaim or failure",
            "STRAT trigger confirms",
            "Structure alignment (HTF/LTF)",
            "Gamma context supportive",
            "Volume / price confirmation",
            "Clean risk to reward",
        ],
        market_filters=[
            {
                "grade": "A",
                "items": [
                    "Strong directional bias",
                    "Clean reactions",
                    "Trend continuation",
                    "Positive gamma",
                    "High volume / liquidity",
                ],
                "action": "Full size",
                "detail": "Up to 2 trades",
                "tone": "green",
            },
            {
                "grade": "B",
                "items": [
                    "Mixed / slower market",
                    "Choppy at times",
                    "Moderate volatility",
                    "Unclear momentum",
                ],
                "action": "Use caution",
                "detail": "1 trade or small size",
                "tone": "gold",
            },
            {
                "grade": "C",
                "items": [
                    "FOMC / high impact news",
                    "OPEX chaos",
                    "Low volume",
                    "Heavy chop / fakeouts",
                ],
                "action": "No trade day",
                "detail": "Protect capital",
                "tone": "red",
            },
        ],
        execution_rules=[
            ("Only take A+ setups", "Patience is a position."),
            ("Max 2 trades per day", "Quality over quantity."),
            ("Stop is 20% always", "No exceptions. Ever."),
            ("Take profit at 25% max", "Take 70-80% off. Let the runner work."),
            ("One loss = consider done", "Do not force trade #2."),
        ],
        mindset_checks=[
            "Angry or frustrated",
            "Rushed or distracted",
            "Trying to make it back",
            "Sleep deprived",
            "Doubting the plan",
            "Emotionally off",
        ],
        weekly_projection=[
            ("Mon", "2", "Clean trend day", "Win +$250", "Win +$250", "+$500", "$50,500"),
            ("Tue", "2", "Choppy morning / better afternoon", "Loss -$200", "Win +$250", "+$50", "$50,550"),
            ("Wed", "1", "Perfect setup patience pays", "Win +$250", "No trade", "+$250", "$50,800"),
            ("Thu", "2", "Mixed market / harder tape", "Loss -$200", "Loss -$200", "-$400", "$50,400"),
            ("Fri", "1", "Strong move into close", "Win +$250", "No trade", "+$250", "$50,650"),
        ],
        weekly_outlook=[
            "8 trades taken",
            "5 wins / 3 losses",
            "Win rate: 62.5%",
            "Net: +$650",
            "Consistency compounds faster than prediction.",
        ],
        if_wrong=[
            "2 losses in a row: stop, reset, review",
            "Daily loss hit (-$400): done for the day",
            "Weekly loss hit (-$800 to -$1,000): reduce size or take 1-2 days off",
            "Below $49,800: defense mode, reduce size, stop trading if needed",
        ],
        journal_prompts=[
            "What was the setup?",
            "Why did I take it?",
            "What was my plan?",
            "How did I execute?",
            "What did I feel?",
            "What can I improve?",
        ],
        scaling_rule=[
            "Consistent for 2-3+ months",
            "Rule adherence above 90%",
            "Emotions controlled",
            "Drawdowns under control",
            "Journaling and review consistent",
        ],
        core_principles=[
            ("Protect capital", "You cannot trade without capital."),
            ("Be patient", "The market will still be here tomorrow."),
            ("Execute the plan", "Process over outcomes."),
            ("Compound consistency", "Small wins build freedom."),
        ],
    )
    return render_page(content, active="the-plan", title="McCain Capital · The Plan")
