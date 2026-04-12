"""Playbook doctrine page service."""

from __future__ import annotations

from flask import render_template

from mccain_capital.services.ui import render_page


def playbook_page():
    summary_cards = [
        {
            "label": "Mindset",
            "tone": "gold",
            "body": "Think in probabilities. Accept uncertainty. Stay emotionally neutral.",
        },
        {
            "label": "Execution",
            "tone": "green",
            "body": "Take defined setups. Define risk first. Manage without drama.",
        },
        {
            "label": "Market Reading",
            "tone": "blue",
            "body": "Read structure, trend, levels, and bar behavior with clarity.",
        },
    ]

    doctrine_sections = [
        {
            "id": "mindset",
            "eyebrow": "Mindset Doctrine",
            "title": "Trade the series. Protect mental capital.",
            "intro": "Psychology is not separate from performance. It determines whether edge survives pressure.",
            "tone": "gold",
            "blocks": [
                {
                    "title": "Core Beliefs",
                    "kind": "bullets",
                    "items": [
                        "I do not need to know what happens next.",
                        "Anything can happen.",
                        "Every trade is unique.",
                        "My edge plays out over a series, not one trade.",
                        "Losses are information, not identity.",
                    ],
                },
                {
                    "title": "Pre-Trade Mindset Check",
                    "kind": "checklist",
                    "items": [
                        "Am I calm?",
                        "Am I forcing action?",
                        "Am I trying to make money fast?",
                        "Am I following process or emotion?",
                    ],
                },
                {
                    "title": "Emotional Warning Signs",
                    "kind": "signals",
                    "items": ["Revenge", "FOMO", "Hesitation", "Oversizing", "Needing to be right"],
                },
                {
                    "title": "Reset Protocol",
                    "kind": "steps",
                    "items": [
                        "Step back",
                        "Breathe",
                        "Re-state setup",
                        "Re-state risk",
                        "Skip if unclear",
                    ],
                },
                {
                    "title": "Quotes / Principles",
                    "kind": "quotes",
                    "items": [
                        "Best loser wins.",
                        "Protect mental capital before you protect ego.",
                        "Confidence comes from process, not prediction.",
                    ],
                },
            ],
        },
        {
            "id": "execution",
            "eyebrow": "Execution Doctrine",
            "title": "Define the trade before the trade defines you.",
            "intro": "Execution quality comes from eliminating drama before entry and enforcing structure during management.",
            "tone": "green",
            "blocks": [
                {
                    "title": "Setup Filter",
                    "kind": "bullets",
                    "items": [
                        "Clear structure",
                        "Defined invalidation",
                        "Acceptable spread and liquidity",
                        "Room to target",
                        "Matches playbook criteria",
                    ],
                },
                {
                    "title": "Entry Checklist",
                    "kind": "checklist",
                    "items": [
                        "Trend confirmed?",
                        "Key level identified?",
                        "Trigger present?",
                        "Risk defined before entry?",
                        "Position size correct?",
                    ],
                },
                {
                    "title": "Risk Rules",
                    "kind": "signals",
                    "items": [
                        "No undefined risk",
                        "No oversized entries",
                        "No averaging losers",
                        "No emotional add-ons",
                        "Stop belongs where thesis breaks",
                    ],
                },
                {
                    "title": "In-Trade Management",
                    "kind": "steps",
                    "items": [
                        "Manage around structure, not noise.",
                        "Reduce only for thesis damage, not discomfort.",
                        "Do not convert a trade into an opinion.",
                        "If the level fails, act immediately.",
                    ],
                },
                {
                    "title": "Post-Trade Review Prompts",
                    "kind": "quotes",
                    "items": [
                        "Did I follow process?",
                        "Did I manage risk correctly?",
                        "Did I break any rule?",
                        "Was the loss acceptable?",
                        "What will I repeat or stop?",
                    ],
                },
            ],
            "quick_tiles": [
                "Is this my setup?",
                "Is risk defined?",
                "Am I early?",
                "Am I chasing?",
                "What invalidates the trade?",
                "Can I accept the loss right now?",
            ],
        },
        {
            "id": "market-reading",
            "eyebrow": "Market Reading Doctrine",
            "title": "Read price with structure first, pattern second.",
            "intro": "The chart is not a collection of candles. It is a structure map with context, pressure, and timing.",
            "tone": "blue",
            "blocks": [
                {
                    "title": "Trend Identification",
                    "kind": "bullets",
                    "items": [
                        "Higher highs plus higher lows = uptrend",
                        "Lower highs plus lower lows = downtrend",
                        "Overlapping structure = chop",
                    ],
                },
                {
                    "title": "Key Levels",
                    "kind": "bullets",
                    "items": [
                        "Prior highs and lows",
                        "Support and resistance",
                        "Reaction zones",
                        "Breakdown and breakout pivots",
                    ],
                },
                {
                    "title": "Bar-by-Bar Clues",
                    "kind": "signals",
                    "items": [
                        "Rejection",
                        "Expansion",
                        "Compression",
                        "Failed follow-through",
                        "Strong close vs weak close",
                    ],
                },
                {
                    "title": "Pattern Context",
                    "kind": "steps",
                    "items": [
                        "A pattern only matters if location matters.",
                        "Breakouts need space and participation.",
                        "Reversals need rejection and proof.",
                        "Sideways structure reduces edge.",
                    ],
                },
                {
                    "title": "Timeframe Alignment",
                    "kind": "quotes",
                    "items": [
                        "Higher timeframe bias sets direction.",
                        "Execution timeframe provides the trigger.",
                        "Lower timeframe noise should not override structure.",
                    ],
                },
            ],
        },
    ]

    sidebar = {
        "reminder": "Your job is not to predict. Your job is to execute well.",
        "non_negotiables": [
            "Define risk first",
            "Never chase",
            "Respect invalidation",
            "Stay size-disciplined",
            "Review everything",
        ],
        "before_click": [
            "Is the trend clear?",
            "Is risk defined?",
            "Is this A+?",
            "Am I emotionally neutral?",
            "Can I accept the stop?",
        ],
        "jump_links": [
            {"href": "#mindset", "label": "Mindset"},
            {"href": "#execution", "label": "Execution"},
            {"href": "#market-reading", "label": "Market Reading"},
        ],
    }

    reflection_prompts = [
        "Where did I follow doctrine cleanly today?",
        "Where did emotion try to override structure?",
        "What rule protected me?",
        "What behavior needs to tighten tomorrow?",
    ]

    content = render_template(
        "playbook.html",
        summary_cards=summary_cards,
        doctrine_sections=doctrine_sections,
        sidebar=sidebar,
        reflection_prompts=reflection_prompts,
    )
    return render_page(
        content,
        active="playbook",
        title="McCain Capital · Playbook",
    )
