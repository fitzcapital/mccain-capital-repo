"""Books domain service functions."""

from __future__ import annotations

import os

from flask import abort, render_template, send_file

from mccain_capital.repositories import books as repo
from mccain_capital import runtime as app_runtime
from mccain_capital.services.ui import render_page


def books_page():
    books = repo.list_books()
    shelf_meta = {
        "Trading in the Zone -  Mark Douglas.pdf": {
            "title": "Trading in the Zone",
            "author": "Mark Douglas",
            "focus": "Mindset, probabilities, and execution freedom",
            "why": "Best for resetting your internal state before the open and staying process-first after a streak.",
            "quote": "Think in terms of probabilities.",
            "lane": "Mindset anchor",
            "tone": "mindset",
        },
        "The Disciplined Trader Developing Winning Attitudes  - Mark Douglas.pdf": {
            "title": "The Disciplined Trader",
            "author": "Mark Douglas",
            "focus": "Self-discipline and rule-based behavior",
            "why": "Use this when you need to tighten your structure, especially after good days that can loosen discipline.",
            "quote": "Make up your own rules and then have the discipline to abide by them.",
            "lane": "Discipline builder",
            "tone": "discipline",
        },
        "Best Loser Wins Why Normal Thinking Never Wins the Trading Game - Tom Hougaard.pdf": {
            "title": "Best Loser Wins",
            "author": "Tom Hougaard",
            "focus": "Loss tolerance, aggression, and emotional control",
            "why": "Sharpest read for accepting risk cleanly and not flinching when the setup is still valid.",
            "quote": "The best loser wins.",
            "lane": "Execution edge",
            "tone": "aggression",
        },
        "A-Complete-Guide-To-Volume-Price-Analysis-PDF-Book-Images.pdf": {
            "title": "A Complete Guide to Volume Price Analysis",
            "author": "Anna Coulling",
            "focus": "Volume, context, and price confirmation",
            "why": "Good reference when you want more context behind candles instead of trading structure blindly.",
            "quote": "",
            "lane": "Context read",
            "tone": "context",
        },
        "How to Trade the Highest Probability Opportunities.pdf": {
            "title": "How to Trade the Highest Probability Opportunities",
            "author": "Execution Playbook",
            "focus": "Setup quality and selective trade entry",
            "why": "Best for filtering the board and forcing yourself to wait for cleaner locations.",
            "quote": "",
            "lane": "Setup filter",
            "tone": "precision",
        },
        "The Strat.pdf": {
            "title": "The Strat",
            "author": "Rob Smith framework",
            "focus": "Timeframe continuity and scenario structure",
            "why": "Use this for fast pattern language, scenario alignment, and cleaner trigger framing.",
            "quote": "",
            "lane": "Pattern engine",
            "tone": "structure",
        },
    }
    tone_map = {
        "mindset": "tradeValueBubble tradeValueBubbleGain",
        "discipline": "tradeValueBubble tradeValueBubbleRisk",
        "aggression": "tradeValueBubble tradeValueBubbleWarm",
        "context": "tradeValueBubble tradeValueBubbleInfo",
        "precision": "tradeValueBubble tradeValueBubbleFlat",
        "structure": "tradeValueBubble tradeValueBubbleInfo",
    }
    enriched = []
    for book in books:
        meta = shelf_meta.get(book["name"], {})
        size_bytes = 0
        try:
            size_bytes = os.path.getsize(book["path"])
        except OSError:
            size_bytes = 0
        size_mb = (size_bytes / (1024 * 1024)) if size_bytes else 0.0
        title = str(meta.get("title") or book["name"].rsplit(".", 1)[0])
        enriched.append(
            {
                **book,
                "title": title,
                "author": str(meta.get("author") or "Trading Reference"),
                "focus": str(meta.get("focus") or "Reference PDF"),
                "why": str(
                    meta.get("why") or "Keep as a quick reference inside your private library."
                ),
                "quote": str(meta.get("quote") or ""),
                "lane": str(meta.get("lane") or "Reference"),
                "tone_class": tone_map.get(
                    str(meta.get("tone") or ""), "tradeValueBubble tradeValueBubbleFlat"
                ),
                "size_label": f"{size_mb:.1f} MB" if size_mb else "PDF",
            }
        )
    featured = next(
        (b for b in enriched if b["name"] == "Trading in the Zone -  Mark Douglas.pdf"),
        enriched[0] if enriched else None,
    )
    content = render_template(
        "books/index.html",
        books=enriched,
        books_dir=app_runtime.books_root(),
        featured=featured,
    )
    return render_page(content, active="books")


def books_open(name: str):
    fn = repo.safe_filename(name)
    path = os.path.join(app_runtime.books_root(), fn)
    if not os.path.exists(path) or not fn.lower().endswith(".pdf"):
        abort(404)
    return send_file(path, as_attachment=False)
