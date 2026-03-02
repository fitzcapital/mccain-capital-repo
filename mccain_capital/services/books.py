"""Books domain service functions."""

from __future__ import annotations

import os

from flask import abort, render_template_string, send_file

from mccain_capital.repositories import books as repo
from mccain_capital.runtime import BOOKS_DIR
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
                "why": str(meta.get("why") or "Keep as a quick reference inside your private library."),
                "quote": str(meta.get("quote") or ""),
                "lane": str(meta.get("lane") or "Reference"),
                "tone_class": tone_map.get(str(meta.get("tone") or ""), "tradeValueBubble tradeValueBubbleFlat"),
                "size_label": f"{size_mb:.1f} MB" if size_mb else "PDF",
            }
        )
    featured = next(
        (b for b in enriched if b["name"] == "Trading in the Zone -  Mark Douglas.pdf"),
        enriched[0] if enriched else None,
    )
    content = render_template_string(
        """
        <div class="card pageHero">
          <div class="toolbar">
            <div class="pageHeroHead">
              <div>
                <div class="pill">📚 Books Desk</div>
                <h2 class="pageTitle">Private Trading Library</h2>
                <div class="pageSub">Mindset, discipline, structure, and execution reads built into your cockpit.</div>
              </div>
              <div class="actionRow">
                <a class="btn" href="/dashboard">Calendar</a>
                <a class="btn" href="/analytics">Analytics</a>
                <a class="btn" href="/trades/playbook">Playbook</a>
              </div>
            </div>
          </div>
        </div>

        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <div class="pill">🧠 Mindset Pull</div>
            {% if featured %}
              <div class="summaryLead" style="margin-top:10px">{{ featured.title }} · {{ featured.author }}</div>
              <div class="supportBody" style="margin-top:8px">
                Before the open, use this shelf to reset your state. The point is not more information. It is cleaner thinking under pressure.
              </div>
              {% if featured.quote %}
                <div class="entryQuote" style="margin-top:10px">“{{ featured.quote }}”</div>
              {% endif %}
              <div class="tiny line16" style="margin-top:10px">{{ featured.why }}</div>
              <div class="rightActions" style="margin-top:12px">
                <a class="btn primary" href="/books/open/{{ featured.name }}">Open {{ featured.title }}</a>
              </div>
            {% else %}
              <div class="meta">No PDFs found yet.</div>
            {% endif %}
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">🗂️ Library Notes</div>
            <div class="tiny" style="margin-top:10px; line-height:1.6">
              Drop PDFs into <b>{{ books_dir }}</b> and refresh.<br>
              Path example: <span class="kbd">./books</span>
            </div>
            <div class="hr"></div>
            <div class="stack8">
              <span class="tradeValueBubble tradeValueBubbleGain">Mindset shelf</span>
              <span class="tradeValueBubble tradeValueBubbleWarm">Execution references</span>
              <span class="tradeValueBubble tradeValueBubbleInfo">Pattern + volume context</span>
            </div>
          </div></div>
        </div>

        <div class="card" style="margin-top:12px"><div class="toolbar">
          <div class="pill">📄 Library ({{ books|length }})</div>
          <div class="hr"></div>

          {% if books|length == 0 %}
            <div class="meta">No PDFs found in <b>{{ books_dir }}</b>.</div>
          {% else %}
            <div class="stratGrid2">
              {% for b in books %}
                <div class="card" style="border-radius:16px">
                  <div class="toolbar">
                    <div class="showcaseHead">
                      <div>
                        <div class="supportLead">{{ b.title }}</div>
                        <div class="tiny line16">{{ b.author }}</div>
                      </div>
                      <span class="{{ b.tone_class }}">{{ b.lane }}</span>
                    </div>
                    <div class="supportBody" style="margin-top:10px">{{ b.focus }}</div>
                    <div class="tiny line16" style="margin-top:8px">{{ b.why }}</div>
                    {% if b.quote %}
                      <div class="entryQuote" style="margin-top:10px">“{{ b.quote }}”</div>
                    {% endif %}
                    <div class="trendChips" style="margin-top:10px">
                      <span class="trendChip">{{ b.size_label }}</span>
                    </div>
                    <div class="rightActions" style="margin-top:12px">
                      <a class="btn primary" href="/books/open/{{ b.name }}">Open</a>
                    </div>
                  </div>
                </div>
              {% endfor %}
            </div>
          {% endif %}
        </div></div>
        """,
        books=enriched,
        books_dir=BOOKS_DIR,
        featured=featured,
    )
    return render_page(content, active="books")


def books_open(name: str):
    fn = repo.safe_filename(name)
    path = os.path.join(BOOKS_DIR, fn)
    if not os.path.exists(path) or not fn.lower().endswith(".pdf"):
        abort(404)
    return send_file(path, as_attachment=False)
