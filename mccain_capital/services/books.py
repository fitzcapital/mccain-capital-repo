"""Books domain service functions."""

from __future__ import annotations

import os

from flask import abort, render_template_string, send_file

from mccain_capital import app_core as core
from mccain_capital.repositories import books as repo


def books_page():
    books = repo.list_books()
    content = render_template_string(
        """
        <div class="twoCol">
          <div class="card"><div class="toolbar">
            <div class="pill">📚 Trading Books</div>
            <div class="tiny" style="margin-top:10px; line-height:1.6">
              No web uploading. Drop PDFs into the <b>{{ books_dir }}</b> folder and refresh. ✅<br>
              Path example: <span class="kbd">./books</span>
            </div>
            <div class="hr"></div>
            <div class="rightActions">
              <a class="btn" href="/dashboard">📊 Calendar</a>
              <a class="btn" href="/links">🔗 Links</a>
            </div>
          </div></div>

          <div class="card"><div class="toolbar">
            <div class="pill">⭐ Current Favorites</div>
            <div class="tiny" style="margin-top:10px; line-height:1.6">
              • Trading in the Zone — Mark Douglas<br>
              • The Disciplined Trader — Mark Douglas<br>
              • Best Loser Wins — Tom Hougaard
            </div>
          </div></div>
        </div>

        <div class="card" style="margin-top:12px"><div class="toolbar">
          <div class="pill">📄 Library ({{ books|length }})</div>
          <div class="hr"></div>

          {% if books|length == 0 %}
            <div class="meta">No PDFs found in <b>{{ books_dir }}</b>.</div>
          {% else %}
            <div style="display:grid; gap:10px">
              {% for b in books %}
                <div class="card" style="border-radius:14px">
                  <div class="toolbar" style="display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap">
                    <div><b>{{ b.name }}</b></div>
                    <div class="rightActions">
                      <a class="btn primary" href="/books/open/{{ b.name }}">Open</a>
                    </div>
                  </div>
                </div>
              {% endfor %}
            </div>
          {% endif %}
        </div></div>
        """,
        books=books,
        books_dir=core.BOOKS_DIR,
    )
    return core.render_page(content, active="books")


def books_open(name: str):
    fn = repo.safe_filename(name)
    path = os.path.join(core.BOOKS_DIR, fn)
    if not os.path.exists(path) or not fn.lower().endswith(".pdf"):
        abort(404)
    return send_file(path, as_attachment=False)
