"""Phase 2 journal workflow tests."""

from io import BytesIO

from werkzeug.datastructures import MultiDict

from mccain_capital.repositories import journal as repo
from mccain_capital.runtime import db, now_iso
from mccain_capital.services import journal as journal_service


def test_journal_entry_type_and_linking(app):
    entry_id = repo.create_entry(
        {
            "entry_date": "2026-02-20",
            "market": "SPX",
            "setup": "ORB",
            "grade": "A",
            "pnl": 120.0,
            "mood": "Calm",
            "notes": "Good discipline",
            "entry_type": "trade_debrief",
            "template_payload": {"template_notes": "Stick to size"},
        }
    )

    row = repo.get_entry(entry_id)
    assert row is not None
    assert row["entry_type"] == "trade_debrief"
    assert "template_notes" in (row["template_payload"] or "")

    with db() as conn:
        created = now_iso()
        cur = conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm, gross_pl,
                net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-20",
                "9:35 AM",
                "9:50 AM",
                "SPX",
                "CALL",
                5000.0,
                1.0,
                1.2,
                1,
                100.0,
                1.0,
                21.0,
                20.0,
                20.0,
                50020.0,
                "seed",
                created,
            ),
        )
        trade_id = int(cur.lastrowid)

    repo.set_entry_trade_links(entry_id, [trade_id, trade_id])
    linked = repo.fetch_entry_trade_ids(entry_id)
    assert linked == [trade_id]


def test_weekly_review_route_and_rule_break_aggregation(client):
    repo.ensure_journal_schema()
    with db() as conn:
        created = now_iso()
        conn.execute(
            """
            INSERT INTO entries (
                entry_date, market, setup, grade, pnl, mood, notes, entry_type, template_payload, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2026-02-16",
                "SPX",
                "ORB",
                "B",
                -50.0,
                "Anxious",
                "Chased move",
                "post_market",
                "{}",
                created,
                created,
            ),
        )
        entry_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm, gross_pl,
                net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-16",
                "10:00 AM",
                "10:20 AM",
                "SPX",
                "PUT",
                5000.0,
                1.0,
                0.8,
                1,
                100.0,
                1.0,
                -19.0,
                -20.0,
                -20.0,
                49980.0,
                "seed",
                created,
            ),
        )
        trade_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

        conn.execute(
            """
            INSERT INTO trade_reviews (
                trade_id, setup_tag, session_tag, checklist_score, rule_break_tags, review_note, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (trade_id, "Fade", "Open", 40, "revenge, oversized", "seed", created, created),
        )
        conn.execute(
            "INSERT INTO entry_trade_links (entry_id, trade_id, created_at) VALUES (?, ?, ?)",
            (entry_id, trade_id, created),
        )

    resp = client.get("/journal/review/weekly?week_start=2026-02-16", follow_redirects=True)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Repeated Rule Breaks" in body
    assert "revenge" in body
    assert "Weekly Coach" in body
    assert "One Rule" in body


def test_new_entry_can_auto_link_all_trades_for_day(client):
    repo.ensure_journal_schema()
    with db() as conn:
        created = now_iso()
        for tm, net in [("9:35 AM", 45.0), ("11:10 AM", -20.0)]:
            conn.execute(
                """
                INSERT INTO trades (
                    trade_date, entry_time, exit_time, ticker, opt_type, strike,
                    entry_price, exit_price, contracts, total_spent, comm, gross_pl,
                    net_pl, result_pct, balance, raw_line, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "2026-02-23",
                    tm,
                    "11:30 AM",
                    "SPX",
                    "CALL",
                    5000.0,
                    1.0,
                    1.2,
                    1,
                    100.0,
                    1.0,
                    net + 1.0,
                    net,
                    0.0,
                    50000.0 + net,
                    "seed",
                    created,
                ),
            )

    resp = client.post(
        "/journal/new",
        data={
            "entry_date": "2026-02-23",
            "market": "SPX",
            "setup": "PM review",
            "grade": "B",
            "mood": "Calm",
            "pnl": "25",
            "entry_type": "post_market",
            "link_all_day": "1",
            "template_notes": "full day summary",
            "notes": "Link full trading day",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302

    entries = repo.fetch_entries(d="2026-02-23")
    assert len(entries) == 1
    entry_id = int(entries[0]["id"])
    linked = repo.fetch_entry_trade_ids(entry_id)
    assert len(linked) == 2


def test_journal_feed_prefers_saved_lesson_field(client):
    repo.ensure_journal_schema()
    resp = client.post(
        "/journal/new",
        data={
            "entry_date": "2026-04-08",
            "market": "SPX / VIX 21.19",
            "setup": "Context Review",
            "grade": "A",
            "mood": "Calm",
            "pnl": "813.00",
            "entry_type": "trade_debrief",
            "lesson": "Wait for confirmation before pressing size.",
            "template_notes": "Planned levels and invalidation.",
            "notes": "What I saw:\nSeller pressure faded.\n\nWhat I did:\nStayed patient.\n\nNext time:\nPress only after confirmation.",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Lesson" in body
    assert "Wait for confirmation before pressing size." in body


def test_legacy_new_entry_route_still_works(client):
    resp = client.get("/new", follow_redirects=False)
    assert resp.status_code == 200


def test_plain_new_entry_skips_heavy_market_context(client, monkeypatch):
    with db() as conn:
        created = now_iso()
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm, gross_pl,
                net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-03-18",
                "9:35 AM",
                "9:50 AM",
                "SPX",
                "CALL",
                5000.0,
                1.0,
                1.2,
                1,
                100.0,
                1.0,
                21.0,
                20.0,
                20.0,
                50020.0,
                "seed",
                created,
            ),
        )

    monkeypatch.setattr(
        journal_service,
        "_debrief_market_context",
        lambda _entry_date: (_ for _ in ()).throw(AssertionError("plain new entry should not load market context")),
    )

    resp = client.get("/journal/new?d=2026-03-18", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Capture the Session Cleanly" in body
    assert "Auto Debrief Draft" not in body
    assert "Showing 1 trade for <b>2026-03-18</b>." in body


def test_journal_trades_for_date_endpoint(client):
    with db() as conn:
        created = now_iso()
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm, gross_pl,
                net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-18",
                "9:40 AM",
                "10:00 AM",
                "SPX",
                "CALL",
                5000.0,
                1.0,
                1.3,
                1,
                100.0,
                1.0,
                31.0,
                30.0,
                30.0,
                50030.0,
                "seed",
                created,
            ),
        )

    resp = client.get("/journal/trades-for-date?d=2026-02-18", follow_redirects=True)
    assert resp.status_code == 200
    payload = resp.get_json()
    assert isinstance(payload, dict)
    assert len(payload.get("trades", [])) == 1
    assert payload["trades"][0]["trade_date"] == "2026-02-18"


def test_new_entry_prefill_query_params_render(client):
    resp = client.get(
        "/new?d=2026-02-23&entry_type=trade_debrief&link_all_day=1&setup=Session+Replay+Debrief&grade=TBD&pnl=120.50&notes=Replay+note",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Session Replay Debrief" in body
    assert "Replay note" in body
    assert "trade_debrief" in body


def test_debrief_market_context_falls_back_to_vix_quote(monkeypatch):
    from mccain_capital.services import core as core_service
    from mccain_capital.services import gamma_map_service
    from mccain_capital.services import market_data_service

    monkeypatch.setattr(
        core_service,
        "_market_pulse_snapshot",
        lambda: {
            "quotes": [
                {"label": "SPX", "price": 6123.45},
                {"label": "VIX", "price": None},
            ]
        },
    )
    monkeypatch.setattr(core_service, "_market_pulse_enrich_quotes", lambda quotes, _now: quotes)
    monkeypatch.setattr(core_service, "_market_pulse_context", lambda _quotes: {"structure": "Responsive"})
    monkeypatch.setattr(core_service, "_market_news_snapshot", lambda: {"macro_events": []})
    monkeypatch.setattr(gamma_map_service, "get_gamma_snapshot", lambda: {})
    monkeypatch.setattr(
        market_data_service,
        "get_watchlist_with_fallback",
        lambda symbols: {
            "VIX": {
                "price": 19.40,
                "provider": "yfinance",
                "reason": "yfinance_fallback",
            }
        },
    )

    payload = journal_service._debrief_market_context("2026-03-20")

    assert "VIX 19.40" in payload["headline"]
    assert payload["market"] == "SPX / VIX 19.40"


def test_new_entry_auto_debrief_draft_prefills_richer_context(client, monkeypatch):
    repo.ensure_journal_schema()
    monkeypatch.setattr(
        journal_service,
        "_debrief_market_context",
        lambda _day: {
            "headline": "SPX 6123.45 · Responsive · VIX 19.40",
            "levels": "Put wall 6100 · Gamma flip 6125 · Call wall 6150",
            "macro": "CPI (8:30 AM ET) / FOMC Minutes (2:00 PM ET)",
            "market": "SPX / VIX 19.40",
        },
    )

    with db() as conn:
        created = now_iso()
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm, gross_pl,
                net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-24",
                "9:35 AM",
                "9:48 AM",
                "SPX",
                "CALL",
                5000.0,
                1.0,
                1.4,
                1,
                100.0,
                1.0,
                39.0,
                38.0,
                38.0,
                50038.0,
                "seed",
                created,
            ),
        )
        trade_one = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm, gross_pl,
                net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "2026-02-24",
                "10:22 AM",
                "10:38 AM",
                "SPX",
                "PUT",
                5000.0,
                1.1,
                0.8,
                1,
                110.0,
                1.0,
                -31.0,
                -32.0,
                -29.09,
                50006.0,
                "seed",
                created,
            ),
        )
        trade_two = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.execute(
            """
            INSERT INTO trade_reviews (
                trade_id, setup_tag, strategy_label, session_tag, checklist_score,
                rule_break_tags, review_note, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade_one,
                "Continuation",
                "Trend Continuation",
                "Open Drive",
                88,
                "",
                "Good confirmation.",
                created,
                created,
            ),
        )
        conn.execute(
            """
            INSERT INTO trade_reviews (
                trade_id, setup_tag, strategy_label, session_tag, checklist_score,
                rule_break_tags, review_note, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade_two,
                "Fade",
                "Fade Reversal",
                "Midday",
                42,
                "early entry",
                "Forced the reversal.",
                created,
                created,
            ),
        )

    resp = client.get(
        "/journal/new?d=2026-02-24&entry_type=trade_debrief&link_all_day=1&auto_draft=1",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Auto Debrief Draft" in body
    assert "Session scorecard:" in body
    assert "Market context:" in body
    assert "Execution read:" in body
    assert "Prompt yourself:" in body
    assert "SPX 6123.45" in body
    assert "Put wall 6100" in body
    assert "CPI (8:30 AM ET)" in body
    assert "Trend Continuation" in body
    assert "SPX / VIX 19.40" in body
    assert "Confident but controlled" in body


def test_quick_capture_upload_saves_screenshot_and_serves_asset(client):
    resp = client.post(
        "/journal/new",
        data={
            "entry_date": "2026-02-25",
            "market": "SPX",
            "setup": "Quick Capture",
            "grade": "B",
            "mood": "Focused",
            "pnl": "",
            "entry_type": "trade_debrief",
            "quick_capture": "1",
            "template_notes": "",
            "notes": "Phone note after entry.",
            "capture_screenshot": (BytesIO(b"\x89PNG\r\n\x1a\nfake"), "capture.png"),
        },
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    assert resp.status_code == 302

    entries = repo.fetch_entries(d="2026-02-25")
    assert len(entries) == 1
    row = repo.get_entry(int(entries[0]["id"]))
    payload = journal_service._safe_template_payload(row["template_payload"])
    screenshot_path = str(payload.get("capture_screenshot_path") or "")
    assert screenshot_path.startswith("journal-captures/2026-02-25/")

    asset = client.get(f"/journal/captures/{screenshot_path}", follow_redirects=True)
    assert asset.status_code == 200


def test_quick_capture_upload_saves_multiple_images(client):
    payload = MultiDict(
        [
            ("entry_date", "2026-02-26"),
            ("market", "SPX"),
            ("setup", "Quick Capture"),
            ("grade", "B"),
            ("mood", "Focused"),
            ("pnl", ""),
            ("entry_type", "trade_debrief"),
            ("quick_capture", "1"),
            ("template_notes", ""),
            ("notes", "Two-image note."),
            ("capture_screenshot", (BytesIO(b"\x89PNG\r\n\x1a\nfake-a"), "capture-a.png")),
            ("capture_screenshot", (BytesIO(b"\x89PNG\r\n\x1a\nfake-b"), "capture-b.png")),
        ]
    )
    resp = client.post(
        "/journal/new",
        data=payload,
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    assert resp.status_code == 302

    entries = repo.fetch_entries(d="2026-02-26")
    assert len(entries) == 1
    row = repo.get_entry(int(entries[0]["id"]))
    payload = journal_service._safe_template_payload(row["template_payload"])
    screenshot_paths = list(payload.get("capture_screenshots") or [])
    assert len(screenshot_paths) == 2
    assert screenshot_paths[0].startswith("journal-captures/2026-02-26/")
    assert screenshot_paths[1].startswith("journal-captures/2026-02-26/")
    assert payload.get("capture_screenshot_path") == screenshot_paths[0]

    first_asset = client.get(f"/journal/captures/{screenshot_paths[0]}", follow_redirects=True)
    second_asset = client.get(f"/journal/captures/{screenshot_paths[1]}", follow_redirects=True)
    assert first_asset.status_code == 200
    assert second_asset.status_code == 200
