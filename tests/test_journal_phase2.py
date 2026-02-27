"""Phase 2 journal workflow tests."""

from mccain_capital.repositories import journal as repo
from mccain_capital.runtime import db, now_iso


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
        "/new",
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
