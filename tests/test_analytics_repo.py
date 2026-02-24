"""Analytics repository metric tests."""

from mccain_capital.repositories import analytics as repo
from mccain_capital.runtime import db, now_iso


def _seed_trades():
    rows = [
        ("2026-01-02", "9:35 AM", 100.0, 50100.0, "ORB", "Open", 88, ""),
        ("2026-01-02", "10:20 AM", -50.0, 50050.0, "Fade", "Open", 62, "late-entry"),
        ("2026-01-03", "11:10 AM", 200.0, 50250.0, "ORB", "Midday", 91, ""),
        ("2026-01-03", "2:40 PM", -25.0, 50225.0, "Scalp", "Power Hour", 55, "oversized"),
        ("2026-01-04", "3:05 PM", 0.0, 50225.0, "ORB", "Power Hour", 70, ""),
    ]
    with db() as conn:
        for i, r in enumerate(rows, start=1):
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
                    r[0],
                    r[1],
                    "",
                    "SPX",
                    "CALL",
                    5000.0,
                    1.0,
                    1.0,
                    1,
                    100.0,
                    1.0,
                    r[2] + 1.0,
                    r[2],
                    0.0,
                    r[3],
                    "seed",
                    created,
                ),
            )
            conn.execute(
                """
                INSERT INTO trade_reviews (
                    trade_id, setup_tag, session_tag, checklist_score, rule_break_tags, review_note, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (i, r[4], r[5], r[6], r[7], "seed", created, created),
            )


def test_performance_metrics(app):
    _seed_trades()
    rows = repo.fetch_analytics_rows()
    metrics = repo.performance_metrics(rows)

    assert metrics["total_trades"] == 5
    assert metrics["wins"] == 2
    assert metrics["losses"] == 2
    assert metrics["breakeven"] == 1
    assert metrics["profit_factor"] == 4.0
    assert metrics["expectancy"] == 45.0
    assert metrics["max_drawdown"] == 50.0
    assert metrics["max_win_streak"] == 1
    assert metrics["max_loss_streak"] == 1


def test_group_tables_and_rule_breaks(app):
    _seed_trades()
    rows = repo.fetch_analytics_rows()

    setup = repo.group_table(rows, "setup_tag")
    by_setup = {r["k"]: r for r in setup}
    assert by_setup["ORB"]["count"] == 3

    hours = repo.hour_bucket_table(rows)
    assert any(r["k"] == "09:00" for r in hours)

    breaks = repo.rule_break_counts(rows)
    by_tag = {r["tag"]: r["count"] for r in breaks}
    assert by_tag["late-entry"] == 1
    assert by_tag["oversized"] == 1
