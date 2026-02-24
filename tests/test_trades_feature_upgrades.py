"""Tests for open positions and rebuild reviews feature upgrades."""

from mccain_capital.runtime import db, now_iso


def _insert_trade(
    *,
    trade_date: str,
    ticker: str = "SPX",
    opt_type: str = "CALL",
    strike: float = 5000.0,
    entry_price: float = 1.0,
    exit_price=None,
    contracts: int = 1,
    total_spent: float = 100.0,
    comm: float = 1.0,
    gross_pl=None,
    net_pl=None,
    result_pct=None,
    entry_time: str = "9:35 AM",
    exit_time: str = "",
):
    with db() as conn:
        conn.execute(
            """
            INSERT INTO trades (
                trade_date, entry_time, exit_time, ticker, opt_type, strike,
                entry_price, exit_price, contracts, total_spent, comm,
                gross_pl, net_pl, result_pct, balance, raw_line, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                trade_date,
                entry_time,
                exit_time,
                ticker,
                opt_type,
                strike,
                entry_price,
                exit_price,
                contracts,
                total_spent,
                comm,
                gross_pl,
                net_pl,
                result_pct,
                50000.0,
                "seed",
                now_iso(),
            ),
        )
        row = conn.execute("SELECT last_insert_rowid() AS id").fetchone()
    return int(row["id"])


def test_open_positions_page_lists_incomplete_rows(client):
    _insert_trade(
        trade_date="2026-02-24",
        ticker="SPX",
        opt_type="CALL",
        strike=6000.0,
        exit_price=None,
        net_pl=None,
        contracts=2,
        total_spent=420.0,
        exit_time="",
    )
    _insert_trade(
        trade_date="2026-02-24",
        ticker="QQQ",
        opt_type="PUT",
        strike=500.0,
        exit_price=1.3,
        net_pl=30.0,
        contracts=1,
        total_spent=100.0,
        gross_pl=31.0,
        result_pct=30.0,
        exit_time="9:42 AM",
    )

    resp = client.get("/trades/open-positions", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Open Positions" in resp.data
    assert b"SPX CALL 6000" in resp.data
    assert b"QQQ PUT 500" not in resp.data


def test_rebuild_reviews_creates_missing_review(client):
    trade_id = _insert_trade(
        trade_date="2026-02-20",
        ticker="SPX",
        opt_type="PUT",
        strike=5900.0,
        entry_price=2.0,
        exit_price=2.8,
        contracts=1,
        total_spent=200.0,
        comm=1.0,
        gross_pl=81.0,
        net_pl=80.0,
        result_pct=40.0,
        entry_time="10:10 AM",
        exit_time="10:22 AM",
    )

    resp = client.post(
        "/trades/reviews/rebuild",
        data={
            "start_date": "2026-02-01",
            "end_date": "2026-02-28",
            "scope": "missing",
            "preserve_manual": "1",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"Rebuild Reviews" in resp.data

    with db() as conn:
        row = conn.execute(
            "SELECT setup_tag, checklist_score FROM trade_reviews WHERE trade_id = ?",
            (trade_id,),
        ).fetchone()
    assert row is not None
    assert row["setup_tag"] == "Statement Import"
    assert row["checklist_score"] is not None
