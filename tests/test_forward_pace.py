from mccain_capital.services.forward_pace import build_projection


def test_forward_pace_page_renders(client):
    resp = client.get("/forward-pace", follow_redirects=True)

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Projection Command" in body
    assert "js/forward_pace.js" in body
    assert 'href="/forward-pace"' in body


def test_forward_pace_projection_calculates_tax_and_schedule(client):
    resp = client.post(
        "/api/forward-pace/projection",
        json={
            "base_balance": 50000,
            "gross_payout": 250,
            "payouts_per_week": 5,
            "weeks": 4,
            "fixed_buffer": 5000,
            "buffer_rate": 10,
            "state": "GA",
            "filing_status": "single",
            "start_date": "2026-05-02",
        },
    )

    assert resp.status_code == 200
    projection = resp.get_json()["projection"]
    assert projection["weekly"]["gross"] == 1250
    assert projection["weekly"]["net"] > 0
    assert projection["tax"]["federal_annual"] > 0
    assert projection["tax"]["state_rate"] == 5.19
    assert len(projection["schedule"]) == 4
    assert projection["schedule"][0]["start"] == "2026-05-02"


def test_forward_pace_pdf_download(client):
    resp = client.post(
        "/forward-pace/pdf",
        json={"base_balance": 10000, "gross_payout": 500, "weeks": 2, "state": "TX"},
    )

    assert resp.status_code == 200
    assert resp.mimetype == "application/pdf"
    assert resp.data.startswith(b"%PDF")
    assert "attachment" in resp.headers["Content-Disposition"]


def test_forward_pace_build_projection_clamps_inputs():
    projection = build_projection({"weeks": 500, "payouts_per_week": 0, "buffer_rate": 500})

    assert projection["inputs"]["weeks"] == 104
    assert projection["inputs"]["payouts_per_week"] == 1
    assert projection["inputs"]["buffer_rate"] == 90
