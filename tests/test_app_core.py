"""Core app behavior tests."""


def test_healthz_returns_ok_payload(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["status"] == "ok"
    assert payload["app"] == "mccain-capital"


def test_security_headers_applied(client):
    resp = client.get("/healthz")
    assert resp.headers.get("X-Content-Type-Options") == "nosniff"
    assert resp.headers.get("X-Frame-Options") == "SAMEORIGIN"
    assert resp.headers.get("Referrer-Policy") == "strict-origin-when-cross-origin"
    assert "Content-Security-Policy" in resp.headers


def test_core_pages_are_reachable(client):
    for path in ["/", "/dashboard", "/trades", "/journal", "/calculator", "/payouts"]:
        resp = client.get(path)
        assert resp.status_code == 200, f"Expected 200 for {path}, got {resp.status_code}"


def test_expected_endpoints_registered(app):
    endpoints = {rule.endpoint for rule in app.url_map.iter_rules()}
    expected = {
        "home",
        "healthz",
        "dashboard",
        "trades_page",
        "journal_home",
        "calculator",
        "payouts_page",
        "books_page",
    }
    assert expected.issubset(endpoints)
