from datetime import datetime, timedelta

from mccain_capital import runtime as app_runtime
from mccain_capital.services import twitter_feed_service as svc


def _reset_cache(monkeypatch):
    monkeypatch.setattr(svc, "_CACHE", {"loaded_at": None, "sources": {}})
    monkeypatch.setattr(svc, "_load_disk_cache", lambda: None)
    monkeypatch.setattr(svc, "_save_disk_cache", lambda payload: None)
    monkeypatch.setattr(svc, "_twitterapi_key", lambda: "test-token")
    monkeypatch.setattr(svc, "_try_acquire_refresh_lock", lambda username, now_et: True)
    monkeypatch.setattr(svc, "_release_refresh_lock", lambda username: None)


def test_twitter_feed_classifies_and_aligns_to_structure(monkeypatch):
    now = datetime(2026, 4, 8, 9, 35, tzinfo=app_runtime.TZ)
    _reset_cache(monkeypatch)

    monkeypatch.setattr(
        svc,
        "_run_twitterapi_last_tweets",
        lambda **kwargs: [
            {
                "author": {"userName": kwargs["username"]},
                "fullText": (
                    "Fed and yields drive SPX lower as traders brace for hotter inflation."
                    if kwargs["username"] == "unusual_whales"
                    else "Dealer gamma positioning keeps SPX near call wall and QQQ pinned near resistance."
                ),
                "url": f"https://x.com/{kwargs['username']}/status/1",
                "createdAt": (
                    now - timedelta(minutes=5 if kwargs["username"] == "unusual_whales" else 3)
                ).isoformat(),
            }
        ],
    )

    snapshot = svc.build_twitter_feed_snapshot(
        now_et=now,
        market_structure_snapshot={
            "planning_bias": "bearish_below_local_flip",
            "gamma_regime_label": "Negative Gamma",
            "trade_state_label": "WAIT",
        },
    )

    assert snapshot["status"] == "live"
    assert snapshot["items"]
    first = snapshot["items"][0]
    assert first["source"] == "Unusual Whales"
    assert first["impact"] in {"high", "medium"}
    assert first["trade_relevance"] in {"actionable_now", "watch"}
    assert snapshot["flow_summary"]["tradeability"].startswith("WAIT")


def test_twitter_feed_uses_cache_when_fetch_fails(monkeypatch):
    now = datetime(2026, 4, 8, 9, 35, tzinfo=app_runtime.TZ)
    _reset_cache(monkeypatch)
    monkeypatch.setattr(
        svc,
        "_run_twitterapi_last_tweets",
        lambda **_: [
            {
                "author": {"userName": "unusual_whales"},
                "fullText": "Breaking: CPI cools and SPX futures rally.",
                "url": "https://x.com/unusual_whales/status/11",
                "createdAt": (now - timedelta(minutes=4)).isoformat(),
            }
        ],
    )
    first = svc.build_twitter_feed_snapshot(
        now_et=now,
        market_structure_snapshot={"planning_bias": "bullish_above_local_flip"},
    )
    assert first["status"] == "live"
    assert first["items"]

    monkeypatch.setattr(
        svc, "_run_twitterapi_last_tweets", lambda **_: (_ for _ in ()).throw(RuntimeError("down"))
    )
    second = svc.build_twitter_feed_snapshot(
        now_et=now + timedelta(minutes=6),
        market_structure_snapshot={"planning_bias": "bullish_above_local_flip"},
    )
    assert second["status"] == "delayed"
    assert second["items"]
    assert (
        second["source_note"]
        == "Partial twitterapi.io sync. Showing the latest good posts by source."
    )


def test_twitter_feed_merges_fresh_posts_with_cached_archive(monkeypatch):
    now = datetime(2026, 4, 8, 9, 35, tzinfo=app_runtime.TZ)
    _reset_cache(monkeypatch)
    calls = {"count": 0}

    def fake_fetch(**kwargs):
        calls["count"] += 1
        post_id = str(calls["count"])
        return [
            {
                "author": {"userName": "unusual_whales"},
                "fullText": f"Cached archive post {post_id} for SPY and QQQ.",
                "url": f"https://x.com/unusual_whales/status/{post_id}",
                "createdAt": (now + timedelta(minutes=calls["count"])).isoformat(),
            }
        ]

    monkeypatch.setattr(svc, "_run_twitterapi_last_tweets", fake_fetch)
    first = svc.build_twitter_feed_snapshot(now_et=now, force_refresh=True)
    second = svc.build_twitter_feed_snapshot(now_et=now + timedelta(minutes=5), force_refresh=True)

    urls = {item["url"] for item in second["items"]}
    assert "https://x.com/unusual_whales/status/1" in urls
    assert "https://x.com/unusual_whales/status/2" in urls
    assert second["source_states"]["unusual_whales"]["last_good_count"] == 2


def test_twitter_feed_never_returns_empty_without_cache(monkeypatch):
    now = datetime(2026, 4, 8, 9, 35, tzinfo=app_runtime.TZ)
    _reset_cache(monkeypatch)
    monkeypatch.setattr(
        svc, "_run_twitterapi_last_tweets", lambda **_: (_ for _ in ()).throw(RuntimeError("down"))
    )

    snapshot = svc.build_twitter_feed_snapshot(
        now_et=now,
        market_structure_snapshot={"planning_bias_label": "Bullish above Local Flip"},
    )

    assert snapshot["status"] == "delayed"
    assert snapshot["items"]
    assert snapshot["top_items"]
    assert snapshot["items"][0]["handle"] == "@unusual_whales"


def test_twitter_feed_filters_to_tracked_accounts(monkeypatch):
    now = datetime(2026, 4, 8, 9, 35, tzinfo=app_runtime.TZ)
    _reset_cache(monkeypatch)
    monkeypatch.setattr(
        svc,
        "_run_twitterapi_last_tweets",
        lambda **kwargs: [
            {
                "author": {"userName": "some_random_account"},
                "fullText": "This should not appear.",
                "url": "https://x.com/some_random_account/status/4",
                "createdAt": (now - timedelta(minutes=2)).isoformat(),
            }
        ],
    )

    snapshot = svc.build_twitter_feed_snapshot(
        now_et=now,
        market_structure_snapshot={"planning_bias": "bullish_above_local_flip"},
    )

    assert snapshot["status"] == "delayed"
    assert snapshot["items"][0]["handle"] == "@unusual_whales"


def test_twitter_feed_normalizes_snake_case_actor_fields(monkeypatch):
    now = datetime(2026, 4, 8, 9, 35, tzinfo=app_runtime.TZ)
    _reset_cache(monkeypatch)
    monkeypatch.setattr(
        svc,
        "_run_twitterapi_last_tweets",
        lambda **kwargs: [
            {
                "author": {"screen_name": kwargs["username"]},
                "full_text": "Hotter PPI adds pressure to yields and keeps traders defensive.",
                "url": f"https://x.com/{kwargs['username']}/status/8",
                "created_at": "Tue Apr 08 13:32:00 +0000 2026",
            }
        ],
    )

    snapshot = svc.build_twitter_feed_snapshot(
        now_et=now,
        market_structure_snapshot={"planning_bias": "bearish_below_local_flip"},
    )

    assert snapshot["status"] == "live"
    assert len(snapshot["items"]) == 1
    assert snapshot["items"][0]["handle"] == "@unusual_whales"


def test_twitter_feed_top_items_preserves_full_page_feed_limit(monkeypatch):
    now = datetime(2026, 4, 8, 9, 35, tzinfo=app_runtime.TZ)
    _reset_cache(monkeypatch)

    def _fake_fetch(**kwargs):
        username = kwargs["username"]
        rows = []
        for idx in range(24):
            rows.append(
                {
                    "author": {"userName": username},
                    "text": f"{username} update {idx}",
                    "url": f"https://x.com/{username}/status/{idx}",
                    "createdAt": (now - timedelta(minutes=idx)).isoformat(),
                }
            )
        return rows

    monkeypatch.setattr(svc, "_run_twitterapi_last_tweets", _fake_fetch)

    snapshot = svc.build_twitter_feed_snapshot(now_et=now)

    assert snapshot["status"] == "live"
    assert len(snapshot["top_items"]) == 24
    assert len(snapshot["items"]) == 24


def test_run_twitterapi_last_tweets_paginates_until_limit(monkeypatch):
    calls = []

    def _fake_page(*, username: str, api_key: str, limit: int, cursor: str = ""):
        calls.append({"username": username, "limit": limit, "cursor": cursor})
        if not cursor:
            rows = [
                {"id": f"first-{idx}", "url": f"https://x.com/{username}/status/first-{idx}"}
                for idx in range(20)
            ]
            return (rows, "cursor-2")
        rows = [
            {"id": f"second-{idx}", "url": f"https://x.com/{username}/status/second-{idx}"}
            for idx in range(20)
        ]
        return (rows, "")

    monkeypatch.setattr(svc, "_run_twitterapi_last_tweets_page", _fake_page)

    rows = svc._run_twitterapi_last_tweets(username="unusual_whales", api_key="test-token")

    assert len(rows) == 40
    assert calls[0]["cursor"] == ""
    assert calls[1]["cursor"] == "cursor-2"


def test_twitter_feed_cools_down_source_after_429(monkeypatch):
    now = datetime(2026, 4, 8, 9, 35, tzinfo=app_runtime.TZ)
    _reset_cache(monkeypatch)
    calls = {"count": 0}

    def _fake_fetch(**kwargs):
        calls["count"] += 1
        raise RuntimeError("HTTP Error 429: Too Many Requests")

    monkeypatch.setattr(svc, "_run_twitterapi_last_tweets", _fake_fetch)
    first = svc.build_twitter_feed_snapshot(now_et=now)
    second = svc.build_twitter_feed_snapshot(now_et=now + timedelta(seconds=30))

    assert first["status"] == "delayed"
    assert second["status"] == "delayed"
    assert calls["count"] == 1
    state = second["source_states"]["unusual_whales"]
    assert state["last_status_code"] == 429
    assert state["cooldown_until"]


def test_twitter_feed_uses_last_good_payload_for_single_source(monkeypatch):
    now = datetime(2026, 4, 8, 9, 35, tzinfo=app_runtime.TZ)
    _reset_cache(monkeypatch)
    responses = [
        {
            "author": {"userName": "unusual_whales"},
            "text": "Unusual item",
            "url": "https://x.com/unusual_whales/status/41",
            "createdAt": now.isoformat(),
        }
    ]

    def _fake_fetch(**kwargs):
        if isinstance(responses, Exception):
            raise responses
        return responses

    monkeypatch.setattr(svc, "_run_twitterapi_last_tweets", _fake_fetch)
    first = svc.build_twitter_feed_snapshot(now_et=now)
    responses = RuntimeError("down")
    second = svc.build_twitter_feed_snapshot(now_et=now + timedelta(minutes=6))

    assert first["status"] == "live"
    assert second["status"] == "delayed"
    assert {item["handle"] for item in second["items"]} == {"@unusual_whales"}
