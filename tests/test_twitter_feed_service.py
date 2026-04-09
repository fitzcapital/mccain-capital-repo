from datetime import datetime, timedelta

from mccain_capital import runtime as app_runtime
from mccain_capital.services import twitter_feed_service as svc


def _rss_xml(*items):
    body = "".join(
        f"""
        <item>
          <title>{item.get('title','')}</title>
          <link>{item.get('link','')}</link>
          <description>{item.get('description','')}</description>
          <pubDate>{item.get('pubDate','')}</pubDate>
        </item>
        """
        for item in items
    )
    return f"<rss><channel>{body}</channel></rss>".encode("utf-8")


def _fmt_pub(dt):
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")


def _reset_cache(monkeypatch):
    monkeypatch.setattr(svc, "_CACHE", {"fetched_at": None, "payload": None})
    monkeypatch.setattr(svc, "_load_disk_cache", lambda: None)
    monkeypatch.setattr(svc, "_save_disk_cache", lambda payload: None)


def test_twitter_feed_classifies_and_aligns_to_structure(monkeypatch):
    now = datetime(2026, 4, 8, 9, 35, tzinfo=app_runtime.TZ)
    _reset_cache(monkeypatch)

    def _fetch(url):
        if "KobeissiLetter" in url:
            return _rss_xml(
                {
                    "title": "Fed and yields drive SPX lower as traders brace for hotter inflation",
                    "link": "https://example.com/kobeissi-1",
                    "description": "CPI, yields, and tariff risk keep pressure on SPX and VIX.",
                    "pubDate": _fmt_pub(now - timedelta(minutes=5)),
                }
            )
        return _rss_xml(
            {
                "title": "Unusual Whales: dealer gamma positioning keeps SPX near call wall",
                "link": "https://example.com/uw-1",
                "description": "Options flow remains active with SPX and QQQ pinned near resistance.",
                "pubDate": _fmt_pub(now - timedelta(minutes=3)),
            }
        )

    monkeypatch.setattr(svc, "_fetch_url_bytes", _fetch)

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
    assert first["source"] in {"Kobeissi Letter", "Unusual Whales"}
    assert first["impact"] in {"high", "medium"}
    assert first["trade_relevance"] in {"actionable_now", "watch"}
    assert snapshot["flow_summary"]["tradeability"].startswith("WAIT")


def test_twitter_feed_uses_cache_when_fetch_fails(monkeypatch):
    now = datetime(2026, 4, 8, 9, 35, tzinfo=app_runtime.TZ)
    _reset_cache(monkeypatch)
    monkeypatch.setattr(
        svc,
        "_fetch_url_bytes",
        lambda url: _rss_xml(
            {
                "title": "Breaking: CPI cools and SPX futures rally",
                "link": f"https://example.com/{url.rsplit('/', 2)[-2]}",
                "description": "Macro desks lean risk-on.",
                "pubDate": _fmt_pub(now - timedelta(minutes=4)),
            }
        ),
    )
    first = svc.build_twitter_feed_snapshot(
        now_et=now,
        market_structure_snapshot={"planning_bias": "bullish_above_local_flip"},
    )
    assert first["status"] == "live"
    assert first["items"]

    monkeypatch.setattr(svc, "_fetch_url_bytes", lambda url: (_ for _ in ()).throw(RuntimeError("down")))
    second = svc.build_twitter_feed_snapshot(
        now_et=now + timedelta(minutes=2),
        market_structure_snapshot={"planning_bias": "bullish_above_local_flip"},
    )
    assert second["status"] == "delayed"
    assert second["items"]
    assert second["source_note"].lower().startswith("using cached")


def test_twitter_feed_never_returns_empty_without_cache(monkeypatch):
    now = datetime(2026, 4, 8, 9, 35, tzinfo=app_runtime.TZ)
    _reset_cache(monkeypatch)
    monkeypatch.setattr(svc, "_fetch_url_bytes", lambda url: (_ for _ in ()).throw(RuntimeError("down")))

    snapshot = svc.build_twitter_feed_snapshot(
        now_et=now,
        market_structure_snapshot={"planning_bias_label": "Bullish above Local Flip"},
    )

    assert snapshot["status"] == "delayed"
    assert snapshot["items"]
    assert snapshot["top_items"]
    assert snapshot["items"][0]["handle"] in {"@KobeissiLetter", "@unusual_whales"}
