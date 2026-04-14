from datetime import datetime, timedelta
import urllib.error

from mccain_capital import runtime as app_runtime
from mccain_capital.services import market_feed_service as svc


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


def _reset(monkeypatch, sources):
    monkeypatch.setattr(svc, "RSS_SOURCES", tuple(sources))
    monkeypatch.setattr(svc, "_CACHE", {"fetched_at": None, "payload": None})
    monkeypatch.setattr(svc, "_load_disk_cache", lambda: None)
    monkeypatch.setattr(svc, "_save_disk_cache", lambda payload: None)


def test_market_feed_fetch_and_normalize_success(monkeypatch):
    now = datetime(2026, 4, 5, 10, 0, tzinfo=app_runtime.TZ)
    _reset(
        monkeypatch,
        [
            {
                "key": "reuters",
                "label": "Reuters",
                "url": "https://example.com/reuters.xml",
                "default_category": "market",
            }
        ],
    )
    monkeypatch.setattr(
        svc,
        "_fetch_url_bytes",
        lambda url: _rss_xml(
            {
                "title": "Fed signals rates pause after stronger payrolls",
                "link": "https://example.com/a",
                "description": "Macro desks are repricing yields higher.",
                "pubDate": _fmt_pub(now - timedelta(minutes=8)),
            }
        ),
    )
    snapshot = svc.build_market_feed_snapshot(now_et=now, quotes=[], context={})
    assert snapshot["status"] == "live"
    item = snapshot["top_items"][0]
    assert item["source"] == "Reuters"
    assert item["headline"] == "Fed signals rates pause after stronger payrolls"
    assert item["impact"] == "high"
    assert item["category"] in {"macro", "policy"}
    assert item["score"] > 0


def test_market_feed_deduplicates_similar_headlines_preferring_higher_quality(monkeypatch):
    now = datetime(2026, 4, 5, 10, 0, tzinfo=app_runtime.TZ)
    _reset(
        monkeypatch,
        [
            {
                "key": "reuters",
                "label": "Reuters",
                "url": "https://example.com/r.xml",
                "default_category": "market",
            },
            {
                "key": "marketwatch",
                "label": "MarketWatch",
                "url": "https://example.com/m.xml",
                "default_category": "market",
            },
        ],
    )

    def _fetch(url):
        if url.endswith("r.xml"):
            return _rss_xml(
                {
                    "title": "Stocks rally as Treasury yields ease after CPI",
                    "link": "https://example.com/r1",
                    "description": "SPX futures firm after inflation data.",
                    "pubDate": _fmt_pub(now - timedelta(minutes=10)),
                }
            )
        return _rss_xml(
            {
                "title": "US stocks rally as treasury yields ease after CPI report",
                "link": "https://example.com/m1",
                "description": "Markets respond to inflation data.",
                "pubDate": _fmt_pub(now - timedelta(minutes=9)),
            }
        )

    monkeypatch.setattr(svc, "_fetch_url_bytes", _fetch)
    snapshot = svc.build_market_feed_snapshot(now_et=now, quotes=[], context={})
    assert len(snapshot["top_items"]) == 1
    assert snapshot["top_items"][0]["source"] == "Reuters"
    assert snapshot["top_items"][0]["duplicate_count"] == 2


def test_market_feed_high_impact_keyword_scoring(monkeypatch):
    now = datetime(2026, 4, 5, 10, 0, tzinfo=app_runtime.TZ)
    _reset(
        monkeypatch,
        [
            {
                "key": "fed",
                "label": "Federal Reserve",
                "url": "https://example.com/fed.xml",
                "default_category": "policy",
            }
        ],
    )
    monkeypatch.setattr(
        svc,
        "_fetch_url_bytes",
        lambda url: _rss_xml(
            {
                "title": "FOMC minutes point to inflation and rates risk",
                "link": "https://example.com/fed1",
                "description": "Fed and Treasury commentary lift yield sensitivity.",
                "pubDate": _fmt_pub(now - timedelta(minutes=4)),
            }
        ),
    )
    snapshot = svc.build_market_feed_snapshot(now_et=now, quotes=[], context={})
    assert snapshot["high_impact_items"]
    assert snapshot["high_impact_items"][0]["impact"] == "high"
    assert snapshot["high_impact_items"][0]["score"] >= svc.HIGH_IMPACT_SCORE_THRESHOLD


def test_market_feed_stale_item_score_decays(monkeypatch):
    now = datetime(2026, 4, 5, 10, 0, tzinfo=app_runtime.TZ)
    _reset(
        monkeypatch,
        [
            {
                "key": "reuters",
                "label": "Reuters",
                "url": "https://example.com/stale.xml",
                "default_category": "market",
            }
        ],
    )
    monkeypatch.setattr(
        svc,
        "_fetch_url_bytes",
        lambda url: _rss_xml(
            {
                "title": "Treasury yields jump after CPI",
                "link": "https://example.com/old",
                "description": "Macro pressure builds.",
                "pubDate": _fmt_pub(now - timedelta(hours=14)),
            },
            {
                "title": "Treasury yields jump after CPI",
                "link": "https://example.com/new",
                "description": "Macro pressure builds.",
                "pubDate": _fmt_pub(now - timedelta(minutes=20)),
            },
        ),
    )
    snapshot = svc.build_market_feed_snapshot(now_et=now, quotes=[], context={})
    assert snapshot["top_items"][0]["url"] == "https://example.com/new"


def test_market_feed_quiet_window_state(monkeypatch):
    now = datetime(2026, 4, 5, 10, 0, tzinfo=app_runtime.TZ)
    _reset(
        monkeypatch,
        [
            {
                "key": "marketwatch",
                "label": "MarketWatch",
                "url": "https://example.com/quiet.xml",
                "default_category": "market",
            }
        ],
    )
    monkeypatch.setattr(
        svc,
        "_fetch_url_bytes",
        lambda url: _rss_xml(
            {
                "title": "Analysts discuss portfolio ideas for spring",
                "link": "https://example.com/q1",
                "description": "A low-signal feature story.",
                "pubDate": _fmt_pub(now - timedelta(minutes=90)),
            }
        ),
    )
    snapshot = svc.build_market_feed_snapshot(now_et=now, quotes=[], context={})
    assert snapshot["status"] == "quiet"
    assert snapshot["top_items"] == []


def test_market_feed_degraded_when_one_source_fails(monkeypatch):
    now = datetime(2026, 4, 5, 10, 0, tzinfo=app_runtime.TZ)
    _reset(
        monkeypatch,
        [
            {
                "key": "reuters",
                "label": "Reuters",
                "url": "https://example.com/live.xml",
                "default_category": "market",
            },
            {
                "key": "nasdaq",
                "label": "Nasdaq",
                "url": "https://example.com/fail.xml",
                "default_category": "market",
            },
        ],
    )

    def _fetch(url):
        if url.endswith("fail.xml"):
            raise urllib.error.URLError("down")
        return _rss_xml(
            {
                "title": "Stocks sell off as yields climb",
                "link": "https://example.com/live",
                "description": "Risk-off tone builds.",
                "pubDate": _fmt_pub(now - timedelta(minutes=7)),
            }
        )

    monkeypatch.setattr(svc, "_fetch_url_bytes", _fetch)
    snapshot = svc.build_market_feed_snapshot(now_et=now, quotes=[], context={})
    assert snapshot["status"] == "degraded"
    assert "Nasdaq" in snapshot["sources_failed"]
