from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime
import hashlib
import html
import json
import re
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterable, List, Optional, Tuple

from mccain_capital import runtime as app_runtime

CACHE_TTL_SECONDS = 60
RSS_TIMEOUT_SECONDS = 1.5
MAX_VISIBLE_ITEMS = 5
MAX_SOURCE_ITEMS = 8
MAX_AGE_MINUTES = 18 * 60
QUIET_SCORE_THRESHOLD = 34
HIGH_IMPACT_SCORE_THRESHOLD = 56
DEDUP_SIMILARITY_THRESHOLD = 0.86
MARKET_WATCH_BREADTH_SYMBOLS = {"SPY", "QQQ", "IWM", "NVDA", "MSFT", "AAPL", "AMZN", "META", "TSLA"}

YAHOO_BUNDLE_RSS_URL = "https://feeds.finance.yahoo.com/rss/2.0/headline"
YAHOO_BUNDLE_SYMBOLS = "^GSPC,SPY,QQQ,IWM,NVDA,MSFT,AAPL,AMZN,META,TSLA"

RSS_SOURCES: Tuple[Dict[str, Any], ...] = (
    {
        "key": "reuters_business",
        "label": "Reuters",
        "url": "https://feeds.reuters.com/reuters/businessNews",
        "source_quality": 14,
        "default_category": "market",
        "group": "high_impact",
    },
    {
        "key": "fed_press",
        "label": "Federal Reserve",
        "url": "https://www.federalreserve.gov/feeds/press_all.xml",
        "source_quality": 16,
        "default_category": "policy",
        "group": "high_impact",
    },
    {
        "key": "fed_speeches",
        "label": "Federal Reserve",
        "url": "https://www.federalreserve.gov/feeds/speeches_testimony.xml",
        "source_quality": 15,
        "default_category": "policy",
        "group": "high_impact",
    },
    {
        "key": "yahoo_bundle",
        "label": "Yahoo Finance",
        "url": YAHOO_BUNDLE_RSS_URL + "?" + urllib.parse.urlencode({"s": YAHOO_BUNDLE_SYMBOLS, "region": "US", "lang": "en-US"}),
        "source_quality": 10,
        "default_category": "market",
        "group": "market",
    },
    {
        "key": "investing_macro",
        "label": "Investing.com",
        "url": "https://www.investing.com/rss/market_overview.rss",
        "source_quality": 8,
        "default_category": "macro",
        "group": "market",
    },
    {
        "key": "marketwatch",
        "label": "MarketWatch",
        "url": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
        "source_quality": 9,
        "default_category": "market",
        "group": "market",
    },
    {
        "key": "nasdaq_markets",
        "label": "Nasdaq",
        "url": "https://www.nasdaq.com/feed/rssoutbound?category=Markets",
        "source_quality": 8,
        "default_category": "market",
        "group": "market",
    },
)

HIGH_IMPACT_KEYWORDS: Dict[str, int] = {
    "fed": 16,
    "powell": 18,
    "fomc": 18,
    "cpi": 16,
    "ppi": 14,
    "inflation": 14,
    "jobs": 14,
    "payrolls": 16,
    "recession": 12,
    "treasury": 12,
    "tariff": 18,
    "trump": 14,
    "china": 12,
    "war": 16,
    "oil": 12,
    "rates": 13,
    "yields": 13,
    "ismp": 10,
    "ism": 12,
    "consumer sentiment": 12,
    "retail sales": 12,
    "gdp": 12,
}

MARKET_KEYWORDS: Dict[str, int] = {
    "stocks": 6,
    "futures": 8,
    "rally": 7,
    "selloff": 8,
    "volatility": 8,
    "risk-on": 8,
    "risk-off": 8,
    "treasury yields": 8,
    "dollar": 7,
    "breadth": 8,
    "spx": 10,
    "s&p": 10,
    "spy": 8,
    "qqq": 8,
    "iwm": 8,
    "vix": 8,
}

COMPANY_KEYWORDS: Dict[str, int] = {
    "earnings": 7,
    "guidance": 8,
    "downgrade": 7,
    "upgrade": 6,
    "ceo": 5,
    "tesla": 9,
    "nvidia": 9,
    "apple": 8,
    "microsoft": 8,
    "amazon": 8,
    "meta": 8,
}

SOURCE_QUALITY_OVERRIDES = {
    "Reuters": 14,
    "Federal Reserve": 16,
    "Yahoo Finance": 10,
    "MarketWatch": 9,
    "Nasdaq": 8,
    "Investing.com": 8,
}

_CACHE: Dict[str, Any] = {"fetched_at": None, "payload": None}


def _cache_file() -> str:
    return app_runtime.upload_path(".market_news_cache.json")


def _load_disk_cache() -> Optional[Dict[str, Any]]:
    try:
        with open(_cache_file(), "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _save_disk_cache(payload: Dict[str, Any]) -> None:
    try:
        with open(_cache_file(), "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
    except OSError:
        return


def _clean_html(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_pub_date(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
    except Exception:
        return None
    try:
        return dt.astimezone(app_runtime.TZ)
    except Exception:
        return None


def _fetch_url_bytes(url: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/rss+xml,application/xml,text/xml,*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=RSS_TIMEOUT_SECONDS) as resp:
        return resp.read()


def _extract_items(xml_body: bytes) -> List[ET.Element]:
    root = ET.fromstring(xml_body)
    return list(root.findall(".//item"))


def _tokens(value: str) -> List[str]:
    return [token for token in re.findall(r"[a-z0-9]+", value.lower()) if len(token) > 2]


def _canonical_title(value: str) -> str:
    return " ".join(_tokens(value))


def _similar_titles(a: str, b: str) -> bool:
    left = _canonical_title(a)
    right = _canonical_title(b)
    if not left or not right:
        return False
    if left == right:
        return True
    ratio = SequenceMatcher(None, left, right).ratio()
    if ratio >= DEDUP_SIMILARITY_THRESHOLD:
        return True
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    if not left_tokens or not right_tokens:
        return False
    overlap = len(left_tokens & right_tokens) / max(len(left_tokens), len(right_tokens))
    return overlap >= 0.8


def _extract_symbols(text: str) -> List[str]:
    raw = text.lower()
    found: List[str] = []
    symbol_map = {
        "spx": "SPX",
        "s&p": "SPX",
        "spy": "SPY",
        "qqq": "QQQ",
        "iwm": "IWM",
        "vix": "VIX",
        "tsla": "TSLA",
        "nvda": "NVDA",
        "aapl": "AAPL",
        "msft": "MSFT",
        "amzn": "AMZN",
        "meta": "META",
    }
    for key, symbol in symbol_map.items():
        if key in raw and symbol not in found:
            found.append(symbol)
    return found


def _keyword_hits(text: str, mapping: Dict[str, int]) -> Tuple[List[str], int]:
    hits: List[str] = []
    score = 0
    raw = text.lower()
    for keyword, weight in mapping.items():
        if keyword in raw:
            hits.append(keyword)
            score += int(weight)
    return hits, score


def _categorize_item(text: str, default_category: str) -> str:
    high_hits, _ = _keyword_hits(text, HIGH_IMPACT_KEYWORDS)
    market_hits, market_score = _keyword_hits(text, MARKET_KEYWORDS)
    company_hits, company_score = _keyword_hits(text, COMPANY_KEYWORDS)
    if any(term in text.lower() for term in ("fed", "powell", "fomc", "treasury", "rates", "yield", "policy", "tariff", "white house")):
        return "policy"
    if high_hits and any(term in text.lower() for term in ("cpi", "ppi", "inflation", "jobs", "payrolls", "gdp", "retail sales", "consumer sentiment", "ism")):
        return "macro"
    if company_score > market_score:
        return "company"
    if market_hits or default_category == "market":
        return "market"
    return default_category or "market"


def _score_item(
    *,
    title: str,
    summary: str,
    source_label: str,
    category: str,
    published_at: Optional[datetime],
    now_et: datetime,
) -> Tuple[int, List[str]]:
    text = f"{title} {summary}".lower()
    keywords: List[str] = []
    total = 0
    high_hits, high_score = _keyword_hits(text, HIGH_IMPACT_KEYWORDS)
    market_hits, market_score = _keyword_hits(text, MARKET_KEYWORDS)
    company_hits, company_score = _keyword_hits(text, COMPANY_KEYWORDS)
    keywords.extend(high_hits + market_hits + company_hits)
    total += high_score + market_score + company_score
    total += SOURCE_QUALITY_OVERRIDES.get(source_label, 6)
    total += {"macro": 18, "policy": 18, "market": 10, "company": 8}.get(category, 6)

    if published_at is not None:
        age_minutes = max(0, int((now_et - published_at).total_seconds() // 60))
        if age_minutes <= 15:
            total += 16
        elif age_minutes <= 60:
            total += 10
        elif age_minutes <= 180:
            total += 6
        elif age_minutes <= 360:
            total += 2
        elif age_minutes <= 720:
            total -= 4
        else:
            total -= 12
    else:
        total -= 8

    return max(0, total), list(dict.fromkeys(keywords))[:6]


def _impact_from_score(category: str, score: int, keywords: Iterable[str]) -> str:
    keyword_set = {str(word) for word in keywords}
    if category in {"macro", "policy"} and keyword_set:
        return "high" if score >= 42 else "medium"
    if score >= HIGH_IMPACT_SCORE_THRESHOLD:
        return "high"
    if score >= 30:
        return "medium"
    return "low"


def _impact_label(value: str) -> str:
    return {"high": "HIGH", "medium": "MED", "low": "LOW"}.get(str(value or "").lower(), "LOW")


def _category_label(value: str) -> str:
    return {
        "macro": "Macro",
        "market": "Market",
        "company": "Company",
        "policy": "Policy",
        "high_impact": "High Impact",
    }.get(str(value or "").lower(), "Market")


def _normalize_feed_item(row: Dict[str, Any], *, now_et: datetime) -> Dict[str, Any]:
    title = _clean_html(row.get("title") or row.get("headline") or "")
    summary = _clean_html(row.get("summary") or row.get("description") or "")
    source_label = str(row.get("source_label") or row.get("source") or "Source").strip() or "Source"
    published_at = row.get("published_at")
    category = _categorize_item(f"{title} {summary}", str(row.get("default_category") or "market"))
    score, keywords = _score_item(
        title=title,
        summary=summary,
        source_label=source_label,
        category=category,
        published_at=published_at if isinstance(published_at, datetime) else None,
        now_et=now_et,
    )
    impact = _impact_from_score(category, score, keywords)
    age_minutes = None
    published_iso = ""
    published_label = "Just now"
    if isinstance(published_at, datetime):
        age_minutes = max(0, int((now_et - published_at).total_seconds() // 60))
        published_iso = published_at.isoformat()
        if age_minutes < 60:
            published_label = f"{age_minutes}m ago"
        elif age_minutes < 24 * 60:
            published_label = f"{max(1, age_minutes // 60)}h ago"
        else:
            published_label = f"{max(1, age_minutes // (24 * 60))}d ago"

    uid_seed = f"{source_label}|{title}|{published_iso}|{row.get('url') or ''}"
    return {
        "id": hashlib.sha1(uid_seed.encode("utf-8")).hexdigest()[:16],
        "source": source_label,
        "source_label": source_label,
        "title": title or "Market headline",
        "headline": title or "Market headline",
        "summary": summary or "No summary available.",
        "url": str(row.get("url") or "").strip(),
        "published_at": published_iso,
        "published_label": published_label,
        "age_minutes": age_minutes,
        "category": category,
        "category_label": _category_label(category),
        "impact": impact,
        "impact_label": _impact_label(impact),
        "symbols": _extract_symbols(f"{title} {summary}"),
        "keywords": keywords,
        "score": score,
        "duplicate_count": 1,
    }


def _fetch_source_rows(source_cfg: Dict[str, Any]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Optional[str]]:
    try:
        xml_body = _fetch_url_bytes(str(source_cfg.get("url") or ""))
        items = _extract_items(xml_body)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ET.ParseError, ValueError):
        return source_cfg, [], "fetch_failed"

    rows: List[Dict[str, Any]] = []
    for item in items[:MAX_SOURCE_ITEMS]:
        title = (item.findtext("title") or "").strip()
        if not title:
            continue
        rows.append(
            {
                "title": title,
                "summary": item.findtext("description") or "",
                "url": (item.findtext("link") or "").strip(),
                "published_at": _parse_pub_date(item.findtext("pubDate")),
                "source_label": source_cfg.get("label") or "Source",
                "default_category": source_cfg.get("default_category") or "market",
            }
        )
    return source_cfg, rows, None


def _dedupe_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ranked = sorted(
        items,
        key=lambda item: (
            -int(item.get("score") or 0),
            SOURCE_QUALITY_OVERRIDES.get(str(item.get("source_label") or ""), 6),
            -(int(item.get("age_minutes")) if item.get("age_minutes") is not None else 9999),
        ),
    )
    kept: List[Dict[str, Any]] = []
    for item in ranked:
        duplicate = None
        for prior in kept:
            if item.get("url") and item.get("url") == prior.get("url"):
                duplicate = prior
                break
            if _similar_titles(str(item.get("headline") or ""), str(prior.get("headline") or "")):
                duplicate = prior
                break
        if duplicate is not None:
            duplicate["duplicate_count"] = int(duplicate.get("duplicate_count") or 1) + 1
            if int(item.get("score") or 0) > int(duplicate.get("score") or 0):
                preserved_dupes = int(duplicate.get("duplicate_count") or 1)
                duplicate.clear()
                duplicate.update(item)
                duplicate["duplicate_count"] = preserved_dupes
            continue
        kept.append(dict(item))
    return kept


def _watch_strip_summary(quotes: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    rows = [row for row in list(quotes or []) if isinstance(row, dict) and str(row.get("label") or "").strip()]
    breadth_rows = [
        row for row in rows
        if str(row.get("label") or "").strip().upper() in MARKET_WATCH_BREADTH_SYMBOLS
    ]
    advancers = sum(1 for row in breadth_rows if isinstance(row.get("change_pct"), (int, float)) and float(row.get("change_pct")) > 0)
    decliners = sum(1 for row in breadth_rows if isinstance(row.get("change_pct"), (int, float)) and float(row.get("change_pct")) < 0)
    positive = sorted(
        [row for row in breadth_rows if isinstance(row.get("change_pct"), (int, float))],
        key=lambda row: float(row.get("change_pct") or 0),
        reverse=True,
    )
    leaders = [str(row.get("label") or "—") for row in positive if float(row.get("change_pct") or 0) > 0][:3]
    laggard = next((row for row in reversed(positive) if float(row.get("change_pct") or 0) < 0), None)
    risk_tone = "Broad Risk-On" if advancers > decliners + 2 else "Risk-Off" if decliners > advancers + 2 else "Mixed Tape"
    return {
        "breadth": f"{advancers}/{decliners}",
        "risk_tone": risk_tone,
        "leaders": ", ".join(leaders) if leaders else "—",
        "drag": (
            f"{laggard.get('label')} {float(laggard.get('change_pct') or 0):+.2f}%"
            if laggard is not None
            else "—"
        ),
    }


def _now_summary(*, quotes: Optional[List[Dict[str, Any]]], context: Optional[Dict[str, Any]], snapshot: Dict[str, Any]) -> Dict[str, str]:
    strip = _watch_strip_summary(quotes)
    quote_rows = [row for row in list(quotes or []) if isinstance(row, dict)]
    laggard = None
    for row in quote_rows:
        pct = row.get("change_pct")
        if not isinstance(pct, (int, float)):
            continue
        if laggard is None or float(pct) < float(laggard.get("change_pct") or 0):
            laggard = row
    context = dict(context or {})
    feed_state = (
        f"Monitoring {', '.join(snapshot.get('sources_monitored') or [])}"
        if snapshot.get("sources_monitored")
        else "Monitoring tracked RSS sources"
    )
    return {
        "spx_focus": str(context.get("headline_note") or "Watching SPX and leadership rotation.").strip(),
        "leadership": str(strip.get("risk_tone") or context.get("leadership") or "Mixed tape").strip(),
        "weakness": (
            f"{laggard.get('label')} {float(laggard.get('change_pct') or 0):+.2f}%"
            if laggard is not None and isinstance(laggard.get("change_pct"), (int, float))
            else str(strip.get("drag") or "No clear laggard").strip()
        ),
        "feed_state": feed_state,
        "watch_strip": strip,
    }


def build_market_feed_snapshot(
    *,
    now_et: Optional[datetime] = None,
    quotes: Optional[List[Dict[str, Any]]] = None,
    context: Optional[Dict[str, Any]] = None,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    now_et = now_et or app_runtime.now_et()
    cached_at = _CACHE.get("fetched_at")
    cached_payload = _CACHE.get("payload")
    if (
        not force_refresh
        and isinstance(cached_at, datetime)
        and isinstance(cached_payload, dict)
        and (now_et - cached_at).total_seconds() < CACHE_TTL_SECONDS
    ):
        payload = dict(cached_payload)
        payload["now_summary"] = _now_summary(quotes=quotes, context=context, snapshot=payload)
        return payload

    disk_payload = _load_disk_cache() or {}
    raw_items: List[Dict[str, Any]] = []
    failures: List[str] = []
    monitored = [str(cfg.get("label") or cfg.get("key") or "Source") for cfg in RSS_SOURCES]

    with ThreadPoolExecutor(max_workers=min(6, len(RSS_SOURCES))) as pool:
        futures = [pool.submit(_fetch_source_rows, cfg) for cfg in RSS_SOURCES]
        for future in as_completed(futures):
            source_cfg, rows, err = future.result()
            if err:
                failures.append(str(source_cfg.get("label") or source_cfg.get("key") or "Source"))
                continue
            raw_items.extend(rows)

    normalized = [
        _normalize_feed_item(row, now_et=now_et)
        for row in raw_items
        if isinstance(row, dict)
    ]
    fresh_items = [
        item
        for item in normalized
        if item.get("age_minutes") is None or int(item.get("age_minutes") or 0) <= MAX_AGE_MINUTES
    ]
    deduped = _dedupe_items(fresh_items)
    deduped.sort(
        key=lambda item: (
            -int(item.get("score") or 0),
            0 if str(item.get("impact") or "") == "high" else 1 if str(item.get("impact") or "") == "medium" else 2,
            int(item.get("age_minutes") or 99999),
        )
    )

    high_impact_items = [item for item in deduped if str(item.get("impact") or "") == "high"][:MAX_VISIBLE_ITEMS]
    market_items = [item for item in deduped if str(item.get("category") or "") == "market"][:MAX_VISIBLE_ITEMS]
    company_items = [item for item in deduped if str(item.get("category") or "") == "company"][:MAX_VISIBLE_ITEMS]
    top_items = deduped[:MAX_VISIBLE_ITEMS]

    if failures and top_items:
        status = "degraded"
    elif failures and not top_items:
        status = "error"
    elif top_items and max(int(item.get("score") or 0) for item in top_items) < QUIET_SCORE_THRESHOLD:
        status = "quiet"
    elif top_items:
        status = "live"
    else:
        status = "quiet"

    source_note = {
        "live": "Live RSS market drivers scored from Reuters, Fed, Yahoo Finance, Investing.com, MarketWatch, and Nasdaq.",
        "quiet": "No major market drivers in the current refresh window. Monitoring tracked sources for fresh catalysts.",
        "degraded": "Some feeds failed, but the remaining RSS sources are still updating market drivers.",
        "error": "RSS intelligence is unavailable right now. Monitoring will resume on the next refresh.",
    }[status]

    payload = {
        "status": status,
        "updated_at": now_et.isoformat(),
        "sources_monitored": monitored,
        "sources_failed": failures,
        "source_note": source_note,
        "high_impact_items": high_impact_items,
        "market_items": market_items,
        "company_items": company_items,
        "top_items": top_items if status in {"live", "degraded"} else [],
        "all_items": deduped,
    }
    payload["now_summary"] = _now_summary(quotes=quotes, context=context, snapshot=payload)

    if status in {"live", "degraded", "quiet"}:
        _CACHE["fetched_at"] = now_et
        _CACHE["payload"] = dict(payload)
        _save_disk_cache(payload)
        return payload

    if isinstance(disk_payload, dict) and disk_payload.get("top_items"):
        cached = dict(disk_payload)
        cached["status"] = "degraded"
        cached["source_note"] = "Using cached RSS intelligence while live feeds retry."
        cached["now_summary"] = _now_summary(quotes=quotes, context=context, snapshot=cached)
        _CACHE["fetched_at"] = now_et
        _CACHE["payload"] = dict(cached)
        return cached

    _CACHE["fetched_at"] = now_et
    _CACHE["payload"] = dict(payload)
    return payload
