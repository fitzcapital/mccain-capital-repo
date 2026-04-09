from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
import html
import json
import re
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any, Dict, Iterable, List, Optional, Tuple

from mccain_capital import runtime as app_runtime

RSS_TIMEOUT_SECONDS = 2.0
CACHE_TTL_SECONDS = 60
DEFAULT_LIMIT = 15
MAX_SOURCE_ITEMS = 10
MAX_ITEM_AGE_HOURS = 48
LAST_SUCCESS_MAX_AGE_HOURS = 24

TWITTER_RSS_SOURCES: Tuple[Dict[str, str], ...] = (
    {
        "source": "Kobeissi Letter",
        "handle": "@KobeissiLetter",
        "url": "https://nitter.poast.org/KobeissiLetter/rss",
        "profile_url": "https://x.com/KobeissiLetter",
    },
    {
        "source": "Unusual Whales",
        "handle": "@unusual_whales",
        "url": "https://nitter.poast.org/unusual_whales/rss",
        "profile_url": "https://x.com/unusual_whales",
    },
)

IMPACT_WEIGHTS = {"high": 3, "medium": 2, "low": 1}
RELEVANCE_WEIGHTS = {"actionable_now": 3, "watch": 2, "context_only": 1}

SYMBOL_MAP: Tuple[Tuple[str, str], ...] = (
    (r"\bSPX\b", "SPX"),
    (r"\bSPY\b", "SPY"),
    (r"\bQQQ\b", "QQQ"),
    (r"\bVIX\b", "VIX"),
    (r"\bNVDA\b", "NVDA"),
    (r"\bAAPL\b", "AAPL"),
    (r"\bTSLA\b", "TSLA"),
    (r"\bMETA\b", "META"),
    (r"\bMSFT\b", "MSFT"),
    (r"\bAMD\b", "AMD"),
    (r"\bIWM\b", "IWM"),
    (r"\bDXY\b", "DXY"),
    (r"\bTNX\b", "TNX"),
)

_CACHE: Dict[str, Any] = {"fetched_at": None, "payload": None}


def _cache_file() -> str:
    return app_runtime.upload_path(".twitter_feed_cache.json")


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


def _clean_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _truncate(value: str, limit: int = 220) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    trimmed = text[: max(0, limit - 1)].rstrip(" .,;:-")
    return f"{trimmed}…"


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


def _age_label(published_at: Optional[datetime], now_et: datetime) -> str:
    if published_at is None:
        return "Just now"
    delta = max(timedelta(0), now_et - published_at)
    minutes = int(delta.total_seconds() // 60)
    if minutes < 1:
        return "Just now"
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    days = hours // 24
    return f"{days}d ago"


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


def _extract_symbols(text: str) -> List[str]:
    found: List[str] = []
    for pattern, symbol in SYMBOL_MAP:
        if re.search(pattern, text, flags=re.IGNORECASE):
            found.append(symbol)
    return found


def _contains_any(text: str, phrases: Iterable[str]) -> bool:
    lowered = text.lower()
    return any(phrase in lowered for phrase in phrases)


def _classify_category(text: str) -> str:
    lowered = text.lower()
    if _contains_any(lowered, ("fed", "cpi", "ppi", "inflation", "jobs", "payroll", "yield", "treasury", "fomc")):
        return "macro"
    if _contains_any(lowered, ("tariff", "trump", "sanction", "government", "white house", "policy", "congress", "geopolitical", "war")):
        return "policy"
    if _contains_any(lowered, ("gamma", "dealer", "positioning", "options flow", "0dte", "call wall", "put wall", "flip")):
        return "flow"
    if _contains_any(lowered, ("earnings", "guidance", "ceo", "stock", "shares")):
        return "company"
    if _contains_any(lowered, ("spx", "spy", "qqq", "vix", "futures", "dow", "nasdaq", "russell")):
        return "market"
    return "market"


def _classify_impact(text: str) -> str:
    lowered = text.lower()
    if _contains_any(lowered, ("breaking", "fed", "fomc", "cpi", "inflation", "yield", "war", "tariff", "geopolitical", "sanction", "jobs", "payroll")):
        return "high"
    if _contains_any(lowered, ("earnings", "upgrade", "downgrade", "positioning", "options", "dealer", "gamma")):
        return "medium"
    return "low"


def _classify_market_bias(text: str) -> str:
    lowered = text.lower()
    bullish_terms = ("cooling inflation", "cuts", "rally", "soft landing", "buying", "bid", "easing", "disinflation", "breakout", "squeeze")
    bearish_terms = ("hot inflation", "yields rising", "hawkish", "selloff", "risk-off", "breakdown", "recession", "tariff", "war", "higher yields", "dump")
    bullish_hits = sum(1 for phrase in bullish_terms if phrase in lowered)
    bearish_hits = sum(1 for phrase in bearish_terms if phrase in lowered)
    if bullish_hits > bearish_hits:
        return "bullish"
    if bearish_hits > bullish_hits:
        return "bearish"
    return "neutral"


def _classify_volatility_bias(text: str) -> str:
    lowered = text.lower()
    if _contains_any(lowered, ("breaking", "uncertainty", "war", "tariff", "volatility", "headline risk", "selloff", "risk-off")):
        return "higher"
    if _contains_any(lowered, ("calm", "easing", "stabilization", "stabilising", "soft landing", "cooling", "contained")):
        return "lower"
    return "neutral"


def _trade_relevance(category: str, impact: str, symbols: List[str], text: str) -> str:
    lowered = text.lower()
    if impact == "high" and category in {"macro", "policy", "flow"}:
        return "actionable_now"
    if symbols or _contains_any(lowered, ("spx", "qqq", "vix", "fed", "gamma", "call wall", "put wall", "flip")):
        return "watch"
    return "context_only"


def _planning_direction(structure: Dict[str, Any]) -> str:
    bias = str(structure.get("planning_bias") or "").strip().lower()
    if bias in {"bullish_above_local_flip", "above_call_wall_extension_risk"}:
        return "bullish"
    if bias in {"bearish_below_local_flip", "below_put_wall_breakdown_risk"}:
        return "bearish"
    if bias == "neutral_between_levels":
        return "neutral"
    return "neutral"


def _gamma_alignment(item: Dict[str, Any], structure: Dict[str, Any]) -> Dict[str, str]:
    planning_direction = _planning_direction(structure)
    item_bias = str(item.get("market_bias") or "neutral").lower()
    if item_bias == "neutral" or planning_direction == "neutral":
        return {"alignment": "neutral", "alignment_label": "Mixed with plan"}
    if item_bias == planning_direction:
        return {"alignment": "aligned", "alignment_label": "Aligned with plan"}
    return {"alignment": "conflicted", "alignment_label": "Conflicts with plan"}


def _category_label(value: str) -> str:
    return {
        "macro": "Macro",
        "policy": "Policy",
        "market": "Market",
        "flow": "Flow",
        "company": "Company",
    }.get(value, "Market")


def _impact_label(value: str) -> str:
    return {"high": "HIGH", "medium": "MEDIUM", "low": "LOW"}.get(value, "LOW")


def _relevance_label(value: str) -> str:
    return {
        "actionable_now": "ACTIONABLE NOW",
        "watch": "WATCH",
        "context_only": "CONTEXT",
    }.get(value, "CONTEXT")


def _bias_label(value: str) -> str:
    return {
        "bullish": "Bullish",
        "bearish": "Bearish",
        "neutral": "Neutral",
    }.get(value, "Neutral")


def _vol_label(value: str) -> str:
    return {
        "higher": "Higher",
        "lower": "Lower",
        "neutral": "Neutral",
    }.get(value, "Neutral")


def _normalize_rss_item(source: Dict[str, str], row: ET.Element, now_et: datetime, structure: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    title = _clean_text(row.findtext("title"))
    description = _clean_text(row.findtext("description"))
    text = _truncate(title or description, limit=280)
    if not text:
        return None
    published_at = _parse_pub_date(row.findtext("pubDate"))
    if published_at and (now_et - published_at) > timedelta(hours=MAX_ITEM_AGE_HOURS):
        return None
    category = _classify_category(f"{title} {description}")
    impact = _classify_impact(f"{title} {description}")
    market_bias = _classify_market_bias(f"{title} {description}")
    volatility_bias = _classify_volatility_bias(f"{title} {description}")
    symbols = _extract_symbols(f"{title} {description}")
    trade_relevance = _trade_relevance(category, impact, symbols, f"{title} {description}")
    item = {
        "source": source["source"],
        "source_label": source["source"],
        "handle": source["handle"],
        "published_at": published_at.isoformat() if published_at else "",
        "published_label": _age_label(published_at, now_et),
        "age_label": _age_label(published_at, now_et),
        "text": text,
        "headline": text,
        "summary": description or text,
        "url": str(row.findtext("link") or source["profile_url"]).strip(),
        "category": category,
        "category_label": _category_label(category),
        "impact": impact,
        "impact_label": _impact_label(impact),
        "market_bias": market_bias,
        "market_bias_label": _bias_label(market_bias),
        "volatility_bias": volatility_bias,
        "volatility_bias_label": _vol_label(volatility_bias),
        "trade_relevance": trade_relevance,
        "trade_relevance_label": _relevance_label(trade_relevance),
        "symbols": symbols,
        "symbols_label": ", ".join(symbols) if symbols else "None",
    }
    item.update(_gamma_alignment(item, structure))
    if item["alignment"] == "conflicted" and item["trade_relevance"] == "actionable_now":
        item["trade_relevance"] = "watch"
        item["trade_relevance_label"] = _relevance_label("watch")
    return item


def _fetch_source_items(source: Dict[str, str], now_et: datetime, structure: Dict[str, Any]) -> List[Dict[str, Any]]:
    xml_body = _fetch_url_bytes(source["url"])
    rows = _extract_items(xml_body)
    items: List[Dict[str, Any]] = []
    for row in rows[:MAX_SOURCE_ITEMS]:
        item = _normalize_rss_item(source, row, now_et, structure)
        if item:
            items.append(item)
    return items


def _sort_key(item: Dict[str, Any]) -> Tuple[int, int, str]:
    published_at = str(item.get("published_at") or "")
    impact = IMPACT_WEIGHTS.get(str(item.get("impact") or "low"), 1)
    relevance = RELEVANCE_WEIGHTS.get(str(item.get("trade_relevance") or "context_only"), 1)
    return (impact + relevance, 1 if item.get("alignment") == "aligned" else 0, published_at)


def _summarize_bias(items: List[Dict[str, Any]]) -> str:
    scores = {"bullish": 0, "bearish": 0, "neutral": 0}
    for item in items:
        bias = str(item.get("market_bias") or "neutral")
        scores[bias] = scores.get(bias, 0) + IMPACT_WEIGHTS.get(str(item.get("impact") or "low"), 1)
    ranked = max(scores, key=scores.get)
    return _bias_label(ranked)


def _summarize_volatility(items: List[Dict[str, Any]]) -> str:
    scores = {"higher": 0, "lower": 0, "neutral": 0}
    for item in items:
        bias = str(item.get("volatility_bias") or "neutral")
        scores[bias] = scores.get(bias, 0) + IMPACT_WEIGHTS.get(str(item.get("impact") or "low"), 1)
    ranked = max(scores, key=scores.get)
    return _vol_label(ranked)


def _summarize_gamma_alignment(items: List[Dict[str, Any]], structure: Dict[str, Any]) -> str:
    aligned = sum(1 for item in items if item.get("alignment") == "aligned")
    conflicted = sum(1 for item in items if item.get("alignment") == "conflicted")
    gamma_regime = str(structure.get("gamma_regime_label") or structure.get("gamma_regime") or "Unconfirmed").strip()
    if aligned > conflicted:
        return f"Supportive · {gamma_regime}"
    if conflicted > aligned:
        return f"Conflicted · {gamma_regime}"
    return f"Mixed · {gamma_regime}"


def _summarize_tradeability(structure: Dict[str, Any]) -> str:
    tradeability = str(
        structure.get("trade_state_label")
        or structure.get("execution_regime_label")
        or structure.get("tradeability")
        or "Planning only"
    ).strip()
    planning_bias = str(structure.get("planning_bias_label") or "").strip()
    if planning_bias:
        return f"{tradeability} · {planning_bias}"
    return tradeability or "Planning only"


def _now_summary(items: List[Dict[str, Any]], structure: Dict[str, Any], status: str) -> Dict[str, str]:
    if not items:
        return {
            "spx_focus": "No fresh source flow yet",
            "leadership": "Macro bias unavailable",
            "weakness": "Volatility unavailable",
            "feed_state": "Twitter flow sync delayed",
        }
    return {
        "spx_focus": f"Macro: {_summarize_bias(items)}",
        "leadership": f"Volatility: {_summarize_volatility(items)}",
        "weakness": f"Gamma: {_summarize_gamma_alignment(items, structure)}",
        "feed_state": f"{'Live' if status == 'live' else 'Delayed'} · {_summarize_tradeability(structure)}",
    }


def _fallback_seed_items(now_et: datetime, structure: Dict[str, Any]) -> List[Dict[str, Any]]:
    planning_bias = str(structure.get("planning_bias_label") or "Hold to the shared gamma plan").strip()
    seed_texts = (
        {
            "source": "Kobeissi Letter",
            "handle": "@KobeissiLetter",
            "url": "https://x.com/KobeissiLetter",
            "text": f"Feed delayed. Preserve macro context and keep planning around {planning_bias.lower()}.",
            "category": "macro",
            "impact": "medium",
        },
        {
            "source": "Unusual Whales",
            "handle": "@unusual_whales",
            "url": "https://x.com/unusual_whales",
            "text": "Feed delayed. Wait for fresh flow posts before upgrading a setup from context to trigger.",
            "category": "flow",
            "impact": "medium",
        },
    )
    items: List[Dict[str, Any]] = []
    for seed in seed_texts:
        market_bias = _planning_direction(structure)
        item = {
            "source": seed["source"],
            "source_label": seed["source"],
            "handle": seed["handle"],
            "published_at": now_et.isoformat(),
            "published_label": "Cached backup",
            "age_label": "Cached backup",
            "text": seed["text"],
            "headline": seed["text"],
            "summary": "Using backup feed shell until a fresh post sync succeeds.",
            "url": seed["url"],
            "category": seed["category"],
            "category_label": _category_label(seed["category"]),
            "impact": seed["impact"],
            "impact_label": _impact_label(seed["impact"]),
            "market_bias": market_bias,
            "market_bias_label": _bias_label(market_bias),
            "volatility_bias": "neutral",
            "volatility_bias_label": "Neutral",
            "trade_relevance": "watch",
            "trade_relevance_label": "WATCH",
            "symbols": ["SPX", "QQQ", "VIX"],
            "symbols_label": "SPX, QQQ, VIX",
            "synthetic": True,
        }
        item.update(_gamma_alignment(item, structure))
        items.append(item)
    return items


def _cache_is_fresh(cached_at: Optional[datetime], now_et: datetime) -> bool:
    if cached_at is None:
        return False
    return (now_et - cached_at).total_seconds() <= CACHE_TTL_SECONDS


def _deserialize_cache(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    items = list(payload.get("items") or [])
    if not items:
        return None
    return payload


def fetch_twitter_feed(
    limit: int = DEFAULT_LIMIT,
    *,
    now_et: Optional[datetime] = None,
    market_structure_snapshot: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    snapshot = build_twitter_feed_snapshot(
        limit=limit,
        now_et=now_et,
        market_structure_snapshot=market_structure_snapshot,
    )
    return list(snapshot.get("items") or [])


def build_twitter_feed_snapshot(
    limit: int = DEFAULT_LIMIT,
    *,
    now_et: Optional[datetime] = None,
    market_structure_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now_et = now_et or app_runtime.now_et()
    structure = dict(market_structure_snapshot or {})
    cached_at = _CACHE.get("fetched_at")
    cached_payload = _deserialize_cache(dict(_CACHE.get("payload") or {}))
    if cached_payload and _cache_is_fresh(cached_at, now_et):
        return dict(cached_payload)

    items: List[Dict[str, Any]] = []
    failed_sources: List[str] = []

    with ThreadPoolExecutor(max_workers=len(TWITTER_RSS_SOURCES)) as pool:
        futures = {
            pool.submit(_fetch_source_items, source, now_et, structure): source
            for source in TWITTER_RSS_SOURCES
        }
        for future in as_completed(futures):
            source = futures[future]
            try:
                items.extend(list(future.result() or []))
            except Exception:
                failed_sources.append(source["source"])

    deduped: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for item in items:
        key = (str(item.get("handle") or ""), str(item.get("text") or "").lower())
        prior = deduped.get(key)
        if prior is None or str(item.get("published_at") or "") > str(prior.get("published_at") or ""):
            deduped[key] = item

    sorted_items = sorted(deduped.values(), key=_sort_key, reverse=True)[:limit]
    status = "live"
    source_note = "Live flow sync from Kobeissi Letter and Unusual Whales."

    if not sorted_items:
        cached_items = list(cached_payload.get("items") or [])[:limit] if cached_payload else []
        disk_cache = _deserialize_cache(dict(_load_disk_cache() or {}))
        if cached_items:
            sorted_items = cached_items
            status = "delayed"
            source_note = "Using cached Twitter flow snapshot while the latest sync is delayed."
        elif disk_cache:
            sorted_items = list(disk_cache.get("items") or [])[:limit]
            status = "delayed"
            source_note = "Using cached Twitter flow snapshot while the latest sync is delayed."
        else:
            sorted_items = _fallback_seed_items(now_et, structure)[: max(2, min(limit, 4))]
            status = "delayed"
            source_note = "Feed sync delayed. Using backup flow shell until the next valid Twitter sync."
    elif failed_sources:
        status = "delayed"
        source_note = "Partial Twitter sync. Showing the latest valid flow from available sources."

    flow_summary = {
        "macro_bias": _summarize_bias(sorted_items),
        "volatility": _summarize_volatility(sorted_items),
        "gamma_alignment": _summarize_gamma_alignment(sorted_items, structure),
        "tradeability": _summarize_tradeability(structure),
    }
    flow_summary["headline"] = (
        f"Macro: {flow_summary['macro_bias']} | "
        f"Vol: {flow_summary['volatility']} | "
        f"Gamma: {flow_summary['gamma_alignment']} -> {flow_summary['tradeability']}"
    )

    snapshot = {
        "status": status,
        "feed_state": status,
        "source_note": source_note,
        "updated_at": now_et.isoformat(),
        "sources_monitored": [source["handle"] for source in TWITTER_RSS_SOURCES],
        "tracked_accounts": [source["handle"] for source in TWITTER_RSS_SOURCES],
        "items": list(sorted_items),
        "top_items": list(sorted_items[: min(5, limit)]),
        "available": bool(sorted_items),
        "flow_summary": flow_summary,
        "now_summary": _now_summary(sorted_items, structure, status),
    }

    if status == "live":
        _CACHE["fetched_at"] = now_et
        _CACHE["payload"] = dict(snapshot)
        _save_disk_cache(snapshot)
    return snapshot
