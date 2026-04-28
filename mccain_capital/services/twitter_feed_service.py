from __future__ import annotations

import errno
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
import html
import json
import logging
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, List, Optional, Tuple

from mccain_capital import runtime as app_runtime

LOGGER = logging.getLogger(__name__)

TWITTERAPI_TIMEOUT_SECONDS = 15.0
SOURCE_STALE_SECONDS = 60
PAGE_CACHE_TTL_SECONDS = {
    "dashboard": 180,
    "market-pulse": 75,
}
COOLDOWN_429_SECONDS = 300
REFRESH_LOCK_TTL_SECONDS = 20
DEFAULT_LIMIT = 15
DEFAULT_DASHBOARD_LIMIT = 5
DEFAULT_MARKET_PULSE_LIMIT = 15
MAX_MARKET_PULSE_LIMIT = 100
TWITTERAPI_PAGE_SIZE = 100
TWITTERAPI_MAX_PAGES = 1
MAX_ITEM_AGE_HOURS = 96
MAX_CACHED_SOURCE_ITEMS = 250
TWITTERAPI_BASE_URL = "https://api.twitterapi.io"

TRACKED_SOURCES: Tuple[Dict[str, str], ...] = (
    {
        "source": "Unusual Whales",
        "handle": "@unusual_whales",
        "username": "unusual_whales",
        "profile_url": "https://x.com/unusual_whales",
    },
)

SOURCE_BY_USERNAME = {str(source["username"]).strip().lower(): source for source in TRACKED_SOURCES}

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

_CACHE: Dict[str, Any] = {"loaded_at": None, "sources": {}}


def _cache_file() -> str:
    return app_runtime.upload_path(".twitter_feed_cache.json")


def _lock_file(username: str) -> str:
    return app_runtime.upload_path(f".twitter_feed_{username}.lock")


def _default_source_state(source: Dict[str, str]) -> Dict[str, Any]:
    return {
        "handle": str(source["handle"]),
        "username": str(source["username"]),
        "source": str(source["source"]),
        "last_success_at": "",
        "last_attempt_at": "",
        "cooldown_until": "",
        "last_status_code": None,
        "last_error": "",
        "last_good_payload": [],
        "last_good_count": 0,
        "last_raw_payload": [],
        "last_raw_count": 0,
    }


def _parse_dt(value: Any) -> Optional[datetime]:
    return _parse_datetime(value)


def _format_et_label(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    try:
        return dt.astimezone(app_runtime.TZ).strftime("%b %d %I:%M %p ET")
    except Exception:
        return ""


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


def _normalize_source_store(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    sources: Dict[str, Any] = {}
    raw_sources = dict((payload or {}).get("sources") or {})
    for source in TRACKED_SOURCES:
        username = str(source["username"])
        existing = dict(raw_sources.get(username) or {})
        state = _default_source_state(source)
        state.update(existing)
        state["last_good_payload"] = [
            row for row in list(state.get("last_good_payload") or []) if isinstance(row, dict)
        ]
        state["last_raw_payload"] = [
            row for row in list(state.get("last_raw_payload") or []) if isinstance(row, dict)
        ]
        state["last_good_count"] = int(
            state.get("last_good_count") or len(state["last_good_payload"])
        )
        state["last_raw_count"] = int(state.get("last_raw_count") or len(state["last_raw_payload"]))
        sources[username] = state
    return {"sources": sources}


def _load_source_store() -> Dict[str, Any]:
    cached_sources = dict(_CACHE.get("sources") or {})
    if cached_sources:
        return _normalize_source_store({"sources": cached_sources})
    disk_payload = _normalize_source_store(_load_disk_cache())
    _CACHE["loaded_at"] = app_runtime.now_et()
    _CACHE["sources"] = dict(disk_payload.get("sources") or {})
    return disk_payload


def _save_source_store(store: Dict[str, Any]) -> None:
    normalized = _normalize_source_store(store)
    _CACHE["loaded_at"] = app_runtime.now_et()
    _CACHE["sources"] = dict(normalized.get("sources") or {})
    _save_disk_cache(normalized)


def _tracked_handles() -> List[str]:
    return [str(source["handle"]) for source in TRACKED_SOURCES]


def _twitterapi_key() -> str:
    return str(
        os.environ.get("TWITTERAPI_IO_API_KEY")
        or app_runtime.get_setting_value("twitterapi_io_api_key", "")
        or ""
    ).strip()


def _try_acquire_refresh_lock(username: str, now_et: datetime) -> bool:
    path = _lock_file(username)
    stale_cutoff = now_et - timedelta(seconds=REFRESH_LOCK_TTL_SECONDS)
    try:
        if os.path.exists(path):
            mtime = datetime.fromtimestamp(os.path.getmtime(path), tz=app_runtime.TZ)
            if mtime <= stale_cutoff:
                os.remove(path)
    except OSError:
        pass
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            return False
        raise
    try:
        os.write(fd, now_et.isoformat().encode("utf-8"))
    finally:
        os.close(fd)
    return True


def _release_refresh_lock(username: str) -> None:
    try:
        os.remove(_lock_file(username))
    except OSError:
        return


def _resolve_stale_after_seconds(page_type: str) -> int:
    normalized = str(page_type or "").strip().lower()
    return int(PAGE_CACHE_TTL_SECONDS.get(normalized, SOURCE_STALE_SECONDS))


def _resolve_snapshot_limit(page_type: str, requested_limit: int) -> int:
    normalized = str(page_type or "").strip().lower()
    if normalized == "dashboard":
        default_limit = DEFAULT_DASHBOARD_LIMIT
    elif normalized == "market-pulse":
        default_limit = DEFAULT_MARKET_PULSE_LIMIT
    else:
        default_limit = DEFAULT_LIMIT
    return max(1, min(MAX_MARKET_PULSE_LIMIT, int(requested_limit or default_limit)))


def _source_is_stale(
    state: Dict[str, Any], now_et: datetime, *, stale_after_seconds: int = SOURCE_STALE_SECONDS
) -> bool:
    last_success_at = _parse_dt(state.get("last_success_at"))
    if last_success_at is None:
        return True
    return (now_et - last_success_at).total_seconds() >= max(1, int(stale_after_seconds))


def _source_in_cooldown(state: Dict[str, Any], now_et: datetime) -> bool:
    cooldown_until = _parse_dt(state.get("cooldown_until"))
    return bool(cooldown_until and cooldown_until > now_et)


def _mark_source_success(
    state: Dict[str, Any],
    *,
    now_et: datetime,
    items: List[Dict[str, Any]],
    raw_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    state["last_attempt_at"] = now_et.isoformat()
    state["last_success_at"] = now_et.isoformat()
    state["cooldown_until"] = ""
    state["last_status_code"] = 200
    state["last_error"] = ""
    cached_items = list(state.get("last_good_payload") or [])
    cached_raw_items = list(state.get("last_raw_payload") or [])
    merged_items = _merge_cached_items(items, cached_items, limit=MAX_CACHED_SOURCE_ITEMS)
    merged_raw_items = _merge_cached_items(raw_items, cached_raw_items, limit=MAX_CACHED_SOURCE_ITEMS)
    state["last_good_payload"] = merged_items
    state["last_good_count"] = len(merged_items)
    state["last_raw_payload"] = merged_raw_items
    state["last_raw_count"] = len(merged_raw_items)
    return state


def _mark_source_failure(
    state: Dict[str, Any],
    *,
    now_et: datetime,
    status_code: Optional[int],
    error: str,
) -> Dict[str, Any]:
    state["last_attempt_at"] = now_et.isoformat()
    state["last_status_code"] = status_code
    state["last_error"] = error
    if status_code == 429:
        state["cooldown_until"] = (now_et + timedelta(seconds=COOLDOWN_429_SECONDS)).isoformat()
    return state


def _extract_twitterapi_rows_and_cursor(payload: Any) -> Tuple[List[Dict[str, Any]], str]:
    if not isinstance(payload, dict):
        return ([], "")

    possible_blocks = [payload]
    data = payload.get("data")
    if isinstance(data, dict):
        possible_blocks.insert(0, data)

    rows: List[Dict[str, Any]] = []
    cursor = ""
    for block in possible_blocks:
        tweets = block.get("tweets") or block.get("items") or block.get("results") or []
        if isinstance(tweets, list) and tweets:
            rows = [row for row in tweets if isinstance(row, dict)]
            if rows:
                break

    for block in possible_blocks:
        for key in (
            "next_cursor",
            "nextCursor",
            "cursor",
            "continuation_token",
            "continuationToken",
        ):
            value = str(block.get(key) or "").strip()
            if value:
                cursor = value
                break
        if cursor:
            break

    return (rows, cursor)


def _run_twitterapi_last_tweets_page(
    *, username: str, api_key: str, limit: int, cursor: str = ""
) -> Tuple[List[Dict[str, Any]], str]:
    query_payload = {
        "userName": username,
        "includeReplies": "false",
        "count": str(limit),
        "limit": str(limit),
    }
    if cursor:
        query_payload["cursor"] = cursor
    query = urllib.parse.urlencode(query_payload)
    url = f"{TWITTERAPI_BASE_URL}/twitter/user/last_tweets?{query}"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "X-API-Key": api_key,
            "Accept": "application/json",
            "User-Agent": "mccain-capital/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=TWITTERAPI_TIMEOUT_SECONDS) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return _extract_twitterapi_rows_and_cursor(payload)


def _run_twitterapi_last_tweets(*, username: str, api_key: str, limit: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen_keys: set[str] = set()
    cursor = ""
    for _ in range(TWITTERAPI_MAX_PAGES):
        remaining = max(1, min(TWITTERAPI_PAGE_SIZE, int(limit) - len(rows)))
        page_rows, next_cursor = _run_twitterapi_last_tweets_page(
            username=username,
            api_key=api_key,
            limit=remaining,
            cursor=cursor,
        )
        if not page_rows:
            break
        page_added = 0
        for row in page_rows:
            dedupe_key = str(row.get("id") or row.get("tweetId") or row.get("url") or "").strip()
            if dedupe_key and dedupe_key in seen_keys:
                continue
            if dedupe_key:
                seen_keys.add(dedupe_key)
            rows.append(row)
            page_added += 1
            if len(rows) >= limit:
                return rows[:limit]
        if page_added == 0:
            break
        next_cursor = str(next_cursor or "").strip()
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    return rows[:limit]


def _clean_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _truncate(value: str, limit: int = 280) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    trimmed = text[: max(0, limit - 1)].rstrip(" .,;:-")
    return f"{trimmed}…"


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=app_runtime.TZ)
        except Exception:
            return None
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.isdigit():
        try:
            return datetime.fromtimestamp(float(raw), tz=app_runtime.TZ)
        except Exception:
            return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(app_runtime.TZ)
    except Exception:
        pass
    try:
        return parsedate_to_datetime(raw).astimezone(app_runtime.TZ)
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
    if _contains_any(
        lowered, ("fed", "cpi", "ppi", "inflation", "jobs", "payroll", "yield", "treasury", "fomc")
    ):
        return "macro"
    if _contains_any(
        lowered,
        (
            "tariff",
            "trump",
            "sanction",
            "government",
            "white house",
            "policy",
            "congress",
            "geopolitical",
            "war",
        ),
    ):
        return "policy"
    if _contains_any(
        lowered,
        ("gamma", "dealer", "positioning", "options flow", "0dte", "call wall", "put wall", "flip"),
    ):
        return "flow"
    if _contains_any(lowered, ("earnings", "guidance", "ceo", "stock", "shares")):
        return "company"
    if _contains_any(lowered, ("spx", "spy", "qqq", "vix", "futures", "dow", "nasdaq", "russell")):
        return "market"
    return "market"


def _classify_impact(text: str) -> str:
    lowered = text.lower()
    if _contains_any(
        lowered,
        (
            "breaking",
            "fed",
            "fomc",
            "cpi",
            "ppi",
            "inflation",
            "yield",
            "war",
            "tariff",
            "geopolitical",
            "sanction",
            "jobs",
            "payroll",
        ),
    ):
        return "high"
    if _contains_any(
        lowered, ("earnings", "upgrade", "downgrade", "positioning", "options", "dealer", "gamma")
    ):
        return "medium"
    return "low"


def _classify_market_bias(text: str) -> str:
    lowered = text.lower()
    bullish_terms = (
        "cooling inflation",
        "cuts",
        "rally",
        "soft landing",
        "buying",
        "bid",
        "easing",
        "disinflation",
        "breakout",
        "squeeze",
    )
    bearish_terms = (
        "hot inflation",
        "yields rising",
        "hawkish",
        "selloff",
        "risk-off",
        "breakdown",
        "recession",
        "tariff",
        "war",
        "higher yields",
        "dump",
    )
    bullish_hits = sum(1 for phrase in bullish_terms if phrase in lowered)
    bearish_hits = sum(1 for phrase in bearish_terms if phrase in lowered)
    if bullish_hits > bearish_hits:
        return "bullish"
    if bearish_hits > bullish_hits:
        return "bearish"
    return "neutral"


def _classify_volatility_bias(text: str) -> str:
    lowered = text.lower()
    if _contains_any(
        lowered,
        (
            "breaking",
            "uncertainty",
            "war",
            "tariff",
            "volatility",
            "headline risk",
            "selloff",
            "risk-off",
        ),
    ):
        return "higher"
    if _contains_any(
        lowered,
        ("calm", "easing", "stabilization", "stabilising", "soft landing", "cooling", "contained"),
    ):
        return "lower"
    return "neutral"


def _trade_relevance(category: str, impact: str, symbols: List[str], text: str) -> str:
    lowered = text.lower()
    if impact == "high" and category in {"macro", "policy", "flow"}:
        return "actionable_now"
    if symbols or _contains_any(
        lowered, ("spx", "qqq", "vix", "fed", "gamma", "call wall", "put wall", "flip")
    ):
        return "watch"
    return "context_only"


def _planning_direction(structure: Dict[str, Any]) -> str:
    bias = str(structure.get("planning_bias") or "").strip().lower()
    if bias in {"bullish_above_local_flip", "above_call_wall_extension_risk"}:
        return "bullish"
    if bias in {"bearish_below_local_flip", "below_put_wall_breakdown_risk"}:
        return "bearish"
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
    return {"bullish": "Bullish", "bearish": "Bearish", "neutral": "Neutral"}.get(value, "Neutral")


def _vol_label(value: str) -> str:
    return {"higher": "Higher", "lower": "Lower", "neutral": "Neutral"}.get(value, "Neutral")


def _actor_username(item: Dict[str, Any]) -> str:
    author = item.get("author")
    if isinstance(author, dict):
        for key in ("userName", "username", "screenName", "screen_name", "handle"):
            raw = str(author.get(key) or "").strip()
            if raw:
                return raw.lstrip("@")
    for key in ("userName", "username", "screenName", "screen_name", "handle"):
        raw = str(item.get(key) or "").strip()
        if raw:
            return raw.lstrip("@")
    url = str(item.get("url") or item.get("tweetUrl") or "").strip()
    match = re.search(r"x\.com/([^/]+)/status/", url)
    return str((match.group(1) if match else "") or "").strip().lstrip("@")


def _actor_text(item: Dict[str, Any]) -> str:
    for key in ("fullText", "full_text", "text", "tweetText", "content", "description"):
        text = _clean_text(item.get(key))
        if text:
            return _truncate(text)
    return ""


def _actor_url(item: Dict[str, Any], username: str, profile_url: str) -> str:
    for key in ("url", "tweetUrl", "twitterUrl"):
        raw = str(item.get(key) or "").strip()
        if raw:
            return raw
    post_id = str(item.get("id") or item.get("tweetId") or "").strip()
    if username and post_id:
        return f"https://x.com/{username}/status/{post_id}"
    return profile_url


def _normalize_raw_item(
    item: Dict[str, Any],
    *,
    now_et: datetime,
    source: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    text = _actor_text(item)
    if not text:
        return None
    published_at = _parse_datetime(
        item.get("createdAt")
        or item.get("created_at")
        or item.get("timestamp")
        or item.get("publishedAt")
        or item.get("timeParsed")
    )
    return {
        "handle": source["handle"],
        "source": source["source"],
        "source_label": source["source"],
        "text": text,
        "headline": text,
        "summary": text,
        "url": _actor_url(item, source["username"], source["profile_url"]),
        "published_at": published_at.isoformat() if published_at else "",
        "published_et_label": _format_et_label(published_at),
        "published_label": _age_label(published_at, now_et),
        "age_label": _age_label(published_at, now_et),
        "raw": True,
    }


def _normalize_actor_item(
    item: Dict[str, Any],
    *,
    now_et: datetime,
    structure: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    username = _actor_username(item).lower()
    source = SOURCE_BY_USERNAME.get(username)
    if not source:
        return None
    text = _actor_text(item)
    if not text:
        return None
    published_at = _parse_datetime(
        item.get("createdAt")
        or item.get("created_at")
        or item.get("timestamp")
        or item.get("publishedAt")
        or item.get("timeParsed")
    )
    if published_at and (now_et - published_at) > timedelta(hours=MAX_ITEM_AGE_HOURS):
        return None
    category = _classify_category(text)
    impact = _classify_impact(text)
    market_bias = _classify_market_bias(text)
    volatility_bias = _classify_volatility_bias(text)
    symbols = _extract_symbols(text)
    trade_relevance = _trade_relevance(category, impact, symbols, text)
    normalized = {
        "source": source["source"],
        "source_label": source["source"],
        "handle": source["handle"],
        "published_at": published_at.isoformat() if published_at else "",
        "published_et_label": _format_et_label(published_at),
        "published_label": _age_label(published_at, now_et),
        "age_label": _age_label(published_at, now_et),
        "text": text,
        "headline": text,
        "summary": text,
        "url": _actor_url(item, source["username"], source["profile_url"]),
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
    normalized.update(_gamma_alignment(normalized, structure))
    if (
        normalized["alignment"] == "conflicted"
        and normalized["trade_relevance"] == "actionable_now"
    ):
        normalized["trade_relevance"] = "watch"
        normalized["trade_relevance_label"] = _relevance_label("watch")
    return normalized


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
    return _bias_label(max(scores, key=scores.get))


def _summarize_volatility(items: List[Dict[str, Any]]) -> str:
    scores = {"higher": 0, "lower": 0, "neutral": 0}
    for item in items:
        bias = str(item.get("volatility_bias") or "neutral")
        scores[bias] = scores.get(bias, 0) + IMPACT_WEIGHTS.get(str(item.get("impact") or "low"), 1)
    return _vol_label(max(scores, key=scores.get))


def _summarize_gamma_alignment(items: List[Dict[str, Any]], structure: Dict[str, Any]) -> str:
    aligned = sum(1 for item in items if item.get("alignment") == "aligned")
    conflicted = sum(1 for item in items if item.get("alignment") == "conflicted")
    gamma_regime = str(
        structure.get("gamma_regime_label") or structure.get("gamma_regime") or "Unconfirmed"
    ).strip()
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
    return f"{tradeability} · {planning_bias}" if planning_bias else tradeability or "Planning only"


def _now_summary(
    items: List[Dict[str, Any]], structure: Dict[str, Any], status: str
) -> Dict[str, str]:
    if not items:
        return {
            "spx_focus": "No fresh source flow yet",
            "leadership": "Macro bias unavailable",
            "weakness": "Volatility unavailable",
            "feed_state": "twitterapi.io sync delayed",
        }
    return {
        "spx_focus": f"Macro: {_summarize_bias(items)}",
        "leadership": f"Volatility: {_summarize_volatility(items)}",
        "weakness": f"Gamma: {_summarize_gamma_alignment(items, structure)}",
        "feed_state": f"{'Live' if status == 'live' else 'Delayed'} · {_summarize_tradeability(structure)}",
    }


def _fallback_seed_items(now_et: datetime, structure: Dict[str, Any]) -> List[Dict[str, Any]]:
    planning_bias = str(
        structure.get("planning_bias_label") or "Hold to the shared gamma plan"
    ).strip()
    seed_texts = (
        {
            "source": "Unusual Whales",
            "handle": "@unusual_whales",
            "url": "https://x.com/unusual_whales",
            "text": f"Feed delayed. Preserve macro context and keep planning around {planning_bias.lower()}.",
            "category": "macro",
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


def _dedupe_items(items: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    deduped: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for item in items:
        key = (str(item.get("handle") or ""), str(item.get("text") or "").lower())
        prior = deduped.get(key)
        if prior is None or str(item.get("published_at") or "") > str(
            prior.get("published_at") or ""
        ):
            deduped[key] = item
    return sorted(deduped.values(), key=_sort_key, reverse=True)[:limit]


def _sort_raw_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _key(item: Dict[str, Any]) -> int:
        published = _parse_datetime(
            item.get("published_at") or item.get("createdAt") or item.get("created_at")
        )
        return int(published.timestamp()) if published else 0

    return sorted(items, key=_key, reverse=True)


def _cache_item_key(item: Dict[str, Any]) -> Tuple[str, str, str]:
    url = str(item.get("url") or item.get("tweetUrl") or item.get("twitterUrl") or "").strip()
    post_id = str(item.get("id") or item.get("tweetId") or "").strip()
    text = str(item.get("text") or item.get("headline") or item.get("summary") or "").strip().lower()
    return (url, post_id, text)


def _merge_cached_items(
    fresh_items: List[Dict[str, Any]],
    cached_items: List[Dict[str, Any]],
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for item in [*cached_items, *fresh_items]:
        if not isinstance(item, dict):
            continue
        key = _cache_item_key(item)
        if not any(key):
            continue
        prior = merged.get(key)
        if prior is None or str(item.get("published_at") or "") >= str(
            prior.get("published_at") or ""
        ):
            merged[key] = item
    return _sort_raw_items(list(merged.values()))[: max(1, int(limit))]


def _source_payload_items(
    source_state: Dict[str, Any],
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    return list(source_state.get("last_good_payload") or [])[:limit]


def _refresh_source_state(
    source_state: Dict[str, Any],
    *,
    source: Dict[str, str],
    api_key: str,
    now_et: datetime,
    structure: Dict[str, Any],
    limit: int,
    force_refresh: bool,
    stale_after_seconds: int,
) -> Dict[str, Any]:
    username = str(source["username"])
    source_state["last_attempt_at"] = now_et.isoformat()

    if _source_in_cooldown(source_state, now_et):
        LOGGER.info(
            "twitter feed cooldown skip for %s until %s",
            username,
            source_state.get("cooldown_until") or "",
        )
        return source_state

    cached_count = int(source_state.get("last_good_count") or 0)
    if (
        not force_refresh
        and cached_count >= limit
        and not _source_is_stale(source_state, now_et, stale_after_seconds=stale_after_seconds)
    ):
        LOGGER.info(
            "twitter feed cache hit for %s using last success at %s",
            username,
            source_state.get("last_success_at") or "",
        )
        return source_state

    if not _try_acquire_refresh_lock(username, now_et):
        LOGGER.info("twitter feed refresh already in progress for %s", username)
        return source_state

    try:
        raw_items = _run_twitterapi_last_tweets(username=username, api_key=api_key, limit=limit)
        raw_normalized = [
            row
            for row in (
                _normalize_raw_item(item, now_et=now_et, source=source) for item in raw_items
            )
            if row
        ]
        normalized_items = _dedupe_items(
            [
                normalized
                for normalized in (
                    _normalize_actor_item(raw_item, now_et=now_et, structure=structure)
                    for raw_item in raw_items
                )
                if normalized
            ],
            limit,
        )
        LOGGER.info(
            "twitter feed fetch success for %s with %s items",
            username,
            len(normalized_items),
        )
        return _mark_source_success(
            source_state,
            now_et=now_et,
            items=normalized_items,
            raw_items=raw_normalized,
        )
    except urllib.error.HTTPError as exc:
        status_code = int(getattr(exc, "code", 0) or 0)
        error = f"HTTP Error {status_code}: {getattr(exc, 'reason', '')}".strip()
        if status_code == 429:
            LOGGER.warning("twitter feed 429 for %s; cooling down source", username)
        else:
            LOGGER.warning("twitter feed fetch failed for %s: %s", username, error)
        return _mark_source_failure(
            source_state,
            now_et=now_et,
            status_code=status_code or None,
            error=error,
        )
    except Exception as exc:
        error_text = str(exc)
        status_code = 429 if "429" in error_text else None
        if status_code == 429:
            LOGGER.warning("twitter feed 429 for %s; cooling down source", username)
        else:
            LOGGER.warning("twitter feed fetch failed for %s: %s", username, error_text)
        return _mark_source_failure(
            source_state,
            now_et=now_et,
            status_code=status_code,
            error=error_text,
        )
    finally:
        _release_refresh_lock(username)


def _build_combined_snapshot_from_store(
    *,
    store: Dict[str, Any],
    now_et: datetime,
    structure: Dict[str, Any],
    limit: int,
    stale_after_seconds: int,
) -> Dict[str, Any]:
    primary_label = str(TRACKED_SOURCES[0]["source"]) if TRACKED_SOURCES else "Primary source"
    source_states = dict(store.get("sources") or {})
    items = _dedupe_items(
        [
            row
            for source in TRACKED_SOURCES
            for row in _source_payload_items(
                dict(source_states.get(str(source["username"])) or {}),
                limit=limit,
            )
        ],
        limit,
    )
    raw_items: List[Dict[str, Any]] = []
    for source in TRACKED_SOURCES:
        state = dict(source_states.get(str(source["username"])) or {})
        raw_items.extend(list(state.get("last_raw_payload") or []))
    raw_items = _sort_raw_items(raw_items)

    any_live = False
    any_error = False
    available_sources = 0
    unavailable_sources = 0
    for source in TRACKED_SOURCES:
        state = dict(source_states.get(str(source["username"])) or {})
        payload = list(state.get("last_good_payload") or [])
        if payload:
            available_sources += 1
        else:
            unavailable_sources += 1
        if state.get("last_error") or state.get("cooldown_until"):
            any_error = True
        if (
            payload
            and not _source_is_stale(state, now_et, stale_after_seconds=stale_after_seconds)
            and not _source_in_cooldown(state, now_et)
        ):
            any_live = True

    if items:
        if any_error:
            status = "delayed"
            source_note = "Partial twitterapi.io sync. Showing the latest good posts by source."
        elif any_live:
            status = "live"
            source_note = f"Live flow sync from twitterapi.io for {primary_label}."
        else:
            status = "delayed"
            source_note = "Using cached twitterapi.io snapshot while the latest sync is delayed."
    else:
        status = "delayed"
        source_note = (
            "twitterapi.io sync delayed. Using backup flow shell until the next valid sync."
        )
        items = _fallback_seed_items(now_et, structure)[: max(2, min(limit, 4))]

    source_states_public = {}
    for source in TRACKED_SOURCES:
        username = str(source["username"])
        state = dict(source_states.get(username) or _default_source_state(source))
        source_states_public[username] = {
            "handle": state.get("handle") or source["handle"],
            "last_success_at": state.get("last_success_at") or "",
            "last_attempt_at": state.get("last_attempt_at") or "",
            "cooldown_until": state.get("cooldown_until") or "",
            "last_status_code": state.get("last_status_code"),
            "last_error": state.get("last_error") or "",
            "last_good_count": int(state.get("last_good_count") or 0),
        }

    flow_summary = {
        "macro_bias": _summarize_bias(items),
        "volatility": _summarize_volatility(items),
        "gamma_alignment": _summarize_gamma_alignment(items, structure),
        "tradeability": _summarize_tradeability(structure),
    }
    flow_summary["headline"] = (
        f"Macro: {flow_summary['macro_bias']} | "
        f"Vol: {flow_summary['volatility']} | "
        f"Gamma: {flow_summary['gamma_alignment']} -> {flow_summary['tradeability']}"
    )

    return {
        "status": status,
        "feed_state": status,
        "source_note": source_note,
        "updated_at": now_et.isoformat(),
        "sources_monitored": _tracked_handles(),
        "tracked_accounts": _tracked_handles(),
        "items": list(items),
        "top_items": list(items[:limit]),
        "raw_items": list(raw_items),
        "available": bool(items),
        "flow_summary": flow_summary,
        "now_summary": _now_summary(items, structure, status),
        "source_states": source_states_public,
        "available_source_count": available_sources,
        "unavailable_source_count": unavailable_sources,
    }


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
    force_refresh: bool = False,
    page_type: str = "",
) -> Dict[str, Any]:
    now_et = now_et or app_runtime.now_et()
    structure = dict(market_structure_snapshot or {})
    limit = _resolve_snapshot_limit(page_type, limit)
    stale_after_seconds = _resolve_stale_after_seconds(page_type)
    store = _load_source_store()
    api_key = _twitterapi_key()
    if api_key and force_refresh:
        sources = dict(store.get("sources") or {})
        for source in TRACKED_SOURCES:
            username = str(source["username"])
            state = dict(sources.get(username) or _default_source_state(source))
            sources[username] = _refresh_source_state(
                state,
                source=source,
                api_key=api_key,
                now_et=now_et,
                structure=structure,
                limit=limit,
                force_refresh=force_refresh,
                stale_after_seconds=stale_after_seconds,
            )
        store["sources"] = sources
        _save_source_store(store)

    return _build_combined_snapshot_from_store(
        store=store,
        now_et=now_et,
        structure=structure,
        limit=limit,
        stale_after_seconds=stale_after_seconds,
    )
